/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi

import org.apache.hudi.DataSourceReadOptions.{VECTOR_INDEX_NAME, VECTOR_QUERY_NPROBES, VECTOR_QUERY_VECTOR}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.index.vector.{VectorDistanceMetric, VectorIndexOptions, VectorIndexPruner}
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil, RawKey, VectorClusterRawKey}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.JavaConverters._

class VectorIndexSupport(spark: SparkSession,
                         tableSchema: HoodieSchema,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient)
  extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) {

  override def getIndexName: String = VectorIndexSupport.INDEX_NAME

  override def isIndexAvailable: Boolean =
    metadataConfig.isEnabled &&
      metaClient.getIndexMetadata.isPresent &&
      metaClient.getIndexMetadata.get.getIndexDefinitions.values.asScala.exists(
        _.getIndexType == HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX)

  override def canPruneWithEmptyDataFilters(options: Map[String, String]): Boolean =
    VectorIndexSupport.resolveQuerySpec(metaClient, options).nonEmpty

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean): Option[Set[String]] = {
    if (!isIndexAvailable) {
      Option.empty
    } else {
      VectorIndexSupport.resolveQuerySpec(metaClient, fileIndex.options).flatMap { querySpec =>
        resolveVectorSchema(querySpec.sourceColumn).flatMap { vectorSchema =>
          if (vectorSchema.getDimension != querySpec.queryVector.length) {
            Option.empty
          } else {
            loadPruner(querySpec, vectorSchema, prunedPartitionsAndFileSlices).flatMap { pruner =>
              val candidateFileGroups = pruner.probe(querySpec.queryVector, querySpec.numProbes).asScala.toSet
              if (candidateFileGroups.isEmpty) {
                Option.empty
              } else {
                Some(VectorIndexSupport.collectCandidateFileNames(
                  candidateFileGroups,
                  prunedPartitionsAndFileSlices,
                  fileIndex.includeLogFiles))
              }
            }
          }
        }
      }
    }
  }

  override def invalidateCaches(): Unit = {
    // No long-lived cache yet. Vector metadata can change after index rebuild/rebalance,
    // so we always reload from MDT on demand.
  }

  private def resolveVectorSchema(sourceColumn: String): Option[HoodieSchema.Vector] = {
    if (tableSchema == null) {
      Option.empty
    } else {
      Option(org.apache.hudi.common.schema.HoodieSchemaUtils.getNestedField(tableSchema, sourceColumn).orElse(null))
        .map(_.getRight.schema().getNonNullType)
        .collect { case vector: HoodieSchema.Vector => vector }
    }
  }

  private def loadPruner(querySpec: VectorIndexSupport.QuerySpec,
                         vectorSchema: HoodieSchema.Vector,
                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])])
  : Option[VectorIndexPruner] = {
    val partitionFilter = prunedPartitionsAndFileSlices
      .flatMap(_._1.map(_.getPath))
      .toSet
    val effectivePartitionFilter =
      if (partitionFilter.nonEmpty) Some(partitionFilter) else None

    val bootstrapRecords = metadataTable
      .getRecordsByKeyPrefixes(HoodieListData.eager(VectorIndexSupport.bootstrapLookupKeys), querySpec.indexPartition, true)
      .collectAsList()
      .asScala
      .toSeq

    val centroidsOpt = VectorIndexSupport.extractCentroids(bootstrapRecords, vectorSchema)

    centroidsOpt.filter(_.nonEmpty).flatMap { centroids =>
      val topClusterIds = VectorIndexSupport.findTopClusters(centroids, querySpec.queryVector, querySpec.numProbes, querySpec.metric)

      val clusterToFileGroups =
        VectorIndexSupport.resolveCurrentGenerationId(bootstrapRecords)
          .map { generationId =>
            val clusterKeys = topClusterIds.map(clusterId => new VectorClusterRawKey(generationId, clusterId): RawKey).toList.asJava
            metadataTable
              .getRecordsByKeyPrefixes(HoodieListData.eager(clusterKeys), querySpec.indexPartition, true)
              .collectAsList()
              .asScala
              .toSeq
          }
          .map(records => VectorIndexSupport.buildClusterMapFromClusterRecords(records, effectivePartitionFilter))
          .filter(_.nonEmpty)
          .getOrElse(VectorIndexSupport.buildClusterMapFromLegacyFgRecords(bootstrapRecords, effectivePartitionFilter))

      if (clusterToFileGroups.isEmpty) {
        Option.empty
      } else {
        val clusterToFileGroupsJava = new java.util.HashMap[Integer, java.util.Set[String]]()
        clusterToFileGroups.foreach { case (clusterId, fileGroups) =>
          clusterToFileGroupsJava.put(Int.box(clusterId), fileGroups.asJava)
        }
        Some(new VectorIndexPruner(centroids, clusterToFileGroupsJava, querySpec.metric))
      }
    }
  }
}

object VectorIndexSupport {
  val INDEX_NAME = "vector_index"
  private val bootstrapLookupKeys = java.util.Arrays.asList[RawKey](
    new RawKey {
      override def encode(): String = HoodieTableMetadataUtil.VECTOR_INDEX_CENTROIDS_KEY
    },
    new RawKey {
      override def encode(): String = HoodieTableMetadataUtil.VECTOR_INDEX_MANIFEST_KEY
    },
    new RawKey {
      override def encode(): String = HoodieTableMetadataUtil.VECTOR_INDEX_GENERATION_MANIFEST_KEY_PREFIX
    },
    new RawKey {
      override def encode(): String = HoodieTableMetadataUtil.VECTOR_INDEX_FG_MAPPING_KEY_PREFIX
    })

  case class QuerySpec(indexPartition: String,
                       sourceColumn: String,
                       queryVector: Array[Float],
                       numProbes: Int,
                       metric: VectorDistanceMetric)

  def resolveQuerySpec(metaClient: HoodieTableMetaClient, options: Map[String, String]): Option[QuerySpec] = {
    val indexNameOpt = options.get(VECTOR_INDEX_NAME.key)
    val queryVectorOpt = options.get(VECTOR_QUERY_VECTOR.key)
    if (indexNameOpt.isEmpty || queryVectorOpt.isEmpty || !metaClient.getIndexMetadata.isPresent) {
      Option.empty
    } else {
      val normalizedIndexName = normalizeIndexName(indexNameOpt.get)
      Option(metaClient.getIndexMetadata.get.getIndexDefinitions.get(normalizedIndexName))
        .filter(_.getIndexType == HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX)
        .map { indexDefinition =>
          QuerySpec(
            normalizedIndexName,
            indexDefinition.getSourceFields.get(0),
            parseQueryVector(queryVectorOpt.get),
            options.get(VECTOR_QUERY_NPROBES.key).map(_.toInt).getOrElse(VectorIndexOptions.getNumProbes(indexDefinition.getIndexOptions)),
            VectorIndexOptions.getMetric(indexDefinition.getIndexOptions))
        }
    }
  }

  def normalizeIndexName(indexName: String): String = {
    if (indexName.startsWith(HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX)) {
      indexName
    } else {
      HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX_PREFIX + indexName
    }
  }

  def parseQueryVector(raw: String): Array[Float] = {
    raw.trim
      .stripPrefix("[")
      .stripSuffix("]")
      .split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .map(_.toFloat)
  }

  def deserializeCentroids(bytes: ByteBuffer, vectorSchema: HoodieSchema.Vector): Array[Array[Float]] = {
    val elementType = vectorSchema.getVectorElementType
    val dimension = vectorSchema.getDimension
    val duplicate = bytes.duplicate().order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER)
    val bytesPerCentroid = dimension * elementType.getElementSize
    val centroidCount =
      if (bytesPerCentroid == 0) 0 else duplicate.remaining() / bytesPerCentroid

    Array.tabulate(centroidCount) { _ =>
      Array.tabulate(dimension) { _ =>
        elementType match {
          case HoodieSchema.Vector.VectorElementType.FLOAT => duplicate.getFloat()
          case HoodieSchema.Vector.VectorElementType.DOUBLE => duplicate.getDouble().toFloat
          case HoodieSchema.Vector.VectorElementType.INT8 => duplicate.get().toFloat
        }
      }
    }
  }

  private[hudi] def extractCentroids(records: Seq[HoodieRecord[HoodieMetadataPayload]],
                                     vectorSchema: HoodieSchema.Vector): Option[Array[Array[Float]]] = {
    records
      .find(_.getRecordKey == HoodieTableMetadataUtil.VECTOR_INDEX_CENTROIDS_KEY)
      .flatMap(extractVectorMetadata)
      .map(info => deserializeCentroids(info.getCentroidBytes, vectorSchema))
  }

  private[hudi] def resolveCurrentGenerationId(records: Seq[HoodieRecord[HoodieMetadataPayload]]): Option[Int] = {
    val manifestGenerationOpt = records
      .find(_.getRecordKey == HoodieTableMetadataUtil.VECTOR_INDEX_MANIFEST_KEY)
      .flatMap(extractVectorMetadata)
      .flatMap(info => parseGenerationId(info.getGenerationId))

    manifestGenerationOpt.orElse {
      records
        .flatMap(record => extractVectorMetadata(record).flatMap(info => parseGenerationId(info.getGenerationId)))
        .sorted
        .lastOption
    }
  }

  private[hudi] def findTopClusters(centroids: Array[Array[Float]],
                                    queryVector: Array[Float],
                                    numProbes: Int,
                                    metric: VectorDistanceMetric): Seq[Int] = {
    new VectorIndexPruner(centroids, Collections.emptyMap[Integer, java.util.Set[String]](), metric)
      .findTopClusters(queryVector, Math.min(math.max(1, numProbes), centroids.length))
      .toSeq
  }

  private[hudi] def buildClusterMapFromClusterRecords(records: Seq[HoodieRecord[HoodieMetadataPayload]],
                                                      partitionFilter: Option[Set[String]]): Map[Int, Set[String]] = {
    records
      .flatMap(extractVectorMetadata)
      .filter(info => info.getEntryType == HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER)
      .filter(info => Option(info.getPartitionPath).forall(path => partitionFilter.forall(_.contains(path))))
      .foldLeft(Map.empty[Int, Set[String]]) { (acc, info) =>
        val fileGroups = Option(info.getFileGroupIds).map(_.asScala.toSet).getOrElse(Set.empty[String])
        if (fileGroups.isEmpty) {
          acc
        } else {
          acc.updated(info.getClusterId, acc.getOrElse(info.getClusterId, Set.empty[String]) ++ fileGroups)
        }
      }
  }

  private[hudi] def buildClusterMapFromLegacyFgRecords(records: Seq[HoodieRecord[HoodieMetadataPayload]],
                                                       partitionFilter: Option[Set[String]]): Map[Int, Set[String]] = {
    records
      .filter(record => HoodieTableMetadataUtil.isVectorIndexFgMappingKey(record.getRecordKey))
      .flatMap(extractVectorMetadata)
      .filter(info => Option(info.getPartitionPath).forall(path => partitionFilter.forall(_.contains(path))))
      .foldLeft(Map.empty[Int, Set[String]]) { (acc, info) =>
        val fileGroups = Option(info.getFileGroupIds).map(_.asScala.toSet).getOrElse(Set.empty[String])
        if (fileGroups.isEmpty) {
          acc
        } else {
          acc.updated(info.getClusterId, acc.getOrElse(info.getClusterId, Set.empty[String]) ++ fileGroups)
        }
      }
  }

  private def extractVectorMetadata(record: HoodieRecord[HoodieMetadataPayload]) = {
    val payload = record.getData.asInstanceOf[HoodieMetadataPayload]
    if (payload.getVectorIndexMetadata.isPresent) Some(payload.getVectorIndexMetadata.get) else None
  }

  private def parseGenerationId(rawGenerationId: String): Option[Int] = {
    Option(rawGenerationId).flatMap { generationId =>
      scala.util.Try(Integer.parseUnsignedInt(generationId)).toOption
        .orElse(scala.util.Try(Integer.parseInt(generationId)).toOption)
    }
  }

  def collectCandidateFileNames(candidateFileGroups: Set[String],
                                prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                includeLogFiles: Boolean): Set[String] = {
    prunedPartitionsAndFileSlices.flatMap {
      case (_, fileSlices) =>
        fileSlices
          .filter(fs => candidateFileGroups.contains(fs.getFileId))
          .flatMap { fileSlice =>
            val baseFileName = Option(fileSlice.getBaseFile.orElse(null)).map(_.getFileName).toSeq
            val logFileNames =
              if (includeLogFiles) fileSlice.getLogFiles.iterator.asScala.map(_.getFileName).toSeq else Seq.empty
            baseFileName ++ logFileNames
          }
    }.toSet
  }
}
