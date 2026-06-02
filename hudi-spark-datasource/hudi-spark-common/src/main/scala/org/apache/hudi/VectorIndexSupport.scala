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

import org.apache.hudi.DataSourceReadOptions.{VECTOR_INDEX_NAME, VECTOR_QUERY_NPROBES, VECTOR_QUERY_TOPK, VECTOR_QUERY_VECTOR}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.index.vector.{VectorDistanceMetric, VectorIndexMdtSearchUtils, VectorIndexMetadataCache, VectorIndexOptions, VectorIndexPruner}
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil, RawKey, VectorClusterRawKey}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Expression

import java.nio.ByteBuffer
import java.util.Collections

import scala.collection.JavaConverters._

class VectorIndexSupport(spark: SparkSession,
                         tableSchema: HoodieSchema,
                         metadataConfig: HoodieMetadataConfig,
                         metaClient: HoodieTableMetaClient)
  extends SparkBaseIndexSupport(spark, metadataConfig, metaClient) with Logging {

  /**
   * Driver-side metadata cache. Holds centroids, manifest, quantizer config, and
   * all cluster manifests. Loaded once from MDT, reused across queries until the
   * timeline advances. Volatile for safe publication across threads.
   */
  @volatile private var metadataCacheHolder: VectorIndexMetadataCache = _

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
            Option(getOrLoadCache(querySpec.indexPartition, vectorSchema)).flatMap { cache =>
              val topClusters = cache.findTopClusters(querySpec.queryVector, querySpec.numProbes, querySpec.metric)
              if (topClusters.isEmpty) {
                Option.empty
              } else {
                querySpec.topK match {
                  case Some(topK) =>
                    computeFineCandidateFiles(cache, querySpec, topClusters, topK, prunedPartitionsAndFileSlices, fileIndex.includeLogFiles)
                  case None =>
                    computeCoarseCandidateFiles(cache, topClusters, prunedPartitionsAndFileSlices, fileIndex.includeLogFiles)
                }
              }
            }
          }
        }
      }
    }
  }

  override def invalidateCaches(): Unit = {
    metadataCacheHolder = null
  }

  // ---- Private: Cache management ----------------------------------------

  private def getOrLoadCache(indexPartition: String, vectorSchema: HoodieSchema.Vector): VectorIndexMetadataCache = {
    val currentInstant = metaClient.getActiveTimeline.lastInstant()
      .map[String](_.requestedTime)
      .orElse("")

    val existing = metadataCacheHolder
    if (existing != null && !existing.isStaleFor(currentInstant)) {
      existing
    } else {
      // ONE MDT round trip to load everything
      val loaded = VectorIndexMetadataCache.load(metadataTable, indexPartition, vectorSchema, currentInstant)
      if (loaded != null) {
        metadataCacheHolder = loaded
      }
      loaded
    }
  }

  // ---- Private: Coarse pruning (cluster → file groups, no posting IO) ----

  private def computeCoarseCandidateFiles(cache: VectorIndexMetadataCache,
                                          topClusters: Array[Int],
                                          prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                          includeLogFiles: Boolean): Option[Set[String]] = {
    val candidateFileGroups = cache.getFileGroupsForClusters(topClusters, null).asScala.toSet
    if (candidateFileGroups.isEmpty) {
      Option.empty
    } else {
      Some(VectorIndexSupport.collectCandidateFileNames(candidateFileGroups, prunedPartitionsAndFileSlices, includeLogFiles))
    }
  }

  // ---- Private: Fine-grained pruning (posting scan + RaBitQ + top-K) ----

  private def computeFineCandidateFiles(cache: VectorIndexMetadataCache,
                                        querySpec: VectorIndexSupport.QuerySpec,
                                        topClusters: Array[Int],
                                        topK: Int,
                                        prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                        includeLogFiles: Boolean): Option[Set[String]] = {
    // All from cache: shard counts for probed clusters
    val shardCounts = cache.getShardCounts(topClusters)

    // ONE MDT IO: prefix scan posting rows for probed cluster shards
    val postings = VectorIndexMdtSearchUtils.readPostingMatches(
      metadataTable,
      querySpec.indexPartition,
      cache.getGenerationId,
      shardCounts,
      false)

    // Pure CPU: RaBitQ approximate scoring
    val scored = VectorIndexMdtSearchUtils.scorePostingMatches(
      postings,
      querySpec.queryVector,
      cache.getDimension,
      cache.getQuantizerSeed,
      cache.isAssumeNormalized)

    // Pure CPU: bounded top-K selection
    val topKResults = VectorIndexMdtSearchUtils.selectTopK(scored, topK)

    val topKList = try {
      topKResults.collectAsList()
    } finally {
      topKResults.unpersistWithDependencies()
    }

    if (topKList.isEmpty) {
      Option.empty
    } else {
      // Extract ONLY the file groups containing top-K records
      val targetFileGroups = topKList.asScala.map(_.getFileGroupId).filter(_ != null).toSet
      if (targetFileGroups.isEmpty) {
        Option.empty
      } else {
        Some(VectorIndexSupport.collectCandidateFileNames(targetFileGroups, prunedPartitionsAndFileSlices, includeLogFiles))
      }
    }
  }

  // ---- Private: Schema resolution ---------------------------------------

  private def resolveVectorSchema(sourceColumn: String): Option[HoodieSchema.Vector] = {
    if (tableSchema == null) {
      Option.empty
    } else {
      Option(org.apache.hudi.common.schema.HoodieSchemaUtils.getNestedField(tableSchema, sourceColumn).orElse(null))
        .map(_.getRight.schema().getNonNullType)
        .collect { case vector: HoodieSchema.Vector => vector }
    }
  }
}

object VectorIndexSupport {
  val INDEX_NAME = "vector_index"

  case class QuerySpec(indexPartition: String,
                       sourceColumn: String,
                       queryVector: Array[Float],
                       numProbes: Int,
                       metric: VectorDistanceMetric,
                       topK: Option[Int])

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
            VectorIndexOptions.getMetric(indexDefinition.getIndexOptions),
            options.get(VECTOR_QUERY_TOPK.key).map(_.toInt))
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

  // ---- Legacy helpers (kept for backward compatibility with existing tests) ----

  def deserializeCentroids(bytes: ByteBuffer, vectorSchema: HoodieSchema.Vector): Array[Array[Float]] = {
    VectorIndexMetadataCache.deserializeCentroids(bytes, vectorSchema)
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

  private[hudi] def mergeClusterMaps(primary: Map[Int, Set[String]],
                                     fallback: Map[Int, Set[String]]): Map[Int, Set[String]] = {
    fallback.foldLeft(primary) { case (acc, (clusterId, fileGroups)) =>
      if (fileGroups.isEmpty) {
        acc
      } else {
        acc.updated(clusterId, acc.getOrElse(clusterId, Set.empty[String]) ++ fileGroups)
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
}
