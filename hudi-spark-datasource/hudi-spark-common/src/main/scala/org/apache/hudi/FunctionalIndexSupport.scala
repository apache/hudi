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

import org.apache.hudi.FunctionalIndexSupport._
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.HoodieSparkFunctionalIndex.SPARK_FUNCTION_MAP
import org.apache.hudi.RecordLevelIndexSupport.filterQueryWithRecordKey
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.{ColumnIndexID, PartitionIndexID}
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil, MetadataPartitionType}
import org.apache.hudi.util.JFunction
import org.apache.spark.sql.HoodieUnsafeUtils.{createDataFrameFromInternalRows, createDataFrameFromRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.JavaConverters._

class FunctionalIndexSupport(spark: SparkSession,
                             metadataConfig: HoodieMetadataConfig,
                             metaClient: HoodieTableMetaClient)
  extends SparkBaseIndexSupport (spark, metadataConfig, metaClient) {

  // NOTE: Since [[metadataConfig]] is transient this has to be eagerly persisted, before this will be passed on to the executor
  private val inMemoryProjectionThreshold = metadataConfig.getColumnStatsIndexInMemoryProjectionThreshold

  override def getIndexName: String = FunctionalIndexSupport.INDEX_NAME

  override def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                         queryFilters: Seq[Expression],
                                         queryReferencedColumns: Seq[String],
                                         prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         shouldPushDownFilesFilter: Boolean
                                        ): Option[Set[String]] = {
    lazy val functionalIndexPartitionOpt = getFunctionalIndexPartitionAndLiterals(queryFilters)
    if (isIndexAvailable && queryFilters.nonEmpty && functionalIndexPartitionOpt.nonEmpty) {
      val (indexPartition, literals) = functionalIndexPartitionOpt.get
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      if (indexDefinition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)) {
        val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
        val (prunedPartitions, prunedFileNames) = getPrunedPartitionsAndFileNames(prunedPartitionsAndFileSlices)
        val indexDf = loadFunctionalIndexDataFrame(indexPartition, prunedPartitions, readInMemory)
        Some(getCandidateFiles(indexDf, queryFilters, prunedFileNames))
      } else if (indexDefinition.getIndexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_BLOOM_FILTERS)) {
        val prunedPartitionAndFileNames = getPrunedPartitionsAndFileNamesMap(prunedPartitionsAndFileSlices, includeLogFiles = true)
        Option.apply(getCandidateFilesForKeys(indexPartition, prunedPartitionAndFileNames, literals))
      } else {
        Option.empty
      }
    } else {
      Option.empty
    }
  }

  override def invalidateCaches(): Unit = {
    // no caches for this index type, do nothing
  }

  /**
   * Return true if metadata table is enabled and functional index metadata partition is available.
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.isEnabled && metaClient.getIndexMetadata.isPresent && !metaClient.getIndexMetadata.get().getIndexDefinitions.isEmpty
  }

  def filterQueriesWithFunctionalFilterKey(queryFilters: Seq[Expression], sourceFieldOpt: Option[String]): List[Tuple2[Expression, List[String]]] = {
    var functionalIndexQueries: List[Tuple2[Expression, List[String]]] = List.empty
    for (query <- queryFilters) {
      filterQueryWithRecordKey(query, sourceFieldOpt, (expr: Expression) => {
        expr match {
          case expression: UnaryExpression => expression.child
          case other => other
        }
      }).foreach({
        case (exp: Expression, literals: List[String]) =>
          functionalIndexQueries = functionalIndexQueries :+ Tuple2.apply(exp, literals)
      })
    }

    functionalIndexQueries
  }

  /**
   * Searches for an index partition based on the specified index function and target column name.
   *
   * This method looks up the index definitions available in the metadata of a `metaClient` instance
   * and attempts to find an index partition where the index function and the source fields match
   * the provided arguments. If a matching index definition is found, the partition identifier for
   * that index is returned.
   *
   * @param queryFilters A sequence of `Expression` objects to analyze. Each expression should involve a single column
   *                     for the method to consider it (expressions involving multiple columns are skipped).
   * @return An `Option` containing the index partition identifier if a matching index definition is found.
   *         Returns `None` if no matching index definition is found.
   */
  private def getFunctionalIndexPartitionAndLiterals(queryFilters: Seq[Expression]): Option[Tuple2[String, List[String]]] = {
    val indexDefinitions = metaClient.getIndexMetadata.get().getIndexDefinitions.asScala
    if (indexDefinitions.nonEmpty) {
      val functionDefinitions = indexDefinitions.values
        .filter(definition => MetadataPartitionType.fromPartitionPath(definition.getIndexName).equals(MetadataPartitionType.FUNCTIONAL_INDEX))
        .toList
      var indexPartitionAndLiteralsOpt: Option[Tuple2[String, List[String]]] = Option.empty
      functionDefinitions.foreach(indexDefinition => {
        val queryInfoOpt = extractQueryAndLiterals(queryFilters, indexDefinition)
        if (queryInfoOpt.isDefined) {
          indexPartitionAndLiteralsOpt = Option.apply(Tuple2.apply(indexDefinition.getIndexName, queryInfoOpt.get._2))
        }
      })
      indexPartitionAndLiteralsOpt
    } else {
      Option.empty
    }
  }

  /**
   * Extracts mappings from function names to column names from a sequence of expressions.
   *
   * This method iterates over a given sequence of Spark SQL expressions and identifies expressions
   * that contain function calls corresponding to keys in the `SPARK_FUNCTION_MAP`. It supports only
   * expressions that are simple binary expressions involving a single column. If an expression contains
   * one of the functions and operates on a single column, this method maps the function name to the
   * column name.
   */
  private def extractQueryAndLiterals(queryFilters: Seq[Expression], indexDefinition: HoodieIndexDefinition): Option[(Expression, List[String])] = {
    val functionalIndexQueries = filterQueriesWithFunctionalFilterKey(queryFilters, Option.apply(indexDefinition.getSourceFields.get(0)))
    var queryAndLiteralsOpt: Option[(Expression, List[String])] = Option.empty
    functionalIndexQueries.foreach { tuple =>
      val (expr, literals) = (tuple._1, tuple._2)
      val functionNameOption = SPARK_FUNCTION_MAP.asScala.keys.find(expr.toString.contains)
      val functionName = functionNameOption.getOrElse("identity")
      if (indexDefinition.getIndexFunction.equals(functionName)) {
        queryAndLiteralsOpt = Option.apply(Tuple2.apply(expr, literals))
      }
    }
    queryAndLiteralsOpt
  }

  def loadFunctionalIndexDataFrame(indexPartition: String,
                                   prunedPartitions: Set[String],
                                   shouldReadInMemory: Boolean): DataFrame = {
    val colStatsDF = {
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadFunctionalIndexForColumnsInternal(
        indexDefinition.getSourceFields.asScala.toSeq, prunedPartitions, indexPartition, shouldReadInMemory)
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      val catalystRows: HoodieData[InternalRow] = colStatsRecords.mapPartitions(JFunction.toJavaSerializableFunction(it => {
        val converter = AvroConversionUtils.createAvroToInternalRowConverter(HoodieMetadataColumnStats.SCHEMA$, columnStatsRecordStructType)
        it.asScala.map(r => converter(r).orNull).asJava
      }), false)

      if (shouldReadInMemory) {
        // NOTE: This will instantiate a [[Dataset]] backed by [[LocalRelation]] holding all of the rows
        //       of the transposed table in memory, facilitating execution of the subsequently chained operations
        //       on it locally (on the driver; all such operations are actually going to be performed by Spark's
        //       Optimizer)
        createDataFrameFromInternalRows(spark, catalystRows.collectAsList().asScala.toSeq, columnStatsRecordStructType)
      } else {
        createDataFrameFromRDD(spark, HoodieJavaRDD.getJavaRDD(catalystRows), columnStatsRecordStructType)
      }
    }

    colStatsDF.select(targetColumnStatsIndexColumns.map(col): _*)
  }

  private def loadFunctionalIndexForColumnsInternal(targetColumns: Seq[String],
                                                    prunedPartitions: Set[String],
                                                    indexPartition: String,
                                                    shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // Read Metadata Table's Functional Index records into [[HoodieData]] container by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Extracting [[HoodieMetadataColumnStats]] records
    //    - Filtering out nulls
    checkState(targetColumns.nonEmpty)
    val encodedTargetColumnNames = targetColumns.map(colName => new ColumnIndexID(colName).asBase64EncodedString())
    val keyPrefixes = if (prunedPartitions.nonEmpty) {
      prunedPartitions.map(partitionPath =>
        new PartitionIndexID(HoodieTableMetadataUtil.getPartitionIdentifier(partitionPath)).asBase64EncodedString()
      ).flatMap(encodedPartition => {
        encodedTargetColumnNames.map(encodedTargetColumn => encodedTargetColumn.concat(encodedPartition))
      })
    } else {
      encodedTargetColumnNames
    }
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(keyPrefixes.toSeq.asJava, indexPartition, shouldReadInMemory)
    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
      //TODO: [HUDI-8303] Explicit conversion might not be required for Scala 2.12+
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
  }

  def getPrunedPartitionsAndFileNamesMap(prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                         includeLogFiles: Boolean = false): Map[String, Set[String]] = {
    prunedPartitionsAndFileSlices.foldLeft(Map.empty[String, Set[String]]) {
      case (partitionToFileMap, (partitionPathOpt, fileSlices)) =>
        partitionPathOpt match {
          case Some(partitionPath) =>
            val fileNames = fileSlices.flatMap { fileSlice =>
              val baseFile = Option(fileSlice.getBaseFile.orElse(null)).map(_.getFileName).toSeq
              val logFiles = if (includeLogFiles) {
                fileSlice.getLogFiles.iterator().asScala.map(_.getFileName).toSeq
              } else Seq.empty[String]
              baseFile ++ logFiles
            }.toSet

            // Update the map with the new partition and its file names
            partitionToFileMap.updated(partitionPath.path, partitionToFileMap.getOrElse(partitionPath.path, Set.empty) ++ fileNames)
          case None =>
            partitionToFileMap // Skip if no partition path
        }
    }
  }

  private def getCandidateFilesForKeys(indexPartition: String, prunedPartitionAndFileNames: Map[String, Set[String]], keys: List[String]): Set[String] = {
    val candidateFiles = prunedPartitionAndFileNames.flatMap { case (partition, fileNames) =>
      fileNames.filter { fileName =>
        val bloomFilterOpt = toScalaOption(metadataTable.getBloomFilter(partition, fileName, indexPartition))
        bloomFilterOpt match {
          case Some(bloomFilter) =>
            keys.exists(bloomFilter.mightContain)
          case None =>
            true // If bloom filter is empty or undefined, assume the file might contain the record key
        }
      }
    }.toSet

    candidateFiles
  }
}

object FunctionalIndexSupport {
  val INDEX_NAME = "FUNCTIONAL"
  /**
   * Target Column Stats Index columns which internally are mapped onto fields of the corresponding
   * Column Stats record payload ([[HoodieMetadataColumnStats]]) persisted w/in Metadata Table
   */
  private val targetColumnStatsIndexColumns = Seq(
    HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MIN_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_MAX_VALUE,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_NULL_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_VALUE_COUNT,
    HoodieMetadataPayload.COLUMN_STATS_FIELD_COLUMN_NAME
  )

  private val columnStatsRecordStructType: StructType = AvroConversionUtils.convertAvroSchemaToStructType(HoodieMetadataColumnStats.SCHEMA$)
}
