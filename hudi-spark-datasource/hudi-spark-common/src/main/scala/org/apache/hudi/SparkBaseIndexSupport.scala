/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hudi.HoodieFileIndex.DataSkippingFailureMode
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{FileSlice, HoodieIndexDefinition}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.keygen.KeyGenerator
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata}
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}
import scala.util.control.NonFatal

abstract class SparkBaseIndexSupport(spark: SparkSession,
                                     metadataConfig: HoodieMetadataConfig,
                                     metaClient: HoodieTableMetaClient) {
  @transient protected lazy val engineCtx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
  @transient protected lazy val metadataTable: HoodieTableMetadata =
    metaClient.getTableFormat.getMetadataFactory.create(engineCtx, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)

  def getIndexName: String

  def isIndexAvailable: Boolean

  /**
   * Returns true if the query type is supported by the index.
   *
   * TODO: The default implementation should be changed to throw
   * an exception once time travel support for metadata table is added.
   */
  def supportsQueryType(options: Map[String, String]): Boolean = true

  def computeCandidateIsStrict(spark: SparkSession,
                               fileIndex: HoodieFileIndex,
                               queryFilters: Seq[Expression],
                               queryReferencedColumns: Seq[String],
                               prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                               shouldPushDownFilesFilter: Boolean): Option[Set[String]] = {
    try {
      computeCandidateFileNames(fileIndex, queryFilters, queryReferencedColumns, prunedPartitionsAndFileSlices, shouldPushDownFilesFilter)
    } catch {
      case NonFatal(e) =>
        spark.sqlContext.getConf(DataSkippingFailureMode.configName, DataSkippingFailureMode.Fallback.value) match {
          case DataSkippingFailureMode.Fallback.value => Option.empty
          case DataSkippingFailureMode.Strict.value => throw e;
        }
    }
  }

  def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                queryFilters: Seq[Expression],
                                queryReferencedColumns: Seq[String],
                                prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                shouldPushDownFilesFilter: Boolean): Option[Set[String]]

  def invalidateCaches(): Unit

  def getPrunedPartitionsAndFileNames(fileIndex: HoodieFileIndex, prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath],
                                      Seq[FileSlice])]): (Set[String], Set[String]) = {
    val (prunedPartitions, prunedFiles) = prunedPartitionsAndFileSlices.foldLeft((Set.empty[String], Set.empty[String])) {
      case ((partitionSet, fileSet), (partitionPathOpt, fileSlices)) =>
        val updatedPartitionSet = partitionPathOpt.map(_.path).map(partitionSet + _).getOrElse(partitionSet)
        val updatedFileSet = fileSlices.foldLeft(fileSet) { (fileAcc, fileSlice) =>
          val baseFile = Option(fileSlice.getBaseFile.orElse(null)).map(_.getFileName)
          val logFiles = if (fileIndex.includeLogFiles) {
            fileSlice.getLogFiles.iterator().asScala.map(_.getFileName).toSet
          } else Set.empty[String]

          fileAcc ++ baseFile ++ logFiles
        }

        (updatedPartitionSet, updatedFileSet)
    }

    (prunedPartitions, prunedFiles)
  }

  protected def getCandidateFiles(indexDf: DataFrame, queryFilters: Seq[Expression], fileNamesFromPrunedPartitions: Set[String],
                                  isExpressionIndex: Boolean = false, indexDefinitionOpt: Option[HoodieIndexDefinition] = Option.empty): Set[String] = {
    val indexedCols : Seq[String] = if (indexDefinitionOpt.isDefined) {
      indexDefinitionOpt.get.getSourceFields.asScala.toSeq
    } else {
      metaClient.getIndexMetadata.get().getIndexDefinitions.get(PARTITION_NAME_COLUMN_STATS).getSourceFields.asScala.toSeq
    }
    val indexFilter = queryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, isExpressionIndex, indexedCols)).reduce(And)
    if (indexFilter.equals(TrueLiteral)) {
      // if there are any non indexed cols or we can't translate source expr, we have to read all files and may not benefit from col stats lookup.
       fileNamesFromPrunedPartitions
    } else {
      // only lookup in col stats if all filters are eligible to be looked up in col stats index in MDT
      val prunedCandidateFileNames =
        indexDf.where(new Column(indexFilter))
          .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
          .collect()
          .map(_.getString(0))
          .toSet

      // NOTE: Col-Stats Index isn't guaranteed to have complete set of statistics for every
      //       base-file or log file: since it's bound to clustering, which could occur asynchronously
      //       at arbitrary point in time, and is not likely to be touching all of the base files.
      //
      //       To close that gap, we manually compute the difference b/w all indexed (by col-stats-index)
      //       files and all outstanding base-files or log files, and make sure that all base files and
      //       log file not represented w/in the index are included in the output of this method
      val allIndexedFileNames =
      indexDf.select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
        .collect()
        .map(_.getString(0))
        .toSet
      val notIndexedFileNames = fileNamesFromPrunedPartitions -- allIndexedFileNames

      prunedCandidateFileNames ++ notIndexedFileNames
    }
  }

  /**
   * Determines whether it would be more optimal to read Column Stats Index a) in-memory of the invoking process,
   * or b) executing it on-cluster via Spark [[Dataset]] and [[RDD]] APIs
   */
  protected def shouldReadInMemory(fileIndex: HoodieFileIndex, queryReferencedColumns: Seq[String], inMemoryProjectionThreshold: Integer): Boolean = {
    // NOTE: Since executing on-cluster via Spark API has its own non-trivial amount of overhead,
    //       it's most often preferential to fetch index w/in the same process (usually driver),
    //       w/o resorting to on-cluster execution.
    //       For that we use a simple-heuristic to determine whether we should read and process the index in-memory or
    //       on-cluster: total number of rows of the expected projected portion of the index has to be below the
    //       threshold (of 100k records)
    Option(metadataConfig.getColumnStatsIndexProcessingModeOverride) match {
      case Some(mode) =>
        mode == HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY
      case None =>
        fileIndex.getFileSlicesCount * queryReferencedColumns.length < inMemoryProjectionThreshold
    }
  }

  /**
   * Given query filters, it filters the EqualTo and IN queries on record key columns and returns a tuple of
   * list of such queries and list of record key literals present in the query.
   * If record index is not available, it returns empty list for record filters and record keys
   *
   * @param queryFilters The queries that need to be filtered.
   * @return Tuple of List of filtered queries and list of record key literals that need to be matched
   */
  protected def filterQueriesWithRecordKey(queryFilters: Seq[Expression]): (List[Expression], List[String]) = {
    if (!isIndexAvailable) {
      (List.empty, List.empty)
    } else {
      var recordKeyQueries: List[Expression] = List.empty
      var compositeRecordKeys: List[String] = List.empty
      val recordKeyOpt = getRecordKeyConfig
      val isComplexRecordKey = recordKeyOpt.map(recordKeys => recordKeys.length).getOrElse(0) > 1
      recordKeyOpt.foreach { recordKeysArray =>
        // Handle composite record keys
        breakable {
          // Iterate configured record keys and fetch literals for every record key
          for (recordKey <- recordKeysArray) {
            var recordKeys: List[String] = List.empty
            for (query <- queryFilters) {
              {
                RecordLevelIndexSupport.filterQueryWithRecordKey(query, Option.apply(recordKey),
                  if (isComplexRecordKey) {
                    RecordLevelIndexSupport.getComplexKeyLiteralGenerator()
                  } else {
                    RecordLevelIndexSupport.getSimpleLiteralGenerator()
                  }
                ).foreach {
                  case (exp: Expression, recKeys: List[String]) =>
                    recordKeys = recordKeys ++ recKeys
                    recordKeyQueries = recordKeyQueries :+ exp
                }
              }
            }

            if (recordKeys.isEmpty) {
              // No literals found for the record key, therefore filtering can not be performed
              recordKeyQueries = List.empty
              compositeRecordKeys = List.empty
              break()
            } else if (!isComplexRecordKey || compositeRecordKeys.isEmpty) {
              compositeRecordKeys = recordKeys
            } else {
              // Combine literals for this configured record key with literals for the other configured record keys
              // If there are two literals for rk1, rk2, rk3 each. A total of 8 combinations will be generated
              var tempCompositeRecordKeys: List[String] = List.empty
              for (compRecKey <- compositeRecordKeys) {
                for (recKey <- recordKeys) {
                  tempCompositeRecordKeys = tempCompositeRecordKeys :+ (compRecKey + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + recKey)
                }
              }
              compositeRecordKeys = tempCompositeRecordKeys
            }
          }
        }
      }
      (recordKeyQueries, compositeRecordKeys)
    }
  }

  /**
   * Returns the configured record key for the table.
   */
  private def getRecordKeyConfig: Option[Array[String]] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    // Convert the Hudi Option to Scala Option and return if present
    Option(recordKeysOpt.orElse(null)).filter(_.nonEmpty)
  }

}
