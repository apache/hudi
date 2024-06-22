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

import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.FileSlice
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata}
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._

abstract class SparkBaseIndexSupport(spark: SparkSession,
                                     metadataConfig: HoodieMetadataConfig,
                                     metaClient: HoodieTableMetaClient) {
  @transient protected lazy val engineCtx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
  @transient protected lazy val metadataTable: HoodieTableMetadata =
    HoodieTableMetadata.create(engineCtx, metaClient.getStorage, metadataConfig, metaClient.getBasePath.toString)

  def getIndexName: String

  def isIndexAvailable: Boolean

  def computeCandidateFileNames(fileIndex: HoodieFileIndex,
                                queryFilters: Seq[Expression],
                                queryReferencedColumns: Seq[String],
                                prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                shouldPushDownFilesFilter: Boolean): Option[Set[String]]

  def invalidateCaches(): Unit

  protected def getPrunedFileNames(prunedPartitionsAndFileSlices: Seq[(Option[BaseHoodieTableFileIndex.PartitionPath], Seq[FileSlice])],
                                   includeLogFiles: Boolean = false): Set[String] = {
    prunedPartitionsAndFileSlices
      .flatMap {
        case (_, fileSlices) => fileSlices
      }
      .flatMap { fileSlice =>
        val baseFileOption = Option(fileSlice.getBaseFile.orElse(null))
        val logFiles = if (includeLogFiles) {
          fileSlice.getLogFiles.iterator().asScala.map(_.getFileName).toList
        } else Nil
        baseFileOption.map(_.getFileName).toList ++ logFiles
      }
      .toSet
  }

  protected def getCandidateFiles(indexDf: DataFrame, queryFilters: Seq[Expression], prunedFileNames: Set[String]): Set[String] = {
    val indexSchema = indexDf.schema
    val indexFilter = queryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, indexSchema)).reduce(And)
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
    val notIndexedFileNames = prunedFileNames -- allIndexedFileNames

    prunedCandidateFileNames ++ notIndexedFileNames
  }

  /**
   * Determines whether it would be more optimal to read Column Stats Index a) in-memory of the invoking process,
   * or b) executing it on-cluster via Spark [[Dataset]] and [[RDD]] APIs
   */
  protected def shouldReadInMemory(fileIndex: HoodieFileIndex, queryReferencedColumns: Seq[String], inMemoryProjectionThreshold: Integer): Boolean = {
    Option(metadataConfig.getColumnStatsIndexProcessingModeOverride) match {
      case Some(mode) =>
        mode == HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY
      case None =>
        fileIndex.getFileSlicesCount * queryReferencedColumns.length < inMemoryProjectionThreshold
    }
  }

  /**
   * Given query filters, it filters the EqualTo and IN queries on simple record key columns and returns a tuple of
   * list of such queries and list of record key literals present in the query.
   * If record index is not available, it returns empty list for record filters and record keys
   * @param queryFilters The queries that need to be filtered.
   * @return Tuple of List of filtered queries and list of record key literals that need to be matched
   */
  protected def filterQueriesWithRecordKey(queryFilters: Seq[Expression]): (List[Expression], List[String]) = {
    if (!isIndexAvailable) {
      (List.empty, List.empty)
    } else {
      var recordKeyQueries: List[Expression] = List.empty
      var recordKeys: List[String] = List.empty
      for (query <- queryFilters) {
        val recordKeyOpt = getRecordKeyConfig
        RecordLevelIndexSupport.filterQueryWithRecordKey(query, recordKeyOpt).foreach({
          case (exp: Expression, recKeys: List[String]) =>
            recordKeys = recordKeys ++ recKeys
            recordKeyQueries = recordKeyQueries :+ exp
        })
      }

      Tuple2.apply(recordKeyQueries, recordKeys)
    }
  }

  /**
   * Returns the configured record key for the table if it is a simple record key else returns empty option.
   */
  private def getRecordKeyConfig: Option[String] = {
    val recordKeysOpt: org.apache.hudi.common.util.Option[Array[String]] = metaClient.getTableConfig.getRecordKeyFields
    val recordKeyOpt = recordKeysOpt.map[String](JFunction.toJavaFunction[Array[String], String](arr =>
      if (arr.length == 1) {
        arr(0)
      } else {
        null
      }))
    Option.apply(recordKeyOpt.orElse(null))
  }

}
