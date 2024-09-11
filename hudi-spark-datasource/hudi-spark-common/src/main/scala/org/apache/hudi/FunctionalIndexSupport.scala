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
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.model.{FileSlice, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.ColumnIndexID
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction

import org.apache.spark.sql.HoodieUnsafeUtils.{createDataFrameFromInternalRows, createDataFrameFromRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
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
    lazy val functionalIndexPartitionOpt = getFunctionalIndexPartition(queryFilters)
    if (isIndexAvailable && queryFilters.nonEmpty && functionalIndexPartitionOpt.nonEmpty) {
      val readInMemory = shouldReadInMemory(fileIndex, queryReferencedColumns, inMemoryProjectionThreshold)
      val indexDf = loadFunctionalIndexDataFrame(functionalIndexPartitionOpt.get, readInMemory)
      val prunedFileNames = getPrunedFileNames(prunedPartitionsAndFileSlices)
      Some(getCandidateFiles(indexDf, queryFilters, prunedFileNames))
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
  private def getFunctionalIndexPartition(queryFilters: Seq[Expression]): Option[String] = {
    val functionToColumnNames = extractSparkFunctionNames(queryFilters)
    if (functionToColumnNames.nonEmpty) {
      // Currently, only one functional index in the query is supported. HUDI-7620 for supporting multiple functions.
      checkState(functionToColumnNames.size == 1, "Currently, only one function with functional index in the query is supported")
      val (indexFunction, targetColumnName) = functionToColumnNames.head
      val indexDefinitions = metaClient.getIndexMetadata.get().getIndexDefinitions
      indexDefinitions.asScala.foreach {
        case (indexPartition, indexDefinition) =>
          if (indexDefinition.getIndexFunction.equals(indexFunction) && indexDefinition.getSourceFields.contains(targetColumnName)) {
            Option.apply(indexPartition)
          }
      }
      Option.empty
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
  private def extractSparkFunctionNames(queryFilters: Seq[Expression]): Map[String, String] = {
    queryFilters.flatMap { expr =>
      // Support only simple binary expression on single column
      if (expr.references.size == 1) {
        val targetColumnName = expr.references.head.name
        // Check if the expression string contains any of the function names
        val exprString = expr.toString
        SPARK_FUNCTION_MAP.asScala.keys
          .find(exprString.contains)
          .map(functionName => functionName -> targetColumnName)
      } else {
        None // Skip expressions that do not match the criteria
      }
    }.toMap
  }

  private def loadFunctionalIndexDataFrame(indexPartition: String,
                                           shouldReadInMemory: Boolean): DataFrame = {
    val colStatsDF = {
      val indexDefinition = metaClient.getIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      val indexType = indexDefinition.getIndexType
      // NOTE: Currently only functional indexes created using column_stats is supported.
      // HUDI-7007 tracks for adding support for other index types such as bloom filters.
      checkState(indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS),
        s"Index type $indexType is not supported")
      val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadFunctionalIndexForColumnsInternal(
        indexDefinition.getSourceFields.asScala.toSeq, indexPartition, shouldReadInMemory)
      // NOTE: Explicit conversion is required for Scala 2.11
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
                                                    indexPartition: String,
                                                    shouldReadInMemory: Boolean): HoodieData[HoodieMetadataColumnStats] = {
    // Read Metadata Table's Functional Index records into [[HoodieData]] container by
    //    - Fetching the records from CSI by key-prefixes (encoded column names)
    //    - Extracting [[HoodieMetadataColumnStats]] records
    //    - Filtering out nulls
    checkState(targetColumns.nonEmpty)
    val encodedTargetColumnNames = targetColumns.map(colName => new ColumnIndexID(colName).asBase64EncodedString())
    val metadataRecords: HoodieData[HoodieRecord[HoodieMetadataPayload]] =
      metadataTable.getRecordsByKeyPrefixes(encodedTargetColumnNames.asJava, indexPartition, shouldReadInMemory)
    val columnStatsRecords: HoodieData[HoodieMetadataColumnStats] =
    // NOTE: Explicit conversion is required for Scala 2.11
      metadataRecords.map(JFunction.toJavaSerializableFunction(record => {
          toScalaOption(record.getData.getInsertValue(null, null))
            .map(metadataRecord => metadataRecord.asInstanceOf[HoodieMetadataRecord].getColumnStatsMetadata)
            .orNull
        }))
        .filter(JFunction.toJavaSerializableFunction(columnStatsRecord => columnStatsRecord != null))

    columnStatsRecords
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
