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

import org.apache.hadoop.fs.FileStatus
import org.apache.hudi.FunctionalIndexSupport._
import org.apache.hudi.HoodieConversionUtils.toScalaOption
import org.apache.hudi.avro.model.{HoodieMetadataColumnStats, HoodieMetadataRecord}
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieData
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.HoodieRecord
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ValidationUtils.checkState
import org.apache.hudi.common.util.hash.ColumnIndexID
import org.apache.hudi.data.HoodieJavaRDD
import org.apache.hudi.metadata.{HoodieMetadataPayload, HoodieTableMetadata, HoodieTableMetadataUtil}
import org.apache.hudi.util.JFunction
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.HoodieUnsafeUtils.{createDataFrameFromInternalRows, createDataFrameFromRDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{And, Expression}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

class FunctionalIndexSupport(spark: SparkSession,
                             metadataConfig: HoodieMetadataConfig,
                             metaClient: HoodieTableMetaClient) {

  @transient private lazy val engineCtx = new HoodieSparkEngineContext(new JavaSparkContext(spark.sparkContext))
  @transient private lazy val metadataTable: HoodieTableMetadata =
    HoodieTableMetadata.create(engineCtx, metadataConfig, metaClient.getBasePathV2.toString)

  // NOTE: Since [[metadataConfig]] is transient this has to be eagerly persisted, before this will be passed on to the executor
  private val inMemoryProjectionThreshold = metadataConfig.getColumnStatsIndexInMemoryProjectionThreshold

  /**
   * Determines whether it would be more optimal to read Column Stats Index a) in-memory of the invoking process,
   * or b) executing it on-cluster via Spark [[Dataset]] and [[RDD]] APIs
   */
  def shouldReadInMemory(fileIndex: HoodieFileIndex, queryReferencedColumns: Seq[String]): Boolean = {
    Option(metadataConfig.getColumnStatsIndexProcessingModeOverride) match {
      case Some(mode) =>
        mode == HoodieMetadataConfig.COLUMN_STATS_INDEX_PROCESSING_MODE_IN_MEMORY
      case None =>
        fileIndex.getFileSlicesCount * queryReferencedColumns.length < inMemoryProjectionThreshold
    }
  }

  /**
   * Return true if metadata table is enabled and functional index metadata partition is available.
   */
  def isIndexAvailable: Boolean = {
    metadataConfig.enabled && metaClient.getFunctionalIndexMetadata.isPresent && !metaClient.getFunctionalIndexMetadata.get().getIndexDefinitions.isEmpty
  }

  def getPrunedCandidateFileNames(indexPartition: String,
                                  shouldReadInMemory: Boolean,
                                  queryFilters: Seq[Expression]): Set[String] = {
    val indexDf = loadFunctionalIndexDataFrame(indexPartition, shouldReadInMemory)
    val indexSchema = indexDf.schema
    val indexFilter =
      queryFilters.map(translateIntoColumnStatsIndexFilterExpr(_, indexSchema))
        .reduce(And)

    val prunedCandidateFileNames =
      indexDf.where(new Column(indexFilter))
        .select(HoodieMetadataPayload.COLUMN_STATS_FIELD_FILE_NAME)
        .collect()
        .map(_.getString(0))
        .toSet

    prunedCandidateFileNames
  }

  def load(indexPartition: String,
           targetColumns: Seq[String],
           shouldReadInMemory: Boolean): DataFrame = {
    val metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePathV2.toString)
    // Read Metadata Table's Column Stats Index into Spark's [[DataFrame]]
    val colStatsDF = spark.read.format("org.apache.hudi")
      .options(metadataConfig.getProps.asScala)
      .load(s"$metadataTablePath/$indexPartition")

    val requiredIndexColumns =
      targetColumnStatsIndexColumns.map(colName =>
        col(s"${HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS}.${colName}"))

    colStatsDF.where(col(HoodieMetadataPayload.SCHEMA_FIELD_ID_COLUMN_STATS).isNotNull)
      .select(requiredIndexColumns: _*)
  }

  def loadFunctionalIndexDataFrame(indexPartition: String,
                                   shouldReadInMemory: Boolean): DataFrame = {
    val colStatsDF = {
      val indexDefinition = metaClient.getFunctionalIndexMetadata.get().getIndexDefinitions.get(indexPartition)
      val indexType = indexDefinition.getIndexType
      // NOTE: Currently only functional indexes created using column_stats is supported.
      // HUDI-7007 tracks for adding support for other index types such as bloom filters.
      checkState(indexType.equals(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS),
        s"Index type $indexType is not supported")
      val colStatsRecords: HoodieData[HoodieMetadataColumnStats] = loadFunctionalIndexForColumnsInternal(
        indexDefinition.getSourceFields.asScala, indexPartition, shouldReadInMemory)
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
        createDataFrameFromInternalRows(spark, catalystRows.collectAsList().asScala, columnStatsRecordStructType)
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

  /**
   * Returns the list of candidate files which store the provided record keys based on Metadata Table Record Index.
   *
   * @param allFiles   - List of all files which needs to be considered for the query
   * @param recordKeys - List of record keys.
   * @return Sequence of file names which need to be queried
   */
  def getCandidateFiles(allFiles: Seq[FileStatus], recordKeys: List[String]): Set[String] = {
    val recordKeyLocationsMap = metadataTable.readRecordIndex(seqAsJavaListConverter(recordKeys).asJava)
    val fileIdToPartitionMap: mutable.Map[String, String] = mutable.Map.empty
    val candidateFiles: mutable.Set[String] = mutable.Set.empty
    for (location <- collectionAsScalaIterableConverter(recordKeyLocationsMap.values()).asScala) {
      fileIdToPartitionMap.put(location.getFileId, location.getPartitionPath)
    }
    for (file <- allFiles) {
      val fileId = FSUtils.getFileIdFromFilePath(file.getPath)
      val partitionOpt = fileIdToPartitionMap.get(fileId)
      if (partitionOpt.isDefined) {
        candidateFiles += file.getPath.getName
      }
    }
    candidateFiles.toSet
  }
}

object FunctionalIndexSupport {

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
