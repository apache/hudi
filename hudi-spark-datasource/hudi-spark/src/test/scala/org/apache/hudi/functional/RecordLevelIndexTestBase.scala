/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions._
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.data.HoodieListData
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.RawTripTestPayload.recordsToStrings
import org.apache.hudi.common.util.HoodieDataUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.{HoodieBackedTableMetadata, HoodieTableMetadataUtil, MetadataPartitionType}

import org.apache.spark.sql._
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}

import scala.collection.{mutable, JavaConverters}
import scala.collection.JavaConverters._
import scala.util.Using

class RecordLevelIndexTestBase extends HoodieStatsIndexTestBase {
  val metadataOpts: Map[String, String] = Map(
    HoodieMetadataConfig.ENABLE.key -> "true",
    HoodieMetadataConfig.RECORD_INDEX_ENABLE_PROP.key -> "true",
    HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false"
  )
  def commonOpts: Map[String, String] = Map(
    PARTITIONPATH_FIELD.key -> "partition",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
    HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key -> "15"
  ) ++ baseOpts ++ metadataOpts

  val secondaryIndexOpts: Map[String, String] = Map(
    HoodieMetadataConfig.SECONDARY_INDEX_ENABLE_PROP.key -> "true"
  )

  val commonOptsWithSecondaryIndex: Map[String, String] = commonOpts ++ secondaryIndexOpts ++ metadataOpts

  val commonOptsNewTableSITest: Map[String, String] = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    HoodieWriteConfig.TBL_NAME.key -> "trips_table",
    RECORDKEY_FIELD.key -> "uuid",
    SECONDARYKEY_COLUMN_NAME.key -> "city",
    PARTITIONPATH_FIELD.key -> "state",
    PRECOMBINE_FIELD.key -> "ts",
    HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
  ) ++ metadataOpts

  val commonOptsWithSecondaryIndexSITest: Map[String, String] = commonOptsNewTableSITest ++ secondaryIndexOpts

  protected def doWriteAndValidateDataAndRecordIndex(hudiOpts: Map[String, String],
                                                   operation: String,
                                                   saveMode: SaveMode,
                                                   validate: Boolean = true,
                                                   numUpdates: Int = 1,
                                                   onlyUpdates: Boolean = false): DataFrame = {
    var latestBatch: mutable.Buffer[String] = null
    if (operation == UPSERT_OPERATION_OPT_VAL) {
      val instantTime = getInstantTime()
      val records = recordsToStrings(dataGen.generateUniqueUpdates(instantTime, numUpdates))
      if (!onlyUpdates) {
        records.addAll(recordsToStrings(dataGen.generateInserts(instantTime, 1)))
      }
      latestBatch = records.asScala
    } else if (operation == INSERT_OVERWRITE_OPERATION_OPT_VAL) {
      latestBatch = recordsToStrings(dataGen.generateInsertsForPartition(
        getInstantTime(), 5, dataGen.getPartitionPaths.last)).asScala
    } else {
      latestBatch = recordsToStrings(dataGen.generateInserts(getInstantTime(), 5)).asScala
    }
    val latestBatchDf = spark.read.json(spark.sparkContext.parallelize(latestBatch.toSeq, 2))
    latestBatchDf.cache()
    latestBatchDf.write.format("org.apache.hudi")
      .options(hudiOpts)
      .option(DataSourceWriteOptions.OPERATION.key, operation)
      .mode(saveMode)
      .save(basePath)
    val deletedDf = calculateMergedDf(latestBatchDf, operation)
    deletedDf.cache()
    if (!metaClientReloaded) {
      // initialization of meta client is required again after writing data so that
      // latest table configs are picked up
      metaClient = HoodieTableMetaClient.builder().setBasePath(basePath).setConf(storageConf).build()
      metaClientReloaded = true
    }

    if (validate) {
      validateDataAndRecordIndices(hudiOpts, deletedDf)
    }

    deletedDf.unpersist()
    latestBatchDf.unpersist()
    latestBatchDf
  }

  protected def calculateMergedDf(latestBatchDf: DataFrame, operation: String): DataFrame = {
    calculateMergedDf(latestBatchDf, operation, globalIndexEnableUpdatePartitions = false)
  }

  protected def getFileGroupCountForRecordIndex(writeConfig: HoodieWriteConfig): Long = {
    Using(getHoodieTable(metaClient, writeConfig).getMetadataTable.asInstanceOf[HoodieBackedTableMetadata]) { metadataTable =>
      metadataTable.getMetadataFileSystemView.getAllFileGroups(MetadataPartitionType.RECORD_INDEX.getPartitionPath).count
    }.get
  }

  protected def validateDataAndRecordIndices(hudiOpts: Map[String, String],
                                           deletedDf: DataFrame = sparkSession.emptyDataFrame): Unit = {
    val writeConfig = getWriteConfig(hudiOpts)
    val metadata = metadataWriter(writeConfig).getTableMetadata
    val readDf = spark.read.format("hudi").load(basePath)
    readDf.cache()
    val rowArr = readDf.collect()
    val recordIndexMap = HoodieDataUtils.dedupeAndCollectAsMap(metadata.readRecordIndexLocationsWithKeys(
      HoodieListData.eager(JavaConverters.seqAsJavaListConverter(rowArr.map(row => row.getAs("_hoodie_record_key").toString).toList).asJava)))

    assertTrue(rowArr.length > 0)
    for (row <- rowArr) {
      val recordKey: String = row.getAs("_hoodie_record_key")
      val partitionPath: String = row.getAs("_hoodie_partition_path")
      val fileName: String = row.getAs("_hoodie_file_name")
      val recordLocation = recordIndexMap.get(recordKey)
      assertEquals(partitionPath, recordLocation.getPartitionPath)
      assertTrue(fileName.startsWith(recordLocation.getFileId), fileName + " should start with " + recordLocation.getFileId)
    }

    val deletedRows = deletedDf.collect()
    val recordIndexMapForDeletedRows = HoodieDataUtils.dedupeAndCollectAsMap(metadata.readRecordIndexLocationsWithKeys(
      HoodieListData.eager(JavaConverters.seqAsJavaListConverter(deletedRows.map(row => row.getAs("_row_key").toString).toList).asJava)))

    assertEquals(0, recordIndexMapForDeletedRows.size(), "deleted records should not present in RLI")

    assertEquals(rowArr.length, recordIndexMap.keySet.size)
    val estimatedFileGroupCount = HoodieTableMetadataUtil.estimateFileGroupCount(MetadataPartitionType.RECORD_INDEX, rowArr.length, 48,
      writeConfig.getRecordIndexMinFileGroupCount, writeConfig.getRecordIndexMaxFileGroupCount,
      writeConfig.getRecordIndexGrowthFactor, writeConfig.getRecordIndexMaxFileGroupSizeBytes)
    assertEquals(estimatedFileGroupCount, getFileGroupCountForRecordIndex(writeConfig))
    val prevDf = mergedDfList.last.drop("tip_history", "_hoodie_is_deleted")
    val nonMatchingRecords = readDf.drop("_hoodie_commit_time", "_hoodie_commit_seqno", "_hoodie_record_key",
      "_hoodie_partition_path", "_hoodie_file_name", "tip_history", "_hoodie_is_deleted")
      .join(prevDf, prevDf.columns, "leftanti")
    assertEquals(0, nonMatchingRecords.count())
    assertEquals(readDf.count(), prevDf.count())
    readDf.unpersist()
  }
}
