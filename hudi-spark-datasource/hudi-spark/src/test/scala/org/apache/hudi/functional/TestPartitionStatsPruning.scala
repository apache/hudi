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

package org.apache.hudi.functional

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.{HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.{ColumnStatsTestCase, ColumnStatsTestParams}
import org.apache.hudi.storage.StoragePath

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

@Tag("functional-b")
class TestPartitionStatsPruning extends ColumnStatIndexTestBase {

  val DEFAULT_COLUMNS_TO_INDEX = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
    HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8")

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataPSISimple(testCase: ColumnStatsTestCase): Unit = {

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/partition-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataColumnStatsIndex(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "10"
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/partition-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-partition-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-partition-stats-2-index-table.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8"))

    // update list of columns to explicit list of cols.
    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c6,c7,c8" // ignore c4
    )

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update5-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-partition-stats-3-index-table.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c5","c6","c7","c8"))

    // lets explicitly override again. ignore c6
    // update list of columns to explicit list of cols.
    val metadataOpts2 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c7,c8" // ignore c4,c6
    )

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts2, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-partition-stats-4-index-table.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c5","c7","c8"))

    // disable cols stats
    val metadataOpts3 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false"
    )

    // disable col stats
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts3, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidateManually = false))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    validateNonExistantColumnsToIndexDefn(metaClient)
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataColumnStatsIndexInitializationWithUpserts(testCase: ColumnStatsTestCase): Unit = {
    val partitionCol : String = "c8"
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "3"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts0 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false"
    )

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts0, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      shouldValidateColStats = false))

    // delete a subset of recs. this will add a delete log block for MOR table.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts0, commonOpts,
      dataSourcePath = "index/colstats/delete-input-table-json/",
      expectedColStatsSourcePath = "",
      operation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      shouldValidateColStats = false))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c4,c5,c6,c7,c8"
    )

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    var expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap1-partition-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap1-partition-stats-index-table.json"
    }

    // updates a subset which are not deleted and enable col stats and validate bootstrap
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap2-partition-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap2-partition-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update4-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8"))
  }

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataColumnStatsIndexInitializationWithRollbacks(testCase: ColumnStatsTestCase): Unit = {
    val partitionCol : String ="c8"
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      "hoodie.write.markers.type" -> "DIRECT",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    simulateFailureForLatestCommit(testCase.tableType, partitionCol)

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c4,c5,c6,c7,c8"
    )

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // updates a subset which are not deleted and enable col stats and validate bootstrap
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json",
      expectedColStatsSourcePath = "index/colstats/cow-bootstrap-rollback1-partition-stats-index-table.json",
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0,
      shouldValidateColStats = false,
      shouldValidatePartitionStats = true))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getActiveTimeline.getRollbackTimeline.countInstants() > 0)

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8"))
  }

  def simulateFailureForLatestCommit(tableType: HoodieTableType, partitionCol: String) : Unit = {
    // simulate failure for latest commit.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    var baseFileName : String = null
    var logFileName : String = null
    val lastCompletedCommit = metaClient.getActiveTimeline.getCommitsTimeline.filterCompletedInstants().lastInstant().get()
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      val dataFiles = if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath, "/"))
      } else {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath, "9"))
      }
      val logFileFileStatus = dataFiles.stream().filter(fileStatus => fileStatus.getPath.getName.contains(".log")).findFirst().get()
      logFileName = logFileFileStatus.getPath.getName
    } else {
      val dataFiles = if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath.toString))
      } else {
        metaClient.getStorage.listFiles(new StoragePath(metaClient.getBasePath,  "9"))
      }
      val baseFileFileStatus = dataFiles.stream().filter(fileStatus => fileStatus.getPath.getName.contains(lastCompletedCommit.requestedTime)).findFirst().get()
      baseFileName = baseFileFileStatus.getPath.getName
    }

    val latestCompletedFileName = INSTANT_FILE_NAME_GENERATOR.getFileName(lastCompletedCommit)
    metaClient.getStorage.deleteFile(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/timeline/" + latestCompletedFileName))

    // re-create marker for the deleted file.
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/" + logFileName + ".marker.APPEND"))
      } else {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/9/" + logFileName + ".marker.APPEND"))
      }
    } else {
      if (StringUtils.isNullOrEmpty(partitionCol)) {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/" + baseFileName + ".marker.MERGE"))
      } else {
        metaClient.getStorage.create(new StoragePath(metaClient.getBasePath.toString + "/.hoodie/.temp/" + lastCompletedCommit.requestedTime + "/9/" + baseFileName + ".marker.MERGE"))
      }
    }
  }
}
