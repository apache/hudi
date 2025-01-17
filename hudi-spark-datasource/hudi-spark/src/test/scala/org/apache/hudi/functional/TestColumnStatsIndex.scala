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

package org.apache.hudi.functional

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.ColumnStatsIndexSupport.composeIndexSchema
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieConversionUtils.toProperties
import org.apache.hudi.common.config.{HoodieCommonConfig, HoodieMetadataConfig, HoodieStorageConfig}
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieFileGroup, HoodieRecord, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.{HoodieCleanConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestCase
import org.apache.hudi.storage.StoragePath
import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceWriteOptions}
import org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_FILE_NAME_GENERATOR
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hudi.client.common.HoodieSparkEngineContext
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.view.FileSystemViewManager
import org.apache.hudi.common.util.{ParquetUtils, StringUtils}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestCase
import org.apache.hudi.functional.ColumnStatIndexTestBase.ColumnStatsTestParams
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration
import org.apache.hudi.{ColumnStatsIndexSupport, DataSourceWriteOptions, config}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.codegen.TrueLiteral
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, GreaterThan, Literal, Or}
import org.apache.spark.sql.hudi.DataSkippingUtils.translateIntoColumnStatsIndexFilterExpr
import org.apache.spark.sql.types._
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertTrue}
import org.junit.jupiter.api._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{EnumSource, MethodSource, ValueSource}

import java.util.{Collections, Comparator}
import java.util.stream.Collectors
import scala.collection.JavaConverters._
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

@Tag("functional")
class TestColumnStatsIndex extends ColumnStatIndexTestBase {

  val DEFAULT_COLUMNS_TO_INDEX = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
    HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8")

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParams"))
  def testMetadataColumnStatsIndex(testCase: ColumnStatsTestCase): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      "hoodie.compact.inline.max.delta.commits" -> "10"
    ) ++ metadataOpts

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite))

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append))

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW
    var expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated2-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated2-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append))

    validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)

    // update list of columns to explicit list of cols.
    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c6,c7,c8" // ignore c4
    )

    expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated3-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated3-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update5-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c5","c6","c7","c8"))

    // lets explicitly override again. ignore c6
    // update list of columns to explicit list of cols.
    val metadataOpts2 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c7,c8" // ignore c4,c6
    )

    expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated4-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated4-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts2, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c5","c7","c8"))

    // update list of columns to explicit list of cols.
    val metadataOpts3 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c7" // ignore c4,c5,c8.
    )
    // disable col stats
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts3, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false,
      shouldValidateManually = false))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    validateNonExistantColumnsToIndexDefn(metaClient)
  }

  /**
   * We don't have support for nested fields. So, even if explicitly set using cols to index config, hudi is expected to ignore indexing
   * nested fields.
   */
    @Test
  def testMetadataColumnStatsIndexNestedFields(): Unit = {
    val testCase = ColumnStatsTestCase(HoodieTableType.COPY_ON_WRITE, true)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c4,c5,c6,c7,c8,c9.car_brand"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    // expectation is that, the nested field will be ignored and stats will be generated only for top level fields.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = "index/colstats/cow-table-nested.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite), true)

      validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
        HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8"))
  }

  @ParameterizedTest
  @MethodSource(Array("testTableTypePartitionTypeParams"))
  def testMetadataColumnStatsIndexInitializationWithUpserts(tableType: HoodieTableType, partitionCol : String): Unit = {
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true)
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
      PARTITIONPATH_FIELD.key -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key -> "5"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      false,
        numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // delete a subset of recs. this will add a delete log block for MOR table.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/delete-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap1-column-stats-index-table.json"
    }

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // lets validate that we have log files generated in case of MOR table
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      val metaClient = HoodieTableMetaClient.builder().setConf(new HadoopStorageConfiguration(jsc.hadoopConfiguration())).setBasePath(basePath).build()
      val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient, HoodieMetadataConfig.newBuilder().enable(false).build())
      fsv.loadAllPartitions()
      val baseStoragePath = new StoragePath(basePath)
      val allPartitionPaths = fsv.getPartitionPaths
      allPartitionPaths.forEach(partitionPath => {
        val pPath = FSUtils.getRelativePartitionPath(baseStoragePath, partitionPath)
        assertTrue (fsv.getLatestFileSlices(pPath).filter(fileSlice => fileSlice.hasLogFiles).count() > 0)
      })
      fsv.close()
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
      smallFileLimit = 0))

    // trigger one more upsert and compaction (w/ MOR table) and validate.
    val expectedColStatsSourcePath1 = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap2-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap2-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update4-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath1,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)
  }

  @ParameterizedTest
  @MethodSource(Array("testTableTypePartitionTypeParams"))
  def testMetadataColumnStatsIndexInitializationWithRollbacks(tableType: HoodieTableType, partitionCol : String): Unit = {
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true)
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
      shouldValidate = false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    simulateFailureForLatestCommit(tableType, partitionCol)

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // NOTE: MOR and COW have different fixtures since MOR is bearing delta-log files (holding
    //       deferred updates), diverging from COW

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-bootstrap-rollback1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap-rollback1-column-stats-index-table.json"
    }

    metaClient = HoodieTableMetaClient.reload(metaClient)
    val latestCompletedCommit = metaClient.getActiveTimeline.filterCompletedInstants().lastInstant().get().requestedTime

    // updates a subset which are not deleted and enable col stats and validate bootstrap
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      latestCompletedCommit = latestCompletedCommit,
      numPartitions =  1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getActiveTimeline.getRollbackTimeline.countInstants() > 0)

    validateColumnsToIndex(metaClient, DEFAULT_COLUMNS_TO_INDEX)
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

  @Test
  def testMORDeleteBlocks(): Unit = {
    val tableType: HoodieTableType = HoodieTableType.MERGE_ON_READ
    val partitionCol = "c8"
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "5"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    // updates
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = "index/colstats/mor-delete-block1-column-stats-index-table.json"

    // delete a subset of recs. this will add a delete log block for MOR table.
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/delete-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("", "c8"))
  def testColStatsWithCleanCOW(partitionCol: String): Unit = {
    val tableType: HoodieTableType = HoodieTableType.COPY_ON_WRITE
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidate = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // updates 1
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-clean1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-bootstrap-rollback1-column-stats-index-table.json"
    }

    // updates 2
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("", "c8"))
  def testColStatsWithCleanMOR(partitionCol: String): Unit = {
    val tableType: HoodieTableType = HoodieTableType.MERGE_ON_READ
    val testCase = ColumnStatsTestCase(tableType, shouldReadInMemory = true)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "1",
      "hoodie.upsert.shuffle.parallelism" -> "1",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> testCase.tableType.toString,
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> partitionCol,
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key() -> "1",
      HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key() -> "2"
    ) ++ metadataOpts

    // inserts
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/input-table-json",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidate = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    // updates 1
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update2-input-table-json/",
      expectedColStatsSourcePath = null,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidate = false,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    val expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-clean1-column-stats-index-table.json"
    } else {
      "index/colstats/mor-clean1-column-stats-index-table.json"
    }

    // updates 2
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts1, commonOpts,
      dataSourcePath = "index/colstats/update3-input-table-json/",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      numPartitions = 1,
      parquetMaxFileSize = 100 * 1024 * 1024,
      smallFileLimit = 0))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertTrue(metaClient.getActiveTimeline.getCleanerTimeline.countInstants() > 0)
  }

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testMetadataColumnStatsIndexValueCount(tableType: HoodieTableType): Unit = {
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true"
    )

    val commonOpts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    val schema = StructType(StructField("c1", IntegerType, false) :: StructField("c2", StringType, true) :: Nil)
    val inputDF = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(Row(1, "v1"), Row(2, "v2"), Row(3, null), Row(4, "v4"))),
      schema)

    inputDF
      .sort("c1", "c2")
      .write
      .format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, schema, metadataConfig, metaClient)
    columnStatsIndex.loadTransposed(Seq("c2"), false) { transposedDF =>
      val result = transposedDF.select("valueCount", "c2_nullCount")
        .collect().head

      assertTrue(result.getLong(0) == 4)
      assertTrue(result.getLong(1) == 1)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testMetadataColumnStatsWithFilesFilter(shouldReadInMemory: Boolean): Unit = {
    val targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      PARTITIONPATH_FIELD.key() -> "c8",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCommonConfig.RECONCILE_SCHEMA.key -> "true"
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json-partition-pruning").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    val fsv = FileSystemViewManager.createInMemoryFileSystemView(new HoodieSparkEngineContext(jsc), metaClient,
      HoodieMetadataConfig.newBuilder().enable(false).build())
    fsv.loadAllPartitions()

    val partitionPaths = fsv.getPartitionPaths
    val partitionToBaseFiles : java.util.Map[String, java.util.List[StoragePath]] = new java.util.HashMap[String, java.util.List[StoragePath]]

    partitionPaths.forEach(partitionPath =>
      partitionToBaseFiles.put(partitionPath.getName, fsv.getLatestBaseFiles(partitionPath.getName)
        .map[StoragePath](baseFile => baseFile.getStoragePath).collect(Collectors.toList[StoragePath]))
    )
    fsv.close()

    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)
    val requestedColumns = Seq("c1")
    // get all file names
    val stringEncoder: Encoder[String] = org.apache.spark.sql.Encoders.STRING
    val fileNameSet: Set[String] = columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { df =>
      val fileNames: Array[String] = df.select("fileName").as[String](stringEncoder).collect()
      val newFileNameSet: Set[String] = fileNames.toSet
      newFileNameSet
    }
    val targetFileName = fileNameSet.take(2)
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, None, Some(targetFileName)) { df =>
      assertEquals(2, df.collect().length)
      val targetDFFileNameSet: Set[String] = df.select("fileName").as[String](stringEncoder).collect().toSet
      assertEquals(targetFileName, targetDFFileNameSet)
    }

    // fetch stats only for a subset of partitions
    // lets send in all file names, but only 1 partition to test that the prefix based lookup is effective.
    val expectedFileNames = partitionToBaseFiles.get("10").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toSet[String])
    // even though fileNameSet (last arg) contains files from all partitions, since list of partitions passed in is just 1 (i.e 10), we should get matched only w/ files from
    // partition 10.
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, Some(Set("10")), Some(fileNameSet)) { df =>
      assertEquals(expectedFileNames.size(), df.collect().length)
      val targetDFFileNameSet = df.select("fileName").as[String](stringEncoder).collectAsList().stream().collect(Collectors.toSet[String])
      assertEquals(expectedFileNames, targetDFFileNameSet)
    }

    // lets redo for 2 partitions.
    val expectedFileNames1 = partitionToBaseFiles.get("9").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toList[String])
    expectedFileNames1.addAll(partitionToBaseFiles.get("11").stream().map[String](baseFile => baseFile.getName).collect(Collectors.toList[String]))
    Collections.sort(expectedFileNames1)
    columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory, Some(Set("9","11")), Some(fileNameSet)) { df =>
      assertEquals(expectedFileNames1.size, df.collect().length)
      val targetDFFileNameSet = df.select("fileName").as[String](stringEncoder).collectAsList().stream().collect(Collectors.toList[String])
      Collections.sort(targetDFFileNameSet)
      assertEquals(expectedFileNames1, targetDFFileNameSet)
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testMetadataColumnStatsIndexPartialProjection(shouldReadInMemory: Boolean): Unit = {
    val targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true",
      HoodieCommonConfig.RECONCILE_SCHEMA.key -> "true"
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    ////////////////////////////////////////////////////////////////////////
    // Case #1: Empty CSI projection
    //          Projection is requested for columns which are NOT indexed
    //          by the CSI
    ////////////////////////////////////////////////////////////////////////

    {
      // These are NOT indexed
      val requestedColumns = Seq("c4")

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { emptyTransposedColStatsDF =>
        assertEquals(0, emptyTransposedColStatsDF.collect().length)
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Case #2: Partial CSI projection
    //          Projection is requested for set of columns some of which are
    //          NOT indexed by the CSI
    ////////////////////////////////////////////////////////////////////////

    {
      // We have to include "c1", since we sort the expected outputs by this column
      val requestedColumns = Seq("c4", "c1")

      val (expectedColStatsSchema, _) = composeIndexSchema(requestedColumns.sorted, targetColumnsToIndex.toSet, sourceTableSchema)
      // Match against expected column stats table
      val expectedColStatsIndexTableDf =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        assertEquals(expectedColStatsIndexTableDf.schema, partialTransposedColStatsDF.schema)
        // NOTE: We have to drop the `fileName` column as it contains semi-random components
        //       that we can't control in this test. Nevertheless, since we manually verify composition of the
        //       ColStats Index by reading Parquet footers from individual Parquet files, this is not an issue
        assertEquals(asJson(sort(expectedColStatsIndexTableDf)), asJson(sort(partialTransposedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualColStatsTableDF)), asJson(sort(partialTransposedColStatsDF)))
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Case #3: Aligned CSI projection
    //          Projection is requested for set of columns some of which are
    //          indexed only for subset of files
    ////////////////////////////////////////////////////////////////////////

    {
      // NOTE: The update we're writing is intentionally omitting some of the columns
      //       present in an earlier source
      val missingCols = Seq("c2", "c3")
      val partialSourceTableSchema = StructType(sourceTableSchema.fields.filterNot(f => missingCols.contains(f.name)))

      val updateJSONTablePath = getClass.getClassLoader.getResource("index/colstats/partial-another-input-table-json").toString
      val updateDF = spark.read
        .schema(partialSourceTableSchema)
        .json(updateJSONTablePath)

      updateDF.repartition(4)
        .write
        .format("hudi")
        .options(opts)
        .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
        .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
        .mode(SaveMode.Append)
        .save(basePath)

      metaClient = HoodieTableMetaClient.reload(metaClient)

      val requestedColumns = sourceTableSchema.fieldNames

      val (expectedColStatsSchema, _) = composeIndexSchema(requestedColumns.sorted, targetColumnsToIndex.toSet, sourceTableSchema)
      val expectedColStatsIndexUpdatedDF =
        spark.read
          .schema(expectedColStatsSchema)
          .json(getClass.getClassLoader.getResource("index/colstats/updated-partial-column-stats-index-table.json").toString)

      // Collect Column Stats manually (reading individual Parquet files)
      val manualUpdatedColStatsTableDF =
        buildColumnStatsTableManually(basePath, requestedColumns, targetColumnsToIndex, expectedColStatsSchema)

      val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

      // Nevertheless, the last update was written with a new schema (that is a subset of the original table schema),
      // we should be able to read CSI, which will be properly padded (with nulls) after transposition
      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { transposedUpdatedColStatsDF =>
        assertEquals(expectedColStatsIndexUpdatedDF.schema, transposedUpdatedColStatsDF.schema)

        assertEquals(asJson(sort(expectedColStatsIndexUpdatedDF)), asJson(sort(transposedUpdatedColStatsDF.drop("fileName"))))
        assertEquals(asJson(sort(manualUpdatedColStatsTableDF)), asJson(sort(transposedUpdatedColStatsDF)))
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(true, false))
  def testTranslateQueryFiltersIntoColumnStatsIndexFilterExpr(shouldReadInMemory: Boolean): Unit = {
    val targetColumnsToIndex = Seq("c1", "c2", "c3")

    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> targetColumnsToIndex.mkString(",")
    )

    val opts = Map(
      "hoodie.insert.shuffle.parallelism" -> "4",
      "hoodie.upsert.shuffle.parallelism" -> "4",
      HoodieWriteConfig.TBL_NAME.key -> "hoodie_test",
      RECORDKEY_FIELD.key -> "c1",
      PRECOMBINE_FIELD.key -> "c1",
      HoodieTableConfig.POPULATE_META_FIELDS.key -> "true"
    ) ++ metadataOpts

    val sourceJSONTablePath = getClass.getClassLoader.getResource("index/colstats/input-table-json").toString

    // NOTE: Schema here is provided for validation that the input date is in the appropriate format
    val inputDF = spark.read.schema(sourceTableSchema).json(sourceJSONTablePath)

    inputDF
      .sort("c1")
      .repartition(4, new Column("c1"))
      .write
      .format("hudi")
      .options(opts)
      .option(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key, 10 * 1024)
      .option(DataSourceWriteOptions.OPERATION.key, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    metaClient = HoodieTableMetaClient.reload(metaClient)

    val metadataConfig = HoodieMetadataConfig.newBuilder()
      .fromProperties(toProperties(metadataOpts))
      .build()

    ////////////////////////////////////////////////////////////////////////
    // NOTE: Partial CSI projection
    //          Projection is requested for set of columns some of which are
    //          NOT indexed by the CSI
    ////////////////////////////////////////////////////////////////////////

    // We have to include "c1", since we sort the expected outputs by this column
    val requestedColumns = Seq("c4", "c1")
    val columnStatsIndex = new ColumnStatsIndexSupport(spark, sourceTableSchema, metadataConfig, metaClient)

    ////////////////////////////////////////////////////////////////////////
    // Query filter #1: c1 > 1 and c4 > 'c4 filed value'
    //                  We should filter only for c1
    ////////////////////////////////////////////////////////////////////////
    {
      val andConditionFilters = Seq(
        GreaterThan(AttributeReference("c1", IntegerType, nullable = true)(), Literal(1)),
        GreaterThan(AttributeReference("c4", StringType, nullable = true)(), Literal("c4 filed value"))
      )

      val expectedAndConditionIndexedFilter = And(
        GreaterThan(UnresolvedAttribute("c1_maxValue"), Literal(1)),
        Literal(true)
      )

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        val andConditionActualFilter = andConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_,
          indexedCols = targetColumnsToIndex))
          .reduce(And)
        assertEquals(expectedAndConditionIndexedFilter, andConditionActualFilter)
      }
    }

    ////////////////////////////////////////////////////////////////////////
    // Query filter #2: c1 > 1 or c4 > 'c4 filed value'
    //                  Since c4 is not indexed, we cannot filter any data
    ////////////////////////////////////////////////////////////////////////
    {
      val orConditionFilters = Seq(
        Or(GreaterThan(AttributeReference("c1", IntegerType, nullable = true)(), Literal(1)),
          GreaterThan(AttributeReference("c4", StringType, nullable = true)(), Literal("c4 filed value")))
      )

      columnStatsIndex.loadTransposed(requestedColumns, shouldReadInMemory) { partialTransposedColStatsDF =>
        val orConditionActualFilter = orConditionFilters.map(translateIntoColumnStatsIndexFilterExpr(_,
          indexedCols = targetColumnsToIndex))
          .reduce(And)
        assertEquals(Literal("true").toString(), orConditionActualFilter.toString())
      }
    }
  }

  @Test
  def testParquetMetadataRangeExtraction(): Unit = {
    val df = generateRandomDataFrame(spark)

    val pathStr = tempDir.resolve("min-max").toAbsolutePath.toString

    df.write.format("parquet")
      .mode(SaveMode.Overwrite)
      .save(pathStr)

    val utils = new ParquetUtils

    val path = new Path(pathStr)
    val storage = HoodieTestUtils.getStorage(new StoragePath(pathStr))
    val fs = path.getFileSystem(storage.getConf.unwrapAs(classOf[Configuration]))

    val parquetFilePath = new StoragePath(
      fs.listStatus(path).filter(fs => fs.getPath.getName.endsWith(".parquet")).toSeq.head.getPath.toUri)

    val ranges = utils.readColumnStatsFromMetadata(storage, parquetFilePath, Seq("c1", "c2", "c3a", "c3b", "c3c", "c4", "c5", "c6", "c7", "c8").asJava)

    ranges.asScala.foreach(r => {
      // NOTE: Unfortunately Parquet can't compute statistics for Timestamp column, hence we
      //       skip it in our assertions
      if (r.getColumnName.equals("c4")) {
        // scalastyle:off return
        return
        // scalastyle:on return
      }

      val min = r.getMinValue
      val max = r.getMaxValue

      assertNotNull(min)
      assertNotNull(max)
      assertTrue(r.getMinValue.asInstanceOf[Comparable[Object]].compareTo(r.getMaxValue.asInstanceOf[Object]) <= 0)
    })
  }
}
