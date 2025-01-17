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
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.functional.ColumnStatIndexTestBase.{ColumnStatsTestCase, ColumnStatsTestParams}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource

class TestPartitionStatsPruning extends ColumnStatIndexTestBase {

  val DEFAULT_COLUMNS_TO_INDEX = Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
    HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c7","c8")

  @ParameterizedTest
  @MethodSource(Array("testMetadataColumnStatsIndexParamsInMemory"))
  def testMetadataPST(testCase: ColumnStatsTestCase): Unit = {

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
      shouldValidatePartitionSats = true))
  }

  @ParameterizedTest
  //@MethodSource(Array("testMetadataColumnStatsIndexParams"))
  //@Test
  def testMetadataColumnStatsIndex(): Unit = {
    //testCase: ColumnStatsTestCase
    val testCase = ColumnStatsTestCase(HoodieTableType.MERGE_ON_READ, true)
    val metadataOpts = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c4,c5,c6,c8"
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
      expectedColStatsSourcePath = "index/colstats/column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      shouldValidateColStats = false,
      shouldValidatePartitionSats = true))

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts, commonOpts,
      dataSourcePath = "index/colstats/another-input-table-json",
      expectedColStatsSourcePath = "index/colstats/updated-column-stats-index-table.json",
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionSats = true))

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
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionSats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c4","c5","c6","c8"))

    // update list of columns to explicit list of cols.
    /*val metadataOpts1 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "true",
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
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionSats = true))

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

    expectedColStatsSourcePath = if (testCase.tableType == HoodieTableType.COPY_ON_WRITE) {
      "index/colstats/cow-updated4-column-stats-index-table.json"
    } else {
      "index/colstats/mor-updated4-column-stats-index-table.json"
    }

    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts2, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidatePartitionSats = true))

    validateColumnsToIndex(metaClient, Seq(HoodieRecord.COMMIT_TIME_METADATA_FIELD, HoodieRecord.RECORD_KEY_METADATA_FIELD,
      HoodieRecord.PARTITION_PATH_METADATA_FIELD, "c1","c2","c3","c5","c7","c8"))
    */

    // update list of columns to explicit list of cols.
    val metadataOpts3 = Map(
      HoodieMetadataConfig.ENABLE.key -> "true",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "false",
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_PARTITION_STATS.key -> "false",
      HoodieMetadataConfig.COLUMN_STATS_INDEX_FOR_COLUMNS.key -> "c1,c2,c3,c5,c7" // ignore c4,c5,c8.
    )

    // disable col stats
    doWriteAndValidateColumnStats(ColumnStatsTestParams(testCase, metadataOpts3, commonOpts,
      dataSourcePath = "index/colstats/update6-input-table-json",
      expectedColStatsSourcePath = expectedColStatsSourcePath,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      shouldValidateColStats = false,
      shouldValidateManually = false))

    metaClient = HoodieTableMetaClient.reload(metaClient)
    validateNonExistantColumnsToIndexDefn(metaClient)
  }

}
