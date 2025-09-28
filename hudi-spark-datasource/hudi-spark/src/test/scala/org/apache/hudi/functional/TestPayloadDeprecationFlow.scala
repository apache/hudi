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
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, ORDERING_FIELDS, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieMetadataConfig, TypedProperties}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, DefaultHoodieRecordPayload, EventTimeAvroPayload, HoodieRecordMerger, HoodieTableType, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY
import org.apache.hudi.common.model.debezium.{DebeziumConstants, MySqlDebeziumAvroPayload, PostgresDebeziumAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.common.table.HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX
import org.apache.hudi.config.{HoodieArchivalConfig, HoodieCleanConfig, HoodieClusteringConfig, HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.scalatest.Assertions.assertThrows

import scala.jdk.CollectionConverters._

class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  /**
   * Test if the payload based read have the same behavior for different table versions.
   */
  @ParameterizedTest
  @MethodSource(Array("providePayloadClassTestCases"))
  def testMergerBuiltinPayloadUpgradePath(tableType: String,
                                          payloadClazz: String,
                                          useOpAsDeleteStr: String,
                                          expectedConfigs: Map[String, String],
                                          expectedDowngradeConfigs: Map[String, String]): Unit = {
    val useOpAsDelete = useOpAsDeleteStr.equals("true")
    val deleteOpts: Map[String, String] = if (useOpAsDelete) {
      Map(DefaultHoodieRecordPayload.DELETE_KEY -> "Op", DefaultHoodieRecordPayload.DELETE_MARKER -> "D")
    } else {
      Map.empty
    }
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz,
      HoodieMetadataConfig.ENABLE.key() -> "false") ++ deleteOpts

    val columns = Seq("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq",
      DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
    // 1. Add an insert.
    val data = Seq(
      (10, 1L, "rider-A", "driver-A", 19.10, "i", "10.1", 10, 1, "i"),
      (10, 2L, "rider-B", "driver-B", 27.70, "i", "10.1", 10, 1, "i"),
      (10, 3L, "rider-C", "driver-C", 33.90, "i", "10.1", 10, 1, "i"),
      (10, 4L, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),
      (10, 5L, "rider-E", "driver-E", 17.85, "i", "10.1", 10, 1, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    val originalOrderingFields = if (payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      "_event_seq"
    } else if (payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      "_event_lsn"
    } else {
      "ts"
    }
    val expectedOrderingFields = if (payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      "_event_bin_file,_event_pos"
    } else if (payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      "_event_lsn"
    } else {
      originalOrderingFields
    }
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "_event_lsn").
      option(HoodieTableConfig.ORDERING_FIELDS.key(), originalOrderingFields).
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL).
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      option("hoodie.parquet.max.file.size", "2048").
      option("hoodie.parquet.small.file.limit", "1024").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)

    // Verify table was created successfully
    var metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    var tableConfig = metaClient.getTableConfig
    // Verify table version is 8
    assertEquals(8, tableConfig.getTableVersion.versionCode())
    assertTrue(metaClient.getActiveTimeline.firstInstant().isPresent)
    // 2. Add an update.
    val firstUpdateData = Seq(
      (11, 1L, "rider-X", "driver-X", 19.10, "i", "11.1", 11, 1, "i"),
      (12, 1L, "rider-X", "driver-X", 20.10, "D", "12.1", 12, 1, "d"),
      (11, 2L, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      option(HoodieTableConfig.ORDERING_FIELDS.key(), originalOrderingFields).
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())
    val firstUpdateInstantTime = metaClient.getActiveTimeline.getInstants.get(1).requestedTime()

    // 2.5. Add mixed ordering test data to validate proper ordering handling
    // This tests that updates/deletes with lower ordering values are ignored
    // while higher ordering values are applied
    val mixedOrderingData = Seq(
      // Update rider-C with LOWER ordering - should be IGNORED (rider-C has ts=10 originally)
      (8, 3L, "rider-CC", "driver-CC", 30.00, "u", "8.1", 8, 1, "u"),
      // Update rider-C with HIGHER ordering - should be APPLIED
      (11, 3L, "rider-CC", "driver-CC", 35.00, "u", "15.1", 15, 1, "u"),
      // Delete rider-E with LOWER ordering - should be IGNORED (rider-E has ts=10 originally)
      (9, 5L, "rider-EE", "driver-EE", 17.85, "D", "9.1", 9, 1, "d"))
    val mixedOrderingUpdate = spark.createDataFrame(mixedOrderingData).toDF(columns: _*)
    mixedOrderingUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      option(HoodieTableConfig.ORDERING_FIELDS.key(), originalOrderingFields).
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version is still 8 after mixed ordering batch
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())

    // 3. Add an update. This is expected to trigger the upgrade
    val compactionEnabled = if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) "true" else "false"
    val secondUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"),
      // For rider-DD we purposefully deviate and set the _event_seq to be less than the _event_bin_file and _event_pos
      // so that the test will fail if _event_seq is still used for ordering
      (9, 4L, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
      (12, 5L, "rider-EE", "driver-EE", 17.85, "i", "12.1", 12, 1, "i"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), compactionEnabled).
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version as 9.
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    assertEquals(payloadClazz, metaClient.getTableConfig.getLegacyPayloadClass)
    assertEquals(isCDCPayload(payloadClazz) || useOpAsDelete,
      metaClient.getTableConfig.getProps.containsKey(RECORD_MERGE_PROPERTY_PREFIX + DELETE_KEY))
    assertEquals(expectedOrderingFields, metaClient.getTableConfig.getOrderingFieldsStr.orElse(""))

    // 4. Add a trivial update to trigger payload class mismatch.
    val thirdUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"))
    val thirdUpdate = spark.createDataFrame(thirdUpdateData).toDF(columns: _*)
    if (!payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      assertThrows[HoodieException] {
        thirdUpdate.write.format("hudi").
          option(OPERATION.key(), "upsert").
          option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
          option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
          option(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(),
            classOf[MySqlDebeziumAvroPayload].getName).
          mode(SaveMode.Append).
          save(basePath)
      }
    }

    // 5. Add a delete.
    val fourthUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"),
      (12, 5L, "rider-EE", "driver-EE", 17.85, "i", "12.1", 12, 1, "i"))
    val fourthUpdate = spark.createDataFrame(fourthUpdateData).toDF(columns: _*)
    fourthUpdate.write.format("hudi").
      option(OPERATION.key(), "delete").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)

    // 6. Add INSERT operation.
    val insertData = Seq(
      (13, 6L, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
      (13, 7L, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
    val insertDataFrame = spark.createDataFrame(insertData).toDF(columns: _*)
    insertDataFrame.write.format("hudi").
      option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)

    // Final validation of table management operations after all writes
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    validateTableManagementOps(metaClient, tableType,
      expectCompaction = tableType.equals(HoodieTableType.MERGE_ON_READ.name()),
      expectClustering = true,   // Enable clustering validation with lowered thresholds
      expectCleaning = false,     // Enable cleaning validation with lowered thresholds
      expectArchival = false)     // Enable archival validation with lowered thresholds

    // 7. Validate.
    // Validate table configs.
    tableConfig = metaClient.getTableConfig
    expectedConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
      } else {
        assertFalse(tableConfig.contains(key), s"Config $key should not be present")
      }
    }
    // Validate snapshot query.
    val df = spark.read.format("hudi").load(basePath)
    val finalDf = df.select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
      .sort("_event_lsn")
    val expectedData = getExpectedResultForSnapshotQuery(payloadClazz, useOpAsDelete)
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("_event_lsn")
    assertTrue(expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
    // Validate time travel query. Reading from v8 log file will not work for MySQLDebeziumAvroPayload due to the change from a single ordering field, to two ordering fields.
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE.name()) || !payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      val timeTravelDf = spark.read.format("hudi")
        .option("as.of.instant", firstUpdateInstantTime).load(basePath)
        .select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
        .sort("_event_lsn")
      val expectedTimeTravelData = getExpectedResultForTimeTravelQuery(payloadClazz, useOpAsDelete)
      val expectedTimeTravelDf = spark.createDataFrame(
        spark.sparkContext.parallelize(expectedTimeTravelData)).toDF(columns: _*).sort("_event_lsn")
      assertTrue(
        expectedTimeTravelDf.except(timeTravelDf).isEmpty
          && timeTravelDf.except(expectedTimeTravelDf).isEmpty)
    }

    // 8. Downgrade from v9 to v8
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .withSchema(spark.read.format("hudi").load(basePath).schema.json)
      .build()

    new UpgradeDowngrade(metaClient, writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    // Reload metaClient to get updated table config
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()

    // Validate table version is 8
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())

    // Validate downgrade configs
    val downgradedTableConfig = metaClient.getTableConfig
    expectedDowngradeConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, downgradedTableConfig.getString(key), s"Config $key should be $expectedValue after downgrade")
      } else {
        assertFalse(downgradedTableConfig.contains(key), s"Config $key should not be present after downgrade")
      }
    }

    // Validate data consistency after downgrade
    val downgradeDf = spark.read.format("hudi").load(basePath)
    val downgradeFinalDf = downgradeDf.select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME).sort("_event_lsn")
    assertTrue(expectedDf.except(downgradeFinalDf).isEmpty && downgradeFinalDf.except(expectedDf).isEmpty,
      "Data should remain consistent after downgrade")
  }

  @ParameterizedTest
  @MethodSource(Array("providePayloadClassTestCases"))
  def testMergerBuiltinPayloadFromTableCreationPath(tableType: String,
                                                    payloadClazz: String,
                                                    useOpAsDeleteStr: String,
                                                    expectedConfigs: Map[String, String],
                                                    expectedDowngradeConfigs: Map[String, String]): Unit = {
    val useOpAsDelete = useOpAsDeleteStr.equals("true")
    val deleteOpts: Map[String, String] = if (useOpAsDelete) {
      Map(DefaultHoodieRecordPayload.DELETE_KEY -> "Op", DefaultHoodieRecordPayload.DELETE_MARKER -> "D")
    } else {
      Map.empty
    }
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz,
      HoodieMetadataConfig.ENABLE.key() -> "false") ++ deleteOpts
    val columns = Seq("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq",
      DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
    // 1. Add an insert.
    val data = Seq(
      (10, 1L, "rider-A", "driver-A", 19.10, "i", "10.1", 10, 1, "i"),
      (10, 2L, "rider-B", "driver-B", 27.70, "i", "10.1", 10, 1, "i"),
      (10, 3L, "rider-C", "driver-C", 33.90, "i", "10.1", 10, 1, "i"),
      (10, 4L, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),
      (10, 5L, "rider-E", "driver-E", 17.85, "i", "10.1", 10, 1, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    val originalOrderingFields = if (payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      "_event_seq"
    } else {
      "ts"
    }
    val expectedOrderingFields = if (payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      "_event_bin_file,_event_pos"
    } else if (payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      "_event_lsn"
    } else {
      originalOrderingFields
    }
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "_event_lsn").
      option(ORDERING_FIELDS.key(), originalOrderingFields).
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(OPERATION.key(), DataSourceWriteOptions.BULK_INSERT_OPERATION_OPT_VAL).
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option("hoodie.parquet.max.file.size", "2048").
      option("hoodie.parquet.small.file.limit", "1024").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Verify table was created successfully
    var metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    var tableConfig = metaClient.getTableConfig
    // Verify table version is 9
    assertEquals(9, tableConfig.getTableVersion.versionCode())
    assertTrue(metaClient.getActiveTimeline.firstInstant().isPresent)
    // Verify table properties
    expectedConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
      } else {
        assertFalse(tableConfig.contains(key), s"Config $key should not be present")
      }
    }

    // 2. Add an update.
    val firstUpdateData = Seq(
      (11, 1L, "rider-X", "driver-X", 19.10, "i", "11.1", 11, 1, "i"),
      (12, 1L, "rider-X", "driver-X", 20.10, "D", "12.1", 12, 1, "d"),
      (11, 2L, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    // validate ordering fields
    assertEquals(expectedOrderingFields, metaClient.getTableConfig.getOrderingFieldsStr.orElse(""))
    val firstUpdateInstantTime = metaClient.getActiveTimeline.getInstants.get(1).requestedTime()

    // 2.5. Add mixed ordering test data to validate proper ordering handling
    // This tests that updates/deletes with lower ordering values are ignored
    // while higher ordering values are applied
    val mixedOrderingData = Seq(
      // Update rider-C with LOWER ordering - should be IGNORED (rider-C has ts=10 originally)
      (8, 3L, "rider-CC", "driver-CC", 30.00, "u", "8.1", 8, 1, "u"),
      // Update rider-C with HIGHER ordering - should be APPLIED
      (11, 3L, "rider-CC", "driver-CC", 35.00, "u", "15.1", 15, 1, "u"),
      // Delete rider-E with LOWER ordering - should be IGNORED (rider-E has ts=10 originally)
      (9, 5L, "rider-EE", "driver-EE", 17.85, "D", "9.1", 9, 1, "d"))
    val mixedOrderingUpdate = spark.createDataFrame(mixedOrderingData).toDF(columns: _*)
    mixedOrderingUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      options(opts).
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version is still 9 after mixed ordering batch
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())

    // 3. Add an update. This is expected to trigger the upgrade
    val compactionEnabled = if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      "true"
    } else {
      "false"
    }
    val secondUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"),
      // For rider-DD we purposefully deviate and set the _event_seq to be less than the _event_bin_file and _event_pos
      // so that the test will fail if _event_seq is still used for ordering
      (9, 4L, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
      (12, 5L, "rider-EE", "driver-EE", 17.85, "i", "12.1", 12, 1, "i"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), compactionEnabled).
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version as 9.
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    assertEquals(payloadClazz, metaClient.getTableConfig.getLegacyPayloadClass)

    // 4. Add a trivial update to trigger payload class mismatch.
    val thirdUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"))
    val thirdUpdate = spark.createDataFrame(thirdUpdateData).toDF(columns: _*)
    if (!payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      assertThrows[HoodieException] {
        thirdUpdate.write.format("hudi").
          option(OPERATION.key(), "upsert").
          option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
          option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
          option(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(),
            classOf[MySqlDebeziumAvroPayload].getName).
          mode(SaveMode.Append).
          save(basePath)
      }
    }

    // 5. Add a delete.
    val fourthUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1", 12, 1, "i"),
      (12, 5L, "rider-EE", "driver-EE", 17.85, "i", "12.1", 12, 1, "i"))
    val fourthUpdate = spark.createDataFrame(fourthUpdateData).toDF(columns: _*)
    fourthUpdate.write.format("hudi").
      option(OPERATION.key(), "delete").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)

    // 6. Add INSERT operation.
    val insertData = Seq(
      (13, 6L, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
      (13, 7L, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
    val insertDataFrame = spark.createDataFrame(insertData).toDF(columns: _*)
    insertDataFrame.write.format("hudi").
      option(OPERATION.key(), DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL).
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(), "3").
      option(HoodieCleanConfig.AUTO_CLEAN.key(), "false").
      option(HoodieArchivalConfig.AUTO_ARCHIVE.key(), "true").
      option(HoodieArchivalConfig.COMMITS_ARCHIVAL_BATCH_SIZE.key(), "1").
      option(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), "2").
      option(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), "3").
      option(HoodieClusteringConfig.INLINE_CLUSTERING.key(), "true").
      option(HoodieClusteringConfig.INLINE_CLUSTERING_MAX_COMMITS.key(), "2").
      option(HoodieClusteringConfig.PLAN_STRATEGY_SMALL_FILE_LIMIT.key(), "512000").
      option(HoodieClusteringConfig.PLAN_STRATEGY_TARGET_FILE_MAX_BYTES.key(), "512000").
      mode(SaveMode.Append).
      save(basePath)

    // Final validation of table management operations after all writes
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    validateTableManagementOps(metaClient, tableType,
      expectCompaction = tableType.equals(HoodieTableType.MERGE_ON_READ.name()),
      expectClustering = true,   // Enable clustering validation with lowered thresholds
      expectCleaning = false,     // Enable cleaning validation with lowered thresholds
      expectArchival = false)     // Enable archival validation with lowered thresholds

    // 7. Validate.
    // Validate table configs again.
    tableConfig = metaClient.getTableConfig
    expectedConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
      } else {
        assertFalse(tableConfig.contains(key), s"Config $key should not be present")
      }
    }
    // Validate snapshot query.
    val df = spark.read.format("hudi").load(basePath)
    val finalDf = df.select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
      .sort("_event_lsn")
    val expectedData = getExpectedResultForSnapshotQuery(payloadClazz, useOpAsDelete)
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("_event_lsn")
    assertTrue(expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
    // Validate time travel query.
    val timeTravelDf = spark.read.format("hudi")
      .option("as.of.instant", firstUpdateInstantTime).load(basePath)
      .select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME)
      .sort("_event_lsn")
    val expectedTimeTravelData = getExpectedResultForTimeTravelQuery(payloadClazz, useOpAsDelete)
    val expectedTimeTravelDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedTimeTravelData)).toDF(columns: _*).sort("_event_lsn")
    assertTrue(
      expectedTimeTravelDf.except(timeTravelDf).isEmpty
        && timeTravelDf.except(expectedTimeTravelDf).isEmpty)

    // 8. Downgrade from v9 to v8
    val writeConfig = HoodieWriteConfig.newBuilder()
      .withPath(basePath)
      .withSchema(spark.read.format("hudi").load(basePath).schema.json)
      .build()

    new UpgradeDowngrade(metaClient, writeConfig, context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    // Reload metaClient to get updated table config
    metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()

    // Validate table version is 8
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())

    // Validate downgrade configs
    val downgradedTableConfig = metaClient.getTableConfig
    expectedDowngradeConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, downgradedTableConfig.getString(key), s"Config $key should be $expectedValue after downgrade")
      } else {
        assertFalse(downgradedTableConfig.contains(key), s"Config $key should not be present after downgrade")
      }
    }

    // Validate data consistency after downgrade
    val downgradeDf = spark.read.format("hudi").load(basePath)
    val downgradeFinalDf = downgradeDf.select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME, DebeziumConstants.FLATTENED_OP_COL_NAME).sort("_event_lsn")
    assertTrue(expectedDf.except(downgradeFinalDf).isEmpty && downgradeFinalDf.except(expectedDf).isEmpty,
      "Data should remain consistent after downgrade")
  }

  def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath())
      .build()
  }

  def getExpectedResultForSnapshotQuery(payloadClazz: String, usesDeleteMarker: Boolean): Seq[(Int, Long, String, String, Double, String, String, Int, Int, String)] = {
    if (!isCDCPayload(payloadClazz) && !usesDeleteMarker) {
      if (payloadClazz.equals(classOf[PartialUpdateAvroPayload].getName)
        || payloadClazz.equals(classOf[EventTimeAvroPayload].getName)
        || payloadClazz.equals(classOf[DefaultHoodieRecordPayload].getName))
      {
        // Expected results after all operations with _event_lsn collisions:
        // - rider-X (_event_lsn=1): deleted with higher ordering (ts=12)
        // - rider-Y (_event_lsn=2): updated with higher ordering (ts=11)
        // - _event_lsn=3: DELETED by delete operation (was rider-CC with ts=12)
        // - _event_lsn=4: rider-D stays with original data (rider-DD ts=9 < rider-D ts=10)
        // - _event_lsn=5: DELETED by delete operation (was rider-EE with ts=12)
        Seq(
          (12, 1, "rider-X", "driver-X", 20.10, "D", "12.1", 12, 1, "d"),
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      } else {
        // For other payload types (OverwriteWithLatestAvroPayload, OverwriteNonDefaultsWithLatestAvroPayload)
        // These use COMMIT_TIME_ORDERING, so latest write wins regardless of ts value
        // _event_lsn=3: rider-CC overwrites (latest commit), then deleted
        // _event_lsn=4: rider-DD overwrites (latest commit)
        // _event_lsn=5: rider-EE overwrites (latest commit), then deleted
        Seq(
          (12, 1, "rider-X", "driver-X", 20.10, "D", "12.1", 12, 1, "d"),
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      }
    } else {
      // For CDC payloads or when delete markers are used
      if (payloadClazz.equals(classOf[DefaultHoodieRecordPayload].getName)) {
        // Delete markers remove records completely
        // Note: rider-D keeps original values because rider-DD update (ts=9) was rejected (9 < 10)
        // _event_lsn=1 (rider-X) and _event_lsn=5 (rider-EE) are deleted by delete markers
        Seq(
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      } else if (payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
        // AWSDmsAvroPayload uses COMMIT_TIME_ORDERING - latest commit wins regardless of ts value
        // Mixed batch: rider-CC update applies (latest commit)
        // Second update: rider-DD applies (latest commit wins over rider-D)
        // Final: _event_lsn=3 and _event_lsn=5 deleted, _event_lsn=4 has rider-DD
        Seq(
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      } else if (payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
        // PostgresDebeziumAvroPayload uses EVENT_TIME_ORDERING with _event_lsn field
        // But second update applies rider-DD due to later commit time (behaves like commit-time ordering)
        Seq(
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      } else {
        // For MySqlDebeziumAvroPayload
        // Uses EVENT_TIME_ORDERING with _event_seq initially, then _event_bin_file,_event_pos
        // But second update applies rider-DD due to later commit time (behaves like commit-time ordering)
        Seq(
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
          (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1", 12, 1, "i"),
          (13, 6, "rider-G", "driver-G", 25.50, "i", "13.1", 13, 1, "i"),
          (13, 7, "rider-H", "driver-H", 30.25, "i", "13.1", 13, 1, "i"))
      }
    }
  }

  def getExpectedResultForTimeTravelQuery(payloadClazz: String, usesDeleteMarker: Boolean):
  Seq[(Int, Long, String, String, Double, String, String, Int, Int, String)] = {
    // Time travel query shows state after first update but BEFORE mixed ordering batch
    // So rider-C, rider-D, rider-E should still have their original values
    if (!isCDCPayload(payloadClazz) && !usesDeleteMarker) {
      Seq(
        (12, 1, "rider-X", "driver-X", 20.10, "D", "12.1", 12, 1, "d"),
        (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
        (10, 3, "rider-C", "driver-C", 33.90, "i", "10.1", 10, 1, "i"), // Original rider-C before mixed ordering
        (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),   // Original rider-D before mixed ordering
        (10, 5, "rider-E", "driver-E", 17.85, "i", "10.1", 10, 1, "i"))   // Original rider-E before mixed ordering
    } else {
      // For CDC payloads or when delete markers are used
      Seq(
        (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1", 11, 1, "u"),
        (10, 3, "rider-C", "driver-C", 33.90, "i", "10.1", 10, 1, "i"), // Original rider-C before mixed ordering
        (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1", 10, 1, "i"),   // Original rider-D before mixed ordering
        (10, 5, "rider-E", "driver-E", 17.85, "i", "10.1", 10, 1, "i"))   // Original rider-E before mixed ordering
    }
  }

  private def isCDCPayload(payloadClazz: String) = {
    payloadClazz.equals(classOf[AWSDmsAvroPayload].getName) || payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName) || payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)
  }

  /**
   * Helper method to validate that compaction occurred for MOR tables
   */
  def validateCompaction(metaClient: HoodieTableMetaClient, tableType: String): Unit = {
    if (tableType.equals(HoodieTableType.MERGE_ON_READ.name())) {
      metaClient.reloadActiveTimeline()
      val compactionInstants = metaClient.getActiveTimeline
        .getCommitsAndCompactionTimeline
        .getInstants
        .asScala
        .filter(_.getAction.equals("commit"))
      assertTrue(compactionInstants.nonEmpty,
        s"Compaction should have occurred for MOR table but found no commit instants")
    }
  }

  /**
   * Helper method to validate that clustering occurred
   */
  def validateClustering(metaClient: HoodieTableMetaClient): Unit = {
    metaClient.reloadActiveTimeline()
    val clusteringInstants = metaClient.getActiveTimeline
      .getInstants
      .asScala
      .filter(_.getAction.equals("replacecommit")) // check if this is correct
    assertTrue(clusteringInstants.nonEmpty,
      s"Clustering should have occurred but found no replacecommit instants")
  }

  /**
   * Helper method to validate that cleaning occurred
   */
  def validateCleaning(metaClient: HoodieTableMetaClient): Unit = {
    metaClient.reloadActiveTimeline()
    val cleanInstants = metaClient.getActiveTimeline
      .getCleanerTimeline
      .getInstants
      .asScala
    assertTrue(cleanInstants.nonEmpty,
      s"Cleaning should have occurred but found no clean instants")
  }

  /**
   * Helper method to validate that archival occurred
   */
  def validateArchival(metaClient: HoodieTableMetaClient): Unit = {
    val archivedTimeline = metaClient.getArchivedTimeline
    val archivedInstants = archivedTimeline.getInstants.asScala
    assertTrue(archivedInstants.nonEmpty,
      s"Archival should have occurred but found no archived instants")
  }

  /**
   * Helper method to validate all table management operations
   */
  def validateTableManagementOps(metaClient: HoodieTableMetaClient,
                                  tableType: String,
                                  expectCompaction: Boolean = true,
                                  expectClustering: Boolean = true,
                                  expectCleaning: Boolean = true,
                                  expectArchival: Boolean = true): Unit = {
    metaClient.reloadActiveTimeline()

    // Validate compaction for MOR tables
    if (expectCompaction) {
      validateCompaction(metaClient, tableType)
    }

    // Validate clustering
    if (expectClustering) {
      validateClustering(metaClient)
    }

    // Validate cleaning
    if (expectCleaning) {
      validateCleaning(metaClient)
    }

    // Validate archival
    if (expectArchival) {
      validateArchival(metaClient)
    }
  }
}

object TestPayloadDeprecationFlow {
  def providePayloadClassTestCases(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[DefaultHoodieRecordPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[DefaultHoodieRecordPayload].getName,
        "true",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> "D")
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[OverwriteWithLatestAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
        ),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[PartialUpdateAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[PostgresDebeziumAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "FILL_UNAVAILABLE",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE
            -> "__debezium_unavailable_value"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PRECOMBINE_FIELD.key() -> "_event_lsn",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null,
          HoodieTableConfig.ORDERING_FIELDS.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[MySqlDebeziumAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[MySqlDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.ORDERING_FIELDS.key() -> (DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME)),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[MySqlDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PRECOMBINE_FIELD.key() -> "_event_seq",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.ORDERING_FIELDS.key() -> null)),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[AWSDmsAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> "D"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[EventTimeAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[EventTimeAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID
        ),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[EventTimeAvroPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "COPY_ON_WRITE",
        classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"
        ),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[DefaultHoodieRecordPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[DefaultHoodieRecordPayload].getName,
        "true",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> "D")
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[OverwriteWithLatestAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[PartialUpdateAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[PostgresDebeziumAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "FILL_UNAVAILABLE",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE
            -> "__debezium_unavailable_value"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PRECOMBINE_FIELD.key() -> "_event_lsn",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null,
          HoodieTableConfig.ORDERING_FIELDS.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_UNAVAILABLE_VALUE -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[MySqlDebeziumAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[MySqlDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.ORDERING_FIELDS.key() -> (DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME)),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[MySqlDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PRECOMBINE_FIELD.key() -> "_event_seq",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.ORDERING_FIELDS.key() -> null)),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[AWSDmsAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> "D"),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> null,
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[EventTimeAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[EventTimeAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID
        ),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[EventTimeAvroPayload].getName,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
        "false",
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"
        ),
        Map(
          HoodieTableConfig.PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "CUSTOM",
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> null,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> null)
      )
    )
  }
}
