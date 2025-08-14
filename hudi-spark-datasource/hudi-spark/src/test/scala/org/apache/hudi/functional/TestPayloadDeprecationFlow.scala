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
import org.apache.hudi.DataSourceWriteOptions.{OPERATION, PRECOMBINE_FIELD, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, DefaultHoodieRecordPayload, EventTimeAvroPayload, HoodieRecordMerger, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.model.debezium.{MySqlDebeziumAvroPayload, PostgresDebeziumAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
import org.scalatest.Assertions.assertThrows

class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  @ParameterizedTest
  @MethodSource(Array("providePayloadClassTestCases"))
  def testMergerBuiltinPayload(tableType: String,
                               payloadClazz: String,
                               expectedConfigs: Map[String, String]): Unit = {
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz)
    val columns = Seq("ts", "key", "rider", "driver", "fare", "Op")
    // 1. Add an insert.
    val data = Seq(
      (10, "1", "rider-A", "driver-A", 19.10, "i"),
      (10, "2", "rider-B", "driver-B", 27.70, "i"),
      (10, "3", "rider-C", "driver-C", 33.90, "i"),
      (10, "4", "rider-D", "driver-D", 34.15, "i"),
      (10, "5", "rider-E", "driver-E", 17.85, "i"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(PRECOMBINE_FIELD.key(), "ts").
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Verify table was created successfully
    val metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    val tableConfig = metaClient.getTableConfig
    // Verify table version is 9
    assertEquals(9, tableConfig.getTableVersion.versionCode())
    // Verify expected configs are set correctly
    expectedConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
      } else {
        assertFalse(tableConfig.contains(key), s"Config $key should not be present")
      }
    }
    // 2. Add an update.
    val firstUpdateData = Seq(
      (11, "1", "rider-X", "driver-X", 19.10, "D"),
      (11, "2", "rider-Y", "driver-Y", 27.70, "u"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      mode(SaveMode.Append).
      save(basePath)
    // 3. Add an update.
    val secondUpdateData = Seq(
      (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
      (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
      (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
      mode(SaveMode.Append).
      save(basePath)
    // 4. Add a trivial update to trigger payload class mismatch.
    val thirdUpdateData = Seq(
      (12, "3", "rider-CC", "driver-CC", 33.90, "i"))
    val thirdUpdate = spark.createDataFrame(thirdUpdateData).toDF(columns: _*)
    if (!payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      assertThrows[HoodieException] {
        thirdUpdate.write.format("hudi").
          option(OPERATION.key(), "upsert").
          option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
          option(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1").
          option(HoodieTableConfig.PAYLOAD_CLASS_NAME.key(),
            classOf[MySqlDebeziumAvroPayload].getName). // Position is important.
          mode(SaveMode.Append).
          save(basePath)
      }
    }
    // 5. Validate.
    val df = spark.read.format("hudi").load(basePath)
    val finalDf = df.select("ts", "key", "rider", "driver", "fare", "Op").sort("key")

    val expectedData = if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      if (HoodieTableConfig.EVENT_TIME_ORDERING_PAYLOADS.contains(payloadClazz)) {
        Seq(
          (11, "1", "rider-X", "driver-X", 19.10, "D"),
          (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
          (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
          (10, "4", "rider-D", "driver-D", 34.15, "i"),
          (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
      } else {
        Seq(
          (11, "1", "rider-X", "driver-X", 19.10, "D"),
          (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
          (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
          (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
          (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
      }
    } else {
      Seq(
        (11, "2", "rider-Y", "driver-Y", 27.70, "u"),
        (12, "3", "rider-CC", "driver-CC", 33.90, "i"),
        (9, "4", "rider-DD", "driver-DD", 34.15, "i"),
        (12, "5", "rider-EE", "driver-EE", 17.85, "i"))
    }
    val expectedDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("key")
    assertTrue(
      expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
  }
}

// TODO: Add COPY_ON_WRITE table type tests when write path is updated accordingly.
object TestPayloadDeprecationFlow {
  def providePayloadClassTestCases(): java.util.List[Arguments] = {
    java.util.Arrays.asList(
      Arguments.of(
        "MERGE_ON_READ",
        classOf[DefaultHoodieRecordPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[DefaultHoodieRecordPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID)),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[OverwriteWithLatestAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID
        )
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[PartialUpdateAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PartialUpdateAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[PostgresDebeziumAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[PostgresDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID),
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_MARKERS",
          HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX
            + HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER -> "__debezium_unavailable_value"),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[AWSDmsAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
          HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX + DELETE_KEY -> "Op",
          HoodieTableConfig.MERGE_CUSTOM_PROPERTY_PREFIX + DELETE_MARKER -> "D"),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[MySqlDebeziumAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[MySqlDebeziumAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID)),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[EventTimeAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "EVENT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[EventTimeAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID
        )
      ),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
          HoodieTableConfig.PARTIAL_UPDATE_MODE.key() -> "IGNORE_DEFAULTS"
        )
      )
    )
  }
}
