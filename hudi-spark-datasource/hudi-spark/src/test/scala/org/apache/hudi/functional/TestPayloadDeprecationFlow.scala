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
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.model.{AWSDmsAvroPayload, DefaultHoodieRecordPayload, EventTimeAvroPayload, HoodieRecordMerger, OverwriteNonDefaultsWithLatestAvroPayload, OverwriteWithLatestAvroPayload, PartialUpdateAvroPayload}
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

import scala.jdk.CollectionConverters._

class TestPayloadDeprecationFlow extends SparkClientFunctionalTestHarness {
  /**
   * Test if the payload based read have the same behavior for different table versions.
   */
  @ParameterizedTest
  @MethodSource(Array("providePayloadClassTestCases"))
  def testMergerBuiltinPayload(tableType: String,
                               payloadClazz: String,
                               expectedConfigs: Map[String, String]): Unit = {
    val opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClazz)
    val columns = Seq("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq")
    // 1. Add an insert.
    val data = Seq(
      (10, 1L, "rider-A", "driver-A", 19.10, "i", "10.1"),
      (10, 2L, "rider-B", "driver-B", 27.70, "i", "10.1"),
      (10, 3L, "rider-C", "driver-C", 33.90, "i", "10.1"),
      (10, 4L, "rider-D", "driver-D", 34.15, "i", "10.1"),
      (10, 5L, "rider-E", "driver-E", 17.85, "i", "10.1"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    val precombineField = if (payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      "_event_seq"
    } else {
      "ts"
    }
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "_event_lsn").
      option(PRECOMBINE_FIELD.key(), precombineField).
      option(TABLE_TYPE.key(), tableType).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    // Verify table was created successfully
    var metaClient = HoodieTableMetaClient.builder()
      .setBasePath(basePath)
      .setConf(storageConf())
      .build()
    val tableConfig = metaClient.getTableConfig
    // Verify table version is 8
    assertEquals(8, tableConfig.getTableVersion.versionCode())
    assertTrue(metaClient.getActiveTimeline.firstInstant().isPresent)
    val firstInstantTime = metaClient.getActiveTimeline.firstInstant().get().requestedTime()
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
      (11, 1L, "rider-X", "driver-X", 19.10, "D", "11.1"),
      (11, 2L, "rider-Y", "driver-Y", 27.70, "u", "11.1"))
    val firstUpdate = spark.createDataFrame(firstUpdateData).toDF(columns: _*)
    firstUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient);
    assertEquals(8, metaClient.getTableConfig.getTableVersion.versionCode())
    val firstUpdateInstantTime = metaClient.getActiveTimeline.getInstants.get(1).requestedTime()

    // 3. Add an update.
    val secondUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1"),
      (9, 4L, "rider-DD", "driver-DD", 34.15, "i", "9.1"),
      (12, 5L, "rider-EE", "driver-EE", 17.85, "i", "12.1"))
    val secondUpdate = spark.createDataFrame(secondUpdateData).toDF(columns: _*)
    secondUpdate.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "9").
      mode(SaveMode.Append).
      save(basePath)
    // Validate table version.
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(9, metaClient.getTableConfig.getTableVersion.versionCode())
    assertEquals(payloadClazz, metaClient.getTableConfig.getLegacyPayloadClass)
    val compactionInstants = metaClient.getActiveTimeline.getCommitsAndCompactionTimeline.getInstants
    val foundCompaction = compactionInstants.stream().anyMatch(i => i.getAction.equals("commit"))
    assertTrue(foundCompaction)

    // 4. Add a trivial update to trigger payload class mismatch.
    val thirdUpdateData = Seq(
      (12, 3L, "rider-CC", "driver-CC", 33.90, "i", "12.1"))
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

    // 5. Validate.
    // Validate table configs.
    val tableConfig = metaClient.getTableConfig
    expectedConfigs.foreach { case (key, expectedValue) =>
      if (expectedValue != null) {
        assertEquals(expectedValue, tableConfig.getString(key), s"Config $key should be $expectedValue")
      } else {
        assertFalse(tableConfig.contains(key), s"Config $key should not be present")
      }
    }
    // Validate snapshot query.
    val df = spark.read.format("hudi").load(basePath)
    val finalDf = df.select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq").sort("_event_lsn")
    val expectedData = getExpectedResultForSnapshotQuery(payloadClazz)
    val expectedDf = spark.createDataFrame(spark.sparkContext.parallelize(expectedData)).toDF(columns: _*).sort("_event_lsn")
    expectedDf.show(false)
    finalDf.show(false)
    assertTrue(expectedDf.except(finalDf).isEmpty && finalDf.except(expectedDf).isEmpty)
    // Validate time travel query.
    val timeTravelDf = spark.read.format("hudi")
      .option("as.of.instant", firstUpdateInstantTime).load(basePath)
      .select("ts", "_event_lsn", "rider", "driver", "fare", "Op", "_event_seq").sort("_event_lsn")
    timeTravelDf.show(false)
    val expectedTimeTravelData = getExpectedResultForTimeTravelQuery(payloadClazz)
    val expectedTimeTravelDf = spark.createDataFrame(
      spark.sparkContext.parallelize(expectedTimeTravelData)).toDF(columns: _*).sort("_event_lsn")
    expectedTimeTravelDf.show(false)
    timeTravelDf.show(false)
    assertTrue(
      expectedTimeTravelDf.except(timeTravelDf).isEmpty
      && timeTravelDf.except(expectedTimeTravelDf).isEmpty)
  }

  def getWriteConfig(hudiOpts: Map[String, String]): HoodieWriteConfig = {
    val props = TypedProperties.fromMap(hudiOpts.asJava)
    HoodieWriteConfig.newBuilder()
      .withProps(props)
      .withPath(basePath())
      .build()
  }

  def getExpectedResultForSnapshotQuery(payloadClazz: String): Seq[(Int, Long, String, String, Double, String, String)] = {
    if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      if (payloadClazz.equals(classOf[PartialUpdateAvroPayload].getName)
        || payloadClazz.equals(classOf[EventTimeAvroPayload].getName)
        || payloadClazz.equals(classOf[DefaultHoodieRecordPayload].getName)
        || payloadClazz.equals(classOf[PostgresDebeziumAvroPayload].getName)
        || payloadClazz.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
        Seq(
          (11, 1, "rider-X", "driver-X", 19.10, "D", "11.1"),
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1"),
          (12, 3, "rider-CC", "driver-CC", 33.90, "i", "12.1"),
          (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1"),
          (12, 5, "rider-EE", "driver-EE", 17.85, "i", "12.1"))
      } else {
        Seq(
          (11, 1, "rider-X", "driver-X", 19.10, "D", "11.1"),
          (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1"),
          (12, 3, "rider-CC", "driver-CC", 33.90, "i", "12.1"),
          (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1"),
          (12, 5, "rider-EE", "driver-EE", 17.85, "i", "12.1"))
      }
    } else {
      Seq(
        (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1"),
        (12, 3, "rider-CC", "driver-CC", 33.90, "i", "12.1"),
        (9, 4, "rider-DD", "driver-DD", 34.15, "i", "9.1"),
        (12, 5, "rider-EE", "driver-EE", 17.85, "i", "12.1"))
    }
  }

  def getExpectedResultForTimeTravelQuery(payloadClazz: String):
  Seq[(Int, Long, String, String, Double, String, String)] = {
    if (!payloadClazz.equals(classOf[AWSDmsAvroPayload].getName)) {
      Seq(
        (11, 1, "rider-X", "driver-X", 19.10, "D", "11.1"),
        (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1"),
        (10, 3, "rider-C", "driver-C", 33.90, "i", "10.1"),
        (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1"),
        (10, 5, "rider-E", "driver-E", 17.85, "i", "10.1"))
    } else {
      Seq(
        (11, 2, "rider-Y", "driver-Y", 27.70, "u", "11.1"),
        (10, 3, "rider-C", "driver-C", 33.90, "i", "10.1"),
        (10, 4, "rider-D", "driver-D", 34.15, "i", "10.1"),
        (10, 5, "rider-E", "driver-E", 17.85, "i", "10.1"))
    }
  }
}

// TODO: Add COPY_ON_WRITE table type tests when write path is updated accordingly.
// TODO: Add Test for MySqlDebeziumAvroPayload.
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
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + HoodieTableConfig.PARTIAL_UPDATE_CUSTOM_MARKER
            -> "__debezium_unavailable_value"),
      Arguments.of(
        "MERGE_ON_READ",
        classOf[AWSDmsAvroPayload].getName,
        Map(
          HoodieTableConfig.RECORD_MERGE_MODE.key() -> "COMMIT_TIME_ORDERING",
          HoodieTableConfig.LEGACY_PAYLOAD_CLASS_NAME.key() -> classOf[AWSDmsAvroPayload].getName,
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID.key() -> HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID),
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_KEY -> "Op",
          HoodieTableConfig.RECORD_MERGE_PROPERTY_PREFIX + DefaultHoodieRecordPayload.DELETE_MARKER -> "D"),
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
