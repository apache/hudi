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
import org.apache.hudi.DataSourceWriteOptions.{INSERT_OVERWRITE_OPERATION_OPT_VAL, OPERATION, PARTITIONPATH_FIELD, PAYLOAD_CLASS_NAME, RECORD_MERGE_IMPL_CLASSES, RECORDKEY_FIELD, TABLE_TYPE}
import org.apache.hudi.common.config.{HoodieMetadataConfig, HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, EventTimeAvroPayload, HoodieRecordMerger, HoodieTableType, OverwriteNonDefaultsWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.model.debezium.{DebeziumConstants, MySqlDebeziumAvroPayload, PostgresDebeziumAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, PartialUpdateMode}
import org.apache.hudi.common.table.HoodieTableConfig.{DEBEZIUM_UNAVAILABLE_VALUE, PARTIAL_UPDATE_UNAVAILABLE_VALUE, RECORD_MERGE_PROPERTY_PREFIX}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.config.{HoodieCompactionConfig, HoodieWriteConfig}
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.hudi.common.HoodieSparkSqlTestBase.checkAnswer
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

import java.util.{Collections, Properties}

class TestEightToNineUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @MethodSource(Array("payloadConfigs"))
  def testUpgradeDowngradeBetweenEightAndNine(tableType: HoodieTableType,
                                              payloadClass: String): Unit = {
    val partitionFields = "partition:simple"
    val mergerClasses = "org.apache.hudi.DefaultSparkRecordMerger," +
      "org.apache.hudi.OverwriteWithLatestSparkRecordMerger," +
      "org.apache.hudi.common.model.HoodieAvroRecordMerger"
    var hudiOpts = commonOpts ++ Map(
      TABLE_TYPE.key -> tableType.name(),
      PARTITIONPATH_FIELD.key -> partitionFields,
      PAYLOAD_CLASS_NAME.key -> payloadClass,
      RECORD_MERGE_IMPL_CLASSES.key -> mergerClasses,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8",
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
    )
    hudiOpts = hudiOpts ++ Map(HoodieTableConfig.PRECOMBINE_FIELD.key() -> hudiOpts(HoodieTableConfig.ORDERING_FIELDS.key())) - HoodieTableConfig.ORDERING_FIELDS.key()
    val orderingValue = "timestamp"

    // Create a table in table version 8.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    metaClient = getLatestMetaClient(true)
    setupV8OrderingFields(hudiOpts)
    // Assert table version is 8.
    checkResultForVersion8(payloadClass, orderingValue)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations.
    checkResultForVersion8(payloadClass, orderingValue)

    // Upgrade to version 9.
    // Remove the write table version config, such that an upgrade could be triggered.
    hudiOpts = hudiOpts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "9")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Table should be automatically upgraded to version 9.
    // Do validations for table version 9.
    checkResultForVersion9(partitionFields, payloadClass, orderingValue)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations for table version 9.
    checkResultForVersion9(partitionFields, payloadClass, orderingValue)

    // Downgrade to table version 8 explicitly.
    // Note that downgrade is NOT automatic.
    // It has to be triggered explicitly.
    hudiOpts = hudiOpts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    checkResultForVersion8(payloadClass, orderingValue)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations.
    checkResultForVersion8(payloadClass, orderingValue)
  }

  private def setupV8OrderingFields(hudiOpts: Map[String, String]): Unit = {
    val props = new Properties()
    props.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), hudiOpts(HoodieTableConfig.PRECOMBINE_FIELD.key()))
    HoodieTableConfig.updateAndDeleteProps(metaClient.getStorage, metaClient.getMetaPath, props, Collections.singleton(HoodieTableConfig.ORDERING_FIELDS.key()))
    metaClient = getLatestMetaClient(true)
  }

  @Test
  def testUpgradeDowngradeMySqlDebeziumPayload(): Unit = {
    val payloadClass = classOf[MySqlDebeziumAvroPayload].getName
    var opts: Map[String, String] = Map(
      HoodieWriteConfig.WRITE_PAYLOAD_CLASS_NAME.key() -> payloadClass,
      HoodieMetadataConfig.ENABLE.key() -> "false"
    )
    val columns = Seq("ts", "key", "rider", "driver", DebeziumConstants.FLATTENED_FILE_COL_NAME, DebeziumConstants.FLATTENED_POS_COL_NAME,
      DebeziumConstants.ADDED_SEQ_COL_NAME)

    // 1. Add an insert.
    val data = Seq(
      (10, "1", "rider-A", "driver-A", 1, 1, "1.1"),
      (10, "2", "rider-B", "driver-B", 2, 5, "2.5"),
      (10, "3", "rider-C", "driver-C", 3, 10, "3.10"),
      (10, "4", "rider-D", "driver-D", 4, 8, "4.8"),
      (10, "5", "rider-E", "driver-E", 5, 4, "5.4"))
    val inserts = spark.createDataFrame(data).toDF(columns: _*)
    var orderingValue: String = DebeziumConstants.ADDED_SEQ_COL_NAME
    inserts.write.format("hudi").
      option(RECORDKEY_FIELD.key(), "key").
      option(TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name()).
      option(DataSourceWriteOptions.ORDERING_FIELDS.key(), orderingValue).
      option(DataSourceWriteOptions.TABLE_NAME.key(), "test_table").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      options(opts).
      mode(SaveMode.Overwrite).
      save(basePath)
    setupV8OrderingFields(opts ++ Map(HoodieTableConfig.PRECOMBINE_FIELD.key() -> orderingValue))
    checkResultForVersion8(payloadClass, orderingValue)

    // 2. Add an update and upgrade the table to v9
    // first two records with larger ordering values based on debezium payload
    // last two records with smaller ordering values based on debezium payload, below updates should be rejected
    var updateData = Seq(
      (9, "1", "rider-X", "driver-X", 1, 2, "1.2"),
      (9, "2", "rider-Y", "driver-Y", 3, 2, "3.2"),
      (9, "3", "rider-C", "driver-C", 2, 10, "2.10"),
      (9, "4", "rider-D", "driver-D", 4, 7, "4.7")
    )
    var update = spark.createDataFrame(updateData).toDF(columns: _*)
    update.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      mode(SaveMode.Append).
      save(basePath)
    orderingValue = DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME
    checkResultForVersion9("", payloadClass, orderingValue)

    // Downgrade to table version 8 explicitly.
    // Note that downgrade is NOT automatic.
    // It has to be triggered explicitly.
    opts = opts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8")
    new UpgradeDowngrade(metaClient, getWriteConfig(opts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.EIGHT, null)

    // 3. Add an update after downgrade and validate the data
    // first two records with larger ordering values based on debezium payload
    // last two records with smaller ordering values based on debezium payload, below updates should be rejected
    updateData = Seq(
      (8, "1", "rider-X", "driver-X", 1, 3, "1.3"),
      (8, "2", "rider-Y", "driver-Y", 4, 2, "4.2"),
      (8, "3", "rider-C", "driver-C", 2, 10, "2.10"),
      (8, "4", "rider-D", "driver-D", 4, 7, "4.7")
    )
    update = spark.createDataFrame(updateData).toDF(columns: _*)
    update.write.format("hudi").
      option(OPERATION.key(), "upsert").
      option(HoodieCompactionConfig.INLINE_COMPACT.key(), "false").
      option(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), "8").
      mode(SaveMode.Append).
      save(basePath)
    orderingValue = DebeziumConstants.ADDED_SEQ_COL_NAME
    checkResultForVersion8(payloadClass, orderingValue)

    tableName = "testUpgradeDowngradeMySqlDebeziumPayload"
    spark.sql(s"create table testUpgradeDowngradeMySqlDebeziumPayload using hudi location '$basePath'")
    checkAnswer(spark, s"select ts, key, rider, driver, ${DebeziumConstants.FLATTENED_FILE_COL_NAME}, ${DebeziumConstants.FLATTENED_POS_COL_NAME},"
      + s" ${DebeziumConstants.ADDED_SEQ_COL_NAME} from default.$tableName")(
      Seq(8, "1", "rider-X", "driver-X", 1, 3, "1.3"),
      Seq(8, "2", "rider-Y", "driver-Y", 4, 2, "4.2"),
      Seq(10, "3", "rider-C", "driver-C", 3, 10, "3.10"),
      Seq(10, "4", "rider-D", "driver-D", 4, 8, "4.8"),
      Seq(10, "5", "rider-E", "driver-E", 5, 4, "5.4")
    )

    spark.sql(s"drop table default.$tableName")
  }

  def checkResultForVersion8(payloadClass: String, orderingValue: String): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    // Check ordering fields
    assertEquals(orderingValue, metaClient.getTableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD.key()))
    assertTrue(StringUtils.isNullOrEmpty(metaClient.getTableConfig.getString(HoodieTableConfig.ORDERING_FIELDS.key())))
    // The payload class should be maintained.
    assertEquals(payloadClass, metaClient.getTableConfig.getPayloadClass)
    // The partial update mode should not be present
    assertTrue(metaClient.getTableConfig.getPartialUpdateMode.isEmpty)
    if (payloadClass.equals("org.apache.hudi.common.model.EventTimeAvroPayload")) {
      assertEquals(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    } else {
      // The merge mode should be CUSTOM.
      assertEquals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    }
    if (payloadClass.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      assertFalse(metaClient.getTableConfig.getOrderingFieldsStr.isEmpty)
      assertEquals(DebeziumConstants.ADDED_SEQ_COL_NAME, metaClient.getTableConfig.getOrderingFieldsStr.get())
    }
  }

  def checkResultForVersion9(partitionFields: String, payloadClass: String, orderingValue: String): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.NINE, metaClient.getTableConfig.getTableVersion)
    assertEquals(
      partitionFields,
      HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    // Check ordering fields
    assertEquals(orderingValue, metaClient.getTableConfig.getString(HoodieTableConfig.ORDERING_FIELDS.key()))
    assertTrue(StringUtils.isNullOrEmpty(metaClient.getTableConfig.getString(HoodieTableConfig.PRECOMBINE_FIELD.key())))

    assertEquals(payloadClass, metaClient.getTableConfig.getLegacyPayloadClass)
    // Based on the payload and table type, the merge mode is updated accordingly.
    if (payloadClass.equals(classOf[PartialUpdateAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode.get())
    } else if (payloadClass.equals(classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode.get())
    } else if (payloadClass.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.FILL_UNAVAILABLE, metaClient.getTableConfig.getPartialUpdateMode.get())
      val customMarker = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${PARTIAL_UPDATE_UNAVAILABLE_VALUE}")
      assertEquals(DEBEZIUM_UNAVAILABLE_VALUE, customMarker)
    } else if (payloadClass.equals(classOf[AWSDmsAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertTrue(metaClient.getTableConfig.getPartialUpdateMode.isEmpty)
      val deleteField = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${DELETE_KEY}")
      assertEquals(AWSDmsAvroPayload.OP_FIELD, deleteField)
      val deleteMarker = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${DELETE_MARKER}")
      assertEquals(AWSDmsAvroPayload.DELETE_OPERATION_VALUE, deleteMarker)
    } else if (payloadClass.equals(classOf[MySqlDebeziumAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertTrue(metaClient.getTableConfig.getPartialUpdateMode.isEmpty)
      assertEquals(DebeziumConstants.FLATTENED_FILE_COL_NAME + "," + DebeziumConstants.FLATTENED_POS_COL_NAME,
        metaClient.getTableConfig.getOrderingFieldsStr.get())
    } else {
      assertTrue(metaClient.getTableConfig.getPartialUpdateMode.isEmpty)
    }
  }
}

object TestEightToNineUpgrade {
  def payloadConfigs(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      Arguments.of("COPY_ON_WRITE", classOf[EventTimeAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[PostgresDebeziumAvroPayload].getName),
      Arguments.of("COPY_ON_WRITE", classOf[AWSDmsAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[EventTimeAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PostgresDebeziumAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[AWSDmsAvroPayload].getName)
      // MySqlDebeziumPayload to be added.
    )
  }
}
