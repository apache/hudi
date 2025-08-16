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

import org.apache.hudi.DataSourceWriteOptions.{INSERT_OVERWRITE_OPERATION_OPT_VAL, PARTITIONPATH_FIELD, PAYLOAD_CLASS_NAME, RECORD_MERGE_IMPL_CLASSES, TABLE_TYPE, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.common.config.{HoodieStorageConfig, RecordMergeMode}
import org.apache.hudi.common.model.{AWSDmsAvroPayload, EventTimeAvroPayload, HoodieRecordMerger, HoodieTableType, OverwriteNonDefaultsWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, PartialUpdateMode}
import org.apache.hudi.common.table.HoodieTableConfig.{DEBEZIUM_UNAVAILABLE_VALUE, PARTIAL_UPDATE_CUSTOM_MARKER, RECORD_MERGE_PROPERTY_PREFIX}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

class TestEightToNineUpgrade extends RecordLevelIndexTestBase {
  @ParameterizedTest
  @MethodSource(Array("payloadConfigs"))
  def testUpgradeDowngradeBetweenEightAndNine(tableType: HoodieTableType,
                                              payloadClass: String): Unit = {
    val partitionFields = "partition:simple"
    val mergerClasses = "org.apache.hudi.DefaultSparkRecordMerger," +
      "org.apache.hudi.OverwriteWithLatestSparkRecordMerger," +
      "org.apache.hudi.common.model.HoodieAvroRecordMerger"
    var hudiOpts= commonOpts ++ Map(
      TABLE_TYPE.key -> tableType.name(),
      PARTITIONPATH_FIELD.key -> partitionFields,
      PAYLOAD_CLASS_NAME.key -> payloadClass,
      RECORD_MERGE_IMPL_CLASSES.key -> mergerClasses,
      HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "8",
      HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT.key -> "parquet"
    )

    // Create a table in table version 8.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    metaClient = getLatestMetaClient(true)
    // Assert table version is 8.
    checkResultForVersion8(payloadClass)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations.
    checkResultForVersion8(payloadClass)

    // Upgrade to version 9.
    // Remove the write table version config, such that an upgrade could be triggered.
    hudiOpts = hudiOpts ++ Map(HoodieWriteConfig.WRITE_TABLE_VERSION.key -> "9")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Table should be automatically upgraded to version 9.
    // Do validations for table version 9.
    checkResultForVersion9(partitionFields, payloadClass)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations for table version 9.
    checkResultForVersion9(partitionFields, payloadClass)

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
    checkResultForVersion8(payloadClass)
    // Add an extra commit.
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OVERWRITE_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      schemaStr = HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA_WITH_PAYLOAD_SPECIFIC_COLS)
    // Do validations.
    checkResultForVersion8(payloadClass)
  }

  def checkResultForVersion8(payloadClass: String): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    // The payload class should be maintained.
    assertEquals(payloadClass, metaClient.getTableConfig.getPayloadClass)
    // The partial update mode should be NONE.
    assertEquals(PartialUpdateMode.NONE, metaClient.getTableConfig.getPartialUpdateMode)
    if (payloadClass.equals("org.apache.hudi.common.model.EventTimeAvroPayload")) {
      assertEquals(HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    } else {
      // The merge mode should be CUSTOM.
      assertEquals(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    }
  }

  def checkResultForVersion9(partitionFields: String, payloadClass: String): Unit = {
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.NINE, metaClient.getTableConfig.getTableVersion)
    assertEquals(
      partitionFields,
      HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    assertEquals(payloadClass, metaClient.getTableConfig.getLegacyPayloadClass)
    // Based on the payload and table type, the merge mode is updated accordingly.
    if (payloadClass.equals(classOf[PartialUpdateAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode)
    } else if (payloadClass.equals(classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode)
    } else if (payloadClass.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_MARKERS, metaClient.getTableConfig.getPartialUpdateMode)
      val customMarker = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${PARTIAL_UPDATE_CUSTOM_MARKER}")
      assertEquals(DEBEZIUM_UNAVAILABLE_VALUE, customMarker)
    } else if (payloadClass.equals(classOf[AWSDmsAvroPayload].getName)) {
      assertEquals(
        HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID,
        metaClient.getTableConfig.getRecordMergeStrategyId)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      val deleteField = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${DELETE_KEY}")
      assertEquals(AWSDmsAvroPayload.OP_FIELD, deleteField)
      val deleteMarker = metaClient.getTableConfig.getString(s"${RECORD_MERGE_PROPERTY_PREFIX}${DELETE_MARKER}")
      assertEquals(AWSDmsAvroPayload.DELETE_OPERATION_VALUE, deleteMarker)
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
      Arguments.of("COPY_ON_WRITE", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PostgresDebeziumAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[AWSDmsAvroPayload].getName)
      // MySqlDebeziumPayload to be added.
    )
  }
}
