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

import org.apache.hudi.DataSourceWriteOptions.{INSERT_OPERATION_OPT_VAL, PARTITIONPATH_FIELD, PAYLOAD_CLASS_NAME, RECORD_MERGE_MODE, TABLE_TYPE, UPSERT_OPERATION_OPT_VAL}
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{AWSDmsAvroPayload, HoodieTableType, OverwriteNonDefaultsWithLatestAvroPayload, PartialUpdateAvroPayload}
import org.apache.hudi.common.model.DefaultHoodieRecordPayload.{DELETE_KEY, DELETE_MARKER}
import org.apache.hudi.common.model.debezium.PostgresDebeziumAvroPayload
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion, PartialUpdateMode}
import org.apache.hudi.common.table.HoodieTableConfig.{DEBEZIUM_UNAVAILABLE_VALUE, PARTIAL_UPDATE_CUSTOM_MARKER}
import org.apache.hudi.common.util.StringUtils
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.{Arguments, MethodSource}

/**
 * Test upgrade and downgrade process (8 -> 9 -> 8)
 * TODO: after the default table version become 9, we should modify this test accordingly.
 */
class TestEightToNineUpgrade extends RecordLevelIndexTestBase {
  @ParameterizedTest
  @MethodSource(Array("payloadConfigs"))
  def testPartitionFieldsWithUpgrade(tableType: HoodieTableType, payloadClass: String): Unit = {
    val partitionFields = "partition:simple"
    val hudiOptsWithoutLockConfigs = commonOpts ++ Map(
      TABLE_TYPE.key -> tableType.name(),
      PARTITIONPATH_FIELD.key -> partitionFields,
      PAYLOAD_CLASS_NAME.key -> payloadClass,
      RECORD_MERGE_MODE.key -> RecordMergeMode.CUSTOM.name)
    val hudiOpts = hudiOptsWithoutLockConfigs

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    // assert table version is eight and the partition fields in table config has partition type
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(
      partitionFields,
      HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    assertEquals(payloadClass, metaClient.getTableConfig.getPayloadClass)

    // upgrade table props to version 9
    // assert table version is 9 and the partition fields in table config does not have partition type
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.NINE, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.NINE, metaClient.getTableConfig.getTableVersion)
    // The payload class should be maintained.
    assertEquals(payloadClass, metaClient.getTableConfig.getPayloadClass)

    // After upgrade, based on the payload and table type, the merge mode is updated accordingly.
    if (payloadClass.equals(classOf[PartialUpdateAvroPayload].getName)) {
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode)
    } else if (payloadClass.equals(classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName)) {
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_DEFAULTS, metaClient.getTableConfig.getPartialUpdateMode)
    } else if (payloadClass.equals(classOf[PostgresDebeziumAvroPayload].getName)) {
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      assertEquals(PartialUpdateMode.IGNORE_MARKERS, metaClient.getTableConfig.getPartialUpdateMode)
      val mergeProperties = metaClient.getTableConfig.getMergeProperties
      assertFalse(StringUtils.isNullOrEmpty(mergeProperties))
      assertTrue(mergeProperties.contains(
        PARTIAL_UPDATE_CUSTOM_MARKER + "=" + DEBEZIUM_UNAVAILABLE_VALUE))
    } else if (payloadClass.equals(classOf[AWSDmsAvroPayload].getName)) {
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING, metaClient.getTableConfig.getRecordMergeMode)
      val mergeProperties = metaClient.getTableConfig.getMergeProperties
      assertFalse(StringUtils.isNullOrEmpty(mergeProperties))
      assertTrue(mergeProperties.contains(
        DELETE_KEY + "=Op," + DELETE_MARKER + "=D"))
    }

    // auto downgrade the table
    // assert table version is eight and the partition fields in table config has partition type
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields,
      HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    assertEquals(payloadClass, metaClient.getTableConfig.getPayloadClass)
  }

  def payloadConfigs(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of (
      Arguments.of("MERGE_ON_READ", classOf[PartialUpdateAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[OverwriteNonDefaultsWithLatestAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[PostgresDebeziumAvroPayload].getName),
      Arguments.of("MERGE_ON_READ", classOf[AWSDmsAvroPayload].getName)
    )
  }
}
