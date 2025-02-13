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

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.RecordMergeMode
import org.apache.hudi.common.model.{DefaultHoodieRecordPayload, HoodieRecordMerger, HoodieTableType, OverwriteWithLatestAvroPayload}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.config.HoodieLockConfig
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource

class TestSevenToEightUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @CsvSource(value = Array(
    "COPY_ON_WRITE,null",
    "COPY_ON_WRITE,org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "COPY_ON_WRITE,org.apache.hudi.client.transaction.lock.NoopLockProvider",
    "MERGE_ON_READ,null",
    "MERGE_ON_READ,org.apache.hudi.client.transaction.lock.InProcessLockProvider",
    "MERGE_ON_READ,org.apache.hudi.client.transaction.lock.NoopLockProvider"
  ))
  def testPartitionFieldsWithUpgrade(tableType: HoodieTableType, lockProviderClass: String): Unit = {
    val partitionFields = "partition:simple"
    // Downgrade handling for metadata not yet ready.
    val hudiOptsWithoutLockConfigs = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionFields,
      "hoodie.metadata.enable" -> "false",
      // "OverwriteWithLatestAvroPayload" is used to trigger merge mode upgrade/downgrade.
      DataSourceWriteOptions.PAYLOAD_CLASS_NAME.key -> classOf[OverwriteWithLatestAvroPayload].getName,
      DataSourceWriteOptions.RECORD_MERGE_MODE.key -> RecordMergeMode.COMMIT_TIME_ORDERING.name)

    val hudiOpts = if (!lockProviderClass.equals("null")) {
      hudiOptsWithoutLockConfigs ++ Map(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key() -> lockProviderClass)
    } else {
      hudiOptsWithoutLockConfigs
    }

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    // assert table version is eight and the partition fields in table config has partition type
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)

    // downgrade table props to version seven
    // assert table version is seven and the partition fields in table config does not have partition type
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SEVEN, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.SEVEN, metaClient.getTableConfig.getTableVersion)
    assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
    // The payload class should be maintained.
    assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)

    // auto upgrade the table
    // assert table version is eight and the partition fields in table config has partition type
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)

    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // After upgrade, based on the payload and table type, the merge mode is updated accordingly.
    if (HoodieTableType.COPY_ON_WRITE == tableType) {
      assertEquals(classOf[OverwriteWithLatestAvroPayload].getName, metaClient.getTableConfig.getPayloadClass)
      assertEquals(RecordMergeMode.COMMIT_TIME_ORDERING.name, metaClient.getTableConfig.getRecordMergeMode.name)
      assertEquals(HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID, metaClient.getTableConfig.getRecordMergeStrategyId)
    } else {
      assertEquals(classOf[DefaultHoodieRecordPayload].getName, metaClient.getTableConfig.getPayloadClass)
      assertEquals(RecordMergeMode.EVENT_TIME_ORDERING.name, metaClient.getTableConfig.getRecordMergeMode.name)
    }
  }
}
