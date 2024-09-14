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
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient, HoodieTableVersion}
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.table.upgrade.{SparkUpgradeDowngradeHelper, UpgradeDowngrade}
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSevenToEightUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testPartitionFieldsWithUpgrade(tableType: HoodieTableType): Unit = {
    val partitionFields = "partition:simple"
    val hudiOpts = commonOpts ++ Map(
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionFields)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    // assert table version is eight and the partition fields in table config has partition type
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // downgrade table props to version seven
    // assert table version is seven and the partition fields in table config does not have partition type
    new UpgradeDowngrade(metaClient, getWriteConfig(hudiOpts), context, SparkUpgradeDowngradeHelper.getInstance)
      .run(HoodieTableVersion.SEVEN, null)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.SEVEN, metaClient.getTableConfig.getTableVersion)
    assertEquals("partition", HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())

    // auto upgrade the table
    // assert table version is eight and the partition fields in table config has partition type
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, HoodieTableConfig.getPartitionFieldPropForKeyGenerator(metaClient.getTableConfig).get())
  }
}
