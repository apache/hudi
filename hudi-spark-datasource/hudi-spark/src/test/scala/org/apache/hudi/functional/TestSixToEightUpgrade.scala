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
import org.apache.hudi.common.testutils.HoodieTestUtils
import org.apache.hudi.common.util.TableConfigUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.keygen.constant.KeyGeneratorType
import org.apache.hudi.metadata.HoodieTableMetadata
import org.apache.hudi.util.SparkKeyGenUtils
import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

class TestSixToEightUpgrade extends RecordLevelIndexTestBase {

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  def testDowngradeWithMDTAndLogFiles(tableType: HoodieTableType): Unit = {
    val partitionFields = "partition:simple"
    val hudiOpts = commonOpts + (
      DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      DataSourceWriteOptions.KEYGENERATOR_CLASS_NAME.key -> KeyGeneratorType.CUSTOM.getClassName,
      DataSourceWriteOptions.PARTITIONPATH_FIELD.key -> partitionFields)

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite,
      validate = false)
    metaClient = getLatestMetaClient(true)

    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, TableConfigUtils.getPartitionFieldPropWithType(metaClient.getTableConfig).get())

    // downgrade table props
    downgradeTableConfigsFromEightToSix(getWriteConfig(hudiOpts))
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.SIX, metaClient.getTableConfig.getTableVersion)
    assertEquals("partition", TableConfigUtils.getPartitionFieldPropWithType(metaClient.getTableConfig).get())

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append,
      validate = false)
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(HoodieTableVersion.EIGHT, metaClient.getTableConfig.getTableVersion)
    assertEquals(partitionFields, TableConfigUtils.getPartitionFieldPropWithType(metaClient.getTableConfig).get())
  }

  private def downgradeTableConfigsFromEightToSix(cfg: HoodieWriteConfig): Unit = {
    val properties = metaClient.getTableConfig.getProps
    properties.setProperty(HoodieTableConfig.VERSION.key, "6")
    properties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key, SparkKeyGenUtils.getPartitionColumns(cfg.getProps))
    metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType, properties)
    HoodieTableConfig.update(metaClient.getStorage, metaClient.getMetaPath, properties)
    val metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath)
    if (metaClient.getStorage.exists(metadataTablePath)) {
      val mdtMetaClient = HoodieTableMetaClient.builder.setConf(metaClient.getStorageConf.newInstance).setBasePath(metadataTablePath).build
      metaClient.getTableConfig.setTableVersion(HoodieTableVersion.SIX)
      HoodieTableConfig.update(mdtMetaClient.getStorage, mdtMetaClient.getMetaPath, metaClient.getTableConfig.getProps)
    }
  }
}
