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
import org.apache.hudi.common.config.HoodieMetadataConfig
import org.apache.hudi.common.model.HoodieTableType
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.testutils.HoodieTestDataGenerator
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.metadata.MetadataPartitionType

import org.apache.spark.sql.SaveMode
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Tag
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource

import java.util.Collections

@Tag("functional-b")
class TestGlobalRecordLevelIndexTableVersionSix extends TestGlobalRecordLevelIndex {
  override def commonOpts: Map[String, String] = super.commonOpts ++ Map(
    HoodieTableConfig.VERSION.key() -> "6",
    HoodieWriteConfig.WRITE_TABLE_VERSION.key() -> "6"
  )

  @ParameterizedTest
  @EnumSource(classOf[HoodieTableType])
  override def testRLIUpsertAndDropIndex(tableType: HoodieTableType): Unit = {
    val hudiOpts = commonOpts ++ Map(DataSourceWriteOptions.TABLE_TYPE.key -> tableType.name(),
      HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key -> "true")
    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Overwrite)

    val writeConfig = getWriteConfig(hudiOpts)
    writeConfig.setSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
    getHoodieWriteClient(writeConfig).dropIndex(Collections.singletonList(MetadataPartitionType.RECORD_INDEX.getPartitionPath))
    assertEquals(0, getFileGroupCountForRecordIndex(writeConfig))
    metaClient = HoodieTableMetaClient.reload(metaClient)
    assertEquals(0, metaClient.getTableConfig.getMetadataPartitionsInflight.size())
    // only files, col stats, partition stats partition should be present.
    assertEquals(2, metaClient.getTableConfig.getMetadataPartitions.size())

    doWriteAndValidateDataAndRecordIndex(hudiOpts,
      operation = DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL,
      saveMode = SaveMode.Append)
  }
}
