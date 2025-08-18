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

package org.apache.spark.sql.hudi

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator}

import org.apache.spark.sql.{RuntimeConfig, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, HoodieCatalogTable}
import org.apache.spark.sql.internal.{SessionState, SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, spy, when}

/**
 * Tests {@link ProvidesHoodieConfig}
 */
class TestProvidesHoodieConfig {
  @Test
  def testGetPartitionPathFieldWriteConfig(): Unit = {
    val mockTable = mock(classOf[HoodieCatalogTable])
    val partitionFieldNames = "ts,segment"
    val customKeyGenPartitionFieldWriteConfig = "ts:timestamp,segment:simple"
    val tableConfig = new HoodieTableConfig()
    when(mockTable.tableConfig).thenReturn(tableConfig)

    mockPartitionWriteConfigInCatalogProps(mockTable, None)
    assertEquals(
      partitionFieldNames,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        "", partitionFieldNames, mockTable))
    assertEquals(
      partitionFieldNames,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        classOf[ComplexKeyGenerator].getName, partitionFieldNames, mockTable))
    assertEquals(
      partitionFieldNames,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        classOf[CustomKeyGenerator].getName, partitionFieldNames, mockTable))

    mockPartitionWriteConfigInCatalogProps(mockTable, Option(customKeyGenPartitionFieldWriteConfig))
    assertEquals(
      partitionFieldNames,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        "", partitionFieldNames, mockTable))
    assertEquals(
      partitionFieldNames,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        classOf[ComplexKeyGenerator].getName, partitionFieldNames, mockTable))
    assertEquals(
      customKeyGenPartitionFieldWriteConfig,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        classOf[CustomKeyGenerator].getName, partitionFieldNames, mockTable))

    // catalog props do not have write partition path field config set but table config has the
    // partition type in partition field
    mockPartitionWriteConfigInCatalogProps(mockTable, Option.empty)
    tableConfig.setValue(HoodieTableConfig.PARTITION_FIELDS, customKeyGenPartitionFieldWriteConfig)
    when(mockTable.tableConfig).thenReturn(tableConfig)
    assertEquals(
      customKeyGenPartitionFieldWriteConfig,
      ProvidesHoodieConfig.getPartitionPathFieldWriteConfig(
        classOf[CustomKeyGenerator].getName, partitionFieldNames, mockTable))
  }

  @Test
  def testInferPrecombineFieldFromTableConfig(): Unit = {
    // ProvidesHoodieConfig should be able to infer precombine field from table config
    // mock catalogTable
    val mockCatalog = mock(classOf[HoodieCatalogTable])
    // catalogProperties won't be passed in correctly, because they were not synced properly
    when(mockCatalog.catalogProperties).thenReturn(Map.empty[String, String])
    when(mockCatalog.partitionFields).thenReturn(Array("partition"))
    when(mockCatalog.orderingFields).thenCallRealMethod()
    when(mockCatalog.partitionSchema).thenReturn(StructType(Nil))
    when(mockCatalog.primaryKeys).thenReturn(Array("key"))
    when(mockCatalog.table).thenReturn(CatalogTable.apply(
      TableIdentifier.apply("hudi_table", Option.apply("hudi_database")),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty,
      StructType(Nil)))
    val props = new TypedProperties()
    props.setProperty(HoodieTableConfig.ORDERING_FIELDS.key, "segment")
    val mockTableConfig = spy(classOf[HoodieTableConfig])
    when(mockTableConfig.getProps).thenReturn(props)
    when(mockCatalog.tableConfig).thenReturn(mockTableConfig)

    // mock spark session and sqlConf
    val mockSparkSession = mock(classOf[SparkSession])
    val mockSessionState = mock(classOf[SessionState])
    val mockRuntimeConf = mock(classOf[RuntimeConfig])
    val mockSQLConf = mock(classOf[SQLConf])
    val mockSQLContext = mock(classOf[SQLContext])
    when(mockSparkSession.sqlContext).thenReturn(mockSQLContext)
    when(mockSparkSession.sessionState).thenReturn(mockSessionState)
    when(mockRuntimeConf.getOption(any())).thenReturn(Option.empty)
    when(mockSparkSession.conf).thenReturn(mockRuntimeConf)
    when(mockSessionState.conf).thenReturn(mockSQLConf)
    when(mockSQLContext.conf).thenReturn(mockSQLConf)
    when(mockSQLConf.getConf(StaticSQLConf.CATALOG_IMPLEMENTATION)).thenReturn("nothive")
    when(mockSQLConf.getAllConfs).thenReturn(Map.empty[String, String])

    val mockCmd = mock(classOf[ProvidesHoodieConfig])
    when(mockCmd.buildHiveSyncConfig(any(), any(), any(), any()))
      .thenReturn(new HiveSyncConfig(new TypedProperties()))
    when(mockCmd.getDropDupsConfig(any(), any())).thenReturn(Map.empty[String, String])
    when(mockCmd.buildHoodieInsertConfig(any(), any(), any(), any(), any(), any(), any()))
      .thenCallRealMethod()
    val combinedConfig = mockCmd.buildHoodieInsertConfig(
      mockCatalog,
      mockSparkSession,
      isOverwritePartition = false,
      isOverwriteTable = false,
      Map.empty,
      Map.empty,
      Option.empty)

    assertEquals(
      "segment",
      combinedConfig.getOrElse(HoodieTableConfig.ORDERING_FIELDS.key, "")
    )
  }

  private def mockPartitionWriteConfigInCatalogProps(mockTable: HoodieCatalogTable,
                                                     value: Option[String]): Unit = {
    val props = if (value.isDefined) {
      Map(DataSourceWriteOptions.PARTITIONPATH_FIELD.key() -> value.get)
    } else {
      Map[String, String]()
    }
    when(mockTable.catalogProperties).thenReturn(props)
  }
}
