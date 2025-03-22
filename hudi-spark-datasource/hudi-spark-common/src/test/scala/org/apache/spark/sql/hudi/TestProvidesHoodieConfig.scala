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
import org.apache.hudi.DataSourceWriteOptions.{HIVE_STYLE_PARTITIONING, OPERATION, PARTITIONPATH_FIELD, PRECOMBINE_FIELD, RECORDKEY_FIELD, URL_ENCODE_PARTITIONING}
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.hive.HiveSyncConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.internal.{SQLConf, SessionState, StaticSQLConf}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{RuntimeConfig, SQLContext, SparkSession}
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.Arguments.arguments
import org.junit.jupiter.params.provider.{Arguments, MethodSource}
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
  }

  @Test
  def testInferPrecombineFieldFromTableConfig(): Unit = {
    // ProvidesHoodieConfig should be able to infer precombine field from table config
    // mock catalogTable
    val mockCatalog = mock(classOf[HoodieCatalogTable])
    // catalogProperties won't be passed in correctly, because they were not synced properly
    when(mockCatalog.catalogProperties).thenReturn(Map.empty[String, String])
    when(mockCatalog.partitionFields).thenReturn(Array("partition"))
    when(mockCatalog.preCombineKey).thenCallRealMethod()
    when(mockCatalog.partitionSchema).thenReturn(StructType(Nil))
    when(mockCatalog.primaryKeys).thenReturn(Array("key"))
    when(mockCatalog.tableName).thenReturn("hudi_table")
    val props = new TypedProperties()
    props.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key, "segment")
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
      combinedConfig.getOrElse(HoodieTableConfig.PRECOMBINE_FIELD.key, "")
    )

    // write config precombine field should be inferred from table config
    assertEquals(
      "segment",
      combinedConfig.getOrElse(DataSourceWriteOptions.PRECOMBINE_FIELD.key, "")
    )
  }

  @ParameterizedTest
  @MethodSource(Array("testBuildCommonOverridingOptsParams"))
  def testBuildCommonOverridingOpts(primaryKeys: Array[String],
                                    preCombineField: String,
                                    expectedOpts: Map[String, String]): Unit = {
    val mockTableConfig = mock(classOf[HoodieTableConfig])
    when(mockTableConfig.getTableName).thenReturn("myTable")
    when(mockTableConfig.getHiveStylePartitioningEnable).thenReturn("true")
    when(mockTableConfig.getUrlEncodePartitioning).thenReturn("false")
    when(mockTableConfig.getPreCombineField).thenReturn(preCombineField)

    val mockHoodieTable = mock(classOf[HoodieCatalogTable])
    when(mockHoodieTable.tableConfig).thenReturn(mockTableConfig)
    when(mockHoodieTable.tableLocation).thenReturn("/dummy/path")
    when(mockHoodieTable.primaryKeys).thenReturn(primaryKeys)

    val partitionPathField = "partField"
    val result = ProvidesHoodieConfig.buildCommonOverridingOpts(mockHoodieTable, partitionPathField)
    assert(result == expectedOpts)
  }

  @Test
  def testBuildOverridingOptsForDelete(): Unit = {
    val mockTableConfig = mock(classOf[HoodieTableConfig])
    when(mockTableConfig.getTableName).thenReturn("myTable")
    when(mockTableConfig.getHiveStylePartitioningEnable).thenReturn("true")
    when(mockTableConfig.getUrlEncodePartitioning).thenReturn("false")

    val mockHoodieTable = mock(classOf[HoodieCatalogTable])
    when(mockHoodieTable.tableConfig).thenReturn(mockTableConfig)
    when(mockHoodieTable.tableLocation).thenReturn("/dummy/path")
    when(mockHoodieTable.primaryKeys).thenReturn(Array("id1", "id2"))

    val partitionPathField = "partField"

    val expectedOpts = Map(
      "path" -> "/dummy/path",
      HoodieWriteConfig.TBL_NAME.key -> "myTable",
      HIVE_STYLE_PARTITIONING.key -> "true",
      URL_ENCODE_PARTITIONING.key -> "false",
      PARTITIONPATH_FIELD.key -> partitionPathField,
      RECORDKEY_FIELD.key -> "id1,id2",
      OPERATION.key -> DataSourceWriteOptions.DELETE_OPERATION_OPT_VAL
    )

    val result = ProvidesHoodieConfig.buildOverridingOptsForDelete(
      mockHoodieTable, partitionPathField)
    assert(result == expectedOpts)
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

object TestProvidesHoodieConfig {
  def testBuildCommonOverridingOptsParams(): java.util.stream.Stream[Arguments] = {
    java.util.stream.Stream.of(
      arguments(
        Array.empty[String],
        "",
        Map(
          "path" -> "/dummy/path",
          HoodieWriteConfig.TBL_NAME.key -> "myTable",
          HIVE_STYLE_PARTITIONING.key -> "true",
          URL_ENCODE_PARTITIONING.key -> "false",
          PARTITIONPATH_FIELD.key -> "partField"
        )),
      arguments(
        Array("id1", "id2"),
        "",
        Map(
          "path" -> "/dummy/path",
          HoodieWriteConfig.TBL_NAME.key -> "myTable",
          HIVE_STYLE_PARTITIONING.key -> "true",
          URL_ENCODE_PARTITIONING.key -> "false",
          PARTITIONPATH_FIELD.key -> "partField",
          RECORDKEY_FIELD.key -> "id1,id2"
        )),
      arguments(
        Array("id1", "id2"),
        "ts",
        Map(
          "path" -> "/dummy/path",
          HoodieWriteConfig.TBL_NAME.key -> "myTable",
          HIVE_STYLE_PARTITIONING.key -> "true",
          URL_ENCODE_PARTITIONING.key -> "false",
          PARTITIONPATH_FIELD.key -> "partField",
          RECORDKEY_FIELD.key -> "id1,id2",
          PRECOMBINE_FIELD.key -> "ts"
        )))
  }
}
