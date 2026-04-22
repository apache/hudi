/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hudi.v2

import org.apache.hudi.DataSourceReadOptions
import org.apache.hudi.common.model.{HoodieFileFormat, HoodieTableType}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.common.util.{Option => HOption}

import org.apache.spark.sql.SparkSession
import org.junit.jupiter.api.Assertions.{assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

/**
 * Unit tests for [[HoodieV2ReadSupport.isSupportedByDSv2]] gate conditions.
 */
class TestHoodieV2ReadSupport {

  private def mockMetaClient(tableType: HoodieTableType,
                             baseFileFormat: HoodieFileFormat = HoodieFileFormat.PARQUET,
                             multipleBaseFileFormats: Boolean = false,
                             bootstrapBasePath: HOption[String] = HOption.empty[String]()):
  HoodieTableMetaClient = {
    val metaClient = mock(classOf[HoodieTableMetaClient])
    val tableConfig = mock(classOf[HoodieTableConfig])
    when(metaClient.getTableType).thenReturn(tableType)
    when(metaClient.getTableConfig).thenReturn(tableConfig)
    when(tableConfig.getBaseFileFormat).thenReturn(baseFileFormat)
    when(tableConfig.isMultipleBaseFileFormatsEnabled).thenReturn(multipleBaseFileFormats)
    when(tableConfig.getBootstrapBasePath).thenReturn(bootstrapBasePath)
    metaClient
  }

  private def spark: SparkSession = null // unused by current gate logic

  @Test
  def testCowWithDefaultsIsSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty, spark))
  }

  @Test
  def testMorSnapshotIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.MERGE_ON_READ)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty, spark))
  }

  @Test
  def testMorReadOptimizedIsSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.MERGE_ON_READ)
    val opts = Map(DataSourceReadOptions.QUERY_TYPE.key ->
      DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL)
    assertTrue(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts, spark))
  }

  @Test
  def testOrcBaseFormatIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE, baseFileFormat = HoodieFileFormat.ORC)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty, spark))
  }

  @Test
  def testMultipleBaseFormatsIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE, multipleBaseFileFormats = true)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty, spark))
  }

  @Test
  def testIncrementalQueryIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    val opts = Map(DataSourceReadOptions.QUERY_TYPE.key ->
      DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts, spark))
  }

  @Test
  def testCdcFormatIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(HoodieTableType.COPY_ON_WRITE)
    val opts = Map(DataSourceReadOptions.INCREMENTAL_FORMAT.key ->
      DataSourceReadOptions.INCREMENTAL_FORMAT_CDC_VAL)
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, opts, spark))
  }

  @Test
  def testBootstrapTableIsNotSupported(): Unit = {
    val metaClient = mockMetaClient(
      HoodieTableType.COPY_ON_WRITE,
      bootstrapBasePath = HOption.of("/tmp/bootstrap"))
    assertFalse(HoodieV2ReadSupport.isSupportedByDSv2(metaClient, Map.empty, spark))
  }
}
