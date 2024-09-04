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

import org.apache.hudi.DataSourceWriteOptions.PARTITIONPATH_FIELD
import org.apache.hudi.common.config.TypedProperties
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.keygen.{ComplexKeyGenerator, CustomKeyGenerator}
import org.apache.spark.sql.catalyst.catalog.HoodieCatalogTable
import org.apache.spark.sql.internal.SQLConf
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito
import org.mockito.Mockito.when

/**
 * Tests {@link ProvidesHoodieConfig}
 */
class TestProvidesHoodieConfig {
  @Test
  def testGetPartitionPathFieldWriteConfig(): Unit = {
    val mockTable = Mockito.mock(classOf[HoodieCatalogTable])
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
    val mockCatalog = Mockito.mock(classOf[HoodieCatalogTable])
    when(mockCatalog.catalogProperties).thenReturn(Map.empty[String, String])

    val props = new TypedProperties()
    props.setProperty(HoodieTableConfig.PRECOMBINE_FIELD.key, "segment")
    val mockTableConfig = Mockito.mock(classOf[HoodieTableConfig])
    when(mockTableConfig.getProps).thenReturn(props)
    val mockSqlConf = Mockito.mock(classOf[SQLConf])
    when(mockSqlConf.getAllConfs).thenReturn(Map.empty[String, String])

    assertEquals(
      "segment",
      ProvidesHoodieConfig.combineOptions(mockCatalog, mockTableConfig, mockSqlConf, Map.empty)
        .getOrElse(HoodieTableConfig.PRECOMBINE_FIELD.key, ""))
  }

  private def mockPartitionWriteConfigInCatalogProps(mockTable: HoodieCatalogTable,
                                                     value: Option[String]): Unit = {
    val props = if (value.isDefined) {
      Map(PARTITIONPATH_FIELD.key() -> value.get)
    } else {
      Map[String, String]()
    }
    when(mockTable.catalogProperties).thenReturn(props)
  }
}
