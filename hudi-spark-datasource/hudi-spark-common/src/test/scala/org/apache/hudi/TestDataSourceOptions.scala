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

package org.apache.hudi

import org.apache.hudi.common.config.{DFSPropertiesConfiguration, HoodieCommonConfig}
import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql.SQLContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class TestDataSourceOptions {
  @Test
  def testAdvancedConfigs(): Unit = {
    assertTrue(DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.isAdvanced)
    assertEquals(
      HoodieCommonConfig.SCHEMA_EVOLUTION_ENABLE.defaultValue(),
      DataSourceReadOptions.SCHEMA_EVOLUTION_ENABLED.defaultValue())

    assertTrue(DataSourceWriteOptions.RECONCILE_SCHEMA.isAdvanced)
    assertEquals(
      HoodieCommonConfig.RECONCILE_SCHEMA.defaultValue(),
      DataSourceWriteOptions.RECONCILE_SCHEMA.defaultValue())

    assertTrue(DataSourceWriteOptions.DROP_PARTITION_COLUMNS.isAdvanced)
    assertEquals(
      HoodieTableConfig.DROP_PARTITION_COLUMNS.defaultValue(),
      DataSourceWriteOptions.DROP_PARTITION_COLUMNS.defaultValue())
  }

  @Test
  def testReadDefaultsSupportSparkHoodieConfigs(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ))

    assertEquals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, params(DataSourceReadOptions.QUERY_TYPE.key))
    assertTrue(!params.contains("spark.hoodie.datasource.query.type"))
  }

  @Test
  def testReadDefaultsPreferHoodieOverSparkHoodieWhenBothSet(): Unit = {
    val params = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      "hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
    ))

    assertEquals(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, params(DataSourceReadOptions.QUERY_TYPE.key))
  }

  @Test
  def testReadDefaultsConfigHierarchyWithGlobalDFSProps(): Unit = {
    // Set a config in global DFS props (lowest priority)
    DFSPropertiesConfiguration.addToGlobalProps(
      DataSourceReadOptions.QUERY_TYPE.key,
      DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL
    )

    // Test 1: Global DFS props are used when no other configs are set
    val params1 = DataSourceOptionsHelper.parametersWithReadDefaults(Map.empty)
    assertEquals(DataSourceReadOptions.QUERY_TYPE_READ_OPTIMIZED_OPT_VAL, params1(DataSourceReadOptions.QUERY_TYPE.key))

    // Test 2: spark.hoodie.* overrides global DFS props
    val params2 = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL
    ))
    assertEquals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL, params2(DataSourceReadOptions.QUERY_TYPE.key))

    // Test 3: hoodie.* overrides both spark.hoodie.* and global DFS props
    val params3 = DataSourceOptionsHelper.parametersWithReadDefaults(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      "hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
    ))
    assertEquals(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL, params3(DataSourceReadOptions.QUERY_TYPE.key))
  }

  @Test
  def testNormalizeSparkHoodiePrefixStripsSparkPrefix(): Unit = {
    val result = DataSourceOptionsHelper.normalizeSparkHoodiePrefix(Map(
      "spark.hoodie.datasource.query.type" -> "snapshot",
      "spark.hoodie.datasource.hive_sync.use_spark_catalog" -> "true",
      "non.hoodie.key" -> "ignored"
    ))

    assertEquals("snapshot", result("hoodie.datasource.query.type"))
    assertEquals("true", result("hoodie.datasource.hive_sync.use_spark_catalog"))
    assertEquals("ignored", result("non.hoodie.key"))
    assertFalse(result.contains("spark.hoodie.datasource.query.type"))
    assertFalse(result.contains("spark.hoodie.datasource.hive_sync.use_spark_catalog"))
  }

  @Test
  def testNormalizeSparkHoodiePrefixPrefersHoodieOverSparkHoodie(): Unit = {
    val result = DataSourceOptionsHelper.normalizeSparkHoodiePrefix(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      "hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL
    ))

    assertEquals(DataSourceReadOptions.QUERY_TYPE_SNAPSHOT_OPT_VAL,
      result("hoodie.datasource.query.type"))
    assertFalse(result.contains("spark.hoodie.datasource.query.type"))
  }

  @Test
  def testNormalizeSparkHoodiePrefixIsIdempotent(): Unit = {
    val once = DataSourceOptionsHelper.normalizeSparkHoodiePrefix(Map(
      "spark.hoodie.datasource.query.type" -> "snapshot",
      "hoodie.other.key" -> "v"
    ))
    val twice = DataSourceOptionsHelper.normalizeSparkHoodiePrefix(once)

    assertEquals(once, twice)
  }

  @Test
  def testCollectHoodieAndSparkHoodieConfsReturnsCanonicalKeys(): Unit = {
    val sqlContext = mock(classOf[SQLContext])
    when(sqlContext.getAllConfs).thenReturn(Map(
      "spark.hoodie.datasource.query.type" -> DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      "hoodie.datasource.write.operation" -> "upsert",
      "spark.sql.shuffle.partitions" -> "200" // non-hoodie, must be filtered out
    ))

    val result = DataSourceOptionsHelper.collectHoodieAndSparkHoodieConfs(sqlContext, Map.empty)

    assertEquals(DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL,
      result("hoodie.datasource.query.type"))
    assertEquals("upsert", result("hoodie.datasource.write.operation"))
    assertFalse(result.contains("spark.hoodie.datasource.query.type"))
    assertFalse(result.contains("spark.sql.shuffle.partitions"))
  }

  @Test
  def testCollectHoodieAndSparkHoodieConfsExplicitOptionsWin(): Unit = {
    val sqlContext = mock(classOf[SQLContext])
    when(sqlContext.getAllConfs).thenReturn(Map(
      "spark.hoodie.datasource.write.operation" -> "insert",
      "hoodie.datasource.write.precombine.field" -> "ts_from_conf"
    ))

    val result = DataSourceOptionsHelper.collectHoodieAndSparkHoodieConfs(sqlContext, Map(
      "hoodie.datasource.write.operation" -> "upsert",                  // explicit overrides spark.hoodie.*
      "hoodie.datasource.write.precombine.field" -> "ts_from_options"   // explicit overrides hoodie.*
    ))

    assertEquals("upsert", result("hoodie.datasource.write.operation"))
    assertEquals("ts_from_options", result("hoodie.datasource.write.precombine.field"))
  }

  @Test
  def testWriteDefaultsSupportSparkHoodieConfigs(): Unit = {
    val params = HoodieWriterUtils.parametersWithWriteDefaults(Map(
      "spark.hoodie.datasource.write.operation" -> "upsert"
    ))

    assertEquals("upsert", params(DataSourceWriteOptions.OPERATION.key))
    assertFalse(params.contains("spark.hoodie.datasource.write.operation"))
  }

  @Test
  def testWriteDefaultsPreferHoodieOverSparkHoodieWhenBothSet(): Unit = {
    val params = HoodieWriterUtils.parametersWithWriteDefaults(Map(
      "spark.hoodie.datasource.write.operation" -> "insert",
      "hoodie.datasource.write.operation" -> "upsert"
    ))

    assertEquals("upsert", params(DataSourceWriteOptions.OPERATION.key))
    assertFalse(params.contains("spark.hoodie.datasource.write.operation"))
  }

  @AfterEach
  def cleanup(): Unit = {
    DFSPropertiesConfiguration.clearGlobalProps()
  }
}
