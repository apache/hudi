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

package org.apache.spark.sql.hudi.common

import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.table.HoodieTableConfig

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, verify, when}

class TestHoodieFileIndex {
  @Test
  def testDefaultDatabaseName(): Unit = {
    assertEquals("default", HoodieFileIndex.getDatabaseName(new HoodieTableConfig(), null))
    assertEquals("default_db", HoodieFileIndex.getDatabaseName(new HoodieTableConfig(), "default_db"))
  }

  @Test
  def testCatalogFailureOnGuessedDatabaseDoesNotFailRead(): Unit = {
    val tableConfig = new HoodieTableConfig()
    tableConfig.setValue(HoodieTableConfig.NAME, "tbl")
    val catalogError = new Exception("No valid database specified")
    val spark = mockSparkWithFailingTableExists("default", "tbl", catalogError)

    val props = HoodieFileIndex.getConfigProperties(spark, Map.empty, tableConfig)
    verify(spark.catalog).tableExists("default", "tbl")
    assertTrue(props.containsKey(RECORDKEY_FIELD.key))
    assertTrue(props.containsKey(PARTITIONPATH_FIELD.key))
  }

  @Test
  def testCatalogFailureOnRecordedDatabaseIsPropagated(): Unit = {
    val tableConfig = new HoodieTableConfig()
    tableConfig.setValue(HoodieTableConfig.NAME, "tbl")
    tableConfig.setValue(HoodieTableConfig.DATABASE_NAME, "db")
    val catalogError = new Exception("catalog unavailable")
    val spark = mockSparkWithFailingTableExists("db", "tbl", catalogError)

    val thrown = assertThrows(classOf[Exception], () => HoodieFileIndex.getConfigProperties(spark, Map.empty, tableConfig))
    assertSame(catalogError, thrown)
  }

  private def mockSparkWithFailingTableExists(database: String, table: String, error: Exception): SparkSession = {
    val spark = mock(classOf[SparkSession])
    val sessionState = mock(classOf[SessionState])
    val catalog = mock(classOf[Catalog])
    when(spark.sessionState).thenReturn(sessionState)
    when(sessionState.conf).thenReturn(new SQLConf)
    when(spark.catalog).thenReturn(catalog)
    when(catalog.currentDatabase).thenReturn("default")
    when(catalog.tableExists(database, table)).thenAnswer(_ => throw error)
    spark
  }
}
