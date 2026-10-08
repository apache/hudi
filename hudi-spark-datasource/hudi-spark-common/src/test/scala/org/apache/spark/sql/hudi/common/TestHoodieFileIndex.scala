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

import org.apache.hudi.{DataSourceReadOptions, HoodieFileIndex}
import org.apache.hudi.DataSourceWriteOptions.{PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.storage.StoragePath

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, never, verify, when}

import java.net.URI
import java.nio.file.Path

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

  @Test
  def testResolvedIdentifierIsUsedWithoutGuessing(@TempDir dir: Path): Unit = {
    val (spark, catalog, sessionCatalog) = mockSparkWithSessionCatalog()
    when(catalog.tableExists("default", "tbl")).thenAnswer(_ => throw new Exception("No valid database specified"))
    val resolved = TableIdentifier("t_alias", Some("db1"))
    when(sessionCatalog.getTableMetadata(resolved)).thenReturn(hudiCatalogTable(resolved, dir))
    val options = Map(
      DataSourceReadOptions.CATALOG_TABLE_DATABASE.key -> "db1",
      DataSourceReadOptions.CATALOG_TABLE_NAME.key -> "t_alias")

    val props = HoodieFileIndex.getConfigProperties(spark, options, keylessTableConfig("tbl"), Some(storagePath(dir)))
    assertEquals("3", props.getProperty(BUCKETS_KEY))
    verify(catalog, never()).tableExists(anyString(), anyString())
  }

  @Test
  def testGuessedEntryAtAnotherLocationIsNotMerged(@TempDir dir: Path): Unit = {
    val (spark, catalog, sessionCatalog) = mockSparkWithSessionCatalog()
    val guessed = TableIdentifier("tbl", Some("default"))
    when(catalog.tableExists("default", "tbl")).thenReturn(true)
    when(sessionCatalog.getTableMetadata(guessed)).thenReturn(hudiCatalogTable(guessed, dir.resolve("other")))

    val props = HoodieFileIndex.getConfigProperties(spark, Map.empty, keylessTableConfig("tbl"), Some(storagePath(dir)))
    assertFalse(props.containsKey(BUCKETS_KEY))
  }

  @Test
  def testGuessedEntryAtSameLocationIsMerged(@TempDir dir: Path): Unit = {
    val (spark, catalog, sessionCatalog) = mockSparkWithSessionCatalog()
    val guessed = TableIdentifier("tbl", Some("default"))
    when(catalog.tableExists("default", "tbl")).thenReturn(true)
    when(sessionCatalog.getTableMetadata(guessed)).thenReturn(hudiCatalogTable(guessed, dir))

    val props = HoodieFileIndex.getConfigProperties(spark, Map.empty, keylessTableConfig("tbl"), Some(storagePath(dir)))
    assertEquals("3", props.getProperty(BUCKETS_KEY))
  }

  private val BUCKETS_KEY = "hoodie.bucket.index.num.buckets"

  private def keylessTableConfig(name: String): HoodieTableConfig = {
    val tableConfig = new HoodieTableConfig()
    tableConfig.setValue(HoodieTableConfig.NAME, name)
    tableConfig
  }

  private def storagePath(dir: Path): StoragePath = new StoragePath(new URI("file:" + dir.toAbsolutePath.toString))

  private def hudiCatalogTable(id: TableIdentifier, location: Path): CatalogTable = CatalogTable(
    identifier = id,
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI("file:" + location.toAbsolutePath.toString))),
    schema = new StructType(),
    provider = Some("hudi"),
    properties = Map(BUCKETS_KEY -> "3"))

  private def mockSparkWithSessionCatalog(): (SparkSession, Catalog, SessionCatalog) = {
    val spark = mock(classOf[SparkSession])
    val sessionState = mock(classOf[SessionState])
    val catalog = mock(classOf[Catalog])
    val sessionCatalog = mock(classOf[SessionCatalog])
    when(spark.sessionState).thenReturn(sessionState)
    when(sessionState.conf).thenReturn(new SQLConf)
    when(sessionState.newHadoopConf()).thenReturn(new Configuration())
    when(sessionState.catalog).thenReturn(sessionCatalog)
    when(spark.catalog).thenReturn(catalog)
    when(catalog.currentDatabase).thenReturn("default")
    (spark, catalog, sessionCatalog)
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

