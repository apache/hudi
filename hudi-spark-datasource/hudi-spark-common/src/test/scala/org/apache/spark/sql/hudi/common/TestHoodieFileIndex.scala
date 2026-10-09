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

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.table.HoodieTableConfig
import org.apache.hudi.config.HoodieIndexConfig
import org.apache.hudi.index.HoodieIndex.IndexType

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalog.Catalog
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, SessionCatalog}
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.types.StructType
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.mockito.ArgumentMatchers.anyString
import org.mockito.Mockito.{mock, never, verify, when}

import java.net.URI
import java.nio.file.Path

class TestHoodieFileIndex {

  private val bucketsKey = HoodieIndexConfig.BUCKET_INDEX_NUM_BUCKETS.key

  @Test
  def testDefaultDatabaseName(): Unit = {
    assertEquals("default", HoodieFileIndex.getDatabaseName(new HoodieTableConfig(), null))
    assertEquals("default_db", HoodieFileIndex.getDatabaseName(new HoodieTableConfig(), "default_db"))
  }

  @Test
  def testCatalogIsNotConsultedWithoutBucketIndex(): Unit = {
    val (spark, catalog, _) = mockSpark()
    when(catalog.tableExists(anyString(), anyString()))
      .thenThrow(new IllegalStateException("database default is not allowed"))

    val props = HoodieFileIndex.getConfigProperties(spark, Map.empty, tableConfig("tbl"))

    verify(catalog, never()).tableExists(anyString(), anyString())
    assertFalse(props.containsKey(bucketsKey))
  }

  @Test
  def testCatalogPropertiesMergedWhenBucketIndexDeclared(@TempDir dir: Path): Unit = {
    val (spark, catalog, sessionCatalog) = mockSpark()
    val id = TableIdentifier("tbl", Some("default"))
    when(catalog.tableExists("default", "tbl")).thenReturn(true)
    when(sessionCatalog.getTableMetadata(id)).thenReturn(bucketCatalogTable(id, dir))
    val options = Map(HoodieIndexConfig.INDEX_TYPE.key -> IndexType.BUCKET.name)

    val props = HoodieFileIndex.getConfigProperties(spark, options, tableConfig("tbl"))

    assertEquals("3", props.getProperty(bucketsKey))
    verify(catalog).tableExists("default", "tbl")
  }

  @Test
  def testCatalogFailureWithBucketIndexDeclaredDoesNotFailRead(): Unit = {
    val (spark, catalog, _) = mockSpark()
    when(catalog.tableExists(anyString(), anyString()))
      .thenThrow(new IllegalStateException("database default is not allowed"))
    val options = Map(HoodieIndexConfig.INDEX_TYPE.key -> IndexType.BUCKET.name)

    val props = HoodieFileIndex.getConfigProperties(spark, options, tableConfig("tbl"))

    verify(catalog).tableExists("default", "tbl")
    assertFalse(props.containsKey(bucketsKey))
  }

  private def tableConfig(name: String): HoodieTableConfig = {
    val config = new HoodieTableConfig()
    config.setValue(HoodieTableConfig.NAME, name)
    config
  }

  private def bucketCatalogTable(id: TableIdentifier, location: Path): CatalogTable = CatalogTable(
    identifier = id,
    tableType = CatalogTableType.EXTERNAL,
    storage = CatalogStorageFormat.empty.copy(locationUri = Some(new URI("file:" + location.toAbsolutePath))),
    schema = new StructType(),
    provider = Some("hudi"),
    properties = Map(bucketsKey -> "3"))

  private def mockSpark(): (SparkSession, Catalog, SessionCatalog) = {
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
}
