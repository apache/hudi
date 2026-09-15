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

package org.apache.spark.sql.hudi.catalog

import org.apache.hudi.exception.HoodieException
import org.apache.hudi.testutils.HoodieClientTestUtils

import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, StagedTable, SupportsWrite, Table, TableCatalog}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.junit.jupiter.api.{AfterAll, BeforeAll, TestInstance}
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.TestInstance.Lifecycle
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.mockito.Mockito.{mock, when}

import java.util

import scala.collection.JavaConverters._

/**
 * Tests the staging methods of {@link HoodieCatalog} for a non-Hudi table, which stage through
 * {@link BasicStagedTable} and hand the write off to the delegate catalog's table.
 */
@TestInstance(Lifecycle.PER_CLASS)
class TestHoodieCatalogStagedTable {

  private val ident = Identifier.of(Array("default"), "tbl")
  private val schema = StructType(Seq(StructField("id", IntegerType)))
  private val partitions = Array.empty[Transform]
  // Not a Hudi provider, so the staging methods take the BasicStagedTable branch
  private val properties: util.Map[String, String] = Map("provider" -> "parquet").asJava

  private var sparkSession: SparkSession = _

  @BeforeAll
  def setUp(): Unit = {
    val jsc = new JavaSparkContext(
      HoodieClientTestUtils.getSparkConfForTest(classOf[TestHoodieCatalogStagedTable].getName))
    jsc.setLogLevel("ERROR")
    // HoodieCatalog resolves SparkSession.active in its constructor
    sparkSession = SparkSession.builder.config(jsc.getConf).getOrCreate
  }

  @AfterAll
  def tearDown(): Unit = {
    sparkSession.close()
  }

  @ParameterizedTest
  @ValueSource(strings = Array("stageCreate", "stageReplace", "stageCreateOrReplace"))
  def testStagedWriteIsDelegatedToWritableTable(stagingMethod: String): Unit = {
    val delegateTable = mock(classOf[SupportsWrite])
    val info = mock(classOf[LogicalWriteInfo])
    val writeBuilder = mock(classOf[WriteBuilder])
    when(delegateTable.newWriteBuilder(info)).thenReturn(writeBuilder)
    val delegate = mock(classOf[TableCatalog])
    when(delegate.createTable(ident, schema, partitions, properties)).thenReturn(delegateTable)

    val staged = stage(stagingMethod, delegate)

    assertSame(writeBuilder, staged.asInstanceOf[SupportsWrite].newWriteBuilder(info))
  }

  @ParameterizedTest
  @ValueSource(strings = Array("stageCreate", "stageReplace", "stageCreateOrReplace"))
  def testStagedTableIsLoadedWhenDelegateCreateTableReturnsNull(stagingMethod: String): Unit = {
    // V2SessionCatalog, the default delegate, returns null from createTable by design, to save the loadTable call
    val delegate = mock(classOf[TableCatalog])
    val loadedTable = mock(classOf[Table])
    when(loadedTable.schema()).thenReturn(schema)
    when(delegate.loadTable(ident)).thenReturn(loadedTable)

    val staged = stage(stagingMethod, delegate)

    // The staged table is backed by the table that was just created, rather than by null
    assertEquals(schema, staged.schema())
    // It is not writable, so the write is rejected instead of being delegated
    val ex = assertThrows(classOf[HoodieException],
      () => staged.asInstanceOf[SupportsWrite].newWriteBuilder(mock(classOf[LogicalWriteInfo])))
    assertTrue(ex.getMessage.contains("`tbl` does not support writes"))
  }

  private def stage(stagingMethod: String, delegate: TableCatalog): StagedTable = {
    val catalog = new HoodieCatalog()
    catalog.setDelegateCatalog(delegate)
    stagingMethod match {
      case "stageCreate" => catalog.stageCreate(ident, schema, partitions, properties)
      case "stageReplace" => catalog.stageReplace(ident, schema, partitions, properties)
      case "stageCreateOrReplace" => catalog.stageCreateOrReplace(ident, schema, partitions, properties)
    }
  }
}
