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

import org.apache.spark.sql.connector.catalog.{Identifier, SupportsWrite, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, never, verify, when}

class TestBasicStagedTable {

  private val ident = Identifier.of(Array("db"), "tbl")

  @Test
  def testDelegatesMetadataToUnderlyingTableAndLifecycleToCatalog(): Unit = {
    val table = mock(classOf[Table])
    val catalog = mock(classOf[TableCatalog])
    val schema = new StructType().add("id", DataTypes.IntegerType)
    val partitioning = Array[Transform](Expressions.identity("id"))
    val capabilities = java.util.EnumSet.of(TableCapability.BATCH_WRITE)
    val properties = java.util.Collections.singletonMap("k", "v")
    when(table.schema()).thenReturn(schema)
    when(table.partitioning()).thenReturn(partitioning)
    when(table.capabilities()).thenReturn(capabilities)
    when(table.properties()).thenReturn(properties)

    val staged = BasicStagedTable(ident, table, catalog)

    assertEquals("tbl", staged.name())
    assertSame(schema, staged.schema())
    assertSame(partitioning, staged.partitioning())
    assertSame(capabilities, staged.capabilities())
    assertSame(properties, staged.properties())

    // Committing leaves the catalog alone; aborting drops the staged table through it.
    staged.commitStagedChanges()
    verify(catalog, never()).dropTable(ident)
    staged.abortStagedChanges()
    verify(catalog).dropTable(ident)
  }

  @Test
  def testNewWriteBuilderDelegatesToWritableTable(): Unit = {
    val table = mock(classOf[SupportsWrite])
    val info = mock(classOf[LogicalWriteInfo])
    val writeBuilder = mock(classOf[WriteBuilder])
    when(table.newWriteBuilder(info)).thenReturn(writeBuilder)

    val staged = BasicStagedTable(ident, table, mock(classOf[TableCatalog]))

    assertSame(writeBuilder, staged.newWriteBuilder(info))
  }

  @Test
  def testNewWriteBuilderThrowsWhenTableIsNotWritable(): Unit = {
    val staged = BasicStagedTable(ident, mock(classOf[Table]), mock(classOf[TableCatalog]))

    val ex = assertThrows(classOf[HoodieException],
      () => staged.newWriteBuilder(mock(classOf[LogicalWriteInfo])))
    assertTrue(ex.getMessage.contains("`tbl` does not support writes"))
  }
}
