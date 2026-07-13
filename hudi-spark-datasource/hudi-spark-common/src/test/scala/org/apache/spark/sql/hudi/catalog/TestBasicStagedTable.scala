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

import org.apache.spark.sql.connector.catalog.{Identifier, SupportsWrite, Table, TableCatalog}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.junit.jupiter.api.Assertions.{assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class TestBasicStagedTable {

  private val ident = Identifier.of(Array("db"), "tbl")

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
