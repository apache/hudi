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

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.common.util.collection.ClosableIterator

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.io.Closeable
import java.lang.reflect.InvocationTargetException

class TestFileGroupReaderIteratorClose {

  private val inputSchema = new StructType().add("value", IntegerType)
  private val invalidOutputSchema = new StructType().add("missing", IntegerType)

  private class RecordingIterator(closeFailure: Option[RuntimeException]) extends ClosableIterator[InternalRow] {
    var closed = false
    private var available = true

    override def hasNext: Boolean = available

    override def next(): InternalRow = {
      if (!available) throw new NoSuchElementException()
      available = false
      InternalRow(42)
    }

    override def close(): Unit = {
      closed = true
      closeFailure.foreach(failure => throw failure)
    }
  }

  // Exercise the existing projection boundary without adding a production API solely for tests.
  private def project(reader: ClosableIterator[InternalRow], outputSchema: StructType, withPartitions: Boolean): Iterator[InternalRow] = {
    HoodieFileGroupReaderFunction.closeOnFailure(reader) {
      buildProjection(reader, outputSchema, withPartitions)
    }
  }

  private def buildProjection(reader: ClosableIterator[InternalRow], outputSchema: StructType, withPartitions: Boolean): Iterator[InternalRow] = {
    val owner = HoodieFileGroupReaderFunction
    val method = owner.getClass.getDeclaredMethods.find(_.getName.endsWith("appendPartitionAndProject")).get
    method.setAccessible(true)
    try {
      method.invoke(owner, reader, inputSchema,
        if (withPartitions) new StructType().add("partition", IntegerType) else new StructType(), outputSchema,
        InternalRow(7), Set(0)).asInstanceOf[Iterator[InternalRow]]
    } catch {
      case failure: InvocationTargetException => throw failure.getCause
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def failedRealProjectionClosesTheReaderIterator(withPartitions: Boolean): Unit = {
    val reader = new RecordingIterator(None)
    assertThrows(classOf[NoSuchElementException], () => project(reader, invalidOutputSchema, withPartitions))
    assertTrue(reader.closed, "a failed projection must release its reader before a task listener exists")
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def failedCloseIsSuppressedIntoTheRealProjectionFailure(withPartitions: Boolean): Unit = {
    val closeFailure = new RuntimeException("close failed")
    val reader = new RecordingIterator(Some(closeFailure))
    val thrown = assertThrows(classOf[NoSuchElementException], () => project(reader, invalidOutputSchema, withPartitions))
    assertTrue(reader.closed)
    assertEquals(1, thrown.getSuppressed.length)
    assertSame(closeFailure, thrown.getSuppressed.head)
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def builtRealProjectionLeavesTheReaderIteratorOpen(withPartitions: Boolean): Unit = {
    val reader = new RecordingIterator(None)
    val outputSchema = if (withPartitions) inputSchema.add("partition", IntegerType) else inputSchema
    val rows = project(reader, outputSchema, withPartitions)
    try {
      assertFalse(reader.closed, "successful projection leaves the reader open for consumption")
      assertTrue(rows.hasNext)
      val row = rows.next()
      assertEquals(42, row.getInt(0))
      if (withPartitions) assertEquals(7, row.getInt(1))
      assertFalse(rows.hasNext)
    } finally {
      rows.asInstanceOf[Closeable].close()
    }
    assertTrue(reader.closed, "closing projected rows releases the underlying reader")
  }

  @Test
  def failedProjectionClosesTheReaderIterator(): Unit = {
    val reader = new RecordingIterator(None)
    val failure = new IllegalStateException("projection failed")
    val thrown = assertThrows(classOf[IllegalStateException], () =>
      HoodieFileGroupReaderFunction.closeOnFailure(reader)(throw failure))
    assertSame(failure, thrown)
    assertTrue(reader.closed, "the reader iterator is closed when its projection fails")
  }

  @Test
  def failedCloseIsSuppressedIntoTheProjectionFailure(): Unit = {
    val closeFailure = new RuntimeException("close failed")
    val reader = new RecordingIterator(Some(closeFailure))
    val failure = new IllegalStateException("projection failed")
    val thrown = assertThrows(classOf[IllegalStateException], () =>
      HoodieFileGroupReaderFunction.closeOnFailure(reader)(throw failure))
    assertSame(failure, thrown)
    assertEquals(Seq(closeFailure), thrown.getSuppressed.toSeq)
  }

  @Test
  def failureRethrownByCloseStaysTheThrownFailure(): Unit = {
    val failure = new IllegalStateException("read failed")
    val reader = new RecordingIterator(Some(failure))
    val thrown = assertThrows(classOf[IllegalStateException], () =>
      HoodieFileGroupReaderFunction.closeOnFailure(reader)(throw failure))
    assertSame(failure, thrown)
    assertTrue(reader.closed)
    assertEquals(0, thrown.getSuppressed.length)
  }

  @Test
  def builtProjectionLeavesTheReaderIteratorOpen(): Unit = {
    val reader = new RecordingIterator(None)
    val rows = HoodieFileGroupReaderFunction.closeOnFailure(reader)(reader)
    assertSame(reader, rows)
    assertFalse(reader.closed, "built rows leave the reader to the task listener")
  }
}
