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

package org.apache.hudi

import org.apache.hudi.exception.HoodieException
import org.apache.hudi.util.CachingIterator

import org.apache.spark.sql.HoodieSparkCatalogUtils
import org.apache.spark.sql.connector.catalog.{Identifier, Table, TableCapability, TableCatalog}
import org.apache.spark.sql.connector.expressions.{Expressions, Transform}
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.hudi.catalog.BasicStagedTable
import org.apache.spark.sql.hudi.command.HoodieSparkValidateDuplicateKeyRecordMerger
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertThrows, assertTrue, fail}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, verify, when}

/**
 * Coverage for a handful of small, otherwise-untested classes in the Spark datasource:
 * [[CachingIterator]], [[HoodieSparkCatalogUtils]], [[BasicStagedTable]] and
 * [[HoodieSparkValidateDuplicateKeyRecordMerger]].
 */
class TestSparkDatasourceSmallClasses {

  @Test
  def testCachingIteratorYieldsAllElements(): Unit = {
    val it = new SeqCachingIterator(Seq("a", "b", "c"))
    val collected = scala.collection.mutable.ArrayBuffer[String]()
    while (it.hasNext) {
      collected += it.next
    }
    assertEquals(Seq("a", "b", "c"), collected.toSeq)
    assertFalse(it.hasNext)
  }

  @Test
  def testCachingIteratorHasNextIsIdempotent(): Unit = {
    val source = new CountingIterator(Seq("x", "y"))
    val it = new SeqCachingIterator(source)
    // Repeated hasNext without next must not advance the underlying iterator.
    assertTrue(it.hasNext)
    assertTrue(it.hasNext)
    assertTrue(it.hasNext)
    assertEquals(1, source.advances)
    assertEquals("x", it.next)
    assertEquals(1, source.advances)
    assertTrue(it.hasNext)
    assertEquals(2, source.advances)
    assertEquals("y", it.next)
    assertFalse(it.hasNext)
  }

  @Test
  def testMatchBucketTransformMatches(): Unit = {
    val transform: Transform = Expressions.bucket(8, "id")
    HoodieSparkCatalogUtils.MatchBucketTransform.unapply(transform) match {
      case Some((numBuckets, refs, _)) =>
        assertEquals(8, numBuckets)
        assertEquals("id", refs.head.fieldNames()(0))
      case None => fail("expected a bucket transform to match")
    }
  }

  @Test
  def testMatchBucketTransformDoesNotMatchOthers(): Unit = {
    val transform: Transform = Expressions.identity("id")
    assertTrue(HoodieSparkCatalogUtils.MatchBucketTransform.unapply(transform).isEmpty)
  }

  @Test
  def testBasicStagedTableDelegatesToUnderlyingTable(): Unit = {
    val ident = Identifier.of(Array("db"), "tbl")
    val table = mock(classOf[Table])
    val catalog = mock(classOf[TableCatalog])
    val schema = new StructType().add("id", DataTypes.IntegerType)
    when(table.schema()).thenReturn(schema)
    when(table.partitioning()).thenReturn(Array.empty[Transform])
    when(table.capabilities()).thenReturn(java.util.EnumSet.of(TableCapability.BATCH_WRITE))
    when(table.properties()).thenReturn(java.util.Collections.singletonMap("k", "v"))

    val staged = BasicStagedTable(ident, table, catalog)
    assertEquals("tbl", staged.name())
    assertEquals(schema, staged.schema())
    assertEquals(0, staged.partitioning().length)
    assertTrue(staged.capabilities().contains(TableCapability.BATCH_WRITE))
    assertEquals("v", staged.properties().get("k"))

    // commit is a no-op; abort must drop the staged table through the catalog.
    staged.commitStagedChanges()
    staged.abortStagedChanges()
    verify(catalog).dropTable(ident)
  }

  @Test
  def testBasicStagedTableRejectsNonWritableInfo(): Unit = {
    val ident = Identifier.of(Array("db"), "tbl")
    val staged = BasicStagedTable(ident, mock(classOf[Table]), mock(classOf[TableCatalog]))
    val info = mock(classOf[LogicalWriteInfo])
    val ex = assertThrows(classOf[HoodieException], () => staged.newWriteBuilder(info))
    assertTrue(ex.getMessage.contains("does not support writes"))
  }

  @Test
  def testDuplicateKeyMergerMetadata(): Unit = {
    val merger = new HoodieSparkValidateDuplicateKeyRecordMerger
    assertEquals(HoodieSparkValidateDuplicateKeyRecordMerger.STRATEGY_ID, merger.getMergingStrategy)
    // Also pin the raw UUID so an accidental change to the STRATEGY_ID constant itself is caught.
    assertEquals("fb092649-0fdc-4c14-9113-acde3034a6c4", merger.getMergingStrategy)
    // Pre-combining falls back to the default Spark record merger.
    assertTrue(merger.asPreCombiningMode().isInstanceOf[DefaultSparkRecordMerger])
  }
}

/**
 * Concrete [[CachingIterator]] backed by a plain iterator, used to exercise the trait.
 */
private class SeqCachingIterator(source: Iterator[String]) extends CachingIterator[String] {
  def this(elems: Seq[String]) = this(elems.iterator)

  override protected def doHasNext: Boolean = {
    if (source.hasNext) {
      nextRecord = source.next()
      true
    } else {
      false
    }
  }
}

/**
 * Iterator that tracks how many times it has been advanced, to assert idempotency of hasNext.
 */
private class CountingIterator(elems: Seq[String]) extends Iterator[String] {
  private val underlying = elems.iterator
  var advances: Int = 0

  override def hasNext: Boolean = underlying.hasNext

  override def next(): String = {
    advances += 1
    underlying.next()
  }
}
