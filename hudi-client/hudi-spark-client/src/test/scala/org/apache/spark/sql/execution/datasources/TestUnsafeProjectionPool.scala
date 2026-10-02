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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertNotSame, assertSame, assertThrows, assertTrue}
import org.junit.jupiter.api.function.Executable

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.{ConcurrentHashMap, CountDownLatch, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

/**
 * [[UnsafeProjectionPool]] must hand a projection generated for one iterator to a later iterator with the same key,
 * and never to an iterator whose rows can still be in use: not to one alive at the same time on the same thread, not
 * before the first one is exhausted, and not to another thread.
 */
class TestUnsafeProjectionPool {

  private val schema = new StructType().add("id", IntegerType).add("name", StringType).add("ts", LongType)

  /** Generates the identity projection of `schema` and counts the generations. */
  private val generations = new AtomicInteger()

  private def generate(): UnsafeProjection = {
    generations.incrementAndGet()
    UnsafeProjection.create(schema)
  }

  private def row(i: Int): InternalRow = InternalRow(i, UTF8String.fromString(s"name$i"), i * 10L)

  private def rows(from: Int, until: Int): Iterator[InternalRow] = (from until until).iterator.map(row)

  /** Projects `input` with a lease of `key`, the way a reader does for one file. */
  private def project(key: AnyRef, input: Iterator[InternalRow]): Iterator[UnsafeRow] = {
    val lease = UnsafeProjectionPool.lease(key, generate())
    lease.releaseWhenExhausted(input.map(lease(_)))
  }

  private def assertRow(i: Int, actual: InternalRow): Unit = {
    assertEquals(i, actual.getInt(0))
    assertEquals(s"name$i", actual.getUTF8String(1).toString)
    assertEquals(i * 10L, actual.getLong(2))
  }

  @BeforeEach
  def setUp(): Unit = {
    UnsafeProjectionPool.clear()
    generations.set(0)
  }

  @AfterEach
  def tearDown(): Unit = UnsafeProjectionPool.clear()

  @Test
  def testExhaustedIteratorsHandTheirProjectionToTheNextOne(): Unit = {
    val outputRows = (0 until 5).map { file =>
      val iter = project("key", rows(file * 10, file * 10 + 3))
      val first = iter.next()
      assertRow(file * 10, first)
      (1 until 3).foreach(i => assertRow(file * 10 + i, iter.next()))
      assertFalse(iter.hasNext)
      first
    }
    assertEquals(1, generations.get(), "One projection for five iterators of the same key")
    outputRows.foreach(outputRow => assertSame(outputRows.head, outputRow))
    assertEquals(1, UnsafeProjectionPool.idleCount)
  }

  @Test
  def testIteratorsAliveAtTheSameTimeGetTheirOwnProjection(): Unit = {
    val left = project("key", rows(0, 3))
    val right = project("key", rows(100, 103))
    val leftRow = left.next()
    val rightRow = right.next()
    assertNotSame(leftRow, rightRow)
    assertRow(0, leftRow)
    assertRow(100, rightRow)
    assertEquals(2, generations.get())

    // Draining one of them does not touch the rows of the other one.
    left.next()
    left.next()
    assertFalse(left.hasNext)
    val next = project("key", rows(200, 201))
    assertRow(200, next.next())
    assertRow(100, rightRow)
    assertEquals(2, generations.get(), "The third iterator takes the projection of the drained one")
  }

  @Test
  def testProjectionIsNotReusedBeforeItsIteratorIsExhausted(): Unit = {
    val abandoned = project("key", rows(0, 3))
    val kept = abandoned.next()
    // hasNext is still true: the projection stays with the abandoned iterator.
    assertTrue(abandoned.hasNext)
    val later = project("key", rows(100, 101))
    assertRow(100, later.next())
    assertRow(0, kept)
    assertEquals(2, generations.get())
  }

  @Test
  def testReleaseHappensOnce(): Unit = {
    val iter = project("key", rows(0, 1))
    iter.next()
    assertFalse(iter.hasNext)
    assertFalse(iter.hasNext)
    assertEquals(1, UnsafeProjectionPool.idleCount)

    // Two iterators alive at the same time: only one of them can get the returned projection.
    val first = project("key", rows(10, 11))
    val second = project("key", rows(20, 21))
    val firstRow = first.next()
    val secondRow = second.next()
    assertNotSame(firstRow, secondRow)
    assertRow(10, firstRow)
    assertRow(20, secondRow)
    assertEquals(2, generations.get())
  }

  @Test
  def testIteratorWithoutRowsGeneratesNothing(): Unit = {
    val iter = project("key", Iterator.empty)
    assertFalse(iter.hasNext)
    assertEquals(0, generations.get())
    assertEquals(0, UnsafeProjectionPool.idleCount)
  }

  @Test
  def testKeysAreComparedByValue(): Unit = {
    drain(project(("key", 1), rows(0, 2)))
    drain(project(("key", 1), rows(0, 2)))
    assertEquals(1, generations.get())
    drain(project(("key", 2), rows(0, 2)))
    assertEquals(2, generations.get())
  }

  @Test
  def testIdleProjectionsAreBoundedPerThread(): Unit = {
    val keys = 0 until UnsafeProjectionPool.MAX_IDLE_PER_THREAD + 4
    keys.foreach(key => drain(project(Integer.valueOf(key), rows(0, 1))))
    assertEquals(UnsafeProjectionPool.MAX_IDLE_PER_THREAD, UnsafeProjectionPool.idleCount)
    assertEquals(keys.size, generations.get())

    // The least recently returned keys were dropped, the most recent ones are kept.
    drain(project(Integer.valueOf(keys.last), rows(0, 1)))
    assertEquals(keys.size, generations.get())
    drain(project(Integer.valueOf(keys.head), rows(0, 1)))
    assertEquals(keys.size + 1, generations.get())
  }

  @Test
  def testThreadsNeverShareAProjection(): Unit = {
    val threads = 8
    val filesPerThread = 50
    val rowsPerFile = 20
    val outputRowsByThread = new ConcurrentHashMap[Int, java.util.Set[UnsafeRow]]()
    val failures = Collections.synchronizedList(new java.util.ArrayList[Throwable]())
    val start = new CountDownLatch(1)
    val pool = Executors.newFixedThreadPool(threads)
    try {
      (0 until threads).foreach { thread =>
        pool.submit(new Runnable {
          override def run(): Unit = {
            try {
              val outputRows = Collections.newSetFromMap(new java.util.IdentityHashMap[UnsafeRow, java.lang.Boolean]())
              start.await()
              (0 until filesPerThread).foreach { file =>
                val first = thread * 1000000 + file * rowsPerFile
                val iter = project("shared-key", rows(first, first + rowsPerFile))
                var i = first
                while (iter.hasNext) {
                  val outputRow = iter.next()
                  outputRows.add(outputRow)
                  // Another thread writing into this buffer would show up as a wrong value here.
                  assertRow(i, outputRow)
                  i += 1
                }
                assertEquals(first + rowsPerFile, i)
              }
              outputRowsByThread.put(thread, outputRows)
            } catch {
              case t: Throwable => failures.add(t)
            } finally {
              UnsafeProjectionPool.clear()
            }
          }
        })
      }
      start.countDown()
      pool.shutdown()
      assertTrue(pool.awaitTermination(2, TimeUnit.MINUTES))
    } finally {
      pool.shutdownNow()
    }
    assertTrue(failures.isEmpty, s"Failures: $failures")
    assertEquals(threads, generations.get(), "One projection per thread")
    val all = outputRowsByThread.values().asScala.toSeq
    all.foreach(outputRows => assertEquals(1, outputRows.size(), "Each thread reuses its own projection"))
    val distinct = Collections.newSetFromMap(new java.util.IdentityHashMap[UnsafeRow, java.lang.Boolean]())
    all.foreach(distinct.addAll)
    assertEquals(threads, distinct.size(), "No two threads share a projection")
  }

  @Test
  def testProjectionThatWroteALargeRowIsNotKept(): Unit = {
    // A small row: the projection is kept.
    drain(project("key", rows(0, 3)))
    assertEquals(1, UnsafeProjectionPool.idleCount)
    UnsafeProjectionPool.clear()

    // A row just above the bound: its buffer is at least twice that, so the projection is dropped.
    val large = InternalRow(1, UTF8String.fromString("x" * (UnsafeProjectionPool.MAX_RETAINED_ROW_BYTES + 1)), 1L)
    val lease = UnsafeProjectionPool.lease("key", generate())
    val projected = lease.releaseWhenExhausted((rows(0, 2) ++ Iterator(large) ++ rows(5, 7)).map(lease(_)))
    drain(projected)
    assertEquals(0, UnsafeProjectionPool.idleCount, "A projection that wrote a large row is not kept")
    drain(project("key", rows(0, 1)))
    assertEquals(3, generations.get())
  }

  @Test
  def testKeyIsBuiltWhenTheFirstRowIsProjected(): Unit = {
    val keysBuilt = new AtomicInteger()
    def key(): AnyRef = {
      keysBuilt.incrementAndGet()
      "key"
    }
    val empty = UnsafeProjectionPool.lease(key(), generate())
    assertFalse(empty.releaseWhenExhausted(Iterator.empty).hasNext)
    assertEquals(0, keysBuilt.get(), "No key and no projection for an iterator without rows")

    val lease = UnsafeProjectionPool.lease(key(), generate())
    assertEquals(0, keysBuilt.get())
    val iter = lease.releaseWhenExhausted(rows(0, 3).map(lease(_)))
    assertRow(0, iter.next())
    assertRow(1, iter.next())
    assertEquals(1, keysBuilt.get(), "The key is built once, for the first row")
    drain(iter)
    assertEquals(1, keysBuilt.get())
    assertEquals(1, UnsafeProjectionPool.idleCount)
  }

  @Test
  def testGenerationFailureClosesTheReader(): Unit = {
    val closed = new AtomicInteger()
    val reader = new Closeable {
      override def close(): Unit = closed.incrementAndGet()
    }
    val lease = UnsafeProjectionPool.lease("failing-key", throw new IllegalStateException("codegen failed"), reader)
    val iter = lease.releaseWhenExhausted(rows(0, 3).map(lease(_)))
    val failure = assertThrows(classOf[IllegalStateException], new Executable {
      override def execute(): Unit = iter.next()
    })
    assertEquals("codegen failed", failure.getMessage)
    assertEquals(1, closed.get(), "The reader is closed when its projection cannot be generated")
    assertEquals(0, UnsafeProjectionPool.idleCount)
  }

  @Test
  def testSqlConfKeepsOnlySqlConfigs(): Unit = {
    def confOfTask(executionId: String): Map[String, String] = {
      val taskContext = TaskContext.empty()
      taskContext.getLocalProperties.setProperty("spark.sql.execution.id", executionId)
      taskContext.getLocalProperties.setProperty("spark.job.description", s"query $executionId")
      taskContext.getLocalProperties.setProperty("spark.sql.ansi.enabled", "true")
      taskContext.getLocalProperties.setProperty("spark.sql.session.timeZone", "Asia/Tokyo")
      TaskContext.setTaskContext(taskContext)
      try UnsafeProjectionPool.sqlConf finally TaskContext.unset()
    }
    val first = confOfTask("1")
    assertEquals(Some("true"), first.get("spark.sql.ansi.enabled"))
    assertEquals(Some("Asia/Tokyo"), first.get("spark.sql.session.timeZone"))
    assertFalse(first.contains("spark.sql.execution.id"), s"Task local properties are not SQL configs: $first")
    assertFalse(first.contains("spark.job.description"))
    // Two queries with the same SQL configs get the same key.
    assertEquals(first, confOfTask("2"))
  }

  @Test
  def testGenerationsAreCountedPerKeyClass(): Unit = {
    val before = UnsafeProjectionPool.generations(classOf[CountedKey])
    (0 until 3).foreach(_ => drain(project(CountedKey(1), rows(0, 2))))
    drain(project(CountedKey(2), rows(0, 2)))
    assertEquals(before + 2, UnsafeProjectionPool.generations(classOf[CountedKey]))
  }

  @Test
  def testDisabledPoolGeneratesForEveryIterator(): Unit = {
    UnsafeProjectionPool.poolingEnabled = false
    try {
      (0 until 3).foreach(_ => drain(project("key", rows(0, 2))))
      assertEquals(3, generations.get())
      assertEquals(0, UnsafeProjectionPool.idleCount)
    } finally {
      UnsafeProjectionPool.poolingEnabled = true
    }
  }

  private def drain(iter: Iterator[_]): Unit = while (iter.hasNext) iter.next()
}

case class CountedKey(id: Int)
