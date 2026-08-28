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

package org.apache.hudi.util

import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.mutable.ArrayBuffer

/**
 * Tests the [[CachingIterator]] contract: repeated [[CachingIterator.hasNext]] calls step the underlying
 * iterator at most once until [[CachingIterator.next]] consumes the cached record.
 */
class TestCachingIterator {

  @Test
  def testYieldsAllElements(): Unit = {
    val it = new SeqCachingIterator(new CountingIterator(Seq("a", "b", "c")))
    val collected = ArrayBuffer[String]()
    while (it.hasNext) {
      collected += it.next
    }
    assertEquals(Seq("a", "b", "c"), collected.toSeq)
    assertFalse(it.hasNext)
  }

  @Test
  def testHasNextIsIdempotent(): Unit = {
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
    assertEquals(2, source.advances)
  }

  /** Concrete [[CachingIterator]] backed by a plain iterator, used to exercise the trait. */
  private class SeqCachingIterator(source: Iterator[String]) extends CachingIterator[String] {
    override protected def doHasNext: Boolean = {
      if (source.hasNext) {
        nextRecord = source.next()
        true
      } else {
        false
      }
    }
  }

  /** Iterator that counts how many times it has been advanced. */
  private class CountingIterator(elems: Seq[String]) extends Iterator[String] {
    private val underlying = elems.iterator
    var advances: Int = 0

    override def hasNext: Boolean = underlying.hasNext

    override def next(): String = {
      advances += 1
      underlying.next()
    }
  }
}
