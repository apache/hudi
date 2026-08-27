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

package org.apache.hudi.common.util.collection;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestLoserTreeMergeIterator {

  @Test
  void testMergesByRecordKeyThenSourceOrder() {
    AtomicBoolean firstClosed = new AtomicBoolean();
    AtomicBoolean secondClosed = new AtomicBoolean();
    List<Supplier<ClosableIterator<String>>> suppliers = Arrays.asList(
        () -> closableIterator(Arrays.asList("a-first", "c"), firstClosed),
        () -> closableIterator(Arrays.asList("a-second", "b"), secondClosed));
    LoserTreeMergeIterator<String> iterator = new LoserTreeMergeIterator<>(
        suppliers, value -> value.substring(0, 1));

    List<String> actual = new ArrayList<>();
    iterator.forEachRemaining(actual::add);

    assertEquals(Arrays.asList("a-first", "a-second", "b", "c"), actual);
    assertTrue(firstClosed.get());
    assertTrue(secondClosed.get());
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  void testMergesUsingLsmUtf8OrderingAndClosesInputs() {
    AtomicBoolean firstClosed = new AtomicBoolean();
    AtomicBoolean secondClosed = new AtomicBoolean();
    List<Supplier<ClosableIterator<String>>> suppliers = Arrays.asList(
        () -> closableIterator(Arrays.asList("a", "😀"), firstClosed),
        () -> closableIterator(Arrays.asList("b", "Ａ"), secondClosed));

    try (ClosableIterator<String> iterator = new LoserTreeMergeIterator<>(suppliers, value -> value)) {
      List<String> actual = new ArrayList<>();
      iterator.forEachRemaining(actual::add);

      assertEquals(Arrays.asList("a", "b", "Ａ", "😀"), actual);
      assertTrue(firstClosed.get());
      assertTrue(secondClosed.get());
      assertFalse(iterator.hasNext());
    }
  }

  @Test
  void testSupplierFailureClosesCreatedSources() {
    AtomicBoolean firstClosed = new AtomicBoolean();
    List<Supplier<ClosableIterator<Integer>>> suppliers = Arrays.asList(
        () -> closableIterator(Arrays.asList(1, 2), firstClosed),
        () -> {
          throw new IllegalStateException("failed to create source");
        });

    assertThrows(IllegalStateException.class,
        () -> new LoserTreeMergeIterator<>(suppliers, value -> value.toString()));
    assertTrue(firstClosed.get());
  }

  private static <T> ClosableIterator<T> closableIterator(List<T> values, AtomicBoolean closed) {
    Iterator<T> iterator = values.iterator();
    return new ClosableIterator<T>() {
      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public T next() {
        return iterator.next();
      }

      @Override
      public void close() {
        closed.set(true);
      }
    };
  }
}
