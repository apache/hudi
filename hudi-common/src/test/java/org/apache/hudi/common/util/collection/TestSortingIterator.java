/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.util.collection;

import lombok.Getter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSortingIterator {

  @ParameterizedTest
  @MethodSource("sortingTestCases")
  <T extends Comparable<T>> void testSortingWithComparableElements(List<T> unsorted, List<T> expected) throws Exception {
    List<T> result = collectSortedElements(unsorted.iterator());
    assertEquals(expected, result);
  }

  private static Stream<Arguments> sortingTestCases() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6),
            Arrays.asList(1, 1, 2, 3, 4, 5, 6, 9)
        ),
        Arguments.of(
            Arrays.asList("banana", "apple", "cherry", "date"),
            Arrays.asList("apple", "banana", "cherry", "date")
        ),
        Arguments.of(
            Collections.singletonList(42),
            Collections.singletonList(42)
        ),
        Arguments.of(
            Collections.emptyList(),
            Collections.emptyList()
        )
    );
  }

  @Test
  void testEmptyIterator() throws Exception {
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(Collections.emptyIterator())) {
      assertFalse(sortingIterator.hasNext());
    }
  }

  @Test
  void testSingleElement() throws Exception {
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(Arrays.asList(42).iterator())) {
      assertTrue(sortingIterator.hasNext());
      assertEquals(42, sortingIterator.next());
      assertFalse(sortingIterator.hasNext());
    }
  }

  @ParameterizedTest
  @MethodSource("errorTestCases")
  void testErrorCases(List<Object> input, String expectedMessagePart) throws Exception {
    try (ClosableSortingIterator<Object> sortingIterator = new ClosableSortingIterator<>(input.iterator())) {
      IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, sortingIterator::hasNext);
      assertTrue(exception.getMessage().contains(expectedMessagePart));
    }
  }

  private static Stream<Arguments> errorTestCases() {
    return Stream.of(
        Arguments.of(
            Arrays.asList(new Object(), new Object(), new Object()),
            "Elements must implement Comparable interface"
        ),
        Arguments.of(
            Arrays.asList(3, "string", 1, "another", 2),
            "Elements cannot be compared with each other"
        )
    );
  }

  @ParameterizedTest
  @MethodSource("closeableSourceTestCases")
  void testCloseableIteratorSources(boolean isAutoCloseable) throws Exception {
    List<Integer> data = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6);
    List<Integer> expected = Arrays.asList(1, 1, 2, 3, 4, 5, 6, 9);
    
    if (isAutoCloseable) {
      TestAutoCloseableIterator<Integer> sourceIterator = new TestAutoCloseableIterator<>(data.iterator());
      List<Integer> result = collectSortedElements(sourceIterator);
      assertEquals(expected, result);
      assertTrue(sourceIterator.isClosed());
    } else {
      List<Integer> result = collectSortedElements(data.iterator());
      assertEquals(expected, result);
    }
  }

  private static Stream<Arguments> closeableSourceTestCases() {
    return Stream.of(
        Arguments.of(true),  // AutoCloseable source
        Arguments.of(false)  // Regular iterator source
    );
  }

  @Test
  void testCloseCalledMultipleTimes() throws Exception {
    TestAutoCloseableIterator<Integer> sourceIterator = new TestAutoCloseableIterator<>(
        Arrays.asList(3, 1, 4).iterator());
    
    ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator);
    
    // Close multiple times - should not cause issues
    sortingIterator.close();
    sortingIterator.close();
    sortingIterator.close();
    
    // Verify that the source iterator was closed only once
    assertEquals(1, sourceIterator.getCloseCount());
  }

  // Helper method to collect sorted elements from an iterator
  private <T> List<T> collectSortedElements(Iterator<T> sourceIterator) throws Exception {
    try (ClosableSortingIterator<T> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      List<T> result = new ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      return result;
    }
  }

  /**
   * Test implementation of AutoCloseable iterator for testing purposes
   */
  private static class TestAutoCloseableIterator<T> implements Iterator<T>, AutoCloseable {
    private final Iterator<T> delegate;
    @Getter
    private boolean closed = false;
    @Getter
    private int closeCount = 0;

    public TestAutoCloseableIterator(Iterator<T> delegate) {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
      return delegate.hasNext();
    }

    @Override
    public T next() {
      return delegate.next();
    }

    @Override
    public void close() throws Exception {
      if (!closed) {
        closed = true;
        closeCount++;
      }
    }
  }
}