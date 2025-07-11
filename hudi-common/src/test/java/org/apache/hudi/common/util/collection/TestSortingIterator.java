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

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSortingIterator {

  @Test
  public void testSortingWithComparableElements() throws Exception {
    List<Integer> unsorted = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6);
    Iterator<Integer> sourceIterator = unsorted.iterator();
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      List<Integer> sorted = Arrays.asList(1, 1, 2, 3, 4, 5, 6, 9);
      List<Integer> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(sorted, result);
    }
  }

  @Test
  public void testSortingWithStrings() throws Exception {
    List<String> unsorted = Arrays.asList("banana", "apple", "cherry", "date");
    Iterator<String> sourceIterator = unsorted.iterator();
    try (ClosableSortingIterator<String> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      List<String> sorted = Arrays.asList("apple", "banana", "cherry", "date");
      List<String> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(sorted, result);
    }
  }

  @Test
  public void testEmptyIterator() throws Exception {
    List<Integer> empty = Arrays.asList();
    Iterator<Integer> sourceIterator = empty.iterator();
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      assertFalse(sortingIterator.hasNext());
    }
  }

  @Test
  public void testSingleElement() throws Exception {
    List<Integer> single = Arrays.asList(42);
    Iterator<Integer> sourceIterator = single.iterator();
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      assertTrue(sortingIterator.hasNext());
      assertEquals(42, sortingIterator.next());
      assertFalse(sortingIterator.hasNext());
    }
  }

  @Test
  public void testNonComparableElements() throws Exception {
    // Create a list of non-comparable objects
    List<Object> nonComparable = Arrays.asList(new Object(), new Object(), new Object());
    Iterator<Object> sourceIterator = nonComparable.iterator();
    try (ClosableSortingIterator<Object> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      // Should preserve original order for non-comparable elements
      List<Object> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(nonComparable, result);
    }
  }

  @Test
  public void testMixedComparableAndNonComparable() throws Exception {
    // This test verifies that the iterator handles mixed types gracefully
    List<Object> mixed = Arrays.asList(3, "string", 1, new Object(), 2);
    Iterator<Object> sourceIterator = mixed.iterator();
    try (ClosableSortingIterator<Object> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      // Should preserve original order when sorting fails
      List<Object> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(mixed, result);
    }
  }

  @Test
  public void testClosableWithAutoCloseableSource() throws Exception {
    // Create a mock AutoCloseable iterator
    TestAutoCloseableIterator<Integer> sourceIterator = new TestAutoCloseableIterator<>(
        Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6).iterator());
    
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      List<Integer> sorted = Arrays.asList(1, 1, 2, 3, 4, 5, 6, 9);
      List<Integer> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(sorted, result);
    }
    
    // Verify that the source iterator was closed
    assertTrue(sourceIterator.isClosed());
  }

  @Test
  public void testClosableWithNonAutoCloseableSource() throws Exception {
    // Create a regular iterator (not AutoCloseable)
    Iterator<Integer> sourceIterator = Arrays.asList(3, 1, 4, 1, 5, 9, 2, 6).iterator();
    
    try (ClosableSortingIterator<Integer> sortingIterator = new ClosableSortingIterator<>(sourceIterator)) {
      List<Integer> sorted = Arrays.asList(1, 1, 2, 3, 4, 5, 6, 9);
      List<Integer> result = new java.util.ArrayList<>();
      while (sortingIterator.hasNext()) {
        result.add(sortingIterator.next());
      }
      
      assertEquals(sorted, result);
    }
    // Should not throw any exception when closing
  }

  @Test
  public void testCloseCalledMultipleTimes() throws Exception {
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

  /**
   * Test implementation of AutoCloseable iterator for testing purposes
   */
  private static class TestAutoCloseableIterator<T> implements Iterator<T>, AutoCloseable {
    private final Iterator<T> delegate;
    private boolean closed = false;
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

    public boolean isClosed() {
      return closed;
    }

    public int getCloseCount() {
      return closeCount;
    }
  }
} 