/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestClosableSortedDedupingIterator {

  @Test
  public void testEmptyIterator() {
    Iterator<String> emptyIterator = Collections.emptyIterator();
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(emptyIterator);
    
    // Test hasNext() on empty iterator
    assertFalse(dedupingIterator.hasNext());
    
    // Test next() on empty iterator should throw exception
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close() on non-closable iterator
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testSingleElementIterator() {
    List<String> singleElement = Arrays.asList("test");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(singleElement.iterator());
    
    // Test hasNext() returns true for first element
    assertTrue(dedupingIterator.hasNext());
    
    // Test next() returns the element
    assertEquals("test", dedupingIterator.next());
    
    // Test hasNext() returns false after consuming all elements
    assertFalse(dedupingIterator.hasNext());
    
    // Test next() throws exception after consuming all elements
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testMultipleUniqueElements() {
    List<String> uniqueElements = Arrays.asList("a", "b", "c");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(uniqueElements.iterator());
    
    // Test all elements are returned in order
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("c", dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testConsecutiveDuplicates() {
    List<String> elementsWithDuplicates = Arrays.asList("a", "a", "b", "b", "c");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elementsWithDuplicates.iterator());
    
    // Test duplicates are removed
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("c", dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testNonConsecutiveDuplicates() {
    List<String> elementsWithNonConsecutiveDuplicates = Arrays.asList("a", "b", "a", "c", "b");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elementsWithNonConsecutiveDuplicates.iterator());
    
    // Test only consecutive duplicates are removed (non-consecutive duplicates are kept)
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("c", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testAllSameElements() {
    List<String> allSameElements = Arrays.asList("a", "a", "a", "a");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(allSameElements.iterator());
    
    // Test only one element is returned
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testNullElements() {
    List<String> elementsWithNulls = Arrays.asList("a", null, "b", null, "c");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elementsWithNulls.iterator());
    
    // Test null elements are handled correctly
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertNull(dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertNull(dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("c", dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testConsecutiveNullElements() {
    List<String> elementsWithConsecutiveNulls = Arrays.asList("a", null, null, "b", null, null);
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elementsWithConsecutiveNulls.iterator());
    
    // Test consecutive nulls are deduplicated
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertNull(dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertNull(dedupingIterator.next());
    
    // Test no more elements
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testConsecutiveNullElementsDebug() {
    List<String> elementsWithConsecutiveNulls = Arrays.asList("a", null, null, "b", null, null);
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elementsWithConsecutiveNulls.iterator());
    
    // Debug: trace through each step
    System.out.println("Input: " + elementsWithConsecutiveNulls);
    
    // First element
    assertTrue(dedupingIterator.hasNext());
    String first = dedupingIterator.next();
    System.out.println("1. " + first);
    assertEquals("a", first);
    
    // Second element
    assertTrue(dedupingIterator.hasNext());
    String second = dedupingIterator.next();
    System.out.println("2. " + second);
    assertNull(second);
    
    // Third element
    assertTrue(dedupingIterator.hasNext());
    String third = dedupingIterator.next();
    System.out.println("3. " + third);
    assertEquals("b", third);
    
    // Fourth element
    assertTrue(dedupingIterator.hasNext());
    String fourth = dedupingIterator.next();
    System.out.println("4. " + fourth);
    assertNull(fourth);
    
    // No more elements
    assertFalse(dedupingIterator.hasNext());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testMultipleHasNextCalls() {
    List<String> elements = Arrays.asList("a", "a", "b");
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(elements.iterator());
    
    // Test multiple hasNext() calls don't consume elements
    assertTrue(dedupingIterator.hasNext());
    assertTrue(dedupingIterator.hasNext());
    assertTrue(dedupingIterator.hasNext());
    
    // Test next() still returns the correct element
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertFalse(dedupingIterator.hasNext());
    assertFalse(dedupingIterator.hasNext());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testClosableInnerIterator() {
    // Create a proper AutoCloseable iterator
    AutoCloseableIterator autoCloseableIterator = new AutoCloseableIterator(Arrays.asList("a", "a", "b"));
    
    ClosableSortedDedupingIterator<String> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(autoCloseableIterator);
    
    // Test normal iteration
    assertTrue(dedupingIterator.hasNext());
    assertEquals("a", dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals("b", dedupingIterator.next());
    
    assertFalse(dedupingIterator.hasNext());
    
    // Test close() - this will test the closable branch
    assertDoesNotThrow(() -> dedupingIterator.close());
    
    // Verify the inner iterator was closed
    assertTrue(autoCloseableIterator.isClosed());
  }

  // Helper class for testing closable iterator
  private static class AutoCloseableIterator implements Iterator<String>, AutoCloseable {
    private final List<String> data;
    private int index = 0;
    @Getter
    private boolean closed = false;
    
    public AutoCloseableIterator(List<String> data) {
      this.data = data;
    }
    
    @Override
    public boolean hasNext() {
      return index < data.size();
    }
    
    @Override
    public String next() {
      if (index >= data.size()) {
        throw new NoSuchElementException();
      }
      return data.get(index++);
    }
    
    @Override
    public void close() throws Exception {
      closed = true;
    }

  }

  @Test
  public void testIntegerElements() {
    List<Integer> integerElements = Arrays.asList(1, 1, 2, 3, 3, 4);
    ClosableSortedDedupingIterator<Integer> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(integerElements.iterator());
    
    // Test integer deduplication
    assertTrue(dedupingIterator.hasNext());
    assertEquals(Integer.valueOf(1), dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals(Integer.valueOf(2), dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals(Integer.valueOf(3), dedupingIterator.next());
    
    assertTrue(dedupingIterator.hasNext());
    assertEquals(Integer.valueOf(4), dedupingIterator.next());
    
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  @Test
  public void testCustomObjectElements() {
    List<TestObject> customObjects = Arrays.asList(
        new TestObject("a", 1),
        new TestObject("a", 1), // duplicate
        new TestObject("b", 2),
        new TestObject("b", 2)  // duplicate
    );
    
    ClosableSortedDedupingIterator<TestObject> dedupingIterator = 
        new ClosableSortedDedupingIterator<>(customObjects.iterator());
    
    // Test custom object deduplication
    assertTrue(dedupingIterator.hasNext());
    TestObject first = dedupingIterator.next();
    assertEquals("a", first.name);
    assertEquals(1, first.value);
    
    assertTrue(dedupingIterator.hasNext());
    TestObject second = dedupingIterator.next();
    assertEquals("b", second.name);
    assertEquals(2, second.value);
    
    assertFalse(dedupingIterator.hasNext());
    assertThrows(NoSuchElementException.class, () -> dedupingIterator.next());
    
    // Test close()
    assertDoesNotThrow(() -> dedupingIterator.close());
  }

  // Helper class for testing custom objects
  private static class TestObject {
    private final String name;
    private final int value;
    
    public TestObject(String name, int value) {
      this.name = name;
      this.value = value;
    }
    
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      TestObject that = (TestObject) obj;
      return value == that.value && name.equals(that.name);
    }
    
    @Override
    public int hashCode() {
      return name.hashCode() * 31 + value;
    }
  }
} 