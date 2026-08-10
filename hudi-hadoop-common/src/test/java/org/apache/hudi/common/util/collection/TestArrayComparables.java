/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.hudi.common.util.OrderingValues;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link ArrayComparable}.
 */
public class TestArrayComparables {

  @Test
  void testApply() {
    ArrayComparable original = new ArrayComparable(new Comparable[] {1, 2, 3});
    ArrayComparable transformed = original.apply(val -> (Integer) val * 2);

    ArrayComparable expected = new ArrayComparable(new Comparable[] {2, 4, 6});
    assertEquals(expected, transformed, "The new instance should have the transformed values.");
    assertNotEquals(original, transformed, "The original instance should not be mutated.");
  }

  @Test
  void testIsValueSameClass() {
    ArrayComparable a = new ArrayComparable(new Comparable[] {1, "a"});
    ArrayComparable b = new ArrayComparable(new Comparable[] {2, "b"});
    assertTrue(a.isValueSameClass(b));
  }

  @Test
  void testCompareTo() {
    ArrayComparable arrayComparable = new ArrayComparable(new Comparable[] {"a", 1, 3.0});
    ArrayComparable arrayComparableCopy = new ArrayComparable(new Comparable[] {"a", 1, 3.0});

    // Identical objects should compare to 0
    assertEquals(0, arrayComparable.compareTo(arrayComparableCopy));

    // First element is smaller
    assertTrue(arrayComparable.compareTo(new ArrayComparable(new Comparable[] {"b", 2, 4.0})) < 0);
    // Third element is smaller
    assertTrue(arrayComparable.compareTo(new ArrayComparable(new Comparable[] {"a", 1, 5.0})) < 0);
    // Second element is larger
    assertTrue(arrayComparable.compareTo(new ArrayComparable(new Comparable[] {"a", 0, 3.0})) > 0);
  }

  @Test
  void testEquals() {
    ArrayComparable arrayComparable = new ArrayComparable(new Comparable[] {"a", null, 3.0});
    ArrayComparable arrayComparableCopy = new ArrayComparable(new Comparable[] {"a", null, 3.0});

    // Identical objects should be equal
    assertEquals(arrayComparable, arrayComparableCopy);

    // Different length
    assertNotEquals(arrayComparable, new ArrayComparable(new Comparable[] {"a", null}));

    // Different class so can not be equal
    assertNotEquals(OrderingValues.create(new Comparable[] {1}), new ArrayComparable(new Comparable[] {1}));
    // Second element is null in original ArrayComparable object
    assertNotEquals(new ArrayComparable(new Comparable[] {"a", 1, 3.0}), arrayComparable);
  }
}
