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

package org.apache.hudi.common.util;

import org.apache.hudi.common.util.collection.ArrayComparable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link OrderingValues}.
 */
public class TestOrderingValues {

  /**
   * When the ordering field has a null value in the record (e.g. a nullable timestamp column),
   * {@link OrderingValues#create} must return the default ordering value (0) rather than null.
   * A null return would cause NullPointerException in BufferedRecordMergerFactory.shouldKeepNewerRecord
   * when two ordering values are compared via Comparable.compareTo.
   *
   * <p>Note: the ClassCastException that arises when DEFAULT_VALUE (Integer 0) is compared against
   * a real ordering value of a different type (e.g. Long) is guarded at the merger level in
   * shouldKeepNewerRecord via OrderingValues.isSameClass, not at this layer.
   */
  @Test
  void testNullFieldValue_singleField_arrayForm() {
    Comparable result = OrderingValues.create(new Comparable[] {null});
    assertEquals(OrderingValues.getDefault(), result);
  }

  @Test
  void testNullFieldValue_singleField_stringArrayFunction() {
    Comparable result = OrderingValues.create(new String[] {"ts"}, field -> null);
    assertEquals(OrderingValues.getDefault(), result);
  }

  @Test
  void testNullFieldValue_singleField_listFunction() {
    Comparable result = OrderingValues.create(Arrays.asList("ts"), field -> null);
    assertEquals(OrderingValues.getDefault(), result);
  }

  @Test
  void testNullFieldValue_multiField_stringArrayFunction() {
    // When one of multiple ordering fields is null, its slot in ArrayComparable must use the default.
    Comparable result = OrderingValues.create(new String[] {"a", "b"}, field -> field.equals("a") ? 5L : null);
    List<Comparable> values = OrderingValues.getValues((ArrayComparable) result);
    assertEquals(5L, values.get(0));
    assertEquals(OrderingValues.getDefault(), values.get(1));
  }

  @Test
  void testNullFieldValue_multiField_arrayForm() {
    Comparable result = OrderingValues.create(new Comparable[] {null, 42L});
    List<Comparable> values = OrderingValues.getValues((ArrayComparable) result);
    assertEquals(OrderingValues.getDefault(), values.get(0));
    assertEquals(42L, values.get(1));
  }

  @Test
  void testNonNullFieldValuePreserved() {
    Comparable result = OrderingValues.create(new String[] {"ts"}, field -> 100L);
    assertEquals(100L, result);
  }

  @Test
  void testNullVsNull_bothDefaultAfterCoercion() {
    // Two records with null ordering fields both coerce to DEFAULT_VALUE and compare equal.
    Comparable a = OrderingValues.create(new String[] {"ts"}, field -> null);
    Comparable b = OrderingValues.create(new String[] {"ts"}, field -> null);
    assertEquals(OrderingValues.getDefault(), a);
    assertEquals(OrderingValues.getDefault(), b);
    assertEquals(0, a.compareTo(b));
  }
}
