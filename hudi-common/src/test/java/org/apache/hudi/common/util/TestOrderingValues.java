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

import static org.apache.hudi.common.util.OrderingValues.getDefault;
import static org.apache.hudi.common.util.OrderingValues.isBaseOrderingHigher;
import static org.apache.hudi.common.util.OrderingValues.isCommitTimeOrderingValue;
import static org.apache.hudi.common.util.OrderingValues.isSameClass;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOrderingValues {

  @Test
  void testIsCommitTimeOrderingValue() {
    assertTrue(isCommitTimeOrderingValue(null));
    assertTrue(isCommitTimeOrderingValue(getDefault()));
    assertFalse(isCommitTimeOrderingValue(0L));
    assertFalse(isCommitTimeOrderingValue(100L));
  }

  @Test
  void testIsSameClass() {
    assertTrue(isSameClass(1L, 2L));
    assertFalse(isSameClass(1, 2L));
    assertFalse(isSameClass(null, 2L));
    assertFalse(isSameClass(2L, null));
    assertFalse(isSameClass(null, null));
    assertTrue(isSameClass(array(1L, "a"), array(2L, "b")));
    assertFalse(isSameClass(array(1L, "a"), array(getDefault(), "b")));
    assertFalse(isSameClass(array(1L, null), array(2L, "b")));
    assertFalse(isSameClass(array(1L, "a"), array(2L, null)));
  }

  /**
   * A stored null reaches the mergers either as null or as the default value, and ranks below any incoming value.
   */
  @Test
  void testNullOrDefaultBaseRanksLowest() {
    assertFalse(isBaseOrderingHigher(null, 100L));
    assertFalse(isBaseOrderingHigher(getDefault(), 100L));
    assertFalse(isBaseOrderingHigher(getDefault(), -100L));
    assertFalse(isBaseOrderingHigher(getDefault(), "a"));
    assertFalse(isBaseOrderingHigher(null, null));
  }

  /**
   * The default value is a real value of an int ordering field, so it is compared as one.
   */
  @Test
  void testDefaultBaseComparesWithSameClassIncoming() {
    assertTrue(isBaseOrderingHigher(getDefault(), -3));
    assertFalse(isBaseOrderingHigher(getDefault(), 0));
    assertFalse(isBaseOrderingHigher(getDefault(), 3));
  }

  @Test
  void testSameClassValuesCompare() {
    assertTrue(isBaseOrderingHigher(200L, 100L));
    assertFalse(isBaseOrderingHigher(50L, 100L));
    assertFalse(isBaseOrderingHigher(100L, 100L));
  }

  @Test
  void testMismatchedValuesThrow() {
    IllegalArgumentException mismatched = assertThrows(IllegalArgumentException.class, () -> isBaseOrderingHigher(5, 100L));
    assertEquals("Cannot compare ordering values of different types: java.lang.Integer and java.lang.Long", mismatched.getMessage());
    // An incoming null or default value is not a stored null and still fails against a real stored value.
    assertThrows(IllegalArgumentException.class, () -> isBaseOrderingHigher(100L, null));
    assertThrows(IllegalArgumentException.class, () -> isBaseOrderingHigher(100L, getDefault()));
  }

  @Test
  void testMultiFieldValuesComparePerField() {
    assertFalse(isBaseOrderingHigher(array(5L, getDefault()), array(5L, 3L)));
    assertFalse(isBaseOrderingHigher(array(5L, null), array(5L, 3L)));
    assertTrue(isBaseOrderingHigher(array(7L, getDefault()), array(5L, 3L)));
    assertFalse(isBaseOrderingHigher(array(getDefault(), 9L), array(5L, 3L)));
    assertTrue(isBaseOrderingHigher(array(5L, 9L), array(5L, 3L)));
    assertFalse(isBaseOrderingHigher(array(5L, 3L), array(5L, 3L)));
    assertThrows(IllegalArgumentException.class, () -> isBaseOrderingHigher(array(5L, 3L), array(5L, getDefault())));
  }

  private static ArrayComparable array(Comparable... values) {
    return (ArrayComparable) OrderingValues.create(values);
  }
}
