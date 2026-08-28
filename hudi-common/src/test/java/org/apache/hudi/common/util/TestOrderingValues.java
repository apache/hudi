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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOrderingValues {

  @Test
  void isBaseOrderingHigherComparesSameClassValues() {
    assertTrue(OrderingValues.isBaseOrderingHigher(2000L, 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, 2000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, 1000L));
  }

  @Test
  void isBaseOrderingHigherReturnsFalseWhenBaseIsNullOrDefault() {
    // A null or default (commit-time) base ordering value ranks lowest, so it never outranks the
    // incoming record and the incoming record wins by natural order.
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(null, 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), OrderingValues.getDefault()));
    assertFalse(OrderingValues.isBaseOrderingHigher(null, null));
  }

  @Test
  void isBaseOrderingHigherThrowsOnMismatchedClasses() {
    // A real base value against a null/default (int) incoming value is not comparable. The incoming
    // value is expected to be a real value of the same class, so the mismatch is surfaced.
    assertThrows(IllegalArgumentException.class,
        () -> OrderingValues.isBaseOrderingHigher(1000L, OrderingValues.getDefault()));
  }

  @Test
  void isBaseOrderingHigherHandlesMultiFieldOrderingValues() {
    Comparable higher = OrderingValues.create(new Comparable[] {2L, 1L});
    Comparable lower = OrderingValues.create(new Comparable[] {1L, 1L});
    assertTrue(OrderingValues.isBaseOrderingHigher(higher, lower));
    assertFalse(OrderingValues.isBaseOrderingHigher(lower, higher));
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), higher));
    assertThrows(IllegalArgumentException.class,
        () -> OrderingValues.isBaseOrderingHigher(higher, OrderingValues.getDefault()));
  }
}
