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
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestOrderingValues {

  @Test
  void isBaseOrderingHigherComparesSameClassValues() {
    assertTrue(OrderingValues.isBaseOrderingHigher(2000L, 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, 2000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, 1000L));
  }

  @Test
  void isBaseOrderingHigherDefersToNaturalOrderForDifferentClasses() {
    // The default sentinel is an int; the counterpart is a Long. A raw compareTo would throw
    // ClassCastException, so the mixed-class comparison resolves to natural order (not higher).
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, OrderingValues.getDefault()));
  }

  @Test
  void isBaseOrderingHigherTreatsCommitTimeOrderingValuesAsNotHigher() {
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), OrderingValues.getDefault()));
    assertFalse(OrderingValues.isBaseOrderingHigher(null, 1000L));
    assertFalse(OrderingValues.isBaseOrderingHigher(1000L, null));
    assertFalse(OrderingValues.isBaseOrderingHigher(null, null));
  }

  @Test
  void isBaseOrderingHigherHandlesMultiFieldOrderingValues() {
    Comparable higher = OrderingValues.create(new Comparable[] {2L, 1L});
    Comparable lower = OrderingValues.create(new Comparable[] {1L, 1L});
    assertTrue(OrderingValues.isBaseOrderingHigher(higher, lower));
    assertFalse(OrderingValues.isBaseOrderingHigher(lower, higher));
    assertFalse(OrderingValues.isBaseOrderingHigher(OrderingValues.getDefault(), higher));
    assertFalse(OrderingValues.isBaseOrderingHigher(higher, OrderingValues.getDefault()));
  }
}
