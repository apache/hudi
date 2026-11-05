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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieEnumeratorPosition {

  @Test
  void testEmptyAndNullStringsProduceEmptyPosition() {
    HoodieEnumeratorPosition empty = HoodieEnumeratorPosition.empty();

    assertEquals(empty, HoodieEnumeratorPosition.of(null, ""));
    assertFalse(empty.getIssuedInstant().isPresent());
    assertFalse(empty.getIssuedOffset().isPresent());
  }

  @Test
  void testStringAndOptionFactoriesPreservePosition() {
    HoodieEnumeratorPosition fromStrings = HoodieEnumeratorPosition.of("001", "002");
    HoodieEnumeratorPosition fromOptions =
        HoodieEnumeratorPosition.of(Option.of("001"), Option.of("002"));

    assertEquals(fromStrings, fromOptions);
    assertEquals(fromStrings.hashCode(), fromOptions.hashCode());
    assertEquals("001", fromStrings.getIssuedInstant().get());
    assertEquals("002", fromStrings.getIssuedOffset().get());
    assertTrue(fromStrings.toString().contains("001"));
    assertNotEquals(fromStrings, HoodieEnumeratorPosition.empty());
  }
}
