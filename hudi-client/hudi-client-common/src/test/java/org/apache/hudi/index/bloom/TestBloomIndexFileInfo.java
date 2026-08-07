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

package org.apache.hudi.index.bloom;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link BloomIndexFileInfo} accessors and key-range checks.
 */
public class TestBloomIndexFileInfo {

  @Test
  void fileIdOnlyConstructorLeavesRangeUnset() {
    BloomIndexFileInfo info = new BloomIndexFileInfo("f1");
    assertEquals("f1", info.getFileId());
    assertNull(info.getMinRecordKey());
    assertNull(info.getMaxRecordKey());
    assertFalse(info.hasKeyRanges());
  }

  @Test
  void fullConstructorPopulatesRange() {
    BloomIndexFileInfo info = new BloomIndexFileInfo("f1", "key05", "key20");
    assertEquals("key05", info.getMinRecordKey());
    assertEquals("key20", info.getMaxRecordKey());
    assertTrue(info.hasKeyRanges());
  }

  @Test
  void isKeyInRangeIsInclusiveOfBounds() {
    BloomIndexFileInfo info = new BloomIndexFileInfo("f1", "key05", "key20");
    assertTrue(info.isKeyInRange("key05"));
    assertTrue(info.isKeyInRange("key10"));
    assertTrue(info.isKeyInRange("key20"));
    assertFalse(info.isKeyInRange("key04"));
    assertFalse(info.isKeyInRange("key21"));
  }

  @Test
  void isKeyInRangeRequiresBounds() {
    BloomIndexFileInfo noRange = new BloomIndexFileInfo("f1");
    // isKeyInRange does not guard on hasKeyRanges(); with no bounds set it currently
    // fails fast with an NPE from Objects.requireNonNull rather than returning false.
    assertThrows(NullPointerException.class, () -> noRange.isKeyInRange("key10"));
  }

  @Test
  void valueSemanticsForEqualsAndHashCode() {
    BloomIndexFileInfo a = new BloomIndexFileInfo("f1", "min", "max");
    BloomIndexFileInfo same = new BloomIndexFileInfo("f1", "min", "max");
    BloomIndexFileInfo different = new BloomIndexFileInfo("f2", "min", "max");
    assertEquals(a, same);
    assertEquals(a.hashCode(), same.hashCode());
    assertNotEquals(a, different);
  }
}
