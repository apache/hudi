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

package org.apache.hudi.common.model;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodiePreWriteCleanerPolicy}.
 */
public class TestHoodiePreWriteCleanerPolicy {

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {" ", "NONE", "none"})
  void testParsesToNone(String value) {
    assertEquals(HoodiePreWriteCleanerPolicy.NONE, HoodiePreWriteCleanerPolicy.fromString(value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"clean", "CLEAN", "Clean", " clean "})
  void testParsesToClean(String value) {
    assertEquals(HoodiePreWriteCleanerPolicy.CLEAN, HoodiePreWriteCleanerPolicy.fromString(value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"rollback_failed_writes", "ROLLBACK_FAILED_WRITES", " rollback_failed_writes "})
  void testParsesToRollbackFailedWrites(String value) {
    assertEquals(HoodiePreWriteCleanerPolicy.ROLLBACK_FAILED_WRITES,
        HoodiePreWriteCleanerPolicy.fromString(value));
  }

  @ParameterizedTest
  @ValueSource(strings = {"async_rollback_failed_writes", "ASYNC_ROLLBACK_FAILED_WRITES",
      "Async_Rollback_Failed_Writes", " async_rollback_failed_writes "})
  void testParsesToAsyncRollbackFailedWrites(String value) {
    assertEquals(HoodiePreWriteCleanerPolicy.ASYNC_ROLLBACK_FAILED_WRITES,
        HoodiePreWriteCleanerPolicy.fromString(value));
  }

  @Test
  void testAsyncIsDistinctFromSyncRollback() {
    // The async policy must not be mistaken for the synchronous one: the inline pre-write hook is a
    // no-op under async, so treating them as equivalent would silently run the rollback twice.
    HoodiePreWriteCleanerPolicy async = HoodiePreWriteCleanerPolicy.ASYNC_ROLLBACK_FAILED_WRITES;
    assertTrue(async.isAsyncRollbackFailedWrites());
    assertFalse(async.isRollbackFailedWrites());
    assertFalse(async.isClean());
    assertFalse(async.isNone());

    HoodiePreWriteCleanerPolicy sync = HoodiePreWriteCleanerPolicy.ROLLBACK_FAILED_WRITES;
    assertTrue(sync.isRollbackFailedWrites());
    assertFalse(sync.isAsyncRollbackFailedWrites());
  }

  @Test
  void testUnknownValueThrows() {
    assertThrows(IllegalArgumentException.class,
        () -> HoodiePreWriteCleanerPolicy.fromString("not_a_policy"));
  }
}
