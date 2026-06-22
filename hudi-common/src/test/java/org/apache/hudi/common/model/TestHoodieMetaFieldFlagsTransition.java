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

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the meta-field-population state-transition lattice enforced by
 * {@link HoodieMetaFieldFlags#validateTransition(boolean, Set, boolean, Set)}.
 *
 * <p>The lattice (in shorthand: {@code populate.exclude}):
 * <pre>
 *   a (populate=true, exclude=empty)
 *     -> b (populate=true, exclude=subset)
 *     -> c (populate=true, exclude=all 5)
 *     -> off (populate=false)             [terminal/absorbing]
 *
 *   Allowed:   a -> b, a -> c, b -> c, b -> b' (b' superset of b),
 *              any populate=true state -> off,
 *              off -> off.
 *   Rejected:  any narrowing of the exclude set when populate stays true,
 *              off -> a/b/c.
 * </pre>
 */
class TestHoodieMetaFieldFlagsTransition {

  private static final String COMMIT_TIME = HoodieRecord.COMMIT_TIME_METADATA_FIELD;
  private static final String COMMIT_SEQNO = HoodieRecord.COMMIT_SEQNO_METADATA_FIELD;
  private static final String RECORD_KEY = HoodieRecord.RECORD_KEY_METADATA_FIELD;
  private static final String PARTITION_PATH = HoodieRecord.PARTITION_PATH_METADATA_FIELD;
  private static final String FILE_NAME = HoodieRecord.FILENAME_METADATA_FIELD;

  // ------- allowed transitions -------

  @Test
  void allowedEmptyToSubset() {
    assertAllowed(true, empty(),
        true, setOf(RECORD_KEY, PARTITION_PATH));
  }

  @Test
  void allowedEmptyToSingleField() {
    assertAllowed(true, empty(),
        true, setOf(FILE_NAME));
  }

  @Test
  void allowedEmptyToAllFive() {
    assertAllowed(true, empty(),
        true, setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME));
  }

  @Test
  void allowedSubsetToWiderSubset() {
    assertAllowed(true, setOf(RECORD_KEY),
        true, setOf(RECORD_KEY, PARTITION_PATH, FILE_NAME));
  }

  @Test
  void allowedSubsetToAllFive() {
    assertAllowed(true, setOf(RECORD_KEY, PARTITION_PATH, FILE_NAME),
        true, setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME));
  }

  @Test
  void idempotentNoOpFromEmpty() {
    assertAllowed(true, empty(), true, empty());
  }

  @Test
  void idempotentNoOpFromSubset() {
    assertAllowed(true, setOf(RECORD_KEY, FILE_NAME),
        true, setOf(RECORD_KEY, FILE_NAME));
  }

  @Test
  void idempotentNoOpFromAllFive() {
    Set<String> all = setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME);
    assertAllowed(true, all, true, all);
  }

  @Test
  void allowedPopulateTrueToFalseFromEmpty() {
    assertAllowed(true, empty(), false, empty());
  }

  @Test
  void allowedPopulateTrueToFalseFromSubset() {
    // Exclude list on the "after" side is irrelevant once populate=false; still must not throw.
    assertAllowed(true, setOf(RECORD_KEY, FILE_NAME), false, setOf(RECORD_KEY, FILE_NAME));
  }

  @Test
  void allowedPopulateTrueToFalseFromAllFive() {
    Set<String> all = setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME);
    assertAllowed(true, all, false, all);
  }

  @Test
  void allowedPopulateFalseToFalse() {
    assertAllowed(false, empty(), false, empty());
  }

  // ------- rejected transitions -------

  @Test
  void rejectedSubsetToEmpty() {
    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(
            true, setOf(RECORD_KEY, FILE_NAME),
            true, empty()));
    assertTrue(ex.getMessage().contains(RECORD_KEY), ex.getMessage());
    assertTrue(ex.getMessage().contains(FILE_NAME), ex.getMessage());
  }

  @Test
  void rejectedAllFiveToEmpty() {
    Set<String> all = setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME);
    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(true, all, true, empty()));
    // Message lists all five fields being removed.
    for (String name : all) {
      assertTrue(ex.getMessage().contains(name),
          "Expected message to mention " + name + ", was: " + ex.getMessage());
    }
  }

  @Test
  void rejectedAllFiveToSubset() {
    Set<String> all = setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME);
    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(true, all,
            true, setOf(RECORD_KEY, PARTITION_PATH, FILE_NAME)));
    assertTrue(ex.getMessage().contains(COMMIT_TIME), ex.getMessage());
    assertTrue(ex.getMessage().contains(COMMIT_SEQNO), ex.getMessage());
  }

  @Test
  void rejectedSwapFieldInSubset() {
    // Swap = drop FILE_NAME + add COMMIT_SEQNO. Rejected because FILE_NAME was removed.
    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(
            true, setOf(RECORD_KEY, FILE_NAME),
            true, setOf(RECORD_KEY, COMMIT_SEQNO)));
    assertTrue(ex.getMessage().contains(FILE_NAME), ex.getMessage());
  }

  @Test
  void offToPopulateTrueRejected() {
    HoodieException ex = assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(false, empty(), true, empty()));
    assertTrue(ex.getMessage().contains("false to true"), ex.getMessage());
  }

  @Test
  void offToPopulateTrueRejected_evenIfNewExcludeListIsFull() {
    Set<String> all = setOf(COMMIT_TIME, COMMIT_SEQNO, RECORD_KEY, PARTITION_PATH, FILE_NAME);
    assertThrows(HoodieException.class, () ->
        HoodieMetaFieldFlags.validateTransition(false, empty(), true, all));
  }

  // ------- invalid input -------

  @Test
  void unknownFieldInNewExcludeRejected() {
    IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () ->
        HoodieMetaFieldFlags.validateTransition(true, empty(),
            true, setOf(RECORD_KEY, "_hoodie_typo")));
    assertTrue(ex.getMessage().contains("_hoodie_typo"), ex.getMessage());
  }

  @Test
  void nullExcludeSetsTreatedAsEmpty() {
    // null old + null new under populate=true,true should be a no-op (a->a).
    assertDoesNotThrow(() -> HoodieMetaFieldFlags.validateTransition(true, null, true, null));
  }

  // ------- helpers -------

  private static Set<String> empty() {
    return Collections.emptySet();
  }

  private static Set<String> setOf(String... fields) {
    return new HashSet<>(java.util.Arrays.asList(fields));
  }

  private static void assertAllowed(boolean oldPopulate, Set<String> oldExclude,
                                    boolean newPopulate, Set<String> newExclude) {
    assertDoesNotThrow(() ->
        HoodieMetaFieldFlags.validateTransition(oldPopulate, oldExclude, newPopulate, newExclude));
  }
}
