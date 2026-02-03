/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link TableServiceUtils}.
 */
public class TestTableServiceUtils {

  @Test
  public void testIsStaleWhenExceedsMaxProcessingTime() {
    long startTimeMs = 1000L;
    long maxProcessingTimeMs = 500L;
    long currentTimeMs = 2000L;
    // 1000 + 500 = 1500 < 2000, so it's stale
    assertTrue(TableServiceUtils.isStale(startTimeMs, maxProcessingTimeMs, currentTimeMs));
  }

  @Test
  public void testIsNotStaleWhenWithinMaxProcessingTime() {
    long startTimeMs = 1000L;
    long maxProcessingTimeMs = 500L;
    long currentTimeMs = 1400L;
    // 1000 + 500 = 1500 > 1400, so it's not stale
    assertFalse(TableServiceUtils.isStale(startTimeMs, maxProcessingTimeMs, currentTimeMs));
  }

  @Test
  public void testIsNotStaleWhenExactlyAtThreshold() {
    long startTimeMs = 1000L;
    long maxProcessingTimeMs = 500L;
    long currentTimeMs = 1500L;
    // 1000 + 500 = 1500 is NOT < 1500, so it's not stale
    assertFalse(TableServiceUtils.isStale(startTimeMs, maxProcessingTimeMs, currentTimeMs));
  }

  @ParameterizedTest
  @CsvSource({
      "1000, 1000, 2500, true",   // 1000 + 1000 = 2000 < 2500, stale
      "1000, 1000, 1999, false",  // 1000 + 1000 = 2000 > 1999, not stale
      "0, 1000, 1001, true",      // 0 + 1000 = 1000 < 1001, stale
      "0, 1000, 999, false",      // 0 + 1000 = 1000 > 999, not stale
      "0, 0, 1, true"             // 0 + 0 = 0 < 1, stale (edge case: maxProcessingTimeMs = 0)
  })
  public void testIsStaleWithVariousInputs(long startTimeMs, long maxProcessingTimeMs, long currentTimeMs, boolean expectedStale) {
    if (expectedStale) {
      assertTrue(TableServiceUtils.isStale(startTimeMs, maxProcessingTimeMs, currentTimeMs));
    } else {
      assertFalse(TableServiceUtils.isStale(startTimeMs, maxProcessingTimeMs, currentTimeMs));
    }
  }

  @Test
  public void testIsStaleWithCurrentTime() {
    // Test the method that uses System.currentTimeMillis()
    long maxProcessingTimeMs = 60000L; // 1 minute
    // Using a start time far in the past should be stale
    long pastStartTime = System.currentTimeMillis() - 120000L; // 2 minutes ago
    assertTrue(TableServiceUtils.isStale(pastStartTime, maxProcessingTimeMs));

    // Using a start time very recently should not be stale
    long recentStartTime = System.currentTimeMillis() - 10000L; // 10 seconds ago
    assertFalse(TableServiceUtils.isStale(recentStartTime, maxProcessingTimeMs));
  }
}
