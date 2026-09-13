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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SuccessfulRecordCounter}. Covers the driver-side (collected list)
 * counting and error-table unification paths, plus null safety on public entry points.
 */
public class TestSuccessfulRecordCounter {

  @Test
  public void testEmptyInputReturnsZero() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.emptyList(), Option.empty(), false);
    assertEquals(0L, counts.getTotalRecords());
    assertEquals(0L, counts.getTotalErrorRecords());
    assertEquals(0L, counts.getTotalSuccessfulRecords());
    assertFalse(counts.hasErrors());
  }

  @Test
  public void testSingleWriteStatusNoErrors() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(stat(1000L, 0L)), Option.empty(), false);
    assertEquals(1000L, counts.getTotalRecords());
    assertEquals(0L, counts.getTotalErrorRecords());
    assertEquals(1000L, counts.getTotalSuccessfulRecords());
    assertFalse(counts.hasErrors());
  }

  @Test
  public void testMultipleWriteStatusesAreSummed() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Arrays.asList(stat(100L, 5L), stat(200L, 10L)), Option.empty(), false);
    assertEquals(300L, counts.getTotalRecords());
    assertEquals(15L, counts.getTotalErrorRecords());
    assertEquals(285L, counts.getTotalSuccessfulRecords());
    assertTrue(counts.hasErrors());
  }

  @Test
  public void testUnificationDisabledIgnoresErrorTableStatuses() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(stat(50L, 2L)), Option.of(Collections.singletonList(stat(50L, 50L))), false);
    assertEquals(50L, counts.getTotalRecords());
    assertEquals(2L, counts.getTotalErrorRecords());
    assertEquals(48L, counts.getTotalSuccessfulRecords());
  }

  @Test
  public void testHasErrorsBoundary() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(stat(10L, 1L)), Option.empty(), false);
    assertTrue(counts.hasErrors());
  }

  @Test
  public void testUnificationEnabledSumsErrorTable() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Arrays.asList(stat(100L, 5L), stat(200L, 10L)),
        Option.of(Arrays.asList(stat(50L, 50L), stat(25L, 25L))), true);
    assertEquals(375L, counts.getTotalRecords());
    assertEquals(90L, counts.getTotalErrorRecords());
    assertEquals(285L, counts.getTotalSuccessfulRecords());
    assertTrue(counts.hasErrors());
  }

  @Test
  public void testUnificationEnabledWithoutErrorTableStatuses() {
    SuccessfulRecordCounter.Counts counts = SuccessfulRecordCounter.compute(
        Collections.singletonList(stat(100L, 5L)), Option.empty(), true);
    assertEquals(100L, counts.getTotalRecords());
    assertEquals(5L, counts.getTotalErrorRecords());
  }

  @Test
  public void testNullDataTableListRejected() {
    assertThrows(NullPointerException.class, () ->
        SuccessfulRecordCounter.compute(null, Option.empty(), false));
  }

  @Test
  public void testNullErrorTableOptionRejected() {
    assertThrows(NullPointerException.class, () ->
        SuccessfulRecordCounter.compute(Collections.emptyList(), null, false));
  }

  private static WriteStatus stat(long totalRecords, long totalErrorRecords) {
    WriteStatus ws = new WriteStatus(false, 0.0);
    ws.setTotalRecords(totalRecords);
    ws.setTotalErrorRecords(totalErrorRecords);
    return ws;
  }
}
