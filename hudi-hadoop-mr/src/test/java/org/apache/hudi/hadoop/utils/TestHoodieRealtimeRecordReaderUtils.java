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

package org.apache.hudi.hadoop.utils;

import org.apache.hudi.exception.HoodieException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieRealtimeRecordReaderUtils#orderFields}, which maps Hive's
 * {@code hive.io.file.readcolumn.names} and {@code hive.io.file.readcolumn.ids} onto an ordered
 * projection list.
 */
public class TestHoodieRealtimeRecordReaderUtils {

  @Test
  public void testOrderFieldsSortsNamesByTheirHivePosition() {
    assertEquals(Arrays.asList("rider", "driver", "fare"),
        HoodieRealtimeRecordReaderUtils.orderFields("driver,fare,rider", "1,2,0", Collections.emptyList()));
  }

  @Test
  public void testOrderFieldsReturnsEmptyForEmptyInput() {
    assertEquals(Collections.emptyList(),
        HoodieRealtimeRecordReaderUtils.orderFields("", "", Collections.emptyList()));
  }

  /**
   * Hive can repeat a name in the read-column list while keeping ids unique, which the method
   * deliberately tolerates by de-duplicating both sides before pairing them.
   */
  @Test
  public void testOrderFieldsDeduplicatesRepeatedNames() {
    assertEquals(Arrays.asList("rider", "driver"),
        HoodieRealtimeRecordReaderUtils.orderFields("rider,driver,rider", "0,1", Collections.emptyList()));
  }

  /**
   * The counts compared are the de-duplicated ones, so the failure has to report those. Reporting the raw
   * name count instead prints two equal numbers for a real mismatch, which is unusable when diagnosing
   * something like HUDI-1286. This is the case that fails without the production change.
   */
  @Test
  public void testOrderFieldsMismatchReportsDistinctCountsWhenNamesRepeat() {
    HoodieException thrown = assertThrows(HoodieException.class, () ->
        HoodieRealtimeRecordReaderUtils.orderFields("rider,driver,fare,fare", "0,1,2,3", Collections.emptyList()));
    assertTrue(thrown.getMessage().contains("#distinctFieldNames: 3"),
        () -> "Expected the de-duplicated name count, got: " + thrown.getMessage());
    assertTrue(thrown.getMessage().contains("#distinctFieldPositions: 4"),
        () -> "Expected the position count, got: " + thrown.getMessage());
  }

  /** A mismatch with no duplicates on either side still has to carry both projection lists. */
  @Test
  public void testOrderFieldsMismatchReportsBothProjectionLists() {
    HoodieException thrown = assertThrows(HoodieException.class, () ->
        HoodieRealtimeRecordReaderUtils.orderFields("rider,driver", "0,1,2", Collections.emptyList()));
    assertTrue(thrown.getMessage().contains("read column names: [rider,driver]")
            && thrown.getMessage().contains("read column ids: [0,1,2]"),
        () -> "Expected both projection lists, got: " + thrown.getMessage());
  }

  /**
   * HIVE-22438: for {@code SELECT COUNT(*)} on Hive before 3.0.0 the read-column ids arrive empty and Hive
   * combines them into e.g. {@code ",2,0,3"}. {@code cleanProjectionColumnIds} strips only one leading
   * comma, so a blank token can still reach here. It used to fail on {@code Integer.parseInt} with a bare
   * {@code NumberFormatException} carrying neither list.
   */
  @Test
  public void testOrderFieldsIgnoresBlankIdTokens() {
    assertEquals(Arrays.asList("c", "b"),
        HoodieRealtimeRecordReaderUtils.orderFields("b,c", ",2,0", Collections.emptyList()),
        "a leading blank id token should be ignored rather than parsed");
    assertEquals(Arrays.asList("c", "b"),
        HoodieRealtimeRecordReaderUtils.orderFields("b,c", ",,2,0", Collections.emptyList()),
        "cleanProjectionColumnIds strips only one comma, so more than one blank token can arrive");
  }

  /**
   * The shape reported in #14673 - four names against five id tokens, one of them blank - is what the
   * HIVE-22438 combining produces. Dropping the blank leaves four real ids against four names, so it
   * resolves rather than failing at all: the counts only ever disagreed because the blank was counted.
   */
  @Test
  public void testOrderFieldsResolvesShapeFromIssue14673() {
    assertEquals(Arrays.asList("b", "a", "c", "ts"),
        HoodieRealtimeRecordReaderUtils.orderFields("a,b,c,ts", ",2,0,3,5", Collections.emptyList()),
        "the blank id token was the whole mismatch; without it the projection is well formed");
  }

  /**
   * HUDI-5308 (#7355) removed the filter that dropped partitioning fields from the name list before the
   * comparison, so a partition column in that list now counts towards it. Pins that removal.
   */
  @Test
  public void testOrderFieldsNoLongerFiltersPartitionFields() {
    assertThrows(HoodieException.class, () -> HoodieRealtimeRecordReaderUtils.orderFields(
        "rider,driver,partition_path", "0,1", Collections.singletonList("partition_path")));
  }
}
