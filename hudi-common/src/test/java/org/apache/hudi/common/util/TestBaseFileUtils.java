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

import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestBaseFileUtils {
  private static final String PARTITION_PATH = "partition";
  private static final String COLUMN_NAME = "columnName";

  @Test
  public void testGetColumnRangeInPartition() {
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", COLUMN_NAME, 1, 5, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", COLUMN_NAME, 3, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());
    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    // Step 2: Call the Method
    HoodieColumnRangeMetadata<Comparable> result = FileFormatUtils.getColumnRangeInPartition(PARTITION_PATH, COLUMN_NAME, fileColumnRanges, Collections.emptyMap(), indexVersion);
    // Step 3: Assertions
    assertEquals(PARTITION_PATH, result.getFilePath());
    assertEquals(COLUMN_NAME, result.getColumnName());
    assertEquals(Integer.valueOf(1), new Integer(result.getMinValue().toString()));
    assertEquals(Integer.valueOf(8), new Integer(result.getMaxValue().toString()));
    assertEquals(1, result.getNullCount());
    assertEquals(25, result.getValueCount());
    assertEquals(220, result.getTotalSize());
    assertEquals(450, result.getTotalUncompressedSize());
  }

  @Test
  public void testGetColumnRangeInPartitionWithNullMinMax() {
    // This test exercises {@link HoodieColumnRangeMetadata#merge} with one side
    // having a null bound while still containing non-null values
    // (nullCount < valueCount). That is the "stats unreliable" signal — parquet-mr
    // sets it when it had to clear stats (e.g. NaN encountered in a FLOAT/DOUBLE
    // column, or value-size truncation suppressed stats); see apache/hudi#18754.
    //
    // Aggregating a reliable bound with an unreliable null MUST propagate null —
    // otherwise data-skipping would prune a partition based on a partial bound
    // that excludes the unreliable file's actual values. Earlier the merge
    // silently dropped the null side and reported the partial bound, which was
    // a correctness bug.
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", COLUMN_NAME, 1, null, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", COLUMN_NAME, null, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());

    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    HoodieColumnRangeMetadata<Comparable> result = FileFormatUtils.getColumnRangeInPartition(
        PARTITION_PATH, COLUMN_NAME, fileColumnRanges, Collections.emptyMap(), indexVersion);

    assertEquals(PARTITION_PATH, result.getFilePath());
    assertEquals(COLUMN_NAME, result.getColumnName());
    // file1 has nullCount=0 / valueCount=10 with null max — unreliable max →
    //   merged max must be null (was previously asserted as 8, which was
    //   silently dropping the null and reporting file2's max only).
    assertNull(result.getMaxValue(),
        "merged max must propagate null when a child range has unreliable max stats");
    // file2 has nullCount=1 / valueCount=15 with null min — unreliable min →
    //   merged min must be null.
    assertNull(result.getMinValue(),
        "merged min must propagate null when a child range has unreliable min stats");
    // Counts still aggregate accurately so downstream readers can reason about row counts.
    assertEquals(1, result.getNullCount());
    assertEquals(25, result.getValueCount());
    assertEquals(220, result.getTotalSize());
    assertEquals(450, result.getTotalUncompressedSize());
  }

  @Test
  public void testGetColumnRangeInPartitionAllNullColumnDroppedFromMerge() {
    // Companion to the test above: when one side is an all-null column
    // (nullCount == valueCount), its null min/max are legitimately null
    // (no non-null values to bound). It is safe to drop that side from the
    // merge — the other side's bounds remain authoritative.
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    HoodieColumnRangeMetadata<Comparable> reliable = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", COLUMN_NAME, 1, 5, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieColumnRangeMetadata<Comparable> allNull = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", COLUMN_NAME, null, null, 7, 7, 60, 80, ValueMetadata.V1EmptyMetadata.get());

    HoodieColumnRangeMetadata<Comparable> result = FileFormatUtils.getColumnRangeInPartition(
        PARTITION_PATH, COLUMN_NAME, Arrays.asList(reliable, allNull), Collections.emptyMap(), indexVersion);

    assertEquals(Integer.valueOf(1), new Integer(result.getMinValue().toString()),
        "all-null side should be dropped from min/max merge");
    assertEquals(Integer.valueOf(5), new Integer(result.getMaxValue().toString()));
    assertEquals(7, result.getNullCount());
    assertEquals(17, result.getValueCount());
  }

  @Test
  public void testGetColumnRangeInPartitionWithDifferentColumnNameThrowsException() {
    HoodieIndexVersion indexVersion = HoodieIndexVersion.V1;
    // Step 1: Set Up Test Data
    HoodieColumnRangeMetadata<Comparable> fileColumnRange1 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file1", "columnName1", 1, null, 0, 10, 100, 200, ValueMetadata.V1EmptyMetadata.get());
    HoodieColumnRangeMetadata<Comparable> fileColumnRange2 = HoodieColumnRangeMetadata.<Comparable>create(
        "path/to/file2", "columnName2", null, 8, 1, 15, 120, 250, ValueMetadata.V1EmptyMetadata.get());
    List<HoodieColumnRangeMetadata<Comparable>> fileColumnRanges = Arrays.asList(fileColumnRange1, fileColumnRange2);
    // Step 2: Call the Method
    assertThrows(IllegalArgumentException.class, () -> FileFormatUtils.getColumnRangeInPartition(PARTITION_PATH, COLUMN_NAME, fileColumnRanges,
        Collections.emptyMap(), indexVersion));
  }
}
