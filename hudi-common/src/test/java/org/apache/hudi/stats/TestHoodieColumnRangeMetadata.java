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

package org.apache.hudi.stats;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Tests {@link HoodieColumnRangeMetadata#merge} — the helper used by
 * {@code PartitionStatsIndexSupport} (and clustering) to aggregate per-file
 * column ranges into per-partition aggregates.
 *
 * <p>Distinguishes two kinds of "null min/max" inputs:
 * <ul>
 *   <li><b>all-null column</b>: {@code nullCount == valueCount > 0} — min/max are
 *       legitimately null because there are no non-null values to bound. Safe to
 *       drop in merge so the other side's bounds remain authoritative.</li>
 *   <li><b>stats unreliable</b>: {@code nullCount < valueCount} but min/max are
 *       still null — e.g. parquet-mr cleared stats due to NaN, or stats were
 *       omitted due to value-size truncation. Dropping this side's null silently
 *       would let data-skipping prune the partition based on a partial bound
 *       (the read-side half of #18754). The merge must propagate null instead.</li>
 * </ul>
 */
public class TestHoodieColumnRangeMetadata {

  private static final String FILE = "f.parquet";
  private static final String COL = "v";

  private static HoodieColumnRangeMetadata<Double> range(Double min, Double max, long nullCount, long valueCount) {
    return HoodieColumnRangeMetadata.create(
        FILE, COL, min, max, nullCount, valueCount, 0L, 0L, ValueMetadata.V1EmptyMetadata.get());
  }

  @Test
  public void testMerge_bothReliable_takesMinAndMax() {
    HoodieColumnRangeMetadata<Double> a = range(100.0, 200.0, 0L, 10L);
    HoodieColumnRangeMetadata<Double> b = range(150.0, 300.0, 0L, 10L);
    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(a, b);

    assertEquals(100.0, merged.getMinValue());
    assertEquals(300.0, merged.getMaxValue());
    assertEquals(0L, merged.getNullCount());
    assertEquals(20L, merged.getValueCount());
  }

  @Test
  public void testMerge_oneAllNullColumn_dropsItAndKeepsOtherBounds() {
    // All-null is the legitimate "no non-null values" case. The other side's
    // bounds are still authoritative; null counts add.
    HoodieColumnRangeMetadata<Double> reliable = range(100.0, 200.0, 0L, 10L);
    HoodieColumnRangeMetadata<Double> allNull = range(null, null, /*nullCount=*/5L, /*valueCount=*/5L);

    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(reliable, allNull);

    assertEquals(100.0, merged.getMinValue(), "All-null side should not poison the merged min");
    assertEquals(200.0, merged.getMaxValue(), "All-null side should not poison the merged max");
    assertEquals(5L, merged.getNullCount());
    assertEquals(15L, merged.getValueCount());
  }

  @Test
  public void testMerge_oneUnreliable_propagatesNullToProtectAgainstWrongPruning() {
    // Unreliable means: min/max are null even though the file has non-null values
    // (nullCount < valueCount). Dropping null here would let data-skipping prune
    // the partition based on a partial bound that excludes the unreliable file's
    // values. Propagate null so the merged record signals "unknown bound".
    HoodieColumnRangeMetadata<Double> reliable = range(100.0, 200.0, 0L, 10L);
    HoodieColumnRangeMetadata<Double> unreliable = range(null, null, /*nullCount=*/0L, /*valueCount=*/5L);

    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(reliable, unreliable);

    assertNull(merged.getMinValue(),
        "Merging a reliable bound with an unreliable null must propagate null — not silently drop the unreliable side");
    assertNull(merged.getMaxValue(),
        "Same for max");
    // Counts still aggregate accurately so downstream readers can reason about row counts.
    assertEquals(0L, merged.getNullCount());
    assertEquals(15L, merged.getValueCount());
  }

  @Test
  public void testMerge_oneUnreliableOnLeftSide_isSymmetric() {
    // Same scenario but with the unreliable input on the left — must produce the same outcome.
    HoodieColumnRangeMetadata<Double> unreliable = range(null, null, /*nullCount=*/0L, /*valueCount=*/5L);
    HoodieColumnRangeMetadata<Double> reliable = range(100.0, 200.0, 0L, 10L);

    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(unreliable, reliable);

    assertNull(merged.getMinValue());
    assertNull(merged.getMaxValue());
    assertEquals(15L, merged.getValueCount());
  }

  @Test
  public void testMerge_bothUnreliable_propagatesNull() {
    HoodieColumnRangeMetadata<Double> u1 = range(null, null, 0L, 3L);
    HoodieColumnRangeMetadata<Double> u2 = range(null, null, 1L, 5L);

    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(u1, u2);

    assertNull(merged.getMinValue());
    assertNull(merged.getMaxValue());
    assertEquals(1L, merged.getNullCount());
    assertEquals(8L, merged.getValueCount());
  }

  @Test
  public void testMerge_bothAllNull_keepsNullBoundsLegitimately() {
    // Two all-null inputs is still a legitimate all-null aggregate.
    HoodieColumnRangeMetadata<Double> a = range(null, null, 3L, 3L);
    HoodieColumnRangeMetadata<Double> b = range(null, null, 5L, 5L);

    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(a, b);

    assertNull(merged.getMinValue());
    assertNull(merged.getMaxValue());
    assertEquals(8L, merged.getNullCount());
    assertEquals(8L, merged.getValueCount());
  }

  @Test
  public void testMerge_oneSideNull_returnsOther() {
    // The pre-existing contract: merging with a null record returns the other.
    HoodieColumnRangeMetadata<Double> a = range(100.0, 200.0, 0L, 10L);
    HoodieColumnRangeMetadata<Double> merged = HoodieColumnRangeMetadata.merge(a, null);
    assertNotNull(merged);
    assertEquals(100.0, merged.getMinValue());
    assertEquals(200.0, merged.getMaxValue());
  }
}
