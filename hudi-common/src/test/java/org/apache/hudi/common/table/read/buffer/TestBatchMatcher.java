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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BatchSupport;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Matches buffered log records to rows of base batches by position and by key.
 */
class TestBatchMatcher {

  /**
   * A batch is one column: row indexes (Long) or record keys (String).
   */
  private static final BatchSupport<String, Object[]> BATCHES = new BatchSupport<String, Object[]>() {
    @Override
    public ClosableIterator<Object[]> readBaseFile(HoodieBaseFile baseFile, HoodieSchema readSchema, boolean applyFilters) {
      throw new UnsupportedOperationException();
    }

    @Override
    public int numRows(Object[] batch) {
      return batch.length;
    }

    @Override
    public boolean isNullAt(Object[] batch, int column, int row) {
      return batch[row] == null;
    }

    @Override
    public long getLong(Object[] batch, int column, int row) {
      return (Long) batch[row];
    }

    @Override
    public Object getEngineKey(Object[] batch, int column, int row) {
      return "engine-" + batch[row];
    }

    @Override
    public Object toEngineKey(String recordKey) {
      return "engine-" + recordKey;
    }

    @Override
    public String getRow(Object[] batch, int row) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] select(Object[] batch, int numOutputColumns, int numReadColumns, int[] rowIds, int numRows) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toBatch(List<String> rows) {
      throw new UnsupportedOperationException();
    }
  };

  private static BufferedRecord<String> record(String key) {
    return new BufferedRecord<>(key, 0, "row-" + key, null, null);
  }

  @Test
  void testPositionBasedSkipsBatchesWithoutPositionsAndKeepsLeftovers() {
    BufferedRecord<String> at3 = record("k3");
    BufferedRecord<String> at9 = record("k9");
    BufferedRecord<String> past = record("k100");
    @SuppressWarnings("unchecked")
    BatchMatcher<String> matcher = new BatchMatcher.PositionBased<>(new long[] {3, 9, 100}, new BufferedRecord[] {at3, at9, past});

    // Rows 0..4 hold position 3; rows 5..7 hold none; rows 8..11 have a gap (a pruned row group) before position 9
    Object[] first = {0L, 1L, 2L, 3L, 4L};
    assertFalse(matcher.untouched(BATCHES, first, 0, first.length));
    List<BufferedRecord<String>> taken = new ArrayList<>();
    for (int row = 0; row < first.length; row++) {
      taken.add(matcher.take(BATCHES, first, 0, row));
    }
    assertNull(taken.get(2));
    assertSame(at3, taken.get(3));
    assertNull(taken.get(4));
    assertTrue(matcher.untouched(BATCHES, new Object[] {5L, 6L, 7L}, 0, 3));
    Object[] third = {8L, 9L, 10L, 11L};
    assertFalse(matcher.untouched(BATCHES, third, 0, third.length));
    assertNull(matcher.take(BATCHES, third, 0, 0));
    assertSame(at9, matcher.take(BATCHES, third, 0, 1));
    assertNull(matcher.take(BATCHES, third, 0, 1), "A taken record is not returned twice");

    List<BufferedRecord<String>> leftovers = new ArrayList<>();
    matcher.leftovers().forEachRemaining(leftovers::add);
    assertEquals(1, leftovers.size());
    assertSame(past, leftovers.get(0));
  }

  @Test
  void testKeyBasedMatchesByEngineKey() {
    Map<Object, BufferedRecord<String>> byKey = new HashMap<>();
    BufferedRecord<String> a = record("a");
    BufferedRecord<String> b = record("b");
    byKey.put(BATCHES.toEngineKey("a"), a);
    byKey.put(BATCHES.toEngineKey("b"), b);
    BatchMatcher<String> matcher = new BatchMatcher.KeyBased<>(byKey);

    Object[] batch = {"x", "a", null, "a"};
    assertFalse(matcher.untouched(BATCHES, batch, 0, batch.length));
    assertNull(matcher.take(BATCHES, batch, 0, 0));
    assertSame(a, matcher.take(BATCHES, batch, 0, 1));
    assertNull(matcher.take(BATCHES, batch, 0, 2), "A null record key matches nothing");
    assertNull(matcher.take(BATCHES, batch, 0, 3), "A taken record is not returned twice");

    List<BufferedRecord<String>> leftovers = new ArrayList<>();
    matcher.leftovers().forEachRemaining(leftovers::add);
    assertEquals(1, leftovers.size());
    assertSame(b, leftovers.get(0));
  }
}
