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

package org.apache.hudi.sink.buffer;

import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGens;
import org.apache.hudi.sink.bulk.sort.SortOperatorGen;
import org.apache.hudi.sink.exception.MemoryPagesExhaustedException;
import org.apache.hudi.sink.utils.BufferUtils;
import org.apache.hudi.sink.utils.RecordKeySortComparator;
import org.apache.hudi.sink.utils.RecordKeySortKeyComputer;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.operators.sort.HeapSort;
import org.apache.flink.runtime.operators.sort.QuickSort;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.generated.NormalizedKeyComputer;
import org.apache.flink.table.runtime.generated.RecordComparator;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link RowDataBucket}. */
class TestRowDataBucket {

  @ParameterizedTest
  @CsvSource({
      "true, false, 0", "true, false, 1", "true, false, 12",
      "true, false, 10000", "false, false, 10000",
      "true, true, 10000", "false, true, 10000"
  })
  void testSortPreservesArrivalOrder(boolean codegen, boolean heapSort, int count) throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.STRING()),
        DataTypes.FIELD("arrival", DataTypes.INT()),
        DataTypes.FIELD("payload", DataTypes.STRING())).getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(32 * 1024, 4 * 1024 * 1024);
    int initialFreePages = pool.freePages();
    NormalizedKeyComputer keyComputer;
    RecordComparator comparator;
    if (codegen) {
      SortOperatorGen generator = new SortOperatorGen(rowType, new String[] {"uuid"});
      ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
      keyComputer = generator.generateNormalizedKeyComputer("StableKeyComputer").newInstance(classLoader);
      comparator = generator.generateRecordComparator("StableComparator").newInstance(classLoader);
    } else {
      Configuration conf = new Configuration();
      conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");
      conf.set(FlinkOptions.PARTITION_PATH_FIELD, "");
      RowDataKeyGen keyGen = RowDataKeyGens.instance(conf, rowType);
      keyComputer = new RecordKeySortKeyComputer(keyGen, 1);
      comparator = new RecordKeySortComparator(keyGen);
    }
    StableSortBuffer buffer = BufferUtils.createStableSortBuffer(rowType, pool, keyComputer, comparator);
    RowDataBucket bucket = new RowDataBucket(
        "bucket-0", buffer, new BucketInfo(BucketType.INSERT, "file-0", "partition-0"), 256.0);
    List<RowData> expected = new ArrayList<>();
    try {
      for (int i = 0; i < count; i++) {
        // Interleaved keys sharing a normalized prefix exercise full-key comparisons as well as
        // equal keys. The large input spans several sort-index pages and exceeds insertion sort.
        // Variable lengths and records larger than a page exercise byte offsets and page padding.
        String payload = repeat('x', i % 997 == 0 ? 40 * 1024 : i % 257);
        RowData row = GenericRowData.of(StringData.fromString("shared-prefix-" + i % 17), i,
            StringData.fromString(payload));
        row.setRowKind(i % 3 == 0 ? RowKind.DELETE : RowKind.UPDATE_AFTER);
        assertTrue(bucket.writeRow(row));
        expected.add(row);
      }
      expected.sort(Comparator.comparing(row -> row.getString(0).toString()));
      if (heapSort) {
        // QuickSort's fallback uses the index-addressed compare/swap overloads.
        new HeapSort().sort(buffer);
      } else {
        bucket.sort();
      }
      MutableObjectIterator<BinaryRowData> iterator = bucket.getDataIterator();
      BinaryRowData reuse = new BinaryRowData(3);
      boolean[] seen = new boolean[count];
      for (RowData row : expected) {
        BinaryRowData actual = iterator.next(reuse);
        assertNotNull(actual);
        assertEquals(row.getString(0), actual.getString(0));
        int arrival = actual.getInt(1);
        assertFalse(seen[arrival], "each input record must be emitted exactly once");
        seen[arrival] = true;
        assertEquals(StringData.fromString("shared-prefix-" + arrival % 17), actual.getString(0));
        assertEquals(row.getInt(1), arrival, "equal keys must retain arrival order");
        assertEquals(row.getString(2), actual.getString(2));
        assertEquals(arrival % 3 == 0 ? RowKind.DELETE : RowKind.UPDATE_AFTER, actual.getRowKind());
      }
      assertNull(iterator.next(reuse));
    } finally {
      bucket.dispose();
    }
    assertEquals(initialFreePages, pool.freePages());
  }

  @ParameterizedTest
  @CsvSource({
      "false, false, 1", "false, true, 1", "true, false, 1", "true, true, 1",
      "false, false, 17", "false, true, 17", "true, false, 17", "true, true, 17"
  })
  void testFullyDeterminingKeysPreserveOffsetOrder(boolean heapSort, boolean descending, int distinctKeys) throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("key", DataTypes.INT()),
        DataTypes.FIELD("arrival", DataTypes.INT())).getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(128, 1024 * 1024);
    int initialFreePages = pool.freePages();
    NormalizedKeyComputer keyComputer = new NormalizedKeyComputer() {
      @Override
      public void putKey(RowData record, MemorySegment target, int offset) {
        target.putInt(offset, record.getInt(0));
      }

      @Override
      public int compareKey(MemorySegment left, int leftOffset, MemorySegment right, int rightOffset) {
        return Integer.compare(left.getInt(leftOffset), right.getInt(rightOffset));
      }

      @Override
      public void swapKey(MemorySegment left, int leftOffset, MemorySegment right, int rightOffset) {
        int key = left.getInt(leftOffset);
        left.putInt(leftOffset, right.getInt(rightOffset));
        right.putInt(rightOffset, key);
      }

      @Override
      public int getNumKeyBytes() {
        return Integer.BYTES;
      }

      @Override
      public boolean isKeyFullyDetermines() {
        return true;
      }

      @Override
      public boolean invertKey() {
        return descending;
      }
    };
    StableSortBuffer buffer = BufferUtils.createStableSortBuffer(rowType, pool, keyComputer, (left, right) -> {
      throw new AssertionError("Fully determining normalized keys must not deserialize records for comparison");
    });
    List<RowData> expected = new ArrayList<>();
    try {
      for (int i = 0; i < 2000; i++) {
        RowData row = GenericRowData.of(i % distinctKeys - 8, i);
        assertTrue(buffer.write(row));
        expected.add(row);
      }
      Comparator<RowData> comparator = Comparator.comparingInt(row -> row.getInt(0));
      expected.sort(descending ? comparator.reversed() : comparator);

      // Scramble index entries using both swap APIs: byte offsets must continue to represent
      // original append order, rather than the positions at which sorting starts.
      buffer.swap(0, buffer.size() - 1);
      buffer.swap(0, buffer.recordSize(), 1, 0);
      if (heapSort) {
        new HeapSort().sort(buffer);
      } else {
        new QuickSort().sort(buffer);
      }
      MutableObjectIterator<BinaryRowData> iterator = buffer.getIterator();
      BinaryRowData reuse = new BinaryRowData(2);
      for (RowData row : expected) {
        BinaryRowData actual = iterator.next(reuse);
        assertNotNull(actual);
        assertEquals(row.getInt(0), actual.getInt(0));
        assertEquals(row.getInt(1), actual.getInt(1));
      }
      assertNull(iterator.next(reuse));
    } finally {
      buffer.dispose();
    }
    assertEquals(initialFreePages, pool.freePages());
  }

  @Test
  void testResetPreservesArrivalOrderAcrossBatches() throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("key", DataTypes.STRING()),
        DataTypes.FIELD("arrival", DataTypes.INT())).getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(32 * 1024, 1024 * 1024);
    int initialFreePages = pool.freePages();
    SortOperatorGen generator = new SortOperatorGen(rowType, new String[] {"key"});
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    StableSortBuffer buffer = BufferUtils.createStableSortBuffer(rowType, pool,
        generator.generateNormalizedKeyComputer("ResetKeyComputer").newInstance(classLoader),
        generator.generateRecordComparator("ResetComparator").newInstance(classLoader));
    int emptyBufferFreePages = pool.freePages();
    try {
      assertEquals((long) initialFreePages * pool.pageSize(), buffer.getCapacity());
      for (int batch = 0; batch < 3; batch++) {
        buffer.reset();
        assertTrue(buffer.isEmpty());
        assertEquals(0, buffer.getOccupancy());
        assertEquals(emptyBufferFreePages, pool.freePages(), "reset must release pages from the previous batch");
        List<RowData> expected = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
          RowData row = GenericRowData.of(StringData.fromString("shared-prefix-" + i % 17), batch * 2000 + i);
          assertTrue(buffer.write(row));
          expected.add(row);
        }
        assertTrue(buffer.getOccupancy() > 0);
        assertTrue(pool.freePages() < emptyBufferFreePages);
        expected.sort(Comparator.comparing(row -> row.getString(0).toString()));
        new QuickSort().sort(buffer);
        MutableObjectIterator<BinaryRowData> iterator = buffer.getIterator();
        BinaryRowData reuse = new BinaryRowData(2);
        for (RowData row : expected) {
          BinaryRowData actual = iterator.next(reuse);
          assertNotNull(actual);
          assertEquals(row.getString(0), actual.getString(0));
          assertEquals(row.getInt(1), actual.getInt(1));
        }
        assertNull(iterator.next(reuse));
      }
    } finally {
      buffer.dispose();
    }
    assertEquals(initialFreePages, pool.freePages());
  }

  @Test
  void testInsufficientPagesDoNotAllocateBuffer() {
    RowType rowType = (RowType) DataTypes.ROW(DataTypes.FIELD("key", DataTypes.INT())).getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(32 * 1024, 64 * 1024);
    int initialFreePages = pool.freePages();
    assertThrows(MemoryPagesExhaustedException.class, () -> BufferUtils.createStableSortBuffer(rowType, pool));
    assertEquals(initialFreePages, pool.freePages());
  }

  @Test
  void testDivergedBucketCannotBeReusedAndReleasesAllPages() throws Exception {
    RowType rowType = (RowType) DataTypes.ROW(
        DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),
        DataTypes.FIELD("payload", DataTypes.VARCHAR(Integer.MAX_VALUE)))
        .notNull()
        .getLogicalType();
    HeapMemorySegmentPool pool = new HeapMemorySegmentPool(32 * 1024, 256 * 1024);
    int initialFreePages = pool.freePages();
    RowDataBucket bucket = new RowDataBucket(
        "bucket-0",
        BufferUtils.createStableSortBuffer(rowType, pool),
        new BucketInfo(BucketType.INSERT, "file-0", "partition-0"),
        256.0);

    List<String> expectedIds = new ArrayList<>();
    String payload = repeat('x', 64 * 1024);
    boolean writeFailed = false;
    try {
      for (int i = 0; i < 100; i++) {
        String id = "uuid-" + i;
        RowData row = TestData.insertRow(
            rowType, StringData.fromString(id), StringData.fromString(payload));
        if (!bucket.writeRow(row)) {
          writeFailed = true;
          break;
        }
        expectedIds.add(id);
      }

      assertTrue(writeFailed, "the tiny pool should cause StableSortBuffer.write to return false");
      assertTrue(bucket.isDiverged());
      assertFalse(bucket.isEmpty(), "successful rows written before exhaustion should remain readable");

      MutableObjectIterator<BinaryRowData> iterator = bucket.getDataIterator();
      BinaryRowData reuse = new BinaryRowData(rowType.getFieldCount());
      List<String> actualIds = new ArrayList<>();
      BinaryRowData row;
      while ((row = iterator.next(reuse)) != null) {
        actualIds.add(row.getString(0).toString());
        assertTrue(payload.equals(row.getString(1).toString()), "variable-length payload should remain intact");
      }
      assertEquals(expectedIds, actualIds, "only successfully indexed rows should be readable");

      RowData extraRow = TestData.insertRow(
          rowType, StringData.fromString("uuid-extra"), StringData.fromString(payload));
      assertThrows(IllegalStateException.class, () -> bucket.writeRow(extraRow));
    } finally {
      bucket.dispose();
    }

    assertEquals(initialFreePages, pool.freePages(), "disposing the diverged bucket should return every page");
  }

  private static String repeat(char value, int count) {
    return String.valueOf(value).repeat(count);
  }
}
