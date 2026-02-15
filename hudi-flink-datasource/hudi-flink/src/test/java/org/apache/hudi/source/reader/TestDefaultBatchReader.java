/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.source.reader;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link DefaultHoodieBatchReader}.
 */
public class TestDefaultBatchReader {
  private RowType rowType = RowType.of(VarCharType.STRING_TYPE);
  private RecordCloner recordCloner = a -> a;

  @Test
  public void testBatchWithDefaultSize() throws Exception {
    Configuration config = new Configuration();
    // Default batch size is 2048
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(5000);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // First batch should have 2048 records
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> firstBatch = batchIterator.next();
    assertNotNull(firstBatch);
    assertEquals(split.splitId(), firstBatch.nextSplit());

    int firstBatchCount = countRecords(firstBatch);
    assertEquals(2048, firstBatchCount);

    // Second batch should have 2048 records
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> secondBatch = batchIterator.next();
    int secondBatchCount = countRecords(secondBatch);
    assertEquals(2048, secondBatchCount);

    // Third batch should have remaining 904 records (5000 - 2048 - 2048)
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> thirdBatch = batchIterator.next();
    int thirdBatchCount = countRecords(thirdBatch);
    assertEquals(904, thirdBatchCount);

    // No more batches
    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchWithCustomSize() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 100);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(250);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // First batch should have 100 records
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> firstBatch = batchIterator.next();
    assertEquals(100, countRecords(firstBatch));

    // Second batch should have 100 records
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> secondBatch = batchIterator.next();
    assertEquals(100, countRecords(secondBatch));

    // Third batch should have 50 records
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> thirdBatch = batchIterator.next();
    assertEquals(50, countRecords(thirdBatch));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchWithEmptyInput() throws Exception {
    Configuration config = new Configuration();
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = Collections.emptyList();
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchWithSingleRecord() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = Collections.singletonList("single-record");
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
    assertEquals(1, countRecords(batch));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchWithExactBatchSize() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 100);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(100);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
    assertEquals(100, countRecords(batch));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchWithLessThanBatchSize() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 1000);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(50);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
    assertEquals(50, countRecords(batch));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testNextWithoutHasNext() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(5);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Should work without calling hasNext() first
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
    assertEquals(5, countRecords(batch));

    // Calling next() when there's no data should throw
    assertThrows(NoSuchElementException.class, () -> batchIterator.next());

    batchIterator.close();
  }

  @Test
  public void testSeekWithConsumedRecords() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(100);
    // Create a split with 20 already consumed records
    HoodieSourceSplit split = createTestSplit(20);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Should skip first 20 records and return remaining 80 in batches
    int totalRead = 0;
    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      totalRead += countRecords(batch);
    }

    assertEquals(80, totalRead);

    batchIterator.close();
  }

  @Test
  public void testSeekBeyondAvailableRecords() {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(50);
    // Try to consume from position 100, but only 50 records available
    HoodieSourceSplit split = createTestSplit(100);

    assertThrows(IllegalStateException.class, () -> {
      batchReader.batch(split, createClosableIterator(data));
    });
  }

  @Test
  public void testCloseIterator() throws Exception {
    Configuration config = new Configuration();
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(10);
    HoodieSourceSplit split = createTestSplit(0);

    TestClosableIterator<String> closableIterator = new TestClosableIterator(data.iterator());
    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, closableIterator);

    assertFalse(closableIterator.isClosed());

    batchIterator.close();

    assertTrue(closableIterator.isClosed());
  }

  @Test
  public void testMultipleSplitsWithDifferentOffsets() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    // Test first split with no consumed records
    List<String> data1 = createTestData(30);
    HoodieSourceSplit split1 = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> iter1 =
        batchReader.batch(split1, createClosableIterator(data1));
    int total1 = 0;
    while (iter1.hasNext()) {
      total1 += countRecords(iter1.next());
    }
    assertEquals(30, total1);
    iter1.close();

    // Test second split with 10 consumed records
    List<String> data2 = createTestData(30);
    HoodieSourceSplit split2 = createTestSplit(10);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> iter2 =
        batchReader.batch(split2, createClosableIterator(data2));
    int total2 = 0;
    while (iter2.hasNext()) {
      total2 += countRecords(iter2.next());
    }
    assertEquals(20, total2);
    iter2.close();
  }

  @Test
  public void testBatchPreservesRecordOrder() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 5);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // First batch: A, B, C, D, E
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch1 = batchIterator.next();
    List<String> batch1Records = collectRecordData(batch1);
    assertEquals(Arrays.asList("A", "B", "C", "D", "E"), batch1Records);

    // Second batch: F, G, H, I, J
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch2 = batchIterator.next();
    List<String> batch2Records = collectRecordData(batch2);
    assertEquals(Arrays.asList("F", "G", "H", "I", "J"), batch2Records);

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testBatchSizeOfOne() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 1);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(5);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Should get 5 batches of 1 record each
    for (int i = 0; i < 5; i++) {
      assertTrue(batchIterator.hasNext());
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      assertEquals(1, countRecords(batch));
    }

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testLargeBatchSize() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 100000);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(1000);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Should get all 1000 records in one batch
    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
    assertEquals(1000, countRecords(batch));

    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testMultipleHasNextCalls() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, recordCloner);

    List<String> data = createTestData(15);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Multiple hasNext() calls should not affect the result
    assertTrue(batchIterator.hasNext());
    assertTrue(batchIterator.hasNext());
    assertTrue(batchIterator.hasNext());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch1 = batchIterator.next();
    assertEquals(10, countRecords(batch1));

    assertTrue(batchIterator.hasNext());
    assertTrue(batchIterator.hasNext());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch2 = batchIterator.next();
    assertEquals(5, countRecords(batch2));

    assertFalse(batchIterator.hasNext());
    assertFalse(batchIterator.hasNext());

    batchIterator.close();
  }

  @Test
  public void testRecordClonerIsInvoked() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 5);

    // Use a tracking cloner that counts invocations
    TrackingRecordCloner<String> trackingCloner = new TrackingRecordCloner<>();
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, trackingCloner);

    List<String> data = createTestData(12);
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    // Read all batches
    int totalRecords = 0;
    while (batchIterator.hasNext()) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();
      totalRecords += countRecords(batch);
    }

    // Verify cloner was invoked for each record
    assertEquals(12, totalRecords);
    assertEquals(12, trackingCloner.getCloneCount());

    batchIterator.close();
  }

  @Test
  public void testRecordClonerProducesIndependentCopies() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);

    // Use a cloner that creates mutable wrapper objects
    RecordCloner<MutableWrapper> cloner = original -> new MutableWrapper(original.getValue());
    DefaultHoodieBatchReader<MutableWrapper> batchReader = new DefaultHoodieBatchReader<>(config, cloner);

    // Create mutable test data
    List<MutableWrapper> data = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      data.add(new MutableWrapper(i));
    }

    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<MutableWrapper>>> batchIterator =
        batchReader.batch(split, createClosableIteratorForMutable(data));

    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<MutableWrapper>> batch = batchIterator.next();

    // Collect cloned records
    List<MutableWrapper> clonedRecords = new ArrayList<>();
    HoodieRecordWithPosition<MutableWrapper> record;
    while ((record = batch.nextRecordFromSplit()) != null) {
      clonedRecords.add(record.record());
    }

    // Modify original data
    for (MutableWrapper wrapper : data) {
      wrapper.setValue(999);
    }

    // Verify cloned records are unaffected
    for (int i = 0; i < clonedRecords.size(); i++) {
      assertEquals(i, clonedRecords.get(i).getValue(),
          "Cloned record should retain original value");
    }

    batchIterator.close();
  }

  @Test
  public void testClonerWithNullRecords() throws Exception {
    Configuration config = new Configuration();
    config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, 10);

    // Cloner that handles nulls
    RecordCloner<String> nullHandlingCloner = data -> data == null ? null : new String(data);
    DefaultHoodieBatchReader<String> batchReader = new DefaultHoodieBatchReader<>(config, nullHandlingCloner);

    List<String> data = Arrays.asList("A", null, "B", null, "C");
    HoodieSourceSplit split = createTestSplit(0);

    CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> batchIterator =
        batchReader.batch(split, createClosableIterator(data));

    assertTrue(batchIterator.hasNext());
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = batchIterator.next();

    List<String> results = new ArrayList<>();
    HoodieRecordWithPosition<String> record;
    while ((record = batch.nextRecordFromSplit()) != null) {
      results.add(record.record());
    }

    // Verify nulls are preserved
    assertEquals(Arrays.asList("A", null, "B", null, "C"), results);

    batchIterator.close();
  }

  // Helper methods

  private List<String> createTestData(int count) {
    List<String> data = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      data.add("record-" + i);
    }
    return data;
  }

  private HoodieSourceSplit createTestSplit(long consumed) {
    HoodieSourceSplit split = new HoodieSourceSplit(
        1,
        "base-path",
        Option.of(Collections.emptyList()),
        "/test/table",
        "/test/partition",
        "read_optimized",
        "19700101000000000",
        "file-1"
    );
    // Simulate consumed records
    for (long i = 0; i < consumed; i++) {
      split.consume();
    }
    return split;
  }

  private ClosableIterator<String> createClosableIterator(List<String> items) {
    Iterator<String> iterator = items.iterator();
    return new ClosableIterator<String>() {
      @Override
      public void close() {
        // No-op
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public String next() {
        return iterator.next();
      }
    };
  }

  private int countRecords(RecordsWithSplitIds<HoodieRecordWithPosition<String>> records) {
    int count = 0;
    while (records.nextRecordFromSplit() != null) {
      count++;
    }
    return count;
  }

  private List<String> collectRecordData(RecordsWithSplitIds<HoodieRecordWithPosition<String>> records) {
    List<String> result = new ArrayList<>();
    HoodieRecordWithPosition<String> record;
    while ((record = records.nextRecordFromSplit()) != null) {
      result.add(record.record());
    }
    return result;
  }

  private static class TestClosableIterator<T> implements ClosableIterator<T> {
    private final Iterator<T> iterator;
    private boolean closed = false;

    public TestClosableIterator(Iterator<T> iterator) {
      this.iterator = iterator;
    }

    @Override
    public void close() {
      closed = true;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public T next() {
      return iterator.next();
    }

    public boolean isClosed() {
      return closed;
    }
  }

  /**
   * A tracking cloner that counts how many times clone() was called.
   */
  private static class TrackingRecordCloner<T> implements RecordCloner<T> {
    private int cloneCount = 0;

    @Override
    public T clone(T data) {
      cloneCount++;
      return data;
    }

    public int getCloneCount() {
      return cloneCount;
    }
  }

  /**
   * A mutable wrapper class for testing cloning behavior.
   */
  private static class MutableWrapper {
    private int value;

    public MutableWrapper(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }

    public void setValue(int value) {
      this.value = value;
    }
  }

  private ClosableIterator<MutableWrapper> createClosableIteratorForMutable(List<MutableWrapper> items) {
    Iterator<MutableWrapper> iterator = items.iterator();
    return new ClosableIterator<MutableWrapper>() {
      @Override
      public void close() {
        // No-op
      }

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public MutableWrapper next() {
        return iterator.next();
      }
    };
  }
}
