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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.when;

/**
 * Test cases for {@link HoodieSourceSplitReader}.
 */
public class TestHoodieSourceSplitReader {

  private SourceReaderContext mockContext;
  private int subtaskIndex = 0;

  @BeforeEach
  public void setUp() {
    mockContext = Mockito.mock(SourceReaderContext.class);
    when(mockContext.getIndexOfSubtask()).thenReturn(subtaskIndex);
  }

  @Test
  public void testFetchWithNoSplits() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testFetchWithSingleSplit() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2", "record3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    SplitsAddition<HoodieSourceSplit> splitsChange = new SplitsAddition<>(Collections.singletonList(split));
    reader.handleSplitsChanges(splitsChange);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertEquals(split.splitId(), result.nextSplit());
  }

  @Test
  public void testClose() throws Exception {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));
    reader.fetch();

    // Close should not throw exception
    reader.close();

    // After close, fetching should work but return empty results
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testWakeUp() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    // wakeUp is a no-op, should not throw any exception
    reader.wakeUp();
  }

  @Test
  public void testPauseOrResumeSplits() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");

    // pauseOrResumeSplits is currently a no-op, should not throw exception
    reader.pauseOrResumeSplits(
        Collections.singletonList(split1),
        Collections.singletonList(split2)
    );
  }

  @Test
  public void testReaderFunctionCalledCorrectly() throws IOException {
    List<String> testData = Arrays.asList("A", "B", "C");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    reader.fetch();

    assertEquals(1, readerFunction.getReadCount());
    assertEquals(split, readerFunction.getLastReadSplit());
  }

  @Test
  public void testReaderFunctionClosedOnReaderClose() throws Exception {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    reader.close();

    assertTrue(readerFunction.isClosed(), "Reader function should be closed");
  }

  @Test
  public void testFetchEmptyResultWhenNoSplitsAdded() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
    assertEquals(0, readerFunction.getReadCount(), "Should not read any splits");
  }

  @Test
  public void testMiniBatchReading() throws IOException {
    // Create data that will be split into multiple mini batches
    List<String> testData = new ArrayList<>();
    for (int i = 0; i < 5000; i++) {
      testData.add("record-" + i);
    }

    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // Fetch multiple batches from the same split
    // Default batch size is 2048, so we should get 3 batches (2048 + 2048 + 904)
    int totalBatches = 0;
    int totalRecords = 0;

    while (true) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
      String splitId = result.nextSplit();

      if (splitId == null) {
        // Empty result - no more splits
        break;
      }

      totalBatches++;

      // Count records in this batch
      HoodieRecordWithPosition<String> record;
      while ((record = result.nextRecordFromSplit()) != null) {
        totalRecords++;
      }

      // Check if this split is finished
      if (result.finishedSplits().contains(split.splitId())) {
        break;
      }
    }

    // Verify we got multiple batches and all records
    assertTrue(totalBatches >= 3, "Should have at least 3 batches for 5000 records");
    assertEquals(5000, totalRecords, "Should read all 5000 records");
  }

  @Test
  public void testMiniBatchWithSmallBatchSize() throws IOException {
    List<String> testData = Arrays.asList("A", "B", "C", "D", "E", "F", "G", "H", "I", "J");

    // Use a small custom batch size
    TestSplitReaderFunctionWithBatchSize readerFunction =
        new TestSplitReaderFunctionWithBatchSize(testData, 3);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    List<Integer> batchSizes = new ArrayList<>();

    while (true) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
      String splitId = result.nextSplit();

      if (splitId == null) {
        break;
      }

      int batchSize = 0;
      while (result.nextRecordFromSplit() != null) {
        batchSize++;
      }

      if (batchSize > 0) {
        batchSizes.add(batchSize);
      }

      if (result.finishedSplits().contains(split.splitId())) {
        break;
      }
    }

    // With batch size 3 and 10 records, expect: 3, 3, 3, 1
    assertEquals(Arrays.asList(3, 3, 3, 1), batchSizes);
  }

  @Test
  public void testReaderIteratorClosedOnSplitFinish() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // Fetch all batches until split is finished
    while (true) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
      String splitId = result.nextSplit();

      if (splitId == null || result.finishedSplits().contains(split.splitId())) {
        break;
      }

      // Drain the batch
      while (result.nextRecordFromSplit() != null) {
        // Continue
      }
    }

    // After finishing, fetch should return empty result
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> emptyResult = reader.fetch();
    assertNull(emptyResult.nextSplit());
  }

  @Test
  public void testMultipleFetchesFromSameSplit() throws IOException {
    List<String> testData = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      testData.add("record-" + i);
    }

    TestSplitReaderFunctionWithBatchSize readerFunction =
        new TestSplitReaderFunctionWithBatchSize(testData, 10);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(mockContext, readerFunction, null);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // First fetch should return first batch
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result1 = reader.fetch();
    assertEquals(split.splitId(), result1.nextSplit());
    int count1 = 0;
    while (result1.nextRecordFromSplit() != null) {
      count1++;
    }
    assertEquals(10, count1);
    assertTrue(result1.finishedSplits().isEmpty());

    // Second fetch should return second batch from same split
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result2 = reader.fetch();
    assertEquals(split.splitId(), result2.nextSplit());
    int count2 = 0;
    while (result2.nextRecordFromSplit() != null) {
      count2++;
    }
    assertEquals(10, count2);
  }

  /**
   * Helper method to create a test HoodieSourceSplit.
   */
  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "base-path-" + splitNum,
        Option.of(Collections.emptyList()),
        "/test/table",
        "/test/partition",
        "read_optimized",
        "19700101000000000",
        fileId
    );
  }

  /**
   * Test implementation of SplitReaderFunction.
   */
  private static class TestSplitReaderFunction implements SplitReaderFunction<String> {
    private final List<String> testData;
    private int readCount = 0;
    private HoodieSourceSplit lastReadSplit = null;
    private boolean closed = false;

    public TestSplitReaderFunction() {
      this(Collections.emptyList());
    }

    public TestSplitReaderFunction(List<String> testData) {
      this.testData = testData;
    }

    @Override
    public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> read(HoodieSourceSplit split) {
      readCount++;
      lastReadSplit = split;
      ClosableIterator<String> iterator = createClosableIterator(testData);
      DefaultHoodieBatchReader<String> reader = new DefaultHoodieBatchReader<String>(new Configuration());
      return reader.batch(split, iterator);
    }

    @Override
    public void close() throws Exception {
      closed = true;
    }

    public int getReadCount() {
      return readCount;
    }

    public HoodieSourceSplit getLastReadSplit() {
      return lastReadSplit;
    }

    public boolean isClosed() {
      return closed;
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
  }

  /**
   * Test implementation of SplitReaderFunction with custom batch size.
   */
  private static class TestSplitReaderFunctionWithBatchSize implements SplitReaderFunction<String> {
    private final List<String> testData;
    private final int batchSize;

    public TestSplitReaderFunctionWithBatchSize(List<String> testData, int batchSize) {
      this.testData = testData;
      this.batchSize = batchSize;
    }

    @Override
    public CloseableIterator<RecordsWithSplitIds<HoodieRecordWithPosition<String>>> read(HoodieSourceSplit split) {
      ClosableIterator<String> iterator = createClosableIterator(testData);
      Configuration config = new Configuration();
      config.set(FlinkOptions.SOURCE_READER_FETCH_BATCH_RECORD_COUNT, batchSize);
      DefaultHoodieBatchReader<String> reader = new DefaultHoodieBatchReader<String>(config);
      return reader.batch(split, iterator);
    }

    @Override
    public void close() throws Exception {
      // No-op
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
  }
}
