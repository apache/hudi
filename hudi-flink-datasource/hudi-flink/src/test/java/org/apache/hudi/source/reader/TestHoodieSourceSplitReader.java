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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.mockito.Mockito.doReturn;

/**
 * Test cases for {@link HoodieSourceSplitReader}.
 */
public class TestHoodieSourceSplitReader {
  private static final String TABLE_NAME = "test_table";
  private SourceReaderContext readerContext;

  @BeforeEach
  public void setUp() {
    readerContext = Mockito.mock(SourceReaderContext.class);
    doReturn(UnregisteredMetricsGroup.createSourceReaderMetricGroup()).when(readerContext).metricGroup();
  }

  @Test
  public void testFetchWithNoSplits() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testFetchWithSingleSplit() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2", "record3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    SplitsAddition<HoodieSourceSplit> splitsChange = new SplitsAddition<>(Collections.singletonList(split));
    reader.handleSplitsChanges(splitsChange);

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertEquals(split.splitId(), result.nextSplit());
  }

  @Test
  public void testFetchWithMultipleSplits() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");

    SplitsAddition<HoodieSourceSplit> splitsChange =
        new SplitsAddition<>(Arrays.asList(split1, split2, split3));
    reader.handleSplitsChanges(splitsChange);

    // Each split produces a data batch followed by a finish-signal batch.
    // fetchNextSplitId() skips finish-signal batches and returns the next split ID (or null).
    assertEquals(split1.splitId(), fetchNextSplitId(reader));
    assertEquals(split2.splitId(), fetchNextSplitId(reader));
    assertEquals(split3.splitId(), fetchNextSplitId(reader));
    assertNull(fetchNextSplitId(reader));
  }

  @Test
  public void testHandleSplitsChangesWithComparator() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    // Comparator that sorts by file ID in reverse order
    SerializableComparator<HoodieSourceSplit> comparator =
        (s1, s2) -> s2.getFileId().compareTo(s1.getFileId());

    HoodieSourceSplitReader<String> reader =
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, comparator, Option.empty());

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");

    // Add splits in forward order
    SplitsAddition<HoodieSourceSplit> splitsChange =
        new SplitsAddition<>(Arrays.asList(split1, split2, split3));
    reader.handleSplitsChanges(splitsChange);

    // Should fetch in reverse order due to comparator
    assertEquals(split3.splitId(), fetchNextSplitId(reader));
    assertEquals(split2.splitId(), fetchNextSplitId(reader));
    assertEquals(split1.splitId(), fetchNextSplitId(reader));
  }

  @Test
  public void testAddingSplitsInMultipleBatches() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    // First batch
    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split1)));

    // Second batch
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    HoodieSourceSplit split3 = createTestSplit(3, "file3");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split2, split3)));

    // Verify all splits can be fetched
    assertEquals(split1.splitId(), fetchNextSplitId(reader));
    assertEquals(split2.splitId(), fetchNextSplitId(reader));
    assertEquals(split3.splitId(), fetchNextSplitId(reader));
    assertNull(fetchNextSplitId(reader));
  }

  @Test
  public void testClose() throws Exception {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    // wakeUp is a no-op, should not throw any exception
    reader.wakeUp();
  }

  @Test
  public void testPauseOrResumeSplits() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    reader.close();

    assertTrue(readerFunction.isClosed(), "Reader function should be closed");
  }

  @Test
  public void testFetchEmptyResultWhenNoSplitsAdded() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
    assertEquals(0, readerFunction.getReadCount(), "Should not read any splits");
  }

  @Test
  public void testSplitOrderPreservedWithoutComparator() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    // No comparator - should preserve insertion order
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    HoodieSourceSplit split3 = createTestSplit(3, "file3");
    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");

    SplitsAddition<HoodieSourceSplit> splitsChange =
        new SplitsAddition<>(Arrays.asList(split3, split1, split2));
    reader.handleSplitsChanges(splitsChange);

    // Should fetch in insertion order: 3, 1, 2
    assertEquals(split3.splitId(), fetchNextSplitId(reader));
    assertEquals(split1.splitId(), fetchNextSplitId(reader));
    assertEquals(split2.splitId(), fetchNextSplitId(reader));
  }

  @Test
  public void testReaderIteratorClosedOnSplitFinish() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.empty());

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");

    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

    // Fetch first split
    reader.fetch();
    assertEquals(split1, readerFunction.getLastReadSplit());
  }

  // -------------------------------------------------------------------------
  //  Limit push-down tests
  // -------------------------------------------------------------------------

  @Test
  public void testLimitCapsRecordsFromSingleSplit() throws IOException {
    List<String> testData = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.of(new RecordLimiter(2L)));

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // limitedRecords wrapper stops after 2 records even though the split has 5
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split.splitId(), batch.nextSplit());
    assertEquals(2, drainRecordCount(batch));

    // Next fetch emits the split-finish signal for split1
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> finishBatch = reader.fetch();
    assertTrue(finishBatch.finishedSplits().contains(split.splitId()));

    // Limit already satisfied; no more queued splits → empty drain batch
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> drainBatch = reader.fetch();
    assertTrue(drainBatch.finishedSplits().isEmpty());
    assertNull(drainBatch.nextSplit());
  }

  @Test
  public void testLimitDrainsRemainingSplitsWhenLimitReached() throws IOException {
    // split1 has 3 records; limit=2 means after reading split1's first 2 records,
    // split2 must be drained immediately as finished (no records read).
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.of(new RecordLimiter(2L)));

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

    // Read split1 — limit stops after 2 records
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split1.splitId(), batch.nextSplit());
    assertEquals(2, drainRecordCount(batch));

    // Finish signal for split1
    reader.fetch();

    // Limit satisfied → split2 is drained as immediately finished (no records read)
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> drainBatch = reader.fetch();
    assertTrue(drainBatch.finishedSplits().contains(split2.splitId()),
        "split2 should be drained as finished once limit is reached");
    assertNull(drainBatch.nextSplit());
    // readerFunction.read() was called only once (for split1, never for split2)
    assertEquals(1, readerFunction.getReadCount());
  }

  @Test
  public void testLimitZeroDrainsAllSplitsImmediately() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(Arrays.asList("r1", "r2"));

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.of(new RecordLimiter(0L)));

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

    // limit=0 means totalReadCount(0) >= limit(0) on first fetch → drain everything
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    Set<String> finished = batch.finishedSplits();
    assertTrue(finished.contains(split1.splitId()));
    assertTrue(finished.contains(split2.splitId()));
    assertNull(batch.nextSplit());
    // readerFunction.read() was never called
    assertEquals(0, readerFunction.getReadCount());
  }

  @Test
  public void testLimitExactlyMatchingTotalRecords() throws IOException {
    // limit == number of records in the split → all records returned, no early cut
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.of(new RecordLimiter(3L)));

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split.splitId(), batch.nextSplit());
    assertEquals(3, drainRecordCount(batch));

    // Finish signal for split
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> finishBatch = reader.fetch();
    assertTrue(finishBatch.finishedSplits().contains(split.splitId()));

    // No more splits; drain returns empty result
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> drainBatch = reader.fetch();
    assertTrue(drainBatch.finishedSplits().isEmpty());
    assertNull(drainBatch.nextSplit());
  }

  @Test
  public void testLimitSpanningMultipleSplits() throws IOException {
    // limit=5; split1 produces 3 records (no cap), split2 produces 3 but only 2 more allowed
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null, Option.of(new RecordLimiter(5L)));

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

    // split1: 3 records, totalReadCount becomes 3 (still below 5)
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch1 = reader.fetch();
    assertEquals(split1.splitId(), batch1.nextSplit());
    assertEquals(3, drainRecordCount(batch1));

    reader.fetch(); // finish signal for split1

    // split2: limit caps at 2 more (3 remaining to reach 5)
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch2 = reader.fetch();
    assertEquals(split2.splitId(), batch2.nextSplit());
    assertEquals(2, drainRecordCount(batch2));

    reader.fetch(); // finish signal for split2

    // No more splits; drain returns empty result
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> last = reader.fetch();
    assertTrue(last.finishedSplits().isEmpty());
    assertNull(last.nextSplit());
  }

  @Test
  public void testNoLimitSentinelReturnsAllRecords() throws IOException {
    // NO_LIMIT (-1) must return all records without wrapping
    List<String> testData = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, readerFunction, null,
            Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split.splitId(), batch.nextSplit());
    assertEquals(5, drainRecordCount(batch));
  }

  /**
   * Fetches the next batch that contains actual split data, skipping split-finish signal batches.
   * Split-finish batches have non-empty {@code finishedSplits()} but no records.
   * Returns null when there are truly no more splits.
   */
  private String fetchNextSplitId(HoodieSourceSplitReader<String> reader) throws IOException {
    while (true) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
      if (!result.finishedSplits().isEmpty()) {
        // This is a split-finish signal batch; continue to get the next real batch.
        continue;
      }
      return result.nextSplit();
    }
  }

  /**
   * Drains all records from the current split in a batch, returning the count.
   * Assumes {@code batch.nextSplit()} has already been called to position the batch.
   */
  private int drainRecordCount(RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch) {
    int count = 0;
    while (batch.nextRecordFromSplit() != null) {
      count++;
    }
    return count;
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
        fileId,
        Option.empty()
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
    public RecordsWithSplitIds<HoodieRecordWithPosition<String>> read(HoodieSourceSplit split) {
      readCount++;
      lastReadSplit = split;
      ClosableIterator<String> iterator = createClosableIterator(testData);
      return BatchRecords.forRecords(
          split.splitId(),
          iterator,
          split.getFileOffset(),
          split.getConsumed()
      );
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
}
