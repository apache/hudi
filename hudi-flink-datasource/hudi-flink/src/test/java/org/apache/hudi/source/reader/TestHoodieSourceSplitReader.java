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

import org.apache.hudi.common.function.SerializableSupplier;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.source.reader.function.SplitReaderFunction;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SerializableComparator;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  @Test
  public void testFetchWithSingleSplit() throws IOException {
    List<String> testData = Arrays.asList("record1", "record2", "record3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, comparator, Option.empty());

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
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
  public void testWakeUp() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    // wakeUp() now sets a flag but must not throw, and the flag is reset at the top of each fetch()
    // so a wakeUp with no in-flight drain leaves the next fetch() unaffected.
    reader.wakeUp();
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();
    assertNotNull(result);
    assertNull(result.nextSplit());
  }

  // -------------------------------------------------------------------------
  //  wakeUp — cooperative cancellation of the minibatch drain
  // -------------------------------------------------------------------------

  @Test
  public void testWakeUpMidDrainReturnsPartialBatchAndResumes() throws IOException {
    // A wakeUp() landing after the first record stops the drain between records: fetch() returns the
    // 1 record buffered so far as a NON-finishing batch without closing the split; a later fetch()
    // resumes the rest with continuous offsets, and the split is closed only once at true EOF.
    List<String> testData = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());
    boolean[] fired = {false};
    readerFunction.setDrainProbe((buffered, hasNext) -> {
      if (buffered == 1 && !fired[0]) {
        fired[0] = true;
        reader.wakeUp();
      }
    });

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // First fetch: woken after r1 -> partial batch of 1, split not finished, not closed.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b1 = reader.fetch();
    assertEquals(split.splitId(), b1.nextSplit());
    HoodieRecordWithPosition<String> first = b1.nextRecordFromSplit();
    assertNotNull(first);
    assertEquals("r1", first.record());
    assertEquals(1L, first.recordOffset());
    assertNull(b1.nextRecordFromSplit(), "drain must stop at the first record on wake-up");
    assertTrue(b1.finishedSplits().isEmpty(), "woken split must not be finished");
    assertEquals(0, readerFunction.getCloseCurrentSplitCount(), "woken split must not be closed");

    // Second fetch: resumes the remaining records with continuous offsets (2..5).
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b2 = reader.fetch();
    assertEquals(split.splitId(), b2.nextSplit());
    HoodieRecordWithPosition<String> next = b2.nextRecordFromSplit();
    assertNotNull(next);
    assertEquals("r2", next.record());
    assertEquals(2L, next.recordOffset(), "offset continues across the wake boundary");
    assertEquals(3, drainRecordCount(b2), "r3, r4, r5 remain");
    assertTrue(b2.finishedSplits().isEmpty());

    // Third fetch: true EOF -> finish signal, split closed exactly once, opened exactly once.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b3 = reader.fetch();
    assertTrue(b3.finishedSplits().contains(split.splitId()));
    assertEquals(1, readerFunction.getCloseCurrentSplitCount());
    assertEquals(1, readerFunction.getOpenCount());
  }

  @Test
  public void testWakeUpBeforeAnyRecordReturnsEmptyNonFinishingBatch() throws IOException {
    // A wakeUp() landing before any record is buffered must return an empty NON-finishing batch, not
    // a finish signal: the split stays open (not closed) and resumes on the next fetch().
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());
    boolean[] fired = {false};
    readerFunction.setDrainProbe((buffered, hasNext) -> {
      // Wake at the start of the drain (count 0) while data is still available.
      if (buffered == 0 && hasNext && !fired[0]) {
        fired[0] = true;
        reader.wakeUp();
      }
    });

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b1 = reader.fetch();
    assertNull(b1.nextSplit(), "empty non-finishing batch carries no split records");
    assertTrue(b1.finishedSplits().isEmpty(), "must not finish the split on wake-up");
    assertEquals(0, readerFunction.getCloseCurrentSplitCount(), "split must stay open");

    // Next fetch resumes and returns all records (the one-shot wake has fired).
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b2 = reader.fetch();
    assertEquals(split.splitId(), b2.nextSplit());
    assertEquals(3, drainRecordCount(b2));
  }

  @Test
  public void testWakeUpCoincidingWithEofDefersFinishByOneFetch() throws IOException {
    // readBatch returning null is ambiguous between true-EOF and woken-empty. When a wakeUp lands
    // exactly at genuine exhaustion, fetch() returns an empty non-finishing batch once (deferring the
    // finish), and the next fetch() finishes the split - it must never stall.
    List<String> testData = Arrays.asList("r1", "r2");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());
    boolean[] fired = {false};
    readerFunction.setDrainProbe((buffered, hasNext) -> {
      // Wake only at the start of a drain that finds the cursor already exhausted.
      if (buffered == 0 && !hasNext && !fired[0]) {
        fired[0] = true;
        reader.wakeUp();
      }
    });

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // First fetch drains both records (cursor still had data at drain start, so no wake).
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b1 = reader.fetch();
    assertEquals(2, drainRecordCount(b1));
    assertEquals(0, readerFunction.getCloseCurrentSplitCount());

    // Second fetch: cursor is exhausted at drain start -> wake fires -> empty non-finishing, deferred.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b2 = reader.fetch();
    assertTrue(b2.finishedSplits().isEmpty(), "finish deferred by the coinciding wake-up");
    assertEquals(0, readerFunction.getCloseCurrentSplitCount());

    // Third fetch: no wake now -> true EOF finishes and closes the split.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b3 = reader.fetch();
    assertTrue(b3.finishedSplits().contains(split.splitId()));
    assertEquals(1, readerFunction.getCloseCurrentSplitCount());
  }

  @Test
  public void testWakeUpWithLimitReachedStillFinishesSplit() throws IOException {
    // Guard: the woken-resume short-circuit must live strictly on the readBatch-returned-null path.
    // A wakeUp coinciding with the limit-reached path must still finish the split, never loop forever
    // returning non-finishing batches. The limiter wakes the reader exactly when the limit is reached.
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    AtomicReference<HoodieSourceSplitReader<String>> holder = new AtomicReference<>();
    RecordLimiter wakingLimiter = new RecordLimiter(2L) {
      @Override
      public boolean isLimitReached() {
        boolean reached = super.isLimitReached();
        if (reached && holder.get() != null) {
          holder.get().wakeUp();
        }
        return reached;
      }
    };
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(wakingLimiter));
    holder.set(reader);

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // First fetch returns the split data; the limit wrapper caps consumption at 2 records.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b1 = reader.fetch();
    assertEquals(split.splitId(), b1.nextSplit());
    assertEquals(2, drainRecordCount(b1), "limit caps drained records at 2");

    // Second fetch hits the limit-reached path (which wakes the reader); it must finish, not defer.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b2 = reader.fetch();
    assertTrue(b2.finishedSplits().contains(split.splitId()), "limit-reached path must finish the split");
    assertEquals(1, readerFunction.getCloseCurrentSplitCount());
  }

  @Test
  public void testWakeUpPartialBatchRespectsLimit() throws IOException {
    // A partial (woken) batch must not double-count against the pushed-down limit: with limit=3 over a
    // 5-record split and a wake after the first record, the total emitted across batches stays at 3.
    List<String> testData = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(3L)));
    boolean[] fired = {false};
    readerFunction.setDrainProbe((buffered, hasNext) -> {
      if (buffered == 1 && !fired[0]) {
        fired[0] = true;
        reader.wakeUp();
      }
    });

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    int total = 0;
    // Drain fetches until the split is finished; assert the limit caps the total at 3.
    for (int i = 0; i < 10; i++) {
      RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
      if (batch.nextSplit() != null) {
        total += drainRecordCount(batch);
      }
      if (batch.finishedSplits().contains(split.splitId())) {
        break;
      }
    }
    assertEquals(3, total, "pushed-down limit must cap the total across partial woken batches");
  }

  @Test
  public void testCloseReleasesWokenStillOpenSplit() throws Exception {
    // Unit proxy for the real shutdown path (SplitFetcher.run() -> splitReader.close() on the fetcher
    // thread): a split left open by a wake-up is released when the reader is closed.
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());
    boolean[] fired = {false};
    readerFunction.setDrainProbe((buffered, hasNext) -> {
      if (buffered == 1 && !fired[0]) {
        fired[0] = true;
        reader.wakeUp();
      }
    });

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // Woken fetch leaves the split open (not closed).
    reader.fetch();
    assertEquals(0, readerFunction.getCloseCurrentSplitCount());

    // close() on the (split-fetcher) thread releases the still-open split.
    reader.close();
    assertEquals(1, readerFunction.getCloseCurrentSplitCount());
    assertTrue(readerFunction.isClosed());
  }

  @Test
  public void testPauseOrResumeSplits() {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    reader.fetch();

    assertEquals(1, readerFunction.getOpenCount());
    assertEquals(split, readerFunction.getLastReadSplit());
  }

  @Test
  public void testReaderFunctionClosedOnReaderClose() throws Exception {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    reader.close();

    assertTrue(readerFunction.isClosed(), "Reader function should be closed");
  }

  @Test
  public void testEachSplitReaderUsesIndependentReaderFunction() throws Exception {
    List<TestSplitReaderFunction> readerFunctions = new ArrayList<>();
    SerializableSupplier<SplitReaderFunction<String>> readerFunctionSupplier = () -> {
      TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
      readerFunctions.add(readerFunction);
      return readerFunction;
    };

    HoodieSourceSplitReader<String> firstReader = new HoodieSourceSplitReader<>(
        TABLE_NAME, readerContext, readerFunctionSupplier, null, Option.empty());
    HoodieSourceSplitReader<String> secondReader = new HoodieSourceSplitReader<>(
        TABLE_NAME, readerContext, readerFunctionSupplier, null, Option.empty());

    assertEquals(2, readerFunctions.size());
    firstReader.close();
    assertTrue(readerFunctions.get(0).isClosed());
    assertFalse(readerFunctions.get(1).isClosed(),
        "Closing an idle fetcher's split reader must not close the next fetcher's reader function");

    secondReader.close();
    assertTrue(readerFunctions.get(1).isClosed());
  }

  @Test
  public void testFetchEmptyResultWhenNoSplitsAdded() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction();
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> result = reader.fetch();

    assertNotNull(result);
    assertNull(result.nextSplit());
    assertEquals(0, readerFunction.getOpenCount(), "Should not read any splits");
  }

  @Test
  public void testSplitOrderPreservedWithoutComparator() throws IOException {
    List<String> testData = Collections.singletonList("record");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    // No comparator - should preserve insertion order
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
            new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(2L)));

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(2L)));

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
    // readerFunction was opened only once (for split1, never for split2)
    assertEquals(1, readerFunction.getOpenCount());
  }

  @Test
  public void testLimitZeroDrainsAllSplitsImmediately() throws IOException {
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(Arrays.asList("r1", "r2"));

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(0L)));

    HoodieSourceSplit split1 = createTestSplit(1, "file1");
    HoodieSourceSplit split2 = createTestSplit(2, "file2");
    reader.handleSplitsChanges(new SplitsAddition<>(Arrays.asList(split1, split2)));

    // limit=0 means totalReadCount(0) >= limit(0) on first fetch → drain everything
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    Set<String> finished = batch.finishedSplits();
    assertTrue(finished.contains(split1.splitId()));
    assertTrue(finished.contains(split2.splitId()));
    assertNull(batch.nextSplit());
    // readerFunction was never opened
    assertEquals(0, readerFunction.getOpenCount());
  }

  @Test
  public void testLimitExactlyMatchingTotalRecords() throws IOException {
    // limit == number of records in the split → all records returned, no early cut
    List<String> testData = Arrays.asList("r1", "r2", "r3");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);

    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(3L)));

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.of(new RecordLimiter(5L)));

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
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null,
            Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split.splitId(), batch.nextSplit());
    assertEquals(5, drainRecordCount(batch));
  }

  // -------------------------------------------------------------------------
  //  Minibatch / resume tests
  // -------------------------------------------------------------------------

  @Test
  public void testSplitSpanningMultipleMinibatches() throws IOException {
    // A split larger than the mini-batch bound (2048) is emitted as multiple non-finished batches,
    // then one finish signal. The reader function is opened once and closed once across the split,
    // and the record offset stays continuous across the minibatch boundary.
    int n = 2049;
    List<String> testData = IntStream.range(0, n).mapToObj(i -> "r" + i).collect(Collectors.toList());
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    // First minibatch: 2048 records, offsets 1..2048, split not yet finished.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b1 = reader.fetch();
    assertEquals(split.splitId(), b1.nextSplit());
    long lastOffset = 0L;
    int c1 = 0;
    HoodieRecordWithPosition<String> rec;
    while ((rec = b1.nextRecordFromSplit()) != null) {
      lastOffset = rec.recordOffset();
      c1++;
    }
    assertEquals(2048, c1);
    assertEquals(2048L, lastOffset);
    assertTrue(b1.finishedSplits().isEmpty(), "split not finished after the first minibatch");

    // Second minibatch: the remaining record, offset continues at 2049.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b2 = reader.fetch();
    assertEquals(split.splitId(), b2.nextSplit());
    HoodieRecordWithPosition<String> only = b2.nextRecordFromSplit();
    assertNotNull(only);
    assertEquals(2049L, only.recordOffset());
    assertNull(b2.nextRecordFromSplit());
    assertTrue(b2.finishedSplits().isEmpty());

    // Third fetch: split exhausted -> finish signal.
    RecordsWithSplitIds<HoodieRecordWithPosition<String>> b3 = reader.fetch();
    assertTrue(b3.finishedSplits().contains(split.splitId()));

    assertEquals(1, readerFunction.getOpenCount(), "split opened exactly once across minibatches");
    assertEquals(1, readerFunction.getCloseCurrentSplitCount(), "split closed exactly once at EOF");
  }

  @Test
  public void testResumeSkipsConsumedRecords() throws IOException {
    // A recovered split carries a consumed offset; open() skips that many records and the emitted
    // offsets resume at consumed+1.
    List<String> testData = Arrays.asList("r1", "r2", "r3", "r4", "r5");
    TestSplitReaderFunction readerFunction = new TestSplitReaderFunction(testData);
    HoodieSourceSplitReader<String> reader =
        new HoodieSourceSplitReader<>(TABLE_NAME, readerContext, () -> readerFunction, null, Option.empty());

    HoodieSourceSplit split = createTestSplit(1, "file1");
    split.updatePosition(0, 2L); // 2 records already consumed before recovery
    reader.handleSplitsChanges(new SplitsAddition<>(Collections.singletonList(split)));

    RecordsWithSplitIds<HoodieRecordWithPosition<String>> batch = reader.fetch();
    assertEquals(split.splitId(), batch.nextSplit());
    HoodieRecordWithPosition<String> first = batch.nextRecordFromSplit();
    assertNotNull(first);
    assertEquals("r3", first.record(), "should resume past the 2 consumed records");
    assertEquals(3L, first.recordOffset(), "offset resumes at consumed + 1");
    // r4, r5 remain
    assertEquals(2, drainRecordCount(batch));
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
   * Test implementation of the stateful {@link SplitReaderFunction} cursor contract: {@code open}
   * materializes the split's data and honors the consumed-offset skip, {@code readBatch} drains a
   * bounded minibatch, and {@code closeCurrentSplit}/{@code close} release it.
   */
  private static class TestSplitReaderFunction implements SplitReaderFunction<String> {
    private final List<String> testData;
    private int openCount = 0;
    private int closeCurrentSplitCount = 0;
    private HoodieSourceSplit lastReadSplit = null;
    private boolean closed = false;

    // per-split cursor
    private Iterator<String> cursor;
    private long nextRecordOffset;

    // Optional hook invoked as (bufferedCount, cursorHasNext) at readBatch start (count 0) and after
    // each buffered record, so a test can call reader.wakeUp() at a precise point in the drain.
    private BiConsumer<Integer, Boolean> drainProbe;

    void setDrainProbe(BiConsumer<Integer, Boolean> drainProbe) {
      this.drainProbe = drainProbe;
    }

    public TestSplitReaderFunction() {
      this(Collections.emptyList());
    }

    public TestSplitReaderFunction(List<String> testData) {
      this.testData = testData;
    }

    @Override
    public void open(HoodieSourceSplit split) {
      openCount++;
      lastReadSplit = split;
      cursor = testData.iterator();
      long consumed = split.getConsumed();
      for (long i = 0; i < consumed; i++) {
        if (cursor.hasNext()) {
          cursor.next();
        } else {
          throw new IllegalStateException(
              "Invalid starting record offset " + consumed + " for split " + split.splitId());
        }
      }
      nextRecordOffset = consumed;
    }

    @Override
    public BatchRecords<String> readBatch(HoodieSourceSplit split, int batchSize, BooleanSupplier wakeupSignal) {
      List<String> buffer = new ArrayList<>();
      if (drainProbe != null) {
        drainProbe.accept(0, cursor.hasNext());
      }
      while (buffer.size() < batchSize && !wakeupSignal.getAsBoolean() && cursor.hasNext()) {
        buffer.add(cursor.next());
        if (drainProbe != null) {
          drainProbe.accept(buffer.size(), cursor.hasNext());
        }
      }
      if (buffer.isEmpty()) {
        return null;
      }
      long startingRecordOffset = nextRecordOffset;
      nextRecordOffset += buffer.size();
      return BatchRecords.forRecords(split.splitId(), buffer, split.getFileOffset(), startingRecordOffset);
    }

    @Override
    public void closeCurrentSplit() {
      closeCurrentSplitCount++;
      cursor = null;
    }

    @Override
    public void close() {
      // Mirror AbstractSplitReaderFunction#close(): releasing the reader also releases any split
      // still open (e.g. one left open by a wake-up), on the split-fetcher thread.
      closeCurrentSplit();
      closed = true;
    }

    // Number of splits opened; mirrors the old per-split read() count.
    public int getOpenCount() {
      return openCount;
    }

    public int getCloseCurrentSplitCount() {
      return closeCurrentSplitCount;
    }

    public HoodieSourceSplit getLastReadSplit() {
      return lastReadSplit;
    }

    public boolean isClosed() {
      return closed;
    }
  }
}
