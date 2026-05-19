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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.metrics.FlinkStreamReadMetrics;
import org.apache.hudi.source.HoodieScanContext;
import org.apache.hudi.source.split.assign.HoodieSplitNumberAssigner;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieContinuousSplitEnumerator}.
 */
public class TestHoodieContinuousSplitEnumerator {
  private static final String TABLE_NAME = "test-table";
  private MockSplitEnumeratorContext context;
  private DefaultHoodieSplitProvider splitProvider;
  private MockContinuousSplitDiscover splitDiscover;
  private HoodieScanContext scanContext;
  private HoodieContinuousSplitEnumerator enumerator;
  private FlinkStreamReadMetrics mockReaderMetrics;
  private HoodieSourceSplit split1;
  private HoodieSourceSplit split2;

  @BeforeEach
  public void setUp() {
    context = new MockSplitEnumeratorContext();
    splitProvider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 1);

    scanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20231201000000000")
        .maxPendingSplits(1000)
        .build();

    mockReaderMetrics = Mockito.mock(FlinkStreamReadMetrics.class);
    splitDiscover = new MockContinuousSplitDiscover();

    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
  }

  @Test
  public void testStartEnumerator() {
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    // Verify that async task was scheduled
    assertEquals(1, context.getAsyncCallCount(), "Should schedule async split discovery");
  }

  @Test
  public void testEnumeratorWithInitialState() {
    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        Collections.emptyList(), Option.of("20231201120000000"), Option.of("20231201120001000"));

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.of(state));

    assertNotNull(enumerator, "Enumerator should be created with state");
  }

  @Test
  public void testEnumeratorWithoutInitialState() {
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());

    assertNotNull(enumerator, "Enumerator should be created without state");
  }

  @Test
  public void testShouldWaitForMoreSplits() {
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());

    assertTrue(enumerator.shouldWaitForMoreSplits(),
        "Continuous enumerator should always wait for more splits");
  }

  @Test
  public void testSnapshotState() throws Exception {
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));

    HoodieSplitEnumeratorState state = enumerator.snapshotState(1L);

    assertNotNull(state, "Snapshot state should not be null");
    assertNotNull(state.getPendingSplitStates(), "Split state should not be null");
  }

  @Test
  public void testDiscoverSplitsWhenBelowThreshold() {
    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Arrays.asList(split1, split2), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    // Trigger async callback manually
    context.executeAsyncCallbacks();

    // Verify splits were discovered and added
    assertEquals(2, splitProvider.pendingSplitCount(),
        "Should discover and add splits when below threshold");
  }

  @Test
  public void testDiscoverSplitsPausedWhenAboveThreshold() {
    // Fill provider with many splits to exceed threshold
    List<HoodieSourceSplit> manySplits = new ArrayList<>();
    for (int i = 0; i < 1100; i++) {
      manySplits.add(createTestSplit(i, "file" + i));
    }
    splitProvider.onDiscoveredSplits(manySplits);

    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Arrays.asList(split1, split2), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    int initialCount = splitProvider.pendingSplitCount();
    context.executeAsyncCallbacks();

    // Verify no new splits were added due to threshold
    assertEquals(initialCount, splitProvider.pendingSplitCount(),
        "Should not discover splits when above threshold");
  }

  @Test
  public void testProcessDiscoveredSplitsWithMatchingInstant() {
    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Arrays.asList(split1, split2), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();
    context.executeAsyncCallbacks();

    assertEquals(2, splitProvider.pendingSplitCount(),
        "Should add splits when instant matches");
  }

  @Test
  public void testProcessDiscoveredSplitsWithEmptyResult() {
    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Collections.emptyList(), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();
    context.executeAsyncCallbacks();

    assertEquals(0, splitProvider.pendingSplitCount(),
        "Should handle empty split batch");
  }

  @Test
  public void testProcessDiscoveredSplitsWithException() {
    splitDiscover.setThrowException(true);

    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    try {
      context.executeAsyncCallbacks();
    } catch (RuntimeException e) {
      assertTrue(e.getMessage().contains("Failed to discover new splits"),
          "Should throw exception with correct message");
    }
  }

  @Test
  public void testHandleSplitRequest() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.handleSplitRequest(0, "localhost");

    assertTrue(context.getAssignedSplits().containsKey(0),
        "Should assign splits to requesting reader");
  }

  @Test
  public void testAddSplitsBack() {
    enumerator = new HoodieContinuousSplitEnumerator(
            TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    enumerator.addSplitsBack(Arrays.asList(split1), 0);

    assertEquals(1, splitProvider.pendingSplitCount(),
        "Split should be added back to provider");
  }

  /**
   * Verify that when pending splits exceed maxPendingSplits (READ_SPLITS_LIMIT), discovery
   * is paused and the enumerator position is NOT advanced.
   * This ensures that after a pause, the next scan resumes from the correct instant.
   */
  @Test
  public void testReadSplitsLimitPositionPreservedWhenDiscoveryIsPaused() throws Exception {
    // Use a small maxPendingSplits to make it easy to exceed
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 1);
    HoodieScanContext limitedScanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20231201000000000")
        .maxPendingSplits(2)
        .build();

    String knownOffset = "20231201060000000";
    HoodieSplitEnumeratorState initialState = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20231201000000000"),
        Option.of(knownOffset));

    // Fill provider with 3 splits, exceeding the limit of 2
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2, createTestSplit(3, "file3")));

    // Configure discover to return new splits — but they should never be reached
    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Collections.singletonList(createTestSplit(4, "file4")), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, limitedScanContext, Option.of(initialState));
    enumerator.start();
    context.executeAsyncCallbacks();

    // Position must remain at knownOffset; discovery was paused and returned EMPTY (no new offset)
    HoodieSplitEnumeratorState state = enumerator.snapshotState(1L);
    assertTrue(state.getLastEnumeratedInstantOffset().isPresent(),
        "Position offset must not be cleared when discovery is paused by READ_SPLITS_LIMIT");
    assertEquals(knownOffset, state.getLastEnumeratedInstantOffset().get(),
        "Position offset must stay at last consumed offset when paused by READ_SPLITS_LIMIT");
  }

  /**
   * Verify that discovery proceeds when pending splits are exactly at the threshold (not exceeding).
   * The condition is strictly greater-than, so pending == limit should still allow discovery.
   */
  @Test
  public void testReadSplitsLimitDiscoveryProceedsAtExactThreshold() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 1);
    HoodieScanContext limitedScanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20231201000000000")
        .maxPendingSplits(2)
        .build();

    // Add exactly 2 splits — equal to limit, should NOT pause
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));

    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Collections.singletonList(createTestSplit(3, "file3")), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, limitedScanContext, Option.empty());
    enumerator.start();
    context.executeAsyncCallbacks();

    // One new split should have been discovered and added
    assertEquals(3, splitProvider.pendingSplitCount(),
        "Discovery should proceed when pending splits equal (not exceed) the limit");
  }

  /**
   * Verify that discovery pauses when pending splits exceed the threshold by exactly one.
   */
  @Test
  public void testReadSplitsLimitDiscoveryPausedExceedingByOne() throws Exception {
    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 1);
    HoodieScanContext limitedScanContext = HoodieScanContext.builder()
        .conf(conf)
        .path(new StoragePath("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20231201000000000")
        .maxPendingSplits(2)
        .build();

    // Add 3 splits — exceeds limit by 1
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2, createTestSplit(3, "file3")));

    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Collections.singletonList(createTestSplit(4, "file4")), "20231201000000000", "20231201120000000"));

    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, limitedScanContext, Option.empty());
    enumerator.start();
    int countBeforeCallback = splitProvider.pendingSplitCount();
    context.executeAsyncCallbacks();

    assertEquals(countBeforeCallback, splitProvider.pendingSplitCount(),
        "Discovery should pause when pending splits exceed the limit by one");
  }

  /**
   * Verify that the enumerator position is NOT reset when no new commits are found
   * (i.e., when the split discover returns a batch with an empty offset).
   * This is critical for READ_COMMITS_LIMIT: after catching up to the latest commit,
   * subsequent scans must resume from the last consumed offset, not from the beginning.
   */
  @Test
  public void testPositionPreservedWhenNoNewCommits() throws Exception {
    // Initialize enumerator with a known position
    HoodieSplitEnumeratorState initialState = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20231201000000000"),
        Option.of("20231201120000000"));

    splitDiscover.setNextBatch(HoodieContinuousSplitBatch.EMPTY);
    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.of(initialState));
    enumerator.start();
    context.executeAsyncCallbacks();

    // Position must remain at the initial offset, not be reset to empty
    HoodieSplitEnumeratorState state = enumerator.snapshotState(1L);
    assertTrue(state.getLastEnumeratedInstantOffset().isPresent(),
        "Position offset must not be cleared when no new commits are found");
    assertEquals("20231201120000000", state.getLastEnumeratedInstantOffset().get(),
        "Position offset must be preserved at the last consumed offset");
  }

  /**
   * Verify that the enumerator position advances correctly when READ_COMMITS_LIMIT constrains
   * the batch to commits that contain no data (empty commits).
   * The position must advance to the end of the limited batch so the next scan picks up
   * the subsequent commits rather than re-processing the same ones.
   */
  @Test
  public void testReadCommitsLimitPositionAdvancesWithEmptyCommitBatch() throws Exception {
    // Simulate READ_COMMITS_LIMIT returning a limited batch of empty commits:
    // splits are empty but the batch carries a valid offset from the analyzed commits.
    HoodieContinuousSplitBatch emptyCommitsBatch = new HoodieContinuousSplitBatch(
        Collections.emptyList(), "20231201000000000", "20231201120000000");
    splitDiscover.setNextBatch(emptyCommitsBatch);

    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();
    context.executeAsyncCallbacks();

    // Position must advance to the batch offset even though no splits were produced
    HoodieSplitEnumeratorState state = enumerator.snapshotState(1L);
    assertTrue(state.getLastEnumeratedInstantOffset().isPresent(),
        "Position offset must be set after processing a limited batch of empty commits");
    assertEquals("20231201120000000", state.getLastEnumeratedInstantOffset().get(),
        "Position offset must advance to the end of the limited commit batch");
  }

  /**
   * Verify that after READ_COMMITS_LIMIT advances the position to offset X,
   * the next discover call passes X as the lastInstant so that only subsequent
   * commits are scanned (no re-reading of already-consumed commits).
   */
  @Test
  public void testReadCommitsLimitPassesLastInstantToNextDiscover() throws Exception {
    // Initialize enumerator with a known position (simulating state after first limited batch)
    String previousOffset = "20231201120000000";
    HoodieSplitEnumeratorState initialState = new HoodieSplitEnumeratorState(
        Collections.emptyList(),
        Option.of("20231201000000000"),
        Option.of(previousOffset));

    splitDiscover.setNextBatch(HoodieContinuousSplitBatch.EMPTY);
    enumerator = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, context, splitProvider, splitDiscover, scanContext, Option.of(initialState));
    enumerator.start();
    context.executeAsyncCallbacks();

    // The discover call must receive the previous batch's offset so scanning resumes
    // from after the last consumed commit, not from READ_START_COMMIT
    assertEquals(previousOffset, splitDiscover.getLastDiscoveredInstant(),
        "discover must be called with the last consumed offset to avoid re-reading commits");
  }

  /**
   * Verify that when the metricGroup is null (Flink 1.17 path), discovering splits with
   * non-empty results does not throw a NullPointerException.
   */
  @Test
  public void testProcessDiscoveredSplitsWithNullMetricGroup() {
    MockSplitEnumeratorContext nullMetricContext = new MockSplitEnumeratorContext(true);
    splitDiscover.setNextBatch(new HoodieContinuousSplitBatch(
        Arrays.asList(split1, split2), "20231201000000000", "20231201120000000"));

    HoodieContinuousSplitEnumerator enumeratorWithNullMetrics = new HoodieContinuousSplitEnumerator(
        TABLE_NAME, nullMetricContext, splitProvider, splitDiscover, scanContext, Option.empty());
    enumeratorWithNullMetrics.start();
    nullMetricContext.executeAsyncCallbacks();

    assertEquals(2, splitProvider.pendingSplitCount(),
        "Splits should be added to the provider even when enumeratorMetrics is null");
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0" + splitNum + "_20260126034717000.parquet",
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        "19700101000000000",
        fileId,
        Option.empty()
    );
  }

  /**
   * Mock implementation of HoodieContinuousSplitDiscover for testing.
   */
  private static class MockContinuousSplitDiscover implements HoodieContinuousSplitDiscover {
    private HoodieContinuousSplitBatch nextBatch = HoodieContinuousSplitBatch.EMPTY;
    private boolean throwException = false;
    private String lastDiscoveredInstant;

    public void setNextBatch(HoodieContinuousSplitBatch batch) {
      this.nextBatch = batch;
    }

    public void setThrowException(boolean throwException) {
      this.throwException = throwException;
    }

    public String getLastDiscoveredInstant() {
      return lastDiscoveredInstant;
    }

    @Override
    public HoodieContinuousSplitBatch discoverSplits(String lastInstant) {
      this.lastDiscoveredInstant = lastInstant;
      if (throwException) {
        throw new RuntimeException("Mock exception during split discovery");
      }
      return nextBatch;
    }
  }

  /**
   * Mock implementation of SplitEnumeratorContext for testing.
   */
  private static class MockSplitEnumeratorContext implements SplitEnumeratorContext<HoodieSourceSplit> {
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<HoodieSourceSplit>> assignedSplits = new HashMap<>();
    private final List<Integer> noMoreSplitsSignaled = new ArrayList<>();
    private final List<AsyncTask<?>> asyncTasks = new ArrayList<>();
    private final AtomicInteger asyncCallCount = new AtomicInteger(0);
    private final boolean returnNullMetricGroup;

    MockSplitEnumeratorContext() {
      this(false);
    }

    MockSplitEnumeratorContext(boolean returnNullMetricGroup) {
      this.returnNullMetricGroup = returnNullMetricGroup;
    }

    public void registerReader(ReaderInfo readerInfo) {
      registeredReaders.put(readerInfo.getSubtaskId(), readerInfo);
    }

    public void unregisterReader(int subtaskId) {
      registeredReaders.remove(subtaskId);
    }

    public Map<Integer, List<HoodieSourceSplit>> getAssignedSplits() {
      return assignedSplits;
    }

    public List<Integer> getNoMoreSplitsSignaled() {
      return noMoreSplitsSignaled;
    }

    public int getAsyncCallCount() {
      return asyncCallCount.get();
    }

    @SuppressWarnings("unchecked")
    public void executeAsyncCallbacks() {
      for (AsyncTask<?> task : asyncTasks) {
        try {
          Object result = task.callable.call();
          ((BiConsumer<Object, Throwable>) task.handler).accept(result, null);
        } catch (Exception e) {
          ((BiConsumer<Object, Throwable>) task.handler).accept(null, e);
        }
      }
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return returnNullMetricGroup ? null : UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
    }

    @Override
    public void sendEventToSourceReader(int subtaskId, org.apache.flink.api.connector.source.SourceEvent event) {
      // No-op for testing
    }

    @Override
    public int currentParallelism() {
      return registeredReaders.size();
    }

    @Override
    public Map<Integer, ReaderInfo> registeredReaders() {
      return new HashMap<>(registeredReaders);
    }

    @Override
    public void assignSplits(SplitsAssignment<HoodieSourceSplit> newSplitAssignments) {
      newSplitAssignments.assignment().forEach((subtask, splits) -> {
        assignedSplits.computeIfAbsent(subtask, k -> new ArrayList<>()).addAll(splits);
      });
    }

    @Override
    public void assignSplit(HoodieSourceSplit split, int subtask) {
      assignedSplits.computeIfAbsent(subtask, k -> new ArrayList<>()).add(split);
    }

    @Override
    public void signalNoMoreSplits(int subtask) {
      noMoreSplitsSignaled.add(subtask);
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler) {
      asyncCallCount.incrementAndGet();
      asyncTasks.add(new AsyncTask<>(callable, handler));
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {
      asyncCallCount.incrementAndGet();
      asyncTasks.add(new AsyncTask<>(callable, handler));
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
      runnable.run();
    }

    private static class AsyncTask<T> {
      final Callable<T> callable;
      final BiConsumer<T, Throwable> handler;

      AsyncTask(Callable<T> callable, BiConsumer<T, Throwable> handler) {
        this.callable = callable;
        this.handler = handler;
      }
    }
  }

}
