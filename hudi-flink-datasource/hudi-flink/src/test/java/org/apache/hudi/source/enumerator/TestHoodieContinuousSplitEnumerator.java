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

package org.apache.hudi.source.enumerator;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.source.ScanContext;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieContinuousSplitBatch;
import org.apache.hudi.source.split.HoodieContinuousSplitDiscover;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.utils.TestConfigurations;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
  private MockSplitEnumeratorContext context;
  private DefaultHoodieSplitProvider splitProvider;
  private MockContinuousSplitDiscover splitDiscover;
  private ScanContext scanContext;
  private HoodieContinuousSplitEnumerator enumerator;
  private HoodieSourceSplit split1;
  private HoodieSourceSplit split2;
  private HoodieSourceSplit split3;

  @BeforeEach
  public void setUp() {
    context = new MockSplitEnumeratorContext();
    splitProvider = new DefaultHoodieSplitProvider();

    Configuration conf = new Configuration();
    conf.set(FlinkOptions.PATH, "/tmp/test");
    conf.set(FlinkOptions.READ_STREAMING_CHECK_INTERVAL, 1);

    scanContext = TestScanContext.builder()
        .conf(conf)
        .path(new Path("/tmp/test"))
        .rowType(TestConfigurations.ROW_TYPE)
        .startInstant("20231201000000000")
        .maxPendingSplits(1000)
        .build();

    splitDiscover = new MockContinuousSplitDiscover();

    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
    split3 = createTestSplit(3, "file3");
  }

  @Test
  public void testStartEnumerator() {
    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    // Verify that async task was scheduled
    assertEquals(1, context.getAsyncCallCount(), "Should schedule async split discovery");
  }

  @Test
  public void testEnumeratorWithInitialState() {
    HoodieSplitEnumeratorState state = new HoodieSplitEnumeratorState(
        Collections.emptyList(), Option.of("20231201120000000"), Option.of("20231201120001000"));

    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.of(state));

    assertNotNull(enumerator, "Enumerator should be created with state");
  }

  @Test
  public void testEnumeratorWithoutInitialState() {
    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());

    assertNotNull(enumerator, "Enumerator should be created without state");
  }

  @Test
  public void testShouldWaitForMoreSplits() {
    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());

    assertTrue(enumerator.shouldWaitForMoreSplits(),
        "Continuous enumerator should always wait for more splits");
  }

  @Test
  public void testSnapshotState() throws Exception {
    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());
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
        context, splitProvider, splitDiscover, scanContext, Option.empty());
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
        context, splitProvider, splitDiscover, scanContext, Option.empty());
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
        context, splitProvider, splitDiscover, scanContext, Option.empty());
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
        context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();
    context.executeAsyncCallbacks();

    assertEquals(0, splitProvider.pendingSplitCount(),
        "Should handle empty split batch");
  }

  @Test
  public void testProcessDiscoveredSplitsWithException() {
    splitDiscover.setThrowException(true);

    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());
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
        context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.handleSplitRequest(0, "localhost");

    assertTrue(context.getAssignedSplits().containsKey(0),
        "Should assign splits to requesting reader");
  }

  @Test
  public void testAddSplitsBack() {
    enumerator = new HoodieContinuousSplitEnumerator(
        context, splitProvider, splitDiscover, scanContext, Option.empty());
    enumerator.start();

    enumerator.addSplitsBack(Arrays.asList(split1), 0);

    assertEquals(1, splitProvider.pendingSplitCount(),
        "Split should be added back to provider");
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        fileId
    );
  }

  /**
   * Mock implementation of HoodieContinuousSplitDiscover for testing.
   */
  private static class MockContinuousSplitDiscover implements HoodieContinuousSplitDiscover {
    private HoodieContinuousSplitBatch nextBatch = HoodieContinuousSplitBatch.EMPTY;
    private boolean throwException = false;

    public void setNextBatch(HoodieContinuousSplitBatch batch) {
      this.nextBatch = batch;
    }

    public void setThrowException(boolean throwException) {
      this.throwException = throwException;
    }

    @Override
    public HoodieContinuousSplitBatch discoverSplits(String lastInstant) {
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
      return UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
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

  /**
   * Test implementation of ScanContext for testing.
   */
  private static class TestScanContext extends ScanContext {
    private TestScanContext(
        Configuration conf,
        Path path,
        org.apache.flink.table.types.logical.RowType rowType,
        String startInstant,
        long maxPendingSplits) {
      super(conf, path, rowType, startInstant, "",0L, maxPendingSplits, false, false, false, false);
    }

    public static Builder builder() {
      return new Builder();
    }

    public static class Builder {
      private Configuration conf;
      private Path path;
      private org.apache.flink.table.types.logical.RowType rowType;
      private String startInstant;
      private long maxPendingSplits = 1000;

      public Builder conf(Configuration conf) {
        this.conf = conf;
        return this;
      }

      public Builder path(Path path) {
        this.path = path;
        return this;
      }

      public Builder rowType(org.apache.flink.table.types.logical.RowType rowType) {
        this.rowType = rowType;
        return this;
      }

      public Builder startInstant(String startInstant) {
        this.startInstant = startInstant;
        return this;
      }

      public Builder maxPendingSplits(long maxPendingSplits) {
        this.maxPendingSplits = maxPendingSplits;
        return this;
      }

      public ScanContext build() {
        return new TestScanContext(conf, path, rowType, startInstant, maxPendingSplits);
      }
    }
  }
}
