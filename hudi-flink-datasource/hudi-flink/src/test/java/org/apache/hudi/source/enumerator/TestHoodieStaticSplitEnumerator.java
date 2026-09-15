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
import org.apache.hudi.source.split.assign.HoodieSplitNumberAssigner;
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.GlobalHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SplitRequestEvent;

import lombok.Getter;
import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
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
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link HoodieStaticSplitEnumerator}.
 */
public class TestHoodieStaticSplitEnumerator {
  private MockSplitEnumeratorContext context;
  private DefaultHoodieSplitProvider splitProvider;
  private HoodieStaticSplitEnumerator enumerator;
  private HoodieSourceSplit split1;
  private HoodieSourceSplit split2;
  private HoodieSourceSplit split3;

  @BeforeEach
  public void setUp() {
    context = new MockSplitEnumeratorContext();
    splitProvider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));
    enumerator = new HoodieStaticSplitEnumerator("test-table", context, splitProvider);

    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
    split3 = createTestSplit(3, "file3");
  }

  @Test
  public void testStartEnumerator() {
    enumerator.start();
    // Verify start doesn't throw exception
  }

  @Test
  public void testHandleSplitRequest() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.handleSplitRequest(0, "localhost");

    // Check that split was assigned
    assertEquals(1, context.getAssignedSplits().size(), "Should have assigned splits");
    assertTrue(context.getAssignedSplits().containsKey(0), "Should assign to subtask 0");
  }

  @Test
  public void testHandleSourceEvent() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));

    // Send split request event
    SplitRequestEvent event = new SplitRequestEvent(Collections.emptyList(), "localhost");
    enumerator.handleSourceEvent(0, event);

    assertEquals(1, context.getAssignedSplits().size(), "Should assign split after receiving event");
  }

  @Test
  public void testHandleSourceEventWithCompletedSplits() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));

    // First request
    enumerator.handleSplitRequest(0, "localhost");
    int initialCount = splitProvider.pendingSplitCount();

    // Send event with completed splits
    SplitRequestEvent event = new SplitRequestEvent(Arrays.asList(split1.splitId()), "localhost");
    enumerator.handleSourceEvent(0, event);

    // Should assign another split
    assertTrue(context.getAssignedSplits().size() > 0, "Should have assigned splits");
  }

  @Test
  public void testAddSplitsBack() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1));
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.handleSplitRequest(0, "localhost");

    context.registerReader(new ReaderInfo(1, "localhost"));
    enumerator.handleSplitRequest(1, "localhost");

    // Simulate reader failure - add splits back
    enumerator.addSplitsBack(Arrays.asList(split1), 0);

    assertEquals(1, splitProvider.pendingSplitCount(), "Split should be added back to provider");
  }

  @Test
  public void testAddReader() {
    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.addReader(0);
    // Should not throw exception
  }

  @Test
  public void testSnapshotState() throws Exception {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2));

    HoodieSplitEnumeratorState state = enumerator.snapshotState(1L);

    assertNotNull(state, "Snapshot state should not be null");
  }

  @Test
  public void testNoMoreSplitsSignal() {
    enumerator.start();
    context.registerReader(new ReaderInfo(0, "localhost"));

    // Request split when provider is empty
    enumerator.handleSplitRequest(0, "localhost");

    // Should signal no more splits for static enumerator
    assertTrue(context.getNoMoreSplitsSignaled().contains(0),
        "Should signal no more splits when no splits available");
  }

  @Test
  public void testMultipleReaders() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    enumerator.start();

    // Register multiple readers
    context.registerReader(new ReaderInfo(0, "host1"));
    context.registerReader(new ReaderInfo(1, "host2"));

    // Request splits for both readers
    enumerator.handleSplitRequest(0, "host1");
    enumerator.handleSplitRequest(1, "host2");

    // Both readers should get splits
    assertTrue(context.getAssignedSplits().containsKey(0), "Reader 0 should get a split");
    assertTrue(context.getAssignedSplits().containsKey(1), "Reader 1 should get a split");
  }

  @Test
  public void testReaderDisconnectedBeforeAssignment() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1));
    enumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    enumerator.handleSplitRequest(0, "localhost");

    // Unregister reader
    context.unregisterReader(0);

    // Request another split (shouldn't assign to disconnected reader)
    splitProvider.onDiscoveredSplits(Arrays.asList(split2));
    enumerator.handleSplitRequest(0, "localhost");

    // Reader 0 should not receive the second split since it's disconnected
    List<HoodieSourceSplit> assignedToReader0 = context.getAssignedSplits().get(0);
    if (assignedToReader0 != null) {
      assertEquals(1, assignedToReader0.size(), "Disconnected reader should not receive new splits");
    }
  }

  @Test
  public void testHandleSourceEventWithAttemptNumber() {
    splitProvider.onDiscoveredSplits(Arrays.asList(split1));
    enumerator.start();

    context.registerReader(new ReaderInfo(1, "localhost"));

    SplitRequestEvent event = new SplitRequestEvent(Collections.emptyList(), "localhost");
    enumerator.handleSourceEvent(1, 1, event);

    assertTrue(context.getAssignedSplits().size() > 0, "Should assign split via attempt-aware method");
  }

  @Test
  public void testGlobalProviderWorkStealingAcrossSubtasks() {
    // With the shared work-stealing pool a single reader can drain every split: the enumerator no
    // longer pins splits to a subtask. DefaultHoodieSplitProvider with a number/hash assigner would
    // hand most of these to other subtasks and starve reader 0.
    GlobalHoodieSplitProvider globalProvider = new GlobalHoodieSplitProvider();
    HoodieStaticSplitEnumerator globalEnumerator =
        new HoodieStaticSplitEnumerator("test-table", context, globalProvider);
    globalProvider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    globalEnumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    context.registerReader(new ReaderInfo(1, "localhost"));

    // Reader 0 keeps finishing and asking for more; it takes all three splits by itself.
    globalEnumerator.handleSplitRequest(0, "localhost");
    globalEnumerator.handleSplitRequest(0, "localhost");
    globalEnumerator.handleSplitRequest(0, "localhost");

    assertEquals(3, context.getAssignedSplits().get(0).size(),
        "A single reader should be able to steal the entire pool");
    assertFalse(context.getNoMoreSplitsSignaled().contains(0),
        "No-more-splits must not fire while the pool still had splits");
  }

  @Test
  public void testGlobalProviderSignalsNoMoreSplitsOnlyWhenPoolEmpty() {
    GlobalHoodieSplitProvider globalProvider = new GlobalHoodieSplitProvider();
    HoodieStaticSplitEnumerator globalEnumerator =
        new HoodieStaticSplitEnumerator("test-table", context, globalProvider);
    globalProvider.onDiscoveredSplits(Collections.singletonList(split1)); // one split, two readers
    globalEnumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    context.registerReader(new ReaderInfo(1, "localhost"));

    globalEnumerator.handleSplitRequest(0, "localhost"); // reader 0 takes the only split
    globalEnumerator.handleSplitRequest(1, "localhost"); // reader 1 finds the shared pool empty

    assertTrue(context.getAssignedSplits().containsKey(0), "Reader 0 should receive the split");
    assertFalse(context.getNoMoreSplitsSignaled().contains(0),
        "Reader 0 got a split, so it should not be told no-more-splits");
    assertTrue(context.getNoMoreSplitsSignaled().contains(1),
        "Reader 1 should be told no-more-splits once the shared pool is drained");
  }

  @Test
  public void testGlobalProviderAddSplitsBackAfterOtherReadersGotNoMoreSplits() {
    // Failure recovery once the pool has already been drained and some readers have finished:
    // the split a failed reader hands back must land in the shared pool and stay claimable by a
    // subtask that is neither the failed one nor an already-finished one. Under per-subtask
    // pinning it would instead be re-pinned to hash(fileId), possibly a reader that is already
    // done, and never be read.
    GlobalHoodieSplitProvider globalProvider = new GlobalHoodieSplitProvider();
    HoodieStaticSplitEnumerator globalEnumerator =
        new HoodieStaticSplitEnumerator("test-table", context, globalProvider);
    globalProvider.onDiscoveredSplits(Collections.singletonList(split1));
    globalEnumerator.start();

    context.registerReader(new ReaderInfo(0, "localhost"));
    context.registerReader(new ReaderInfo(1, "localhost"));
    context.registerReader(new ReaderInfo(2, "localhost"));

    globalEnumerator.handleSplitRequest(0, "localhost"); // reader 0 takes the only split
    globalEnumerator.handleSplitRequest(1, "localhost"); // pool is drained, reader 1 finishes
    assertTrue(context.getNoMoreSplitsSignaled().contains(1),
        "Reader 1 should already have been told no-more-splits");

    // Reader 0 fails mid-split and its split is returned.
    context.unregisterReader(0);
    globalEnumerator.addSplitsBack(Collections.singletonList(split1), 0);
    assertEquals(1, globalProvider.pendingSplitCount(),
        "Returned split should be back in the shared pool");

    // Reader 2, which has neither failed nor finished, claims it.
    globalEnumerator.handleSplitRequest(2, "localhost");

    assertEquals(Collections.singletonList(split1), context.getAssignedSplits().get(2),
        "A different, still-running subtask should claim the returned split");
    assertFalse(context.getNoMoreSplitsSignaled().contains(2),
        "Reader 2 got the returned split, so it should not be told no-more-splits");
    assertEquals(0, globalProvider.pendingSplitCount(), "Pool should be drained again");
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
   * Mock implementation of SplitEnumeratorContext for testing.
   */
  private static class MockSplitEnumeratorContext implements SplitEnumeratorContext<HoodieSourceSplit> {
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    @Getter
    private final Map<Integer, List<HoodieSourceSplit>> assignedSplits = new HashMap<>();
    @Getter
    private final List<Integer> noMoreSplitsSignaled = new ArrayList<>();
    private final List<Runnable> coordinatorThreadTasks = new ArrayList<>();

    public void registerReader(ReaderInfo readerInfo) {
      registeredReaders.put(readerInfo.getSubtaskId(), readerInfo);
    }

    public void unregisterReader(int subtaskId) {
      registeredReaders.remove(subtaskId);
    }

    @Override
    public SplitEnumeratorMetricGroup metricGroup() {
      return  UnregisteredMetricsGroup.createSplitEnumeratorMetricGroup();
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
      try {
        T result = callable.call();
        handler.accept(result, null);
      } catch (Exception e) {
        handler.accept(null, e);
      }
    }

    @Override
    public <T> void callAsync(Callable<T> callable, BiConsumer<T, Throwable> handler, long initialDelay, long period) {
      callAsync(callable, handler);
    }

    @Override
    public void runInCoordinatorThread(Runnable runnable) {
      coordinatorThreadTasks.add(runnable);
      runnable.run();
    }
  }
}
