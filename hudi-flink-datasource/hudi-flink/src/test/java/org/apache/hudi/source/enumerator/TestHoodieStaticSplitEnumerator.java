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
import org.apache.hudi.source.split.DefaultHoodieSplitProvider;
import org.apache.hudi.source.split.HoodieSourceSplit;
import org.apache.hudi.source.split.SplitRequestEvent;

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
    splitProvider = new DefaultHoodieSplitProvider();
    enumerator = new HoodieStaticSplitEnumerator(context, splitProvider);

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

    HoodieEnumeratorState state = enumerator.snapshotState(1L);

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

    context.registerReader(new ReaderInfo(0, "localhost"));

    SplitRequestEvent event = new SplitRequestEvent(Collections.emptyList(), "localhost");
    enumerator.handleSourceEvent(0, 1, event);

    assertTrue(context.getAssignedSplits().size() > 0, "Should assign split via attempt-aware method");
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "read_optimized",
        fileId
    );
  }

  /**
   * Mock implementation of SplitEnumeratorContext for testing.
   */
  private static class MockSplitEnumeratorContext implements SplitEnumeratorContext<HoodieSourceSplit> {
    private final Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
    private final Map<Integer, List<HoodieSourceSplit>> assignedSplits = new HashMap<>();
    private final List<Integer> noMoreSplitsSignaled = new ArrayList<>();
    private final List<Runnable> coordinatorThreadTasks = new ArrayList<>();

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
