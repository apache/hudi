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

package org.apache.hudi.source.split;

import org.apache.hudi.common.util.Option;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link GlobalHoodieSplitProvider}.
 *
 * <p>The distinguishing behavior from {@link DefaultHoodieSplitProvider} is that splits are NOT
 * pinned to a subtask: any requesting subtask gets the next split from a single shared pool (work
 * stealing), and the pool drains fully regardless of which subtasks do the asking.
 */
public class TestGlobalHoodieSplitProvider {
  private GlobalHoodieSplitProvider provider;
  private HoodieSourceSplit split1;
  private HoodieSourceSplit split2;
  private HoodieSourceSplit split3;

  @BeforeEach
  public void setUp() {
    provider = new GlobalHoodieSplitProvider();
    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
    split3 = createTestSplit(3, "file3");
  }

  @Test
  public void testGetNextFromEmptyProvider() {
    assertFalse(provider.getNext(0, null).isPresent(),
        "Should return empty option when no splits available");
  }

  @Test
  public void testAnySubtaskGetsNextSplit() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Three unrelated subtask ids each pull one split from the shared pool; together they drain it,
    // and every discovered split is handed out exactly once.
    Set<String> served = new HashSet<>();
    served.add(requireSplit(provider.getNext(0, null)).splitId());
    served.add(requireSplit(provider.getNext(5, null)).splitId());
    served.add(requireSplit(provider.getNext(99, "some-host")).splitId());

    assertEquals(
        new HashSet<>(Arrays.asList(split1.splitId(), split2.splitId(), split3.splitId())),
        served,
        "Every split should be served exactly once across arbitrary subtasks");
    assertFalse(provider.getNext(0, null).isPresent(), "Pool should be drained");
    assertEquals(0, provider.pendingSplitCount());
  }

  @Test
  public void testSingleSubtaskCanDrainEntirePool() {
    // Work stealing: one reader may take every split. DefaultHoodieSplitProvider would instead pin
    // most of these to other subtasks and starve this one.
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    assertTrue(provider.getNext(7, null).isPresent());
    assertTrue(provider.getNext(7, null).isPresent());
    assertTrue(provider.getNext(7, null).isPresent());
    assertFalse(provider.getNext(7, null).isPresent(),
        "Fourth request should be empty once the single reader drained the pool");
  }

  @Test
  public void testServedOldestCommitFirstRegardlessOfSubtask() {
    HoodieSourceSplit early = createSplitWithCommit(1, "20260126034716930", "file_early");
    HoodieSourceSplit middle = createSplitWithCommit(2, "20260126034717000", "file_middle");
    HoodieSourceSplit late = createSplitWithCommit(3, "20260126034718000", "file_late");

    // Discover out of order and request from different subtasks: ordering is by commit time, not by
    // requester or insertion order.
    provider.onDiscoveredSplits(Arrays.asList(late, early, middle));

    assertEquals(early.splitId(), requireSplit(provider.getNext(3, null)).splitId());
    assertEquals(middle.splitId(), requireSplit(provider.getNext(8, null)).splitId());
    assertEquals(late.splitId(), requireSplit(provider.getNext(0, null)).splitId());
  }

  @Test
  public void testOnUnassignedSplitsReturnedToPoolForAnySubtask() {
    provider.onDiscoveredSplits(Collections.singletonList(split1));
    HoodieSourceSplit taken = requireSplit(provider.getNext(0, null));
    assertEquals(0, provider.pendingSplitCount());

    // A failed reader hands the split back; a different subtask can pick it up.
    provider.onUnassignedSplits(Collections.singletonList(taken));
    assertEquals(1, provider.pendingSplitCount(), "Returned split should be back in the pool");
    assertEquals(taken.splitId(), requireSplit(provider.getNext(4, null)).splitId());
  }

  @Test
  public void testPendingSplitCount() {
    assertEquals(0, provider.pendingSplitCount(), "Initially should have 0 pending splits");

    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    assertEquals(3, provider.pendingSplitCount());

    provider.getNext(0, null);
    provider.getNext(1, null);
    assertEquals(1, provider.pendingSplitCount(),
        "Count should drop as splits are served to any subtask");
  }

  @Test
  public void testMultipleDiscoveryCalls() {
    provider.onDiscoveredSplits(Collections.singletonList(split1));
    provider.onDiscoveredSplits(Arrays.asList(split2, split3));
    assertEquals(3, provider.pendingSplitCount(), "All discovered splits accumulate in the pool");
  }

  @Test
  public void testEmptyDiscoveredSplits() {
    provider.onDiscoveredSplits(Collections.emptyList());
    assertEquals(0, provider.pendingSplitCount());
    assertFalse(provider.getNext(0, null).isPresent());
  }

  @Test
  public void testState() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    Collection<HoodieSourceSplitState> states = provider.state();
    assertEquals(3, states.size(), "State should contain all pending splits");
    for (HoodieSourceSplitState state : states) {
      assertEquals(HoodieSourceSplitStatus.UNASSIGNED, state.getStatus(),
          "Pending splits should be UNASSIGNED");
    }
  }

  @Test
  public void testStateAfterConsumingSomeSplits() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    provider.getNext(0, null);
    provider.getNext(1, null);

    assertEquals(1, provider.state().size(), "State should only reflect the remaining split");
  }

  @Test
  public void testStateRoundTripsThroughRediscovery() {
    // Mirrors the enumerator restore path: snapshot pending splits, rebuild a fresh provider and
    // re-discover them. No split is lost or duplicated.
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));
    provider.getNext(0, null); // one assigned, two remain pending in the checkpoint

    List<HoodieSourceSplit> checkpointed = new ArrayList<>();
    for (HoodieSourceSplitState state : provider.state()) {
      checkpointed.add(state.getSplit());
    }

    GlobalHoodieSplitProvider restored = new GlobalHoodieSplitProvider();
    restored.onDiscoveredSplits(checkpointed);
    assertEquals(2, restored.pendingSplitCount());
    assertTrue(restored.getNext(0, null).isPresent());
    assertTrue(restored.getNext(0, null).isPresent());
    assertFalse(restored.getNext(0, null).isPresent());
  }

  @Test
  public void testIsAvailable() {
    CompletableFuture<Void> future = provider.isAvailable();
    assertNotNull(future, "isAvailable should return a future");
    assertFalse(future.isDone(), "Future should not be completed with no splits");
    assertSame(future, provider.isAvailable(),
        "The same future should be returned until it completes");
  }

  @Test
  public void testIsAvailableCompletesOnDiscovery() {
    CompletableFuture<Void> future = provider.isAvailable();

    provider.onDiscoveredSplits(Collections.singletonList(split1));

    assertTrue(future.isDone(), "Future should complete once splits land in the pool");
    assertFalse(provider.isAvailable().isDone(),
        "A fresh, uncompleted future should be handed out afterwards");
  }

  @Test
  public void testPendingRecordsUnsupported() {
    assertThrows(UnsupportedOperationException.class, () -> provider.pendingRecords());
  }

  @Test
  public void testConcurrentDrainServesEachSplitExactlyOnce() throws Exception {
    final int splitCount = 500;
    final int readerCount = 8;
    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < splitCount; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    provider.onDiscoveredSplits(splits);

    ConcurrentLinkedQueue<String> served = new ConcurrentLinkedQueue<>();
    CountDownLatch start = new CountDownLatch(1);
    List<Future<?>> drains = new ArrayList<>();
    ExecutorService readers = Executors.newFixedThreadPool(readerCount);
    try {
      for (int reader = 0; reader < readerCount; reader++) {
        final int subtaskId = reader;
        drains.add(readers.submit(() -> {
          start.await();
          Option<HoodieSourceSplit> next;
          while ((next = provider.getNext(subtaskId, null)).isPresent()) {
            served.add(next.get().splitId());
          }
          return null;
        }));
      }
      start.countDown();
      readers.shutdown();
      assertTrue(readers.awaitTermination(30, TimeUnit.SECONDS), "Readers should drain the pool");
      for (Future<?> drain : drains) {
        drain.get(); // surface any failure inside a reader thread
      }
    } finally {
      readers.shutdownNow();
    }

    assertEquals(splitCount, served.size(), "No split should be served twice");
    assertEquals(splitCount, new HashSet<>(served).size(), "No split should be lost");
    assertEquals(0, provider.pendingSplitCount(), "Pool should be fully drained");
  }

  private static HoodieSourceSplit requireSplit(Option<HoodieSourceSplit> option) {
    assertTrue(option.isPresent(), "Expected a split to be available");
    return option.get();
  }

  private HoodieSourceSplit createSplitWithCommit(int splitNum, String latestCommit, String basePath) {
    return new HoodieSourceSplit(
        splitNum,
        basePath,
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        latestCommit,
        "file" + splitNum,
        Option.empty());
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0" + splitNum + "_20260126034717000.parquet",
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        "2026012603471700" + splitNum,
        fileId,
        Option.empty());
  }
}
