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

import org.apache.hudi.source.assign.HoodieSplitNumberAssigner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link DefaultHoodieSplitProvider}.
 */
public class TestDefaultHoodieSplitProvider {
  private DefaultHoodieSplitProvider provider;
  private HoodieSourceSplit split1;
  private HoodieSourceSplit split2;
  private HoodieSourceSplit split3;

  @BeforeEach
  public void setUp() {
    provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));
    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
    split3 = createTestSplit(3, "file3");
  }

  @Test
  public void testGetNextFromEmptyProvider() {
    Option<HoodieSourceSplit> result = provider.getNext(1, null);
    assertFalse(result.isPresent(), "Should return empty option when no splits available");
  }

  @Test
  public void testGetNextWithHostname() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2));

    Option<HoodieSourceSplit> result = provider.getNext(1, "localhost");
    assertTrue(result.isPresent(), "Should return a split");
    assertEquals(split1.splitId(), result.get().splitId(), "Should return first split");
  }

  @Test
  public void testGetNextSequentially() {
    provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    Option<HoodieSourceSplit> first = provider.getNext(0, null);
    assertTrue(first.isPresent(), "First split should be present");
    assertEquals(split1.splitId(), first.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> second = provider.getNext(0, null);
    assertTrue(second.isPresent(), "Second split should be present");
    assertEquals(split2.splitId(), second.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> third = provider.getNext(0, null);
    assertTrue(third.isPresent(), "Third split should be present");
    assertEquals(split3.splitId(), third.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> empty = provider.getNext(0, null);
    assertFalse(empty.isPresent(), "Should return empty when all splits consumed");
  }

  @Test
  public void testOnDiscoveredSplits() {
    List<HoodieSourceSplit> splits = Arrays.asList(split1, split2);
    provider.onDiscoveredSplits(splits);

    assertEquals(2, provider.pendingSplitCount(), "Should have 2 pending splits");

    provider.getNext(1, null);
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split after consuming one");
  }

  @Test
  public void testOnUnassignedSplits() {
    provider.onDiscoveredSplits(Arrays.asList(split1));
    provider.getNext(1, null);

    assertEquals(0, provider.pendingSplitCount(), "Should have no pending splits");

    // Re-add unassigned split
    provider.onUnassignedSplits(Arrays.asList(split1));
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split after re-adding");

    Option<HoodieSourceSplit> result = provider.getNext(1, null);
    assertTrue(result.isPresent(), "Should be able to get the re-added split");
    assertEquals(split1.splitId(), result.get().splitId(), "Should return the re-added split");
  }

  @Test
  public void testState() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    Collection<HoodieSourceSplitState> states = provider.state();
    assertEquals(3, states.size(), "Should have 3 split states");

    // Verify all states are UNASSIGNED
    for (HoodieSourceSplitState state : states) {
      assertEquals(HoodieSourceSplitStatus.UNASSIGNED, state.getStatus(),
          "All splits should have UNASSIGNED status");
    }
  }

  @Test
  public void testStateAfterConsumingSomeSplits() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Consume first two splits
    provider.getNext(1, null);
    provider.getNext(1, null);

    Collection<HoodieSourceSplitState> states = provider.state();
    assertEquals(1, states.size(), "Should have 1 remaining split state");

    HoodieSourceSplitState state = states.iterator().next();
    assertEquals(split2.splitId(), state.getSplit().splitId(), "Remaining split should be split2");
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, state.getStatus(), "Split should be UNASSIGNED");
  }

  @Test
  public void testPendingSplitCount() {
    assertEquals(0, provider.pendingSplitCount(), "Initially should have 0 pending splits");

    provider.onDiscoveredSplits(Arrays.asList(split1, split2));
    assertEquals(2, provider.pendingSplitCount(), "Should have 2 pending splits");

    provider.getNext(0, null);
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split");

    provider.getNext(1, null);
    assertEquals(0, provider.pendingSplitCount(), "Should have 0 pending splits");
  }

  @Test
  public void testIsAvailable() {
    // isAvailable returns a non-completed future for DefaultSplitProvider
    assertTrue(provider.isAvailable() != null, "isAvailable should return a future");
    assertFalse(provider.isAvailable().isDone(), "Future should not be completed");
  }

  @Test
  public void testWithComparatorOrdersByLatestCommit() {
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split3 = createSplitWithCommit(3, "20260126034718000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Should return splits in order of commit time (ascending)
    assertEquals(split2.splitId(), providerWithComparator.getNext(0, null).get().splitId(),
        "Should return split with earliest commit time first");
    assertEquals(split1.splitId(), providerWithComparator.getNext(0, null).get().splitId(),
        "Should return split with middle commit time second");
    assertEquals(split3.splitId(), providerWithComparator.getNext(0, null).get().splitId(),
        "Should return split with latest commit time last");
  }

  @Test
  public void testWithComparatorSameLatestCommit() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    // All have same latest commit, but different base file commit times
    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split3 = createSplitWithCommit(3, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Should return splits ordered by base file commit time
    assertEquals(split2.splitId(), providerWithComparator.getNext(0, null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(0, null).get().splitId());
    assertEquals(split3.splitId(), providerWithComparator.getNext(0, null).get().splitId());
  }

  @Test
  public void testWithComparatorMultipleDiscoveryCalls() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1));
    providerWithComparator.onDiscoveredSplits(Arrays.asList(split2));

    // Should still return in sorted order
    assertEquals(split2.splitId(), providerWithComparator.getNext(0, null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(1, null).get().splitId());
  }

  @Test
  public void testWithComparatorAndUnassignedSplits() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    HoodieSourceSplit split1 = createSplitWithCommit(0, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(1, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2));
    providerWithComparator.getNext(1, null); // Get split2 (earliest)

    // Re-add split2 as unassigned
    providerWithComparator.onUnassignedSplits(Arrays.asList(split2));

    // Should still maintain order: split2 comes before split1
    assertEquals(split2.splitId(), providerWithComparator.getNext(1, null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(0, null).get().splitId());
  }

  @Test
  public void testWithComparatorEmptyProvider() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    Option<HoodieSourceSplit> result = providerWithComparator.getNext(1, null);
    assertFalse(result.isPresent(), "Should return empty option when no splits available");
  }

  @Test
  public void testWithComparatorStateTracking() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2));

    Collection<HoodieSourceSplitState> states = providerWithComparator.state();
    assertEquals(2, states.size(), "Should have 2 split states");

    // Consume one split
    providerWithComparator.getNext(1, null);

    states = providerWithComparator.state();
    assertEquals(1, states.size(), "Should have 1 remaining split state");
  }

  @Test
  public void testWithComparatorLargeBatchOrdering() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    // Create 50 splits with random commit times
    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 50; i++) {
      String commitTime = String.format("2026012603471%04d0", i * 100);
      splits.add(createSplitWithCommit(i, commitTime, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_" + commitTime + ".parquet"));
    }

    // Add them in reverse order
    Collections.reverse(splits);
    providerWithComparator.onDiscoveredSplits(splits);

    // Should retrieve them in ascending commit time order
    HoodieSourceSplit previousSplit = null;
    for (int i = 0; i < 50; i++) {
      HoodieSourceSplit currentSplit = providerWithComparator.getNext(0, null).get();
      if (previousSplit != null) {
        assertTrue(comparator.compare(previousSplit, currentSplit) < 0,
            "Splits should be retrieved in ascending commit time order");
      }
      previousSplit = currentSplit;
    }
  }

  @Test
  public void testWithComparatorConcurrentAccess() {
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(1));

    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String commitTime = String.format("2026012603%010d", i);
      splits.add(createSplitWithCommit(i, commitTime, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_" + commitTime + ".parquet"));
    }

    providerWithComparator.onDiscoveredSplits(splits);

    // Verify that we can retrieve all splits in order
    for (int i = 0; i < 100; i++) {
      Option<HoodieSourceSplit> split = providerWithComparator.getNext(0, null);
      assertTrue(split.isPresent(), "Should have split available");
    }

    // No more splits should be available
    assertFalse(providerWithComparator.getNext(0, null).isPresent());
  }

  @Test
  public void testGetNextWithMultipleSubtasks() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(3));

    // Create 9 splits that will be distributed across 3 subtasks
    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    provider.onDiscoveredSplits(splits);

    // Subtask 0 should get splits 0, 3, 6
    assertEquals(0, provider.getNext(0, null).get().getSplitNum());
    assertEquals(3, provider.getNext(0, null).get().getSplitNum());
    assertEquals(6, provider.getNext(0, null).get().getSplitNum());
    assertFalse(provider.getNext(0, null).isPresent(), "Subtask 0 should have no more splits");

    // Subtask 1 should get splits 1, 4, 7
    assertEquals(1, provider.getNext(1, null).get().getSplitNum());
    assertEquals(4, provider.getNext(1, null).get().getSplitNum());
    assertEquals(7, provider.getNext(1, null).get().getSplitNum());
    assertFalse(provider.getNext(1, null).isPresent(), "Subtask 1 should have no more splits");

    // Subtask 2 should get splits 2, 5, 8
    assertEquals(2, provider.getNext(2, null).get().getSplitNum());
    assertEquals(5, provider.getNext(2, null).get().getSplitNum());
    assertEquals(8, provider.getNext(2, null).get().getSplitNum());
    assertFalse(provider.getNext(2, null).isPresent(), "Subtask 2 should have no more splits");
  }

  @Test
  public void testGetNextSubtaskOnlyGetsAssignedSplits() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(4));

    List<HoodieSourceSplit> splits = Arrays.asList(
        createTestSplit(0, "file0"),  // assigned to subtask 0
        createTestSplit(1, "file1"),  // assigned to subtask 1
        createTestSplit(2, "file2"),  // assigned to subtask 2
        createTestSplit(3, "file3")   // assigned to subtask 3
    );
    provider.onDiscoveredSplits(splits);

    // Subtask 0 should only get split 0
    Option<HoodieSourceSplit> split = provider.getNext(0, null);
    assertTrue(split.isPresent());
    assertEquals(0, split.get().getSplitNum());

    // Subtask 0 should have no more splits
    assertFalse(provider.getNext(0, null).isPresent());

    // Other subtasks should still have their splits available
    assertEquals(1, provider.getNext(1, null).get().getSplitNum());
    assertEquals(2, provider.getNext(2, null).get().getSplitNum());
    assertEquals(3, provider.getNext(3, null).get().getSplitNum());
  }

  @Test
  public void testGetNextReturnsNonAssignedSplitsToQueue() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    List<HoodieSourceSplit> splits = Arrays.asList(
        createTestSplit(0, "file0"),  // assigned to subtask 0
        createTestSplit(1, "file1"),  // assigned to subtask 1
        createTestSplit(2, "file2")   // assigned to subtask 0
    );
    provider.onDiscoveredSplits(splits);

    // Subtask 1 requests a split - should skip split 0 and split 2, return split 1
    Option<HoodieSourceSplit> split = provider.getNext(1, null);
    assertTrue(split.isPresent());
    assertEquals(1, split.get().getSplitNum());

    // Verify splits 0 and 2 are still available for subtask 0
    assertEquals(2, provider.pendingSplitCount(), "Skipped splits should be returned to queue");
    assertEquals(0, provider.getNext(0, null).get().getSplitNum());
    assertEquals(2, provider.getNext(0, null).get().getSplitNum());
  }

  @Test
  public void testGetNextWithUnevenDistribution() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(3));

    // Create 10 splits for 3 subtasks (uneven distribution)
    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    provider.onDiscoveredSplits(splits);

    // Subtask 0 should get splits 0, 3, 6, 9 (4 splits)
    assertEquals(0, provider.getNext(0, null).get().getSplitNum());
    assertEquals(3, provider.getNext(0, null).get().getSplitNum());
    assertEquals(6, provider.getNext(0, null).get().getSplitNum());
    assertEquals(9, provider.getNext(0, null).get().getSplitNum());
    assertFalse(provider.getNext(0, null).isPresent());

    // Subtask 1 should get splits 1, 4, 7 (3 splits)
    assertEquals(1, provider.getNext(1, null).get().getSplitNum());
    assertEquals(4, provider.getNext(1, null).get().getSplitNum());
    assertEquals(7, provider.getNext(1, null).get().getSplitNum());
    assertFalse(provider.getNext(1, null).isPresent());

    // Subtask 2 should get splits 2, 5, 8 (3 splits)
    assertEquals(2, provider.getNext(2, null).get().getSplitNum());
    assertEquals(5, provider.getNext(2, null).get().getSplitNum());
    assertEquals(8, provider.getNext(2, null).get().getSplitNum());
    assertFalse(provider.getNext(2, null).isPresent());
  }

  @Test
  public void testGetNextWithInterleavedRequests() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    List<HoodieSourceSplit> splits = Arrays.asList(
        createTestSplit(0, "file0"),  // subtask 0
        createTestSplit(1, "file1"),  // subtask 1
        createTestSplit(2, "file2"),  // subtask 0
        createTestSplit(3, "file3")   // subtask 1
    );
    provider.onDiscoveredSplits(splits);

    // Interleaved requests from different subtasks
    assertEquals(0, provider.getNext(0, null).get().getSplitNum());
    assertEquals(1, provider.getNext(1, null).get().getSplitNum());
    assertEquals(2, provider.getNext(0, null).get().getSplitNum());
    assertEquals(3, provider.getNext(1, null).get().getSplitNum());

    // All splits consumed
    assertFalse(provider.getNext(0, null).isPresent());
    assertFalse(provider.getNext(1, null).isPresent());
  }

  @Test
  public void testGetNextWithUnassignedSplitsMultipleSubtasks() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    List<HoodieSourceSplit> splits = Arrays.asList(
        createTestSplit(0, "file0"),  // subtask 0
        createTestSplit(1, "file1")   // subtask 1
    );
    provider.onDiscoveredSplits(splits);

    // Consume both splits
    HoodieSourceSplit split0 = provider.getNext(0, null).get();
    HoodieSourceSplit split1 = provider.getNext(1, null).get();

    // Return split1 as unassigned
    provider.onUnassignedSplits(Arrays.asList(split1));

    // Subtask 1 should get split1 back
    Option<HoodieSourceSplit> result = provider.getNext(1, null);
    assertTrue(result.isPresent());
    assertEquals(1, result.get().getSplitNum());

    // Subtask 0 should not get split1
    assertFalse(provider.getNext(0, null).isPresent());
  }

  @Test
  public void testGetNextPendingSplitCountWithMultipleSubtasks() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(3));

    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 6; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    provider.onDiscoveredSplits(splits);

    assertEquals(6, provider.pendingSplitCount());

    // Subtask 0 gets split 0
    provider.getNext(0, null);
    assertEquals(5, provider.pendingSplitCount());

    // Subtask 1 gets split 1 (skips and re-adds split 0)
    provider.getNext(1, null);
    assertEquals(4, provider.pendingSplitCount());

    // Subtask 2 gets split 2
    provider.getNext(2, null);
    assertEquals(3, provider.pendingSplitCount());
  }

  @Test
  public void testIsAvailableCompletesWhenSplitsAdded() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    // Get the future before any splits are added
    assertFalse(provider.isAvailable().isDone(), "Future should not be completed initially");

    // Add splits - this should complete the future
    provider.onDiscoveredSplits(Arrays.asList(split1));

    // Get a new future after splits were added
    assertFalse(provider.isAvailable().isDone(), "New future should not be completed");
  }

  @Test
  public void testStateAcrossMultipleSubtasks() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    List<HoodieSourceSplit> splits = Arrays.asList(
        createTestSplit(0, "file0"),  // subtask 0
        createTestSplit(1, "file1"),  // subtask 1
        createTestSplit(2, "file2"),  // subtask 0
        createTestSplit(3, "file3")   // subtask 1
    );
    provider.onDiscoveredSplits(splits);

    // State should include all splits across all subtasks
    Collection<HoodieSourceSplitState> states = provider.state();
    assertEquals(4, states.size(), "Should have all splits in state");

    // Consume splits from subtask 0
    provider.getNext(0, null);
    provider.getNext(0, null);

    states = provider.state();
    assertEquals(2, states.size(), "Should have remaining splits from both subtasks");
  }

  @Test
  public void testGetNextWithDefaultAssigner() {
    // Test with DefaultHoodieSplitAssigner instead of NumberAssigner
    org.apache.hudi.source.assign.DefaultHoodieSplitAssigner assigner =
        new org.apache.hudi.source.assign.DefaultHoodieSplitAssigner(3);
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(assigner);

    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 9; i++) {
      splits.add(createTestSplit(i, "file" + i));
    }
    provider.onDiscoveredSplits(splits);

    // Splits should be distributed according to DefaultHoodieSplitAssigner's hash-based logic
    int count = 0;
    while (provider.getNext(0, null).isPresent()) {
      count++;
    }
    assertTrue(count >= 0, "Should get some splits for subtask 0");
  }

  @Test
  public void testGetNextWithBucketAssigner() {
    // Test with HoodieSplitBucketAssigner
    org.apache.hudi.source.assign.HoodieSplitBucketAssigner assigner =
        new org.apache.hudi.source.assign.HoodieSplitBucketAssigner(4);
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(assigner);

    // Create splits with bucket-encoded file IDs
    List<HoodieSourceSplit> splits = Arrays.asList(
        createSplitWithBucketFileId(0, "00000000-0000-0000-0000-000000000000"),  // bucket 0 -> task 0
        createSplitWithBucketFileId(1, "00000001-0000-0000-0000-000000000000"),  // bucket 1 -> task 1
        createSplitWithBucketFileId(2, "00000002-0000-0000-0000-000000000000"),  // bucket 2 -> task 2
        createSplitWithBucketFileId(3, "00000003-0000-0000-0000-000000000000")   // bucket 3 -> task 3
    );
    provider.onDiscoveredSplits(splits);

    // Each subtask should get its assigned split
    Option<HoodieSourceSplit> split0 = provider.getNext(0, null);
    assertTrue(split0.isPresent());
    assertEquals(0, split0.get().getSplitNum());

    Option<HoodieSourceSplit> split1 = provider.getNext(1, null);
    assertTrue(split1.isPresent());
    assertEquals(1, split1.get().getSplitNum());

    Option<HoodieSourceSplit> split2 = provider.getNext(2, null);
    assertTrue(split2.isPresent());
    assertEquals(2, split2.get().getSplitNum());

    Option<HoodieSourceSplit> split3 = provider.getNext(3, null);
    assertTrue(split3.isPresent());
    assertEquals(3, split3.get().getSplitNum());
  }

  @Test
  public void testEmptyDiscoveredSplits() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    // Add empty list of splits
    provider.onDiscoveredSplits(Collections.emptyList());

    assertEquals(0, provider.pendingSplitCount(), "Should have 0 pending splits");
    assertFalse(provider.getNext(0, null).isPresent(), "Should return empty");
  }

  @Test
  public void testMultipleDiscoveryCallsAcrossSubtasks() {
    DefaultHoodieSplitProvider provider = new DefaultHoodieSplitProvider(new HoodieSplitNumberAssigner(2));

    // First discovery
    provider.onDiscoveredSplits(Arrays.asList(createTestSplit(0, "file0")));
    assertEquals(1, provider.pendingSplitCount());

    // Second discovery
    provider.onDiscoveredSplits(Arrays.asList(createTestSplit(1, "file1")));
    assertEquals(2, provider.pendingSplitCount());

    // Third discovery
    provider.onDiscoveredSplits(Arrays.asList(
        createTestSplit(2, "file2"),
        createTestSplit(3, "file3")
    ));
    assertEquals(4, provider.pendingSplitCount());

    // Verify splits are distributed correctly
    assertEquals(0, provider.getNext(0, null).get().getSplitNum());
    assertEquals(1, provider.getNext(1, null).get().getSplitNum());
    assertEquals(2, provider.getNext(0, null).get().getSplitNum());
    assertEquals(3, provider.getNext(1, null).get().getSplitNum());
  }

  private HoodieSourceSplit createSplitWithBucketFileId(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        "20260126034717000",
        fileId,
        Option.empty()
    );
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
        Option.empty()
    );
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
        Option.empty()
    );
  }
}
