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
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    provider = new DefaultHoodieSplitProvider();
    split1 = createTestSplit(1, "file1");
    split2 = createTestSplit(2, "file2");
    split3 = createTestSplit(3, "file3");
  }

  @Test
  public void testGetNextFromEmptyProvider() {
    Option<HoodieSourceSplit> result = provider.getNext(null);
    assertFalse(result.isPresent(), "Should return empty option when no splits available");
  }

  @Test
  public void testGetNextWithHostname() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2));

    Option<HoodieSourceSplit> result = provider.getNext("localhost");
    assertTrue(result.isPresent(), "Should return a split");
    assertEquals(split1.splitId(), result.get().splitId(), "Should return first split");
  }

  @Test
  public void testGetNextSequentially() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    Option<HoodieSourceSplit> first = provider.getNext(null);
    assertTrue(first.isPresent(), "First split should be present");
    assertEquals(split1.splitId(), first.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> second = provider.getNext(null);
    assertTrue(second.isPresent(), "Second split should be present");
    assertEquals(split2.splitId(), second.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> third = provider.getNext(null);
    assertTrue(third.isPresent(), "Third split should be present");
    assertEquals(split3.splitId(), third.get().splitId(), "Should return splits in order");

    Option<HoodieSourceSplit> empty = provider.getNext(null);
    assertFalse(empty.isPresent(), "Should return empty when all splits consumed");
  }

  @Test
  public void testOnDiscoveredSplits() {
    List<HoodieSourceSplit> splits = Arrays.asList(split1, split2);
    provider.onDiscoveredSplits(splits);

    assertEquals(2, provider.pendingSplitCount(), "Should have 2 pending splits");

    provider.getNext(null);
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split after consuming one");
  }

  @Test
  public void testOnUnassignedSplits() {
    provider.onDiscoveredSplits(Arrays.asList(split1));
    provider.getNext(null);

    assertEquals(0, provider.pendingSplitCount(), "Should have no pending splits");

    // Re-add unassigned split
    provider.onUnassignedSplits(Arrays.asList(split1));
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split after re-adding");

    Option<HoodieSourceSplit> result = provider.getNext(null);
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
      assertEquals(HoodieSourceSplitStatus.UNASSIGNED, state.status(),
          "All splits should have UNASSIGNED status");
    }
  }

  @Test
  public void testStateAfterConsumingSomeSplits() {
    provider.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Consume first two splits
    provider.getNext(null);
    provider.getNext(null);

    Collection<HoodieSourceSplitState> states = provider.state();
    assertEquals(1, states.size(), "Should have 1 remaining split state");

    HoodieSourceSplitState state = states.iterator().next();
    assertEquals(split3.splitId(), state.split().splitId(), "Remaining split should be split3");
    assertEquals(HoodieSourceSplitStatus.UNASSIGNED, state.status(), "Split should be UNASSIGNED");
  }

  @Test
  public void testPendingSplitCount() {
    assertEquals(0, provider.pendingSplitCount(), "Initially should have 0 pending splits");

    provider.onDiscoveredSplits(Arrays.asList(split1, split2));
    assertEquals(2, provider.pendingSplitCount(), "Should have 2 pending splits");

    provider.getNext(null);
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 pending split");

    provider.getNext(null);
    assertEquals(0, provider.pendingSplitCount(), "Should have 0 pending splits");
  }

  @Test
  public void testIsAvailable() {
    // isAvailable returns a non-completed future for DefaultSplitProvider
    assertTrue(provider.isAvailable() != null, "isAvailable should return a future");
    assertFalse(provider.isAvailable().isDone(), "Future should not be completed");
  }

  @Test
  public void testMultipleDiscoveredSplitsCalls() {
    provider.onDiscoveredSplits(Arrays.asList(split1));
    assertEquals(1, provider.pendingSplitCount(), "Should have 1 split");

    provider.onDiscoveredSplits(Arrays.asList(split2, split3));
    assertEquals(3, provider.pendingSplitCount(), "Should have 3 splits total");

    // Verify order is maintained
    assertEquals(split1.splitId(), provider.getNext(null).get().splitId());
    assertEquals(split2.splitId(), provider.getNext(null).get().splitId());
    assertEquals(split3.splitId(), provider.getNext(null).get().splitId());
  }

  @Test
  public void testWithComparatorOrdersByLatestCommit() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split3 = createSplitWithCommit(3, "20260126034718000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Should return splits in order of commit time (ascending)
    assertEquals(split2.splitId(), providerWithComparator.getNext(null).get().splitId(),
        "Should return split with earliest commit time first");
    assertEquals(split1.splitId(), providerWithComparator.getNext(null).get().splitId(),
        "Should return split with middle commit time second");
    assertEquals(split3.splitId(), providerWithComparator.getNext(null).get().splitId(),
        "Should return split with latest commit time last");
  }

  @Test
  public void testWithComparatorSameLatestCommit() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    // All have same latest commit, but different base file commit times
    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");
    HoodieSourceSplit split3 = createSplitWithCommit(3, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034718000.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2, split3));

    // Should return splits ordered by base file commit time
    assertEquals(split2.splitId(), providerWithComparator.getNext(null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(null).get().splitId());
    assertEquals(split3.splitId(), providerWithComparator.getNext(null).get().splitId());
  }

  @Test
  public void testWithComparatorMultipleDiscoveryCalls() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1));
    providerWithComparator.onDiscoveredSplits(Arrays.asList(split2));

    // Should still return in sorted order
    assertEquals(split2.splitId(), providerWithComparator.getNext(null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(null).get().splitId());
  }

  @Test
  public void testWithComparatorAndUnassignedSplits() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2));
    providerWithComparator.getNext(null); // Get split2 (earliest)

    // Re-add split2 as unassigned
    providerWithComparator.onUnassignedSplits(Arrays.asList(split2));

    // Should still maintain order: split2 comes before split1
    assertEquals(split2.splitId(), providerWithComparator.getNext(null).get().splitId());
    assertEquals(split1.splitId(), providerWithComparator.getNext(null).get().splitId());
  }

  @Test
  public void testWithComparatorEmptyProvider() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    Option<HoodieSourceSplit> result = providerWithComparator.getNext(null);
    assertFalse(result.isPresent(), "Should return empty option when no splits available");
  }

  @Test
  public void testWithComparatorStateTracking() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    HoodieSourceSplit split1 = createSplitWithCommit(1, "20260126034717000", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034717000.parquet");
    HoodieSourceSplit split2 = createSplitWithCommit(2, "20260126034716930", "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_20260126034716930.parquet");

    providerWithComparator.onDiscoveredSplits(Arrays.asList(split1, split2));

    Collection<HoodieSourceSplitState> states = providerWithComparator.state();
    assertEquals(2, states.size(), "Should have 2 split states");

    // Consume one split
    providerWithComparator.getNext(null);

    states = providerWithComparator.state();
    assertEquals(1, states.size(), "Should have 1 remaining split state");
  }

  @Test
  public void testWithComparatorNullComparator() {
    assertThrows(IllegalArgumentException.class, () -> new DefaultHoodieSplitProvider(null),
        "Should throw exception when comparator is null");
  }

  @Test
  public void testWithComparatorLargeBatchOrdering() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

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
      HoodieSourceSplit currentSplit = providerWithComparator.getNext(null).get();
      if (previousSplit != null) {
        assertTrue(comparator.compare(previousSplit, currentSplit) < 0,
            "Splits should be retrieved in ascending commit time order");
      }
      previousSplit = currentSplit;
    }
  }

  @Test
  public void testWithComparatorConcurrentAccess() {
    HoodieSourceSplitComparator comparator = new HoodieSourceSplitComparator();
    DefaultHoodieSplitProvider providerWithComparator = new DefaultHoodieSplitProvider(comparator);

    List<HoodieSourceSplit> splits = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      String commitTime = String.format("2026012603%010d", i);
      splits.add(createSplitWithCommit(i, commitTime, "40e603a8-3cc1-4d09-b0a5-1432992b4bf7_1-0-1_" + commitTime + ".parquet"));
    }

    providerWithComparator.onDiscoveredSplits(splits);

    // Verify that we can retrieve all splits in order
    for (int i = 0; i < 100; i++) {
      Option<HoodieSourceSplit> split = providerWithComparator.getNext(null);
      assertTrue(split.isPresent(), "Should have split available");
    }

    // No more splits should be available
    assertFalse(providerWithComparator.getNext(null).isPresent());
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
        "file" + splitNum
    );
  }

  private HoodieSourceSplit createTestSplit(int splitNum, String fileId) {
    return new HoodieSourceSplit(
        splitNum,
        "basePath_" + splitNum,
        Option.empty(),
        "/table/path",
        "/table/path/partition1",
        "read_optimized",
        "19700101000000000",
        fileId
    );
  }
}
