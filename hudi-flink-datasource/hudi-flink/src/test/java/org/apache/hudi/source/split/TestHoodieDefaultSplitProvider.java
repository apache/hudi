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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for {@link DefaultHoodieSplitProvider}.
 */
public class TestHoodieDefaultSplitProvider {
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
}
