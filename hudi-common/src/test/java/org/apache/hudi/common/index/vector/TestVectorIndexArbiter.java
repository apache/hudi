/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.index.vector.VectorIndexArbiter.Decision;
import org.apache.hudi.common.index.vector.VectorIndexArbiter.ExclusionCounts;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for {@link VectorIndexArbiter} — the RFC-109 finalist arbiter decision table.
 */
class TestVectorIndexArbiter {

  private static HoodieRecordGlobalLocation loc(String partition, String instant, String fileId) {
    return new HoodieRecordGlobalLocation(partition, instant, fileId, 42L);
  }

  @Test
  void hitAndLocationMatchesServes() {
    HoodieRecordGlobalLocation current = loc("2024/01", "t100", "fg-1");
    assertEquals(Decision.SERVE,
        VectorIndexArbiter.classify("2024/01", "fg-1", "t100", current));
  }

  @Test
  void hitButDifferentInstantIsStale() {
    // Same file group + partition, newer base instant (e.g. compaction rewrote the slice).
    HoodieRecordGlobalLocation current = loc("2024/01", "t200", "fg-1");
    assertEquals(Decision.STALE,
        VectorIndexArbiter.classify("2024/01", "fg-1", "t100", current));
  }

  @Test
  void hitButDifferentFileGroupIsStale() {
    // The cluster-move case: record was updated and now lives in a different file group.
    HoodieRecordGlobalLocation current = loc("2024/01", "t200", "fg-9");
    assertEquals(Decision.STALE,
        VectorIndexArbiter.classify("2024/01", "fg-1", "t100", current));
  }

  @Test
  void hitButDifferentPartitionIsStale() {
    HoodieRecordGlobalLocation current = loc("2024/02", "t100", "fg-1");
    assertEquals(Decision.STALE,
        VectorIndexArbiter.classify("2024/01", "fg-1", "t100", current));
  }

  @Test
  void rliMissIsDeleted() {
    assertEquals(Decision.DELETED,
        VectorIndexArbiter.classify("2024/01", "fg-1", "t100", null));
  }

  @Test
  void nullPostingInstantAgainstRealLocationIsStale() {
    // A locator missing its base instant cannot claim positional trust; it is not a delete.
    HoodieRecordGlobalLocation current = loc("2024/01", "t100", "fg-1");
    assertEquals(Decision.STALE,
        VectorIndexArbiter.classify("2024/01", "fg-1", null, current));
  }

  @Test
  void exclusionCountsSplitStaleAndDeleted() {
    ExclusionCounts counts = new ExclusionCounts();
    counts.record(Decision.SERVE);
    counts.record(Decision.STALE);
    counts.record(Decision.STALE);
    counts.record(Decision.DELETED);
    assertEquals(2L, counts.stale());
    assertEquals(1L, counts.deleted());
    assertEquals(3L, counts.total());
    assertEquals("arbiterExclusions{stale=2, deleted=1}", counts.toString());
  }
}
