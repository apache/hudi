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

package org.apache.hudi.common.index.vector.search;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the continuation core (RFC-109 v3 §10): windowing the retained ordered pool into batches
 * without rescanning, and accumulating K live exact results with dedup. Simulates stale/deleted
 * crowding that forces the reranker to draw additional batches.
 */
public class TestVectorContinuation {

  private static VectorRowRequest row(String key, double dist, VectorCandidateState state) {
    return new VectorRowRequest(key, 0L, state, dist);
  }

  /** Drives the continuation loop exactly as the reranker will: draw while needsMore && hasMore. */
  private static VectorTopKAccumulator drive(List<VectorRowRequest> pool, int topK,
                                             int initial, int batch, int max, int[] outConsumedBatches) {
    VectorTopKAccumulator acc = new VectorTopKAccumulator(topK);
    VectorContinuationController<VectorRowRequest> ctl =
        new VectorContinuationController<>(pool, initial, batch, max);
    int batches = 0;
    while (acc.needsMore() && ctl.hasMore()) {
      List<VectorRowRequest> b = ctl.nextBatch();
      batches++;
      for (VectorRowRequest r : b) {
        if (r.getState() != VectorCandidateState.DELETED) { // live (SERVE or STALE-fetched-live)
          acc.offer(r.getRecordKey(), r.getApproximateDistance(), null);
        }
      }
    }
    if (outConsumedBatches != null) {
      outConsumedBatches[0] = batches;
      outConsumedBatches[1] = ctl.consumed();
    }
    return acc;
  }

  @Test
  void staleDeletedCrowdingTriggersContinuationUntilKLive() {
    List<VectorRowRequest> pool = new ArrayList<>();
    // rows 0-2 DELETED, row 3 STALE(live); rows 4-19 SERVE(live). Distances = index.
    pool.add(row("r0", 0, VectorCandidateState.DELETED));
    pool.add(row("r1", 1, VectorCandidateState.DELETED));
    pool.add(row("r2", 2, VectorCandidateState.DELETED));
    pool.add(row("r3", 3, VectorCandidateState.STALE));
    for (int i = 4; i < 20; i++) {
      pool.add(row("r" + i, i, VectorCandidateState.SERVE));
    }

    int[] out = new int[2];
    VectorTopKAccumulator acc = drive(pool, 5, 4, 4, 4096, out);

    assertEquals(5, acc.liveCount(), "must accumulate exactly K live despite crowding");
    assertFalse(acc.needsMore());
    // batch1 (rows 0-3) -> 1 live; batch2 (rows 4-7) -> 4 live => 5. Two batches, consumed 8.
    assertEquals(2, out[0], "should have drawn 2 batches");
    assertEquals(8, out[1], "consumed 8 retained candidates (no rescan)");

    List<VectorSearchResult> top = acc.topK();
    assertEquals(5, top.size());
    assertEquals("r3", top.get(0).getRecordKey(), "nearest live is the STALE-but-live r3");
    assertEquals("r7", top.get(4).getRecordKey());
  }

  @Test
  void exhaustsPoolAndReturnsPartialWhenTooFewLive() {
    List<VectorRowRequest> pool = new ArrayList<>();
    pool.add(row("a", 0, VectorCandidateState.SERVE));
    pool.add(row("b", 1, VectorCandidateState.DELETED));
    pool.add(row("c", 2, VectorCandidateState.SERVE));
    pool.add(row("d", 3, VectorCandidateState.DELETED));
    pool.add(row("e", 4, VectorCandidateState.SERVE));

    int[] out = new int[2];
    VectorTopKAccumulator acc = drive(pool, 5, 4, 4, 4096, out);

    assertEquals(3, acc.liveCount(), "only 3 live exist -> partial");
    assertTrue(acc.needsMore(), "still needs more but pool is exhausted");
    assertEquals(5, out[1], "consumed the whole retained pool, no more");
  }

  @Test
  void accumulatorDedupsByRecordKeyKeepingSmallerDistance() {
    VectorTopKAccumulator acc = new VectorTopKAccumulator(3);
    acc.offer("x", 5.0, null);
    acc.offer("x", 2.0, null); // same key, smaller distance wins
    acc.offer("y", 3.0, null);
    assertEquals(2, acc.liveCount(), "duplicate key counted once");
    assertEquals("x", acc.topK().get(0).getRecordKey());
    assertEquals(2.0, acc.topK().get(0).getDistance(), 1e-9);
  }

  @Test
  void maxRerankCandidatesCapsTheRetainedWindow() {
    List<VectorRowRequest> pool = new ArrayList<>();
    for (int i = 0; i < 100; i++) {
      pool.add(row("r" + i, i, VectorCandidateState.DELETED)); // all deleted -> never satisfies K
    }
    int[] out = new int[2];
    VectorTopKAccumulator acc = drive(pool, 5, 4, 4, 12, out); // cap at 12
    assertEquals(0, acc.liveCount());
    assertEquals(12, out[1], "must not draw beyond maxRerankCandidates");
  }
}
