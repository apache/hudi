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

import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorStalePolicy;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class TestMdtVectorCandidateSource {

  @Test
  void scansOnceAndRetainsCanonicalOverlayPool() {
    AtomicInteger scans = new AtomicInteger();
    AtomicInteger scanLimit = new AtomicInteger();
    VectorPostingScanner scanner = (plan, context, packedCandidateLimit) -> {
      scans.incrementAndGet();
      scanLimit.set(packedCandidateLimit);
      return new VectorPostingScanResult(
          Arrays.asList(candidate("deleted", 1, 1), candidate("moved", 1, 2),
              candidate("backfill", 1, 3)),
          Collections.singletonList(candidate("moved", 2, 4)),
          Arrays.asList(new VectorPostingKey(1, 0, "deleted"),
              new VectorPostingKey(1, 0, "moved")));
    };
    MdtVectorCandidateSource source = new MdtVectorCandidateSource(scanner, 2);

    VectorCandidatePool pool = source.scan(plan(2), null);
    List<VectorCandidate> firstBatch = pool.nextBatch().collectAsList();

    assertEquals(1, scans.get());
    assertEquals(4, scanLimit.get());
    assertEquals(Arrays.asList("backfill", "moved"),
        Arrays.asList(firstBatch.get(0).getRecordKey(), firstBatch.get(1).getRecordKey()));
    assertEquals(2, firstBatch.get(1).getClusterId());
    assertFalse(pool.hasMore());
  }

  private static VectorSearchPlan plan(int maxRerankCandidates) {
    VectorSearchBudget budget = new VectorSearchBudget(
        5000, maxRerankCandidates, 1, maxRerankCandidates, 10, 1,
        VectorExecutionMode.LOCAL, 10, DeadlinePolicy.FAIL);
    VectorSearchRequest request = new VectorSearchRequest(
        "embedding", new float[] {1f}, VectorDistanceMetric.L2, 1, 1, 1, true,
        VectorStalePolicy.FAIL, "001", budget);
    VectorSearchSnapshot snapshot = new VectorSearchSnapshot(
        "001", new VectorIndexSnapshot(1, 1, 1, "rot-v1", "quant-v1", "001"));
    VectorExecutionDecision decision = new VectorExecutionDecision(
        VectorExecutionMode.LOCAL, VectorExecutionMode.LOCAL,
        maxRerankCandidates, 10, "test");
    return new VectorSearchPlan(request, snapshot, decision);
  }

  private static VectorCandidate candidate(String key, int cluster, double distance) {
    return new VectorCandidate(key, cluster, 0, distance,
        new VectorPostingLocator(1, cluster, 0, 0, 0, "p", "f", "001", 1));
  }
}
