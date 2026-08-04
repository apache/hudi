/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector.search;

import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.index.vector.VectorStalePolicy;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestCommonVectorSearchContinuation {

  @Test
  void drawsAnotherRetainedWindowWithoutRescanning() {
    VectorSearchBudget budget = new VectorSearchBudget(
        5000, 2, 2, 4, 10, 1, VectorExecutionMode.LOCAL, 10, DeadlinePolicy.FAIL);
    VectorSearchRequest request = new VectorSearchRequest(
        "embedding", new float[] {1f}, VectorDistanceMetric.L2, 2, 1, 1, true,
        VectorStalePolicy.FALLBACK, "001", budget);
    List<VectorCandidate> candidates = Arrays.asList(
        candidate("deleted-1", 1), candidate("deleted-2", 2),
        candidate("live-1", 3), candidate("live-2", 4));

    AtomicInteger scans = new AtomicInteger();
    AtomicReference<ListVectorCandidatePool> retainedPool = new AtomicReference<>();
    VectorCandidateSource source = (plan, context) -> {
      scans.incrementAndGet();
      ListVectorCandidatePool pool = new ListVectorCandidatePool(candidates, budget);
      retainedPool.set(pool);
      return pool;
    };
    RecordIndexLookup lookup = (keys, instant) -> {
      java.util.Map<String, HoodieRecordGlobalLocation> locations = new java.util.HashMap<>();
      for (String key : keys) {
        if (key.startsWith("live")) {
          locations.put(key, new HoodieRecordGlobalLocation("p", "001", "f"));
        }
      }
      return locations;
    };
    VectorExactReranker reranker = (tasks, req, snapshot, context) -> {
      java.util.ArrayList<VectorSearchResult> results = new java.util.ArrayList<>();
      for (VectorFetchTask task : tasks.collectAsList()) {
        for (VectorRowRequest row : task.getRequests()) {
          results.add(new VectorSearchResult(
              row.getRecordKey(), row.getApproximateDistance(),
              new HoodieRecordGlobalLocation("p", "001", "f")));
        }
      }
      return org.apache.hudi.common.data.HoodieListData.eager(results);
    };

    CommonVectorSearchExecutor executor = new CommonVectorSearchExecutor(
        ignored -> new VectorSearchSnapshot(
            "001", new VectorIndexSnapshot(1, 1, 1, "rot-v1", "quant-v1")),
        ignored -> new VectorExecutionDecision(
            VectorExecutionMode.LOCAL, VectorExecutionMode.LOCAL, 4, 10, "test"),
        source,
        new RecordIndexVectorCandidateArbiter(lookup),
        new DefaultVectorFetchPlanner(),
        reranker);

    List<VectorSearchResult> results = executor.execute(request, null).collectAsList();

    assertEquals(Arrays.asList("live-1", "live-2"),
        Arrays.asList(results.get(0).getRecordKey(), results.get(1).getRecordKey()));
    assertEquals(1, scans.get(), "continuation must not rescan MDT");
    assertEquals(4, retainedPool.get().consumed(), "second window must come from the retained pool");
  }

  private static VectorCandidate candidate(String key, double distance) {
    return new VectorCandidate(key, 1, 0, distance,
        new VectorPostingLocator(1, 1, 0, 0, 0, "p", "f", "001", 1));
  }
}
