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

import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end wiring test for {@link CommonVectorSearchExecutor} (RFC-104 v3 §11): drives the full
 * stage pipeline with a fake candidate source and reranker but the real
 * {@link RecordIndexVectorCandidateArbiter} and {@link DefaultVectorFetchPlanner}, asserting the
 * snapshot is resolved once, DELETED candidates are dropped end-to-end, and results flow through.
 */
public class TestCommonVectorSearchExecutor {

  private static VectorCandidate candidate(String key, String fileId) {
    VectorPostingLocator loc = new VectorPostingLocator(1, 0, 0, 0L, 0, "p", fileId, "001", 7L);
    return new VectorCandidate(key, 0, 0, 1.0, loc);
  }

  private static VectorSearchRequest request() {
    VectorSearchBudget budget = VectorSearchBudget.defaults(3, 5000L);
    return new VectorSearchRequest("embedding", new float[] {1f, 2f}, VectorDistanceMetric.L2,
        3, 32, 50, true, null, budget);
  }

  @Test
  void drivesFullPipelineAndDropsDeletedEndToEnd() {
    List<VectorCandidate> scanned = new ArrayList<>();
    scanned.add(candidate("k1", "fileA"));
    scanned.add(candidate("k2", "fileA"));
    scanned.add(candidate("k3", "fileB")); // will be DELETED via RLI miss

    AtomicBoolean snapshotResolved = new AtomicBoolean(false);
    VectorSnapshotResolver resolver = req -> {
      snapshotResolved.set(true);
      return new VectorSearchSnapshot("001",
          new VectorIndexSnapshot(1, 0L, 2, 1, "rot-v1", "quant-v1"));
    };

    // Real arbiter with a fake RLI: k1/k2 live & matching (SERVE), k3 absent (DELETED).
    Map<String, HoodieRecordGlobalLocation> rli = new HashMap<>();
    rli.put("k1", new HoodieRecordGlobalLocation("p", "001", "fileA"));
    rli.put("k2", new HoodieRecordGlobalLocation("p", "001", "fileA"));
    RecordIndexLookup lookup = keys -> {
      Map<String, HoodieRecordGlobalLocation> out = new HashMap<>();
      for (String k : keys) {
        if (rli.containsKey(k)) {
          out.put(k, rli.get(k));
        }
      }
      return out;
    };

    VectorCandidateSource source = (plan, ec) -> HoodieListData.eager(scanned);
    VectorCandidateArbiter arbiter = new RecordIndexVectorCandidateArbiter(lookup);
    VectorFetchPlanner planner = new DefaultVectorFetchPlanner();
    // Fake reranker: emit one result per fetched row (distance = approx), preserving live location.
    VectorExactReranker reranker = (tasks, req, snap, ec) -> {
      List<VectorSearchResult> results = new ArrayList<>();
      for (VectorFetchTask task : tasks.collectAsList()) {
        for (VectorRowRequest r : task.getRequests()) {
          results.add(new VectorSearchResult(r.getRecordKey(), r.getApproximateDistance(),
              new HoodieRecordGlobalLocation("p", task.getBaseInstant(), task.getFileId())));
        }
      }
      return HoodieListData.eager(results);
    };

    CommonVectorSearchExecutor executor = new CommonVectorSearchExecutor(
        resolver, new ThresholdVectorExecutionModeSelector(), source, arbiter, planner, reranker);

    List<VectorSearchResult> results = executor.execute(request(), null).collectAsList();

    assertTrue(snapshotResolved.get(), "snapshot must be resolved once at the top of the pipeline");
    assertEquals(2, results.size(), "k3 (DELETED via RLI miss) must be dropped end-to-end");
    List<String> keys = new ArrayList<>();
    for (VectorSearchResult r : results) {
      keys.add(r.getRecordKey());
    }
    assertTrue(keys.contains("k1") && keys.contains("k2"));
    assertTrue(!keys.contains("k3"));
  }
}
