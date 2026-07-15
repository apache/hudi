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

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies {@link DefaultVectorExactReranker} (RFC-104 v3 §10): reads via the injected handle,
 * scores with {@link DefaultExactVectorScorer}, and returns the global exact top-K in ascending
 * distance order, using an in-memory fake read handle.
 */
public class TestDefaultVectorExactReranker {

  /** In-memory handle: returns a VectorRecord per row request, vector looked up by record key. */
  private static HoodieVectorBatchReadHandleSupplier fakeSupplier(Map<String, float[]> vectors) {
    return () -> new HoodieVectorBatchReadHandle() {
      @Override
      public Iterator<VectorRecord> read(VectorFetchTask task, String recordKeyField, String vectorColumn) {
        List<VectorRecord> out = new ArrayList<>();
        for (VectorRowRequest r : task.getRequests()) {
          float[] v = vectors.get(r.getRecordKey());
          if (v != null) {
            out.add(new VectorRecord(r.getRecordKey(), v,
                new HoodieRecordGlobalLocation(task.getPartitionPath(), task.getBaseInstant(), task.getFileId())));
          }
        }
        return out.iterator();
      }

      @Override
      public void close() {
      }
    };
  }

  @Test
  void returnsGlobalExactTopKInAscendingDistance() {
    Map<String, float[]> vectors = new HashMap<>();
    vectors.put("k1", new float[] {1f, 0f, 0f});   // squared L2 vs origin = 1
    vectors.put("k2", new float[] {2f, 0f, 0f});   // 4
    vectors.put("k3", new float[] {3f, 0f, 0f});   // 9
    vectors.put("k4", new float[] {0.5f, 0f, 0f}); // 0.25

    List<VectorRowRequest> rows = new ArrayList<>();
    rows.add(new VectorRowRequest("k1", 0L, VectorCandidateState.SERVE, 0.0));
    rows.add(new VectorRowRequest("k2", 1L, VectorCandidateState.SERVE, 0.0));
    rows.add(new VectorRowRequest("k3", 2L, VectorCandidateState.SERVE, 0.0));
    rows.add(new VectorRowRequest("k4", 3L, VectorCandidateState.SERVE, 0.0));
    VectorFetchTask task = new VectorFetchTask("p", "fileA", "/tmp/fileA.parquet", "001", rows);

    VectorSearchBudget budget = VectorSearchBudget.defaults(2, 5000L);
    VectorSearchRequest request = new VectorSearchRequest(
        "embedding", new float[] {0f, 0f, 0f}, VectorDistanceMetric.L2, 2, 32, 50, true, null, budget);

    DefaultVectorExactReranker reranker = new DefaultVectorExactReranker(
        fakeSupplier(vectors), new DefaultExactVectorScorer(), "record_key", "embedding");

    HoodieData<VectorFetchTask> tasks = HoodieListData.eager(java.util.Collections.singletonList(task));
    List<VectorSearchResult> results = reranker.rerank(tasks, request, null, null).collectAsList();

    assertEquals(2, results.size(), "topK=2");
    assertEquals("k4", results.get(0).getRecordKey(), "nearest is k4 (0.25)");
    assertEquals(0.25, results.get(0).getDistance(), 1e-6);
    assertEquals("k1", results.get(1).getRecordKey(), "second nearest is k1 (1.0)");
    assertEquals(1.0, results.get(1).getDistance(), 1e-6);
  }
}
