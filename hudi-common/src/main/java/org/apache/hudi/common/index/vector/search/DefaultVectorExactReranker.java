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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import java.util.Iterator;
import java.util.List;

/**
 * Default engine-neutral exact reranker (RFC-104 v3 §10). For each fetch task it reads the
 * candidate rows through an injected {@link HoodieVectorBatchReadHandle} (created per partition via
 * {@link HoodieVectorBatchReadHandleSupplier}), scores them with an {@link ExactVectorScorer}
 * (float64, squared-L2 internal), and keeps a per-partition top-K via {@link VectorTopKAccumulator}.
 * The bounded per-partition top-Ks are then merged into the global top-K — the only driver-side
 * collect, and only of at most {@code partitions * topK} results (never the full candidate set),
 * satisfying "collect only final top-K" for distributed execution.
 *
 * <p>This scores exactly the rows the {@link VectorFetchPlanner} produced (DELETED already excluded,
 * STALE via key fallback). Pool-level continuation — re-drawing more candidates when stale/deleted
 * crowding leaves fewer than K live — is orchestrated upstream in the executor using
 * {@link VectorContinuationController}; this reranker scores a given batch of tasks.
 */
public final class DefaultVectorExactReranker implements VectorExactReranker {

  private static final long serialVersionUID = 1L;

  private final HoodieVectorBatchReadHandleSupplier handleSupplier;
  private final ExactVectorScorer scorer;
  private final String recordKeyField;
  private final String vectorColumn;

  public DefaultVectorExactReranker(HoodieVectorBatchReadHandleSupplier handleSupplier,
                                    ExactVectorScorer scorer,
                                    String recordKeyField,
                                    String vectorColumn) {
    this.handleSupplier = handleSupplier;
    this.scorer = scorer;
    this.recordKeyField = recordKeyField;
    this.vectorColumn = vectorColumn;
  }

  @Override
  public HoodieData<VectorSearchResult> rerank(HoodieData<VectorFetchTask> tasks,
                                               VectorSearchRequest request,
                                               VectorSearchSnapshot snapshot,
                                               HoodieEngineContext engineContext) {
    int topK = request.getTopK();
    float[] query = request.getQueryVector();
    VectorDistanceMetric metric = request.getMetric();
    HoodieVectorBatchReadHandleSupplier supplier = this.handleSupplier;
    ExactVectorScorer localScorer = this.scorer;
    String keyField = this.recordKeyField;
    String vecCol = this.vectorColumn;

    // Per-partition: read + score + local top-K.
    HoodieData<VectorSearchResult> perPartition = tasks.mapPartitions(taskIt -> {
      VectorTopKAccumulator acc = new VectorTopKAccumulator(topK);
      if (taskIt.hasNext()) {
        HoodieVectorBatchReadHandle handle = supplier.get();
        try {
          while (taskIt.hasNext()) {
            VectorFetchTask task = taskIt.next();
            Iterator<VectorRecord> records = handle.read(task, keyField, vecCol);
            while (records.hasNext()) {
              VectorRecord rec = records.next();
              double dist = localScorer.distance(query, rec.getVector(), metric);
              acc.offer(rec.getRecordKey(), dist, rec.getLocation());
            }
          }
        } finally {
          closeQuietly(handle);
        }
      }
      return acc.topK().iterator();
    }, false);

    // Global merge of the bounded per-partition top-Ks (final top-K only).
    List<VectorSearchResult> merged = perPartition.collectAsList();
    VectorTopKAccumulator global = new VectorTopKAccumulator(topK);
    for (VectorSearchResult r : merged) {
      global.offer(r.getRecordKey(), r.getDistance(), r.getLocation());
    }
    return HoodieListData.eager(global.topK());
  }

  private static void closeQuietly(HoodieVectorBatchReadHandle handle) {
    try {
      handle.close();
    } catch (Exception ignored) {
      // best-effort close
    }
  }
}
