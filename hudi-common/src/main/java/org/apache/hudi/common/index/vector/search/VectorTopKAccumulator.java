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

import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Accumulates the top-K live exact results during rerank/continuation (RFC-109 §10). Deduplicates
 * by logical record key (keeping the smaller distance) so a record surfaced by both its posting and
 * a key-fallback fetch is counted once, and reports how many live results are held so the
 * continuation loop knows whether it still needs more candidates.
 *
 * <p>Distances are the order-preserving ranking distances from {@link ExactVectorScorer} (smaller =
 * more similar); squared L2 is kept internally.
 */
public final class VectorTopKAccumulator {

  private final int topK;
  private final Map<String, VectorSearchResult> bestByKey;

  public VectorTopKAccumulator(int topK) {
    if (topK <= 0) {
      throw new IllegalArgumentException("topK must be positive, got: " + topK);
    }
    this.topK = topK;
    this.bestByKey = new HashMap<>();
  }

  /** Offer one exact-scored live record; keeps the smaller distance per record key. */
  public void offer(String recordKey, double distance, HoodieRecordGlobalLocation location) {
    VectorSearchResult existing = bestByKey.get(recordKey);
    if (existing == null || distance < existing.getDistance()) {
      bestByKey.put(recordKey, new VectorSearchResult(recordKey, distance, location));
    }
  }

  /** Number of distinct live records accumulated so far. */
  public int liveCount() {
    return bestByKey.size();
  }

  /** Whether fewer than K distinct live results have been accumulated. */
  public boolean needsMore() {
    return bestByKey.size() < topK;
  }

  /** The current top-K live results, ascending by distance (ties broken by record key). */
  public List<VectorSearchResult> topK() {
    List<VectorSearchResult> all = new ArrayList<>(bestByKey.values());
    all.sort(Comparator.comparingDouble(VectorSearchResult::getDistance)
        .thenComparing(VectorSearchResult::getRecordKey));
    if (all.size() > topK) {
      return new ArrayList<>(all.subList(0, topK));
    }
    return all;
  }
}
