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

import java.util.List;

/**
 * Windows a single retained, distance-ordered candidate pool into continuation batches
 * (RFC-109 §10). The candidate scan retains one ordered pool of at most
 * {@code maxRerankCandidates} in a single MDT scan; this controller draws successive batches
 * <em>from that retained pool</em> — it never rescans MDT postings.
 *
 * <p>The first {@link #nextBatch()} returns up to {@code initialRerankCandidates}; subsequent calls
 * return up to {@code rerankBatchSize}, until the pool (bounded by {@code maxRerankCandidates}) is
 * exhausted. The reranker calls {@link #nextBatch()} while {@link #hasMore()} and the top-K
 * accumulator still {@link VectorTopKAccumulator#needsMore() needs more} live results (and the
 * deadline has not passed).
 */
public final class VectorContinuationController<T> {

  private final List<T> orderedPool;
  private final int initialRerankCandidates;
  private final int rerankBatchSize;
  private final int effectiveMax;
  private int cursor;

  public VectorContinuationController(List<T> orderedPool,
                                      int initialRerankCandidates,
                                      int rerankBatchSize,
                                      int maxRerankCandidates) {
    if (initialRerankCandidates <= 0 || rerankBatchSize <= 0) {
      throw new IllegalArgumentException("batch sizes must be positive");
    }
    this.orderedPool = orderedPool;
    this.initialRerankCandidates = initialRerankCandidates;
    this.rerankBatchSize = rerankBatchSize;
    this.effectiveMax = Math.min(orderedPool.size(), Math.max(0, maxRerankCandidates));
    this.cursor = 0;
  }

  /** Whether more retained candidates remain to draw (within {@code maxRerankCandidates}). */
  public boolean hasMore() {
    return cursor < effectiveMax;
  }

  /** Number of candidates drawn so far (monotonic; never exceeds the retained pool bound). */
  public int consumed() {
    return cursor;
  }

  /**
   * Draw the next continuation batch as a window over the retained pool. First call returns up to
   * {@code initialRerankCandidates}; later calls up to {@code rerankBatchSize}. Never rescans.
   */
  public List<T> nextBatch() {
    if (!hasMore()) {
      return java.util.Collections.emptyList();
    }
    int size = cursor == 0 ? initialRerankCandidates : rerankBatchSize;
    int end = Math.min(cursor + size, effectiveMax);
    List<T> batch = orderedPool.subList(cursor, end);
    cursor = end;
    return batch;
  }
}
