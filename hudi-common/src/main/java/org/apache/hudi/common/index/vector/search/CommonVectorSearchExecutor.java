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
import org.apache.hudi.common.index.vector.VectorStalePolicy;
import org.apache.hudi.exception.HoodieIndexException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * The engine-neutral vector-search orchestrator (RFC-109 §11). Pins one snapshot, chooses the
 * execution mode, and drives the stages in order — candidate scan, RLI arbitration, fetch planning,
 * exact rerank — returning the top-K results. It never invokes {@code spark.sql(...)} and never
 * reconstructs SQL/DataFrames; each stage is an injected engine-neutral implementation.
 *
 * <pre>
 *   request
 *     -&gt; resolve + pin snapshot
 *     -&gt; select execution mode (LOCAL/DISTRIBUTED)
 *     -&gt; scan MDT postings (bounded ordered pool)
 *     -&gt; RLI arbitrate (SERVE/STALE, drop DELETED)
 *     -&gt; plan file-slice fetches
 *     -&gt; exact rerank + continuation
 *     -&gt; top-K results
 * </pre>
 */
public final class CommonVectorSearchExecutor implements VectorSearchExecutor {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(CommonVectorSearchExecutor.class);

  private final VectorSnapshotResolver snapshotResolver;
  private final VectorExecutionModeSelector executionModeSelector;
  private final VectorCandidateSource candidateSource;
  private final VectorCandidateArbiter candidateArbiter;
  private final VectorFetchPlanner fetchPlanner;
  private final VectorExactReranker exactReranker;
  private final VectorFallbackSearch fallbackSearch;

  public CommonVectorSearchExecutor(VectorSnapshotResolver snapshotResolver,
                                    VectorExecutionModeSelector executionModeSelector,
                                    VectorCandidateSource candidateSource,
                                    VectorCandidateArbiter candidateArbiter,
                                    VectorFetchPlanner fetchPlanner,
                                    VectorExactReranker exactReranker) {
    this(snapshotResolver, executionModeSelector, candidateSource, candidateArbiter,
        fetchPlanner, exactReranker, null);
  }

  public CommonVectorSearchExecutor(VectorSnapshotResolver snapshotResolver,
                                    VectorExecutionModeSelector executionModeSelector,
                                    VectorCandidateSource candidateSource,
                                    VectorCandidateArbiter candidateArbiter,
                                    VectorFetchPlanner fetchPlanner,
                                    VectorExactReranker exactReranker,
                                    VectorFallbackSearch fallbackSearch) {
    this.snapshotResolver = Objects.requireNonNull(snapshotResolver, "snapshotResolver");
    this.executionModeSelector = Objects.requireNonNull(executionModeSelector, "executionModeSelector");
    this.candidateSource = Objects.requireNonNull(candidateSource, "candidateSource");
    this.candidateArbiter = Objects.requireNonNull(candidateArbiter, "candidateArbiter");
    this.fetchPlanner = Objects.requireNonNull(fetchPlanner, "fetchPlanner");
    this.exactReranker = Objects.requireNonNull(exactReranker, "exactReranker");
    this.fallbackSearch = fallbackSearch;
  }

  @Override
  public HoodieData<VectorSearchResult> execute(VectorSearchRequest request, HoodieEngineContext engineContext) {
    // 1. Pin one snapshot for MDT / RLI / file-slice / base reads.
    VectorSearchSnapshot snapshot = snapshotResolver.resolve(request);
    if (isStale(snapshot)) {
      if (request.getStalePolicy() == VectorStalePolicy.FAIL) {
        throw new HoodieIndexException(staleMessage(snapshot));
      }
      if (request.getStalePolicy() == VectorStalePolicy.FALLBACK) {
        if (fallbackSearch == null) {
          throw new HoodieIndexException(
              staleMessage(snapshot) + "; no exact fallback search is configured");
        }
        return fallbackSearch.execute(request, snapshot, engineContext);
      }
      LOG.warn("{}; continuing because vector.query.stale_policy=WARN", staleMessage(snapshot));
    }
    // 2. Choose execution locality (recorded on the plan for downstream + metrics).
    VectorExecutionDecision decision = executionModeSelector.select(request);
    VectorSearchPlan plan = new VectorSearchPlan(request, snapshot, decision);

    // 3-6. Consume ordered windows from one retained pool. Continuation never rescans MDT.
    VectorCandidatePool pool = candidateSource.scan(plan, engineContext);
    VectorTopKAccumulator results = new VectorTopKAccumulator(request.getTopK());
    while (results.needsMore() && pool.hasMore()) {
      HoodieData<VectorCandidate> candidates = pool.nextBatch();
      HoodieData<ArbitratedVectorCandidate> arbitrated =
          candidateArbiter.arbitrate(candidates, request, snapshot, engineContext);
      HoodieData<VectorFetchTask> tasks = fetchPlanner.plan(arbitrated, snapshot, engineContext);
      for (VectorSearchResult result : exactReranker.rerank(
          tasks, request, snapshot, engineContext).collectAsList()) {
        results.offer(result.getRecordKey(), result.getDistance(), result.getLocation());
      }
    }
    return HoodieListData.eager(results.topK());
  }

  private static boolean isStale(VectorSearchSnapshot snapshot) {
    String coveredInstant = snapshot.getVectorIndex().getLastContiguousSourceInstant();
    return coveredInstant == null
        || compareTimestamps(coveredInstant, LESSER_THAN, snapshot.getTableInstant());
  }

  private static String staleMessage(VectorSearchSnapshot snapshot) {
    return "Vector index generation " + snapshot.getVectorIndex().getGenerationId()
        + " covers source writes through "
        + snapshot.getVectorIndex().getLastContiguousSourceInstant()
        + " but the pinned table instant is " + snapshot.getTableInstant();
  }
}
