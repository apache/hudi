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
import org.apache.hudi.common.engine.HoodieEngineContext;

import java.util.Objects;

/**
 * The engine-neutral vector-search orchestrator (RFC-109 v3 §11). Pins one snapshot, chooses the
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

  private final VectorSnapshotResolver snapshotResolver;
  private final VectorExecutionModeSelector executionModeSelector;
  private final VectorCandidateSource candidateSource;
  private final VectorCandidateArbiter candidateArbiter;
  private final VectorFetchPlanner fetchPlanner;
  private final VectorExactReranker exactReranker;

  public CommonVectorSearchExecutor(VectorSnapshotResolver snapshotResolver,
                                    VectorExecutionModeSelector executionModeSelector,
                                    VectorCandidateSource candidateSource,
                                    VectorCandidateArbiter candidateArbiter,
                                    VectorFetchPlanner fetchPlanner,
                                    VectorExactReranker exactReranker) {
    this.snapshotResolver = Objects.requireNonNull(snapshotResolver, "snapshotResolver");
    this.executionModeSelector = Objects.requireNonNull(executionModeSelector, "executionModeSelector");
    this.candidateSource = Objects.requireNonNull(candidateSource, "candidateSource");
    this.candidateArbiter = Objects.requireNonNull(candidateArbiter, "candidateArbiter");
    this.fetchPlanner = Objects.requireNonNull(fetchPlanner, "fetchPlanner");
    this.exactReranker = Objects.requireNonNull(exactReranker, "exactReranker");
  }

  @Override
  public HoodieData<VectorSearchResult> execute(VectorSearchRequest request, HoodieEngineContext engineContext) {
    // 1. Pin one snapshot for MDT / RLI / file-slice / base reads.
    VectorSearchSnapshot snapshot = snapshotResolver.resolve(request);
    // 2. Choose execution locality (recorded on the plan for downstream + metrics).
    VectorExecutionDecision decision = executionModeSelector.select(request);
    VectorSearchPlan plan = new VectorSearchPlan(request, snapshot, decision);

    // 3-6. Stage pipeline, all on the pinned snapshot.
    HoodieData<VectorCandidate> candidates = candidateSource.scan(plan, engineContext);
    HoodieData<ArbitratedVectorCandidate> arbitrated =
        candidateArbiter.arbitrate(candidates, snapshot, engineContext);
    HoodieData<VectorFetchTask> tasks = fetchPlanner.plan(arbitrated, snapshot, engineContext);
    return exactReranker.rerank(tasks, request, snapshot, engineContext);
  }
}
