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

import java.io.Serializable;
import java.util.Objects;

/**
 * Per-request resource and continuation budget for a vector search (RFC-109 §1).
 *
 * <p>Continuation semantics: the candidate scan retains one ordered pool of at most
 * {@link #maxRerankCandidates} in a single MDT scan; exact rerank consumes it in batches of
 * {@link #rerankBatchSize} starting at {@link #initialRerankCandidates}, drawing more only when
 * stale/deleted finalists leave fewer than K live results. Continuation is a window over the
 * retained pool, never a re-scan.
 *
 * <p>Execution locality: {@link #requestedExecutionMode} plus {@link #localExecutionThreshold}
 * feed the normative selector (§11A) — {@code AUTO} selects {@code LOCAL} iff
 * {@code maxRerankCandidates <= localExecutionThreshold}.
 */
public final class VectorSearchBudget implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Default local-vs-distributed candidate threshold (RFC-109 §11A). */
  public static final int DEFAULT_LOCAL_EXECUTION_THRESHOLD = 8192;
  public static final int DEFAULT_INITIAL_RERANK_CANDIDATES = 256;
  public static final int DEFAULT_RERANK_BATCH_SIZE = 128;
  public static final int DEFAULT_MAX_RERANK_CANDIDATES = 4096;

  private final long timeoutMs;
  private final int initialRerankCandidates;
  private final int rerankBatchSize;
  private final int maxRerankCandidates;
  private final int maxFetchTasks;
  private final int maxFetchConcurrency;
  private final VectorExecutionMode requestedExecutionMode;
  private final int localExecutionThreshold;
  private final DeadlinePolicy deadlinePolicy;

  public VectorSearchBudget(long timeoutMs,
                            int initialRerankCandidates,
                            int rerankBatchSize,
                            int maxRerankCandidates,
                            int maxFetchTasks,
                            int maxFetchConcurrency,
                            VectorExecutionMode requestedExecutionMode,
                            int localExecutionThreshold,
                            DeadlinePolicy deadlinePolicy) {
    this.timeoutMs = timeoutMs;
    this.initialRerankCandidates = initialRerankCandidates;
    this.rerankBatchSize = rerankBatchSize;
    this.maxRerankCandidates = maxRerankCandidates;
    this.maxFetchTasks = maxFetchTasks;
    this.maxFetchConcurrency = maxFetchConcurrency;
    this.requestedExecutionMode = Objects.requireNonNull(requestedExecutionMode, "requestedExecutionMode");
    this.localExecutionThreshold = localExecutionThreshold;
    this.deadlinePolicy = Objects.requireNonNull(deadlinePolicy, "deadlinePolicy");
  }

  /** A sensible default budget for {@code topK}: AUTO execution, partial-on-deadline off (FAIL). */
  public static VectorSearchBudget defaults(int topK, long timeoutMs) {
    int maxRerank = Math.max(DEFAULT_MAX_RERANK_CANDIDATES, topK * 16);
    return new VectorSearchBudget(
        timeoutMs,
        Math.max(DEFAULT_INITIAL_RERANK_CANDIDATES, topK),
        DEFAULT_RERANK_BATCH_SIZE,
        maxRerank,
        Integer.MAX_VALUE,
        Math.max(1, Runtime.getRuntime().availableProcessors()),
        VectorExecutionMode.AUTO,
        DEFAULT_LOCAL_EXECUTION_THRESHOLD,
        DeadlinePolicy.FAIL);
  }

  public long getTimeoutMs() {
    return timeoutMs;
  }

  public int getInitialRerankCandidates() {
    return initialRerankCandidates;
  }

  public int getRerankBatchSize() {
    return rerankBatchSize;
  }

  public int getMaxRerankCandidates() {
    return maxRerankCandidates;
  }

  public int getMaxFetchTasks() {
    return maxFetchTasks;
  }

  public int getMaxFetchConcurrency() {
    return maxFetchConcurrency;
  }

  public VectorExecutionMode getRequestedExecutionMode() {
    return requestedExecutionMode;
  }

  public int getLocalExecutionThreshold() {
    return localExecutionThreshold;
  }

  public DeadlinePolicy getDeadlinePolicy() {
    return deadlinePolicy;
  }
}
