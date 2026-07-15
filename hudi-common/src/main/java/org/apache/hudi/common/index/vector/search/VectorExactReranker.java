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

import java.io.Serializable;

/**
 * Reads candidate vectors through Hudi read handles and produces exact top-K results (RFC-104 v3 §10).
 *
 * <p>Continuation: the candidate scan already retained one ordered pool of at most
 * {@code maxRerankCandidates}. The reranker starts with the initial rerank batch, and continues to
 * draw successive batches <em>from that retained pool</em> when stale/deleted records leave fewer
 * than K live results — it MUST NOT rescan MDT postings per batch. It stops when K live exact
 * results exist, the retained pool is exhausted, or the deadline is reached, returning an explicit
 * {@link VectorSearchStatus} rather than silently returning fewer than K. Executes LOCAL or
 * DISTRIBUTED per the plan's {@link VectorExecutionDecision}.
 */
public interface VectorExactReranker extends Serializable {

  HoodieData<VectorSearchResult> rerank(HoodieData<VectorFetchTask> tasks,
                                        VectorSearchRequest request,
                                        VectorSearchSnapshot snapshot,
                                        HoodieEngineContext engineContext);
}
