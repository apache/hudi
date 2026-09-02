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
 * The resolved, engine-neutral plan for a single vector search: the immutable request, the pinned
 * {@link VectorSearchSnapshot}, and the {@link VectorExecutionDecision} chosen by the selector.
 * Built once by the orchestrator and threaded through every stage so all stages share one snapshot
 * and one execution decision (RFC-109 §11).
 */
public final class VectorSearchPlan implements Serializable {

  private static final long serialVersionUID = 1L;

  private final VectorSearchRequest request;
  private final VectorSearchSnapshot snapshot;
  private final VectorExecutionDecision executionDecision;

  public VectorSearchPlan(VectorSearchRequest request,
                          VectorSearchSnapshot snapshot,
                          VectorExecutionDecision executionDecision) {
    this.request = Objects.requireNonNull(request, "request");
    this.snapshot = Objects.requireNonNull(snapshot, "snapshot");
    this.executionDecision = Objects.requireNonNull(executionDecision, "executionDecision");
  }

  public VectorSearchRequest getRequest() {
    return request;
  }

  public VectorSearchSnapshot getSnapshot() {
    return snapshot;
  }

  public VectorExecutionDecision getExecutionDecision() {
    return executionDecision;
  }
}
