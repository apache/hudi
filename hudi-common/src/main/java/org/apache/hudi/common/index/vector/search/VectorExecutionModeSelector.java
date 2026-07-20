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

/**
 * Chooses LOCAL vs DISTRIBUTED execution for a vector search (RFC-109 v3 §11A).
 *
 * <p>The default rule is normative and fixed for selector version {@code candidate-threshold-v1}:
 * an explicit {@code LOCAL}/{@code DISTRIBUTED} request is honored verbatim, and {@code AUTO}
 * selects {@code LOCAL} iff {@code maxRerankCandidates <= localExecutionThreshold} (default 8192).
 * No candidate-count, file-count, byte-estimate, or engine heuristic may silently alter the
 * {@code AUTO} rule in this version; a future adaptive selector requires a new selector version.
 */
public interface VectorExecutionModeSelector extends Serializable {

  int DEFAULT_LOCAL_EXECUTION_THRESHOLD = VectorSearchBudget.DEFAULT_LOCAL_EXECUTION_THRESHOLD;
  String SELECTOR_VERSION = "candidate-threshold-v1";

  VectorExecutionDecision select(VectorSearchRequest request);
}
