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

/**
 * Normative candidate-threshold execution selector (RFC-109 v3 §11A, selector version
 * {@code candidate-threshold-v1}). Pure function of the request's requested mode and budget;
 * no engine or runtime state, so LOCAL/DISTRIBUTED selection is deterministic and reproducible.
 */
public final class ThresholdVectorExecutionModeSelector implements VectorExecutionModeSelector {

  private static final long serialVersionUID = 1L;

  @Override
  public VectorExecutionDecision select(VectorSearchRequest request) {
    VectorSearchBudget budget = request.getBudget();
    VectorExecutionMode requested = budget.getRequestedExecutionMode();
    int maxRerank = budget.getMaxRerankCandidates();
    int threshold = budget.getLocalExecutionThreshold();

    VectorExecutionMode selected;
    switch (requested) {
      case LOCAL:
        selected = VectorExecutionMode.LOCAL;
        break;
      case DISTRIBUTED:
        selected = VectorExecutionMode.DISTRIBUTED;
        break;
      case AUTO:
      default:
        selected = maxRerank <= threshold ? VectorExecutionMode.LOCAL : VectorExecutionMode.DISTRIBUTED;
        break;
    }
    return new VectorExecutionDecision(requested, selected, maxRerank, threshold, SELECTOR_VERSION);
  }
}
