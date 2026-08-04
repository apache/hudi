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

import org.apache.hudi.common.index.vector.VectorDistanceMetric;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the normative execution-locality rule (RFC-109 §11A) for selector version
 * {@code candidate-threshold-v1}: explicit modes are honored, and AUTO selects LOCAL iff
 * {@code maxRerankCandidates <= localExecutionThreshold} (boundary inclusive).
 */
public class TestThresholdVectorExecutionModeSelector {

  private final VectorExecutionModeSelector selector = new ThresholdVectorExecutionModeSelector();

  private VectorSearchRequest requestWith(VectorExecutionMode mode, int maxRerank, int threshold) {
    VectorSearchBudget budget = new VectorSearchBudget(
        5000L, 256, 128, maxRerank, Integer.MAX_VALUE, 4, mode, threshold, DeadlinePolicy.FAIL);
    return new VectorSearchRequest(
        "embedding", new float[] {0.1f, 0.2f}, VectorDistanceMetric.L2,
        10, 32, 50, true, null, budget);
  }

  @Test
  void autoSelectsLocalAtOrBelowThreshold() {
    VectorExecutionDecision below = selector.select(requestWith(VectorExecutionMode.AUTO, 4096, 8192));
    assertEquals(VectorExecutionMode.LOCAL, below.getSelectedMode());

    // Boundary is inclusive: maxRerank == threshold selects LOCAL.
    VectorExecutionDecision boundary = selector.select(requestWith(VectorExecutionMode.AUTO, 8192, 8192));
    assertEquals(VectorExecutionMode.LOCAL, boundary.getSelectedMode());
  }

  @Test
  void autoSelectsDistributedAboveThreshold() {
    VectorExecutionDecision above = selector.select(requestWith(VectorExecutionMode.AUTO, 8193, 8192));
    assertEquals(VectorExecutionMode.DISTRIBUTED, above.getSelectedMode());
  }

  @Test
  void explicitModesAreHonoredRegardlessOfThreshold() {
    // Explicit LOCAL even when far above threshold.
    VectorExecutionDecision forcedLocal = selector.select(requestWith(VectorExecutionMode.LOCAL, 1_000_000, 8192));
    assertEquals(VectorExecutionMode.LOCAL, forcedLocal.getSelectedMode());

    // Explicit DISTRIBUTED even when well below threshold.
    VectorExecutionDecision forcedDistributed = selector.select(requestWith(VectorExecutionMode.DISTRIBUTED, 8, 8192));
    assertEquals(VectorExecutionMode.DISTRIBUTED, forcedDistributed.getSelectedMode());
  }

  @Test
  void decisionRecordsInputsAndSelectorVersion() {
    VectorExecutionDecision d = selector.select(requestWith(VectorExecutionMode.AUTO, 4096, 8192));
    assertEquals(VectorExecutionMode.AUTO, d.getRequestedMode());
    assertEquals(4096, d.getMaxRerankCandidates());
    assertEquals(8192, d.getLocalExecutionThreshold());
    assertEquals(VectorExecutionModeSelector.SELECTOR_VERSION, d.getSelectorVersion());
  }
}
