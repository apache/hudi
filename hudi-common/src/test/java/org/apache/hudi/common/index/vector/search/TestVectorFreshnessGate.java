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

import org.apache.hudi.common.index.vector.VectorStalePolicy;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestVectorFreshnessGate {

  @Test
  void testFreshCoverageUsesIndexForEveryPolicy() {
    for (VectorStalePolicy policy : VectorStalePolicy.values()) {
      assertEquals(VectorFreshnessDecision.USE_INDEX,
          VectorFreshnessGate.decide("003", "003", policy));
    }
  }

  @Test
  void testStaleCoverageAppliesConfiguredPolicy() {
    assertEquals(VectorFreshnessDecision.FAIL_QUERY,
        VectorFreshnessGate.decide("002", "003", VectorStalePolicy.FAIL));
    assertEquals(VectorFreshnessDecision.USE_INDEX_WITH_WARNING,
        VectorFreshnessGate.decide("002", "003", VectorStalePolicy.WARN));
    assertEquals(VectorFreshnessDecision.EXACT_FALLBACK,
        VectorFreshnessGate.decide("002", "003", VectorStalePolicy.FALLBACK));
  }

  @Test
  void testMissingCoverageIsStale() {
    assertEquals(VectorFreshnessDecision.EXACT_FALLBACK,
        VectorFreshnessGate.decide(null, "003", VectorStalePolicy.FALLBACK));
  }
}
