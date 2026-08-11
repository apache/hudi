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

import org.apache.hudi.common.engine.HoodieEngineContext;

import java.util.List;
import java.util.Objects;

/**
 * Production candidate-source boundary for MDT postings. It performs exactly one posting scan,
 * resolves packed rows with live deltas and logical tombstones globally, and retains one bounded
 * ordered pool for continuation.
 */
public final class MdtVectorCandidateSource implements VectorCandidateSource {

  private static final long serialVersionUID = 1L;

  private final VectorPostingScanner postingScanner;
  private final int overlaySlack;

  public MdtVectorCandidateSource(VectorPostingScanner postingScanner, int overlaySlack) {
    this.postingScanner = Objects.requireNonNull(postingScanner, "postingScanner");
    if (overlaySlack < 0) {
      throw new IllegalArgumentException("overlaySlack must be non-negative");
    }
    this.overlaySlack = overlaySlack;
  }

  @Override
  public VectorCandidatePool scan(VectorSearchPlan plan, HoodieEngineContext engineContext) {
    int maxCandidates = plan.getRequest().getBudget().getMaxRerankCandidates();
    int packedCandidateLimit = Math.addExact(maxCandidates, overlaySlack);
    VectorPostingScanResult scanResult = postingScanner.scan(
        plan, engineContext, packedCandidateLimit);
    List<VectorCandidate> candidates = VectorCandidateOverlay.resolvePostingKeys(
        scanResult.getPackedCandidates(),
        scanResult.getDeltaCandidates(),
        scanResult.getTombstonedPostingKeys(),
        maxCandidates,
        overlaySlack);
    return new ListVectorCandidatePool(candidates, plan.getRequest().getBudget());
  }
}
