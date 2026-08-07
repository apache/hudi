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
import org.apache.hudi.common.index.vector.VectorIndexArbiter;
import org.apache.hudi.common.index.vector.VectorStalePolicy;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot-aware RLI candidate arbiter (RFC-109 §7). Wraps the pure decision core
 * {@link VectorIndexArbiter#classify} with a batched, snapshot-pinned {@link RecordIndexLookup}:
 * per partition it collects candidate keys, performs a single batched RLI lookup, and classifies
 * each candidate against its current live location.
 *
 * <ul>
 *   <li>{@code SERVE}: current location matches the posting locator; positional trust preserved.</li>
 *   <li>{@code STALE}: current location differs; preserved for exact-mode key fallback.</li>
 *   <li>{@code DELETED}: RLI miss; dropped from the output.</li>
 * </ul>
 *
 * <p>Posting locations are treated as hints until this arbitration. The lookup must be pinned to
 * {@code snapshot.tableInstant} by the caller so MDT, RLI, file-slice, and base reads share one
 * instant.
 */
public final class RecordIndexVectorCandidateArbiter implements VectorCandidateArbiter {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RecordIndexVectorCandidateArbiter.class);
  private static final int DEFAULT_LOOKUP_BATCH_SIZE = 1024;

  private final RecordIndexLookup lookup;
  private final int lookupBatchSize;

  public RecordIndexVectorCandidateArbiter(RecordIndexLookup lookup) {
    this(lookup, DEFAULT_LOOKUP_BATCH_SIZE);
  }

  public RecordIndexVectorCandidateArbiter(RecordIndexLookup lookup, int lookupBatchSize) {
    if (lookupBatchSize <= 0) {
      throw new IllegalArgumentException("lookupBatchSize must be positive");
    }
    this.lookup = lookup;
    this.lookupBatchSize = lookupBatchSize;
  }

  @Override
  public HoodieData<ArbitratedVectorCandidate> arbitrate(HoodieData<VectorCandidate> candidates,
                                                         VectorSearchRequest request,
                                                         VectorSearchSnapshot snapshot,
                                                         HoodieEngineContext engineContext) {
    RecordIndexLookup rli = this.lookup;
    int batchSize = this.lookupBatchSize;
    return candidates.mapPartitions(
        it -> arbitratePartition(it, rli, snapshot.getTableInstant(), request.getStalePolicy(), batchSize), true);
  }

  private static Iterator<ArbitratedVectorCandidate> arbitratePartition(
      Iterator<VectorCandidate> candidates,
      RecordIndexLookup rli,
      String tableInstant,
      VectorStalePolicy stalePolicy,
      int batchSize) {
    List<ArbitratedVectorCandidate> out = new ArrayList<>();
    List<VectorCandidate> batch = new ArrayList<>(batchSize);
    int staleCount = 0;
    while (candidates.hasNext()) {
      batch.add(candidates.next());
      if (batch.size() == batchSize) {
        staleCount += arbitrateBatch(batch, rli, tableInstant, stalePolicy, out);
        batch.clear();
      }
    }
    if (!batch.isEmpty()) {
      staleCount += arbitrateBatch(batch, rli, tableInstant, stalePolicy, out);
    }
    if (staleCount > 0 && stalePolicy == VectorStalePolicy.WARN) {
      LOG.warn("Vector index contained {} stale candidates at table instant {}; using RLI fallback locations",
          staleCount, tableInstant);
    }
    return out.iterator();
  }

  private static int arbitrateBatch(
      List<VectorCandidate> batch,
      RecordIndexLookup rli,
      String tableInstant,
      VectorStalePolicy stalePolicy,
      List<ArbitratedVectorCandidate> out) {
    Set<String> keys = new LinkedHashSet<>();
    for (VectorCandidate candidate : batch) {
      keys.add(candidate.getRecordKey());
    }
    Map<String, HoodieRecordGlobalLocation> current =
        rli.lookup(new ArrayList<>(keys), tableInstant);
    int staleCount = 0;
    for (VectorCandidate candidate : batch) {
      HoodieRecordGlobalLocation live = current.get(candidate.getRecordKey());
      VectorPostingLocator locator = candidate.getPostingLocator();
      VectorCandidateState state = toState(VectorIndexArbiter.classify(
          locator == null ? null : locator.getPartitionPath(),
          locator == null ? null : locator.getFileId(),
          locator == null ? null : locator.getBaseInstant(),
          live));
      if (state == VectorCandidateState.DELETED) {
        continue;
      }
      if (state == VectorCandidateState.STALE) {
        staleCount++;
        if (stalePolicy == VectorStalePolicy.FAIL) {
          throw new HoodieException("Stale vector candidate '" + candidate.getRecordKey()
              + "' at table instant " + tableInstant);
        }
      }
      out.add(new ArbitratedVectorCandidate(candidate, state, live));
    }
    return staleCount;
  }

  private static VectorCandidateState toState(VectorIndexArbiter.Decision decision) {
    switch (decision) {
      case SERVE:
        return VectorCandidateState.SERVE;
      case STALE:
        return VectorCandidateState.STALE;
      case DELETED:
      default:
        return VectorCandidateState.DELETED;
    }
  }
}
