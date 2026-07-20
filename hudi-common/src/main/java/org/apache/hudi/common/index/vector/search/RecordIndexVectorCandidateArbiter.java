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
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Snapshot-aware RLI candidate arbiter (RFC-109 v3 §7). Wraps the pure decision core
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

  private final RecordIndexLookup lookup;

  public RecordIndexVectorCandidateArbiter(RecordIndexLookup lookup) {
    this.lookup = lookup;
  }

  @Override
  public HoodieData<ArbitratedVectorCandidate> arbitrate(HoodieData<VectorCandidate> candidates,
                                                         VectorSearchSnapshot snapshot,
                                                         HoodieEngineContext engineContext) {
    RecordIndexLookup rli = this.lookup;
    return candidates.mapPartitions(it -> arbitratePartition(it, rli), true);
  }

  private static Iterator<ArbitratedVectorCandidate> arbitratePartition(Iterator<VectorCandidate> it,
                                                                        RecordIndexLookup rli) {
    List<VectorCandidate> buffered = new ArrayList<>();
    Set<String> keys = new LinkedHashSet<>();
    while (it.hasNext()) {
      VectorCandidate c = it.next();
      buffered.add(c);
      keys.add(c.getRecordKey());
    }
    if (buffered.isEmpty()) {
      return Collections.emptyIterator();
    }

    Map<String, HoodieRecordGlobalLocation> current = rli.lookup(new ArrayList<>(keys));

    List<ArbitratedVectorCandidate> out = new ArrayList<>(buffered.size());
    for (VectorCandidate c : buffered) {
      HoodieRecordGlobalLocation live = current.get(c.getRecordKey());
      VectorPostingLocator loc = c.getPostingLocator();
      VectorIndexArbiter.Decision decision = VectorIndexArbiter.classify(
          loc == null ? null : loc.getPartitionPath(),
          loc == null ? null : loc.getFileId(),
          loc == null ? null : loc.getBaseInstant(),
          live);
      VectorCandidateState state = toState(decision);
      if (state == VectorCandidateState.DELETED) {
        continue; // drop deleted finalists
      }
      out.add(new ArbitratedVectorCandidate(c, state, live));
    }
    return out.iterator();
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
