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
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Default engine-neutral fetch planner (RFC-104 v3 §8). Groups arbitrated candidates by their live
 * file (partition + fileId, resolved by the arbiter against the pinned snapshot) into one
 * {@link VectorFetchTask} per file, so the read handle can coalesce positions within a file.
 *
 * <ul>
 *   <li>{@code SERVE}: the posting row position is preserved for a positional read.</li>
 *   <li>{@code STALE}: the row position is dropped ({@code -1}); the read handle falls back to a
 *       key-based lookup at the live file.</li>
 *   <li>{@code DELETED}: excluded entirely.</li>
 * </ul>
 *
 * <p>Builds no SQL strings and no temporary DataFrames. {@code baseFilePath} is left null here; the
 * read handle resolves the concrete base file from the snapshot's file slice for the fileId.
 */
public final class DefaultVectorFetchPlanner implements VectorFetchPlanner {

  private static final long serialVersionUID = 1L;
  private static final char KEY_SEP = '\u0001';

  @Override
  public HoodieData<VectorFetchTask> plan(HoodieData<ArbitratedVectorCandidate> candidates,
                                          VectorSearchSnapshot snapshot,
                                          HoodieEngineContext engineContext) {
    return candidates
        .flatMapToPair(DefaultVectorFetchPlanner::toFileKeyed)
        .groupByKey()
        .map(entry -> buildTask(entry.getKey(), entry.getValue()));
  }

  /** DELETED candidates and those without a live location are dropped (empty iterator). */
  private static Iterator<Pair<String, ArbitratedVectorCandidate>> toFileKeyed(ArbitratedVectorCandidate c) {
    if (!c.isServable() || c.getLiveLocation() == null) {
      return Collections.emptyIterator();
    }
    HoodieRecordGlobalLocation loc = c.getLiveLocation();
    String key = loc.getPartitionPath() + KEY_SEP + loc.getFileId();
    return Collections.singletonList(Pair.of(key, c)).iterator();
  }

  private static VectorFetchTask buildTask(String fileKey, Iterable<ArbitratedVectorCandidate> group) {
    List<VectorRowRequest> requests = new ArrayList<>();
    String partitionPath = null;
    String fileId = null;
    String baseInstant = null;
    for (ArbitratedVectorCandidate c : group) {
      HoodieRecordGlobalLocation loc = c.getLiveLocation();
      if (partitionPath == null) {
        partitionPath = loc.getPartitionPath();
        fileId = loc.getFileId();
        baseInstant = loc.getInstantTime();
      }
      VectorCandidate cand = c.getCandidate();
      boolean serve = c.getState() == VectorCandidateState.SERVE;
      long rowPosition = serve && cand.getPostingLocator() != null
          ? cand.getPostingLocator().getRowPosition() : -1L;
      requests.add(new VectorRowRequest(
          cand.getRecordKey(), rowPosition, c.getState(), cand.getApproximateDistance()));
    }
    return new VectorFetchTask(partitionPath, fileId, null, baseInstant, requests);
  }
}
