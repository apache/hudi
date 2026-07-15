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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies {@link DefaultVectorFetchPlanner} (RFC-104 v3 §8): grouping by live file, positional row
 * preservation for SERVE, key-fallback ({@code rowPosition = -1}) for STALE, and exclusion of DELETED.
 */
public class TestDefaultVectorFetchPlanner {

  private static VectorCandidate candidate(String key, int cluster, long rowPos, String partition, String fileId) {
    VectorPostingLocator loc = new VectorPostingLocator(
        1, cluster, 0, 0L, 0, partition, fileId, "001", rowPos);
    return new VectorCandidate(key, cluster, 0, 1.0, loc);
  }

  private static ArbitratedVectorCandidate arb(VectorCandidate c, VectorCandidateState state,
                                               String partition, String fileId) {
    HoodieRecordGlobalLocation live = state == VectorCandidateState.DELETED
        ? null : new HoodieRecordGlobalLocation(partition, "001", fileId);
    return new ArbitratedVectorCandidate(c, state, live);
  }

  @Test
  void groupsByFileAndPreservesPositionsAndFallback() {
    List<ArbitratedVectorCandidate> input = new ArrayList<>();
    // File A: two SERVE (positional) + one STALE (key fallback).
    input.add(arb(candidate("k1", 0, 10L, "p", "fileA"), VectorCandidateState.SERVE, "p", "fileA"));
    input.add(arb(candidate("k2", 0, 20L, "p", "fileA"), VectorCandidateState.SERVE, "p", "fileA"));
    input.add(arb(candidate("k3", 0, 30L, "p", "fileA"), VectorCandidateState.STALE, "p", "fileA"));
    // File B: one SERVE.
    input.add(arb(candidate("k4", 1, 5L, "p", "fileB"), VectorCandidateState.SERVE, "p", "fileB"));
    // DELETED: must be dropped.
    input.add(arb(candidate("k5", 1, 7L, "p", "fileB"), VectorCandidateState.DELETED, "p", "fileB"));

    HoodieData<ArbitratedVectorCandidate> data = HoodieListData.eager(input);
    List<VectorFetchTask> tasks = new DefaultVectorFetchPlanner().plan(data, null, null).collectAsList();

    Map<String, VectorFetchTask> byFile = new HashMap<>();
    for (VectorFetchTask t : tasks) {
      byFile.put(t.getFileId(), t);
    }
    assertEquals(2, tasks.size(), "expected one task per live file (A, B)");
    assertNull(byFile.get("fileB").getBaseFilePath(), "baseFilePath resolved later by read handle");

    VectorFetchTask a = byFile.get("fileA");
    assertEquals(3, a.size(), "fileA must contain 3 rows (2 SERVE + 1 STALE), DELETED excluded");
    int positional = 0;
    int fallback = 0;
    for (VectorRowRequest r : a.getRequests()) {
      if (r.getState() == VectorCandidateState.SERVE) {
        assertTrue(r.isPositional() && r.getRowPosition() >= 0, "SERVE must keep its row position");
        positional++;
      } else if (r.getState() == VectorCandidateState.STALE) {
        assertEquals(-1L, r.getRowPosition(), "STALE must drop the row position for key fallback");
        assertTrue(!r.isPositional());
        fallback++;
      }
    }
    assertEquals(2, positional);
    assertEquals(1, fallback);

    // DELETED k5 excluded -> fileB has only 1 row.
    assertEquals(1, byFile.get("fileB").size());
  }

  @Test
  void allDeletedProducesNoTasks() {
    List<ArbitratedVectorCandidate> input = new ArrayList<>();
    input.add(arb(candidate("k1", 0, 1L, "p", "fileA"), VectorCandidateState.DELETED, "p", "fileA"));
    HoodieData<ArbitratedVectorCandidate> data = HoodieListData.eager(input);
    List<VectorFetchTask> tasks = new DefaultVectorFetchPlanner().plan(data, null, null).collectAsList();
    assertTrue(tasks.isEmpty(), "all-DELETED input must produce no fetch tasks");
  }
}
