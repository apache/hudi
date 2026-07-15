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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Verifies {@link RecordIndexVectorCandidateArbiter} (RFC-104 v3 §7): SERVE when the current RLI
 * location matches the posting locator, STALE (preserved) when it differs, DELETED (dropped) on an
 * RLI miss, using a fake snapshot-pinned {@link RecordIndexLookup}.
 */
public class TestRecordIndexVectorCandidateArbiter {

  private static VectorCandidate candidate(String key, String partition, String fileId, String instant) {
    VectorPostingLocator loc = new VectorPostingLocator(
        1, 0, 0, 0L, 0, partition, fileId, instant, 42L);
    return new VectorCandidate(key, 0, 0, 1.0, loc);
  }

  @Test
  void classifiesServeStaleDeletedAndDropsDeleted() {
    List<VectorCandidate> input = new ArrayList<>();
    input.add(candidate("serveKey", "p", "fileA", "001")); // matches -> SERVE
    input.add(candidate("staleKey", "p", "fileA", "001")); // current differs -> STALE
    input.add(candidate("deletedKey", "p", "fileA", "001")); // RLI miss -> DELETED (dropped)

    // Snapshot-pinned RLI state: serveKey matches posting, staleKey moved to fileB@002, deletedKey absent.
    Map<String, HoodieRecordGlobalLocation> rli = new HashMap<>();
    rli.put("serveKey", new HoodieRecordGlobalLocation("p", "001", "fileA"));
    rli.put("staleKey", new HoodieRecordGlobalLocation("p", "002", "fileB"));
    RecordIndexLookup lookup = keys -> {
      Map<String, HoodieRecordGlobalLocation> out = new HashMap<>();
      for (String k : keys) {
        if (rli.containsKey(k)) {
          out.put(k, rli.get(k));
        }
      }
      return out;
    };

    HoodieData<VectorCandidate> data = HoodieListData.eager(input);
    List<ArbitratedVectorCandidate> result =
        new RecordIndexVectorCandidateArbiter(lookup).arbitrate(data, null, null).collectAsList();

    Map<String, ArbitratedVectorCandidate> byKey = new HashMap<>();
    for (ArbitratedVectorCandidate a : result) {
      byKey.put(a.getCandidate().getRecordKey(), a);
    }

    assertEquals(2, result.size(), "DELETED finalist must be dropped");

    ArbitratedVectorCandidate serve = byKey.get("serveKey");
    assertEquals(VectorCandidateState.SERVE, serve.getState());
    assertNotNull(serve.getLiveLocation());
    assertEquals("fileA", serve.getLiveLocation().getFileId());

    ArbitratedVectorCandidate stale = byKey.get("staleKey");
    assertEquals(VectorCandidateState.STALE, stale.getState());
    assertNotNull(stale.getLiveLocation(), "STALE must retain the live location for key fallback");
    assertEquals("fileB", stale.getLiveLocation().getFileId());

    assertNull(byKey.get("deletedKey"), "DELETED must not appear in output");
  }

  @Test
  void allDeletedProducesEmptyOutput() {
    List<VectorCandidate> input = new ArrayList<>();
    input.add(candidate("k1", "p", "f", "001"));
    input.add(candidate("k2", "p", "f", "001"));
    RecordIndexLookup emptyLookup = keys -> new HashMap<>();
    HoodieData<VectorCandidate> data = HoodieListData.eager(input);
    List<ArbitratedVectorCandidate> result =
        new RecordIndexVectorCandidateArbiter(emptyLookup).arbitrate(data, null, null).collectAsList();
    assertEquals(0, result.size(), "all RLI misses -> all DELETED -> empty");
  }
}
