/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector.search;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TestVectorCandidateOverlay {

  private static VectorCandidate candidate(String key, double distance, String fileId) {
    return new VectorCandidate(key, 1, 0, distance,
        new VectorPostingLocator(1, 1, 0, 0, 0, "p", fileId, "001", 1));
  }

  @Test
  void deltaReplacesBaseEvenWhenDeltaDistanceIsWorse() {
    List<VectorCandidate> result = VectorCandidateOverlay.resolve(
        Arrays.asList(candidate("updated", 1, "old"), candidate("other", 3, "f")),
        Collections.singletonList(candidate("updated", 9, "new")),
        Collections.emptySet(), 2, 1);

    assertEquals(Arrays.asList("other", "updated"), keys(result));
    assertEquals("new", result.get(1).getPostingLocator().getFileId());
  }

  @Test
  void tombstoneSuppressesBothBaseAndDeltaCopies() {
    List<VectorCandidate> result = VectorCandidateOverlay.resolve(
        Arrays.asList(candidate("deleted", 1, "old"), candidate("live", 2, "f")),
        Collections.singletonList(candidate("deleted", 0.5, "new")),
        Collections.singleton("deleted"), 2, 1);

    assertEquals(Collections.singletonList("live"), keys(result));
  }

  @Test
  void overlaySlackBackfillsSuppressedBaseFinalist() {
    List<VectorCandidate> base = Arrays.asList(
        candidate("deleted", 1, "f"), candidate("second", 2, "f"), candidate("backfill", 3, "f"));

    List<VectorCandidate> withoutSlack = VectorCandidateOverlay.resolve(
        base, Collections.emptyList(), Collections.singleton("deleted"), 2, 0);
    List<VectorCandidate> withSlack = VectorCandidateOverlay.resolve(
        base, Collections.emptyList(), Collections.singleton("deleted"), 2, 1);

    assertEquals(Collections.singletonList("second"), keys(withoutSlack));
    assertEquals(Arrays.asList("second", "backfill"), keys(withSlack));
  }

  private static List<String> keys(List<VectorCandidate> candidates) {
    return candidates.stream().map(VectorCandidate::getRecordKey).collect(Collectors.toList());
  }
}
