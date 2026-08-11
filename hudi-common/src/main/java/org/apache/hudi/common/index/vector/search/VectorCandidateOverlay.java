/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file for details.
 */

package org.apache.hudi.common.index.vector.search;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** Resolves packed-block candidates with the canonical delta overlay before finalist arbitration. */
public final class VectorCandidateOverlay {

  private static final Comparator<VectorCandidate> ORDER = Comparator
      .comparingDouble(VectorCandidate::getApproximateDistance)
      .thenComparing(VectorCandidate::getRecordKey);

  private VectorCandidateOverlay() {
  }

  /**
   * Retains base candidates with overlay slack, then applies canonical delta precedence and
   * tombstone suppression. A live delta always replaces the packed-block row for the same key,
   * even when its approximate distance is worse; otherwise an update could resurrect stale code.
   */
  public static List<VectorCandidate> resolve(
      Collection<VectorCandidate> baseCandidates,
      Collection<VectorCandidate> deltaCandidates,
      Collection<String> tombstonedDeltaKeys,
      int maxCandidates,
      int overlaySlack) {
    Set<String> tombstones = new HashSet<>(tombstonedDeltaKeys);
    return resolve(baseCandidates, deltaCandidates,
        candidate -> tombstones.contains(candidate.getRecordKey()), maxCandidates, overlaySlack);
  }

  /** Resolves overlay using the canonical cluster/shard/record-key identity of tombstones. */
  public static List<VectorCandidate> resolvePostingKeys(
      Collection<VectorCandidate> baseCandidates,
      Collection<VectorCandidate> deltaCandidates,
      Collection<VectorPostingKey> tombstonedPostingKeys,
      int maxCandidates,
      int overlaySlack) {
    Set<VectorPostingKey> tombstones = new HashSet<>(tombstonedPostingKeys);
    return resolve(baseCandidates, deltaCandidates,
        candidate -> tombstones.contains(VectorPostingKey.fromCandidate(candidate)),
        maxCandidates, overlaySlack);
  }

  private static List<VectorCandidate> resolve(
      Collection<VectorCandidate> baseCandidates,
      Collection<VectorCandidate> deltaCandidates,
      Predicate<VectorCandidate> isTombstoned,
      int maxCandidates,
      int overlaySlack) {
    if (maxCandidates < 0 || overlaySlack < 0) {
      throw new IllegalArgumentException("candidate bounds must be non-negative");
    }
    int retainedBaseCount = Math.min(baseCandidates.size(), Math.addExact(maxCandidates, overlaySlack));
    List<VectorCandidate> orderedBase = new ArrayList<>(baseCandidates);
    orderedBase.sort(ORDER);

    Map<String, VectorCandidate> resolved = new HashMap<>();
    for (int i = 0; i < retainedBaseCount; i++) {
      VectorCandidate candidate = orderedBase.get(i);
      if (!isTombstoned.test(candidate)) {
        resolved.put(candidate.getRecordKey(), candidate);
      }
    }
    for (VectorCandidate delta : deltaCandidates) {
      if (isTombstoned.test(delta)) {
        resolved.remove(delta.getRecordKey());
      } else {
        resolved.put(delta.getRecordKey(), delta);
      }
    }

    List<VectorCandidate> result = new ArrayList<>(resolved.values());
    result.sort(ORDER);
    if (result.size() > maxCandidates) {
      return new ArrayList<>(result.subList(0, maxCandidates));
    }
    return result;
  }
}
