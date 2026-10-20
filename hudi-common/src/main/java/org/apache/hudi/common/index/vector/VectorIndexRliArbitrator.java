/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Applies the RFC-109 record-level-index arbitration contract to vector finalists. */
final class VectorIndexRliArbitrator {

  private VectorIndexRliArbitrator() {
  }

  /**
   * The RFC-109 RLI finalist arbiter. Resolves each finalist's current location from the
   * record-level index (one batched {@code readRecordIndexLocationsWithKeys} over the distinct
   * finalist keys) and tags it with a {@link VectorIndexArbiter.Decision} plus the resolved
   * location.
   *
   * <p>Unlike {@code VectorIndexMdtSearchUtils.attachRecordLocations}, this does <em>not</em> drop candidates: it tags all
   * of them so callers can tally {@code arbiterExclusions.stale} / {@code .deleted} and apply the
   * mode-specific action (approx: exclude STALE + DELETED; exact: key-fallback STALE, exclude
   * DELETED, positional SERVE). Resolved location semantics:
   *
   * <ul>
   *   <li>{@code SERVE}: the posting's own location when present (positional trust), else the RLI
   *       location.</li>
   *   <li>{@code STALE}: the RLI current location, so exact mode can key-fetch at the live slice.</li>
   *   <li>{@code DELETED}: {@code null}.</li>
   * </ul>
   */
  static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(HoodieTableMetadata metadataTable,
                                                                  HoodieData<ScoredVectorPostingMatch> finalists) {
    return arbitrateFinalists(metadataTable, finalists, false);
  }

  static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(
      HoodieTableMetadata metadataTable,
      HoodieData<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    if (partitionedRecordIndex) {
      Map<String, Set<String>> keysByPartition = groupKeysByPartition(
          finalists.map(candidate -> Pair.of(candidate.getPartitionPath(), candidate.getRecordKey()))
              .distinct()
              .collectAsList());
      Map<String, Map<String, HoodieRecordGlobalLocation>> locations =
          readPartitionedRecordIndexLocations(metadataTable, keysByPartition);
      return finalists.map(candidate -> arbitrateCandidate(candidate, locations
          .getOrDefault(candidate.getPartitionPath(), Collections.emptyMap())
          .get(candidate.getRecordKey())));
    }
    // Resolve current RLI locations for the finalist keys into a bounded driver-side map, then
    // attach per candidate via map(...). The finalist set is a bounded candidate pool and
    // {@code finalists} is already persisted upstream, so the two passes are cache hits.
    //
    // This deliberately avoids leftOuterJoin: HoodiePairData.leftOuterJoin requires both operands to
    // share the same backing flavor, but readRecordIndexLocationsWithKeys returns list-backed pair
    // data for a single-slice RLI (the common 1-file-group case) and RDD-backed for multi-slice.
    // Joining an RDD-backed finalist set against list-backed locations throws ClassCastException.
    // Attaching via map(...) preserves the finalists' backing (RDD stays RDD, list stays list).
    List<String> distinctKeys = finalists.map(ScoredVectorPostingMatch::getRecordKey)
        .distinct()
        .collectAsList();
    Map<String, HoodieRecordGlobalLocation> currentLocations = new HashMap<>();
    if (!distinctKeys.isEmpty()) {
      metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(distinctKeys))
          .collectAsList()
          .forEach(pair -> currentLocations.put(pair.getKey(), pair.getValue()));
    }
    return finalists.map(candidate ->
        arbitrateCandidate(candidate, currentLocations.get(candidate.getRecordKey())));
  }

  private static ScoredVectorPostingMatch arbitrateCandidate(
      ScoredVectorPostingMatch candidate,
      HoodieRecordGlobalLocation current) {
    VectorIndexArbiter.Decision decision = VectorIndexArbiter.classify(
        candidate.getPartitionPath(),
        candidate.getFileGroupId(),
        candidate.getBaseInstantTime(),
        current);
    HoodieRecordGlobalLocation resolved;
    switch (decision) {
      case SERVE:
        resolved = candidate.getLocation() != null ? candidate.getLocation() : current;
        break;
      case STALE:
        resolved = current;
        break;
      case DELETED:
      default:
        resolved = null;
        break;
    }
    return candidate.withArbiterVerdict(decision, resolved);
  }

  /**
   * Driver-side finalist arbiter core: classify each already-materialized finalist against a
   * pre-resolved map of current RLI locations (record key -> location, or absent for an RLI miss).
   * Pure and Spark-free so it is directly unit-testable; the {@link HoodieTableMetadata} overload
   * performs the batched RLI lookup and delegates here.
   *
   * <p>Implements the RFC-109 arbiter output contract (see {@link VectorIndexArbiter}):
   * hit+match -> SERVE (positional trust preserved via the posting's own location when present),
   * hit+differ -> STALE (resolved to the live RLI location), miss -> DELETED (dropped).
   */
  static VectorIndexMdtSearchUtils.ArbitrationResult arbitrateMaterializedFinalists(
      List<ScoredVectorPostingMatch> finalists,
      Map<String, HoodieRecordGlobalLocation> currentLocations) {
    List<ScoredVectorPostingMatch> serve = new ArrayList<>();
    List<ScoredVectorPostingMatch> stale = new ArrayList<>();
    long deleted = 0L;
    for (ScoredVectorPostingMatch candidate : finalists) {
      HoodieRecordGlobalLocation current = currentLocations.get(candidate.getRecordKey());
      VectorIndexArbiter.Decision decision = VectorIndexArbiter.classify(
          candidate.getPartitionPath(),
          candidate.getFileGroupId(),
          candidate.getBaseInstantTime(),
          current);
      switch (decision) {
        case SERVE:
          serve.add(candidate.withArbiterVerdict(
              decision, candidate.getLocation() != null ? candidate.getLocation() : current));
          break;
        case STALE:
          stale.add(candidate.withArbiterVerdict(decision, current));
          break;
        case DELETED:
        default:
          deleted++;
          break;
      }
    }
    return new VectorIndexMdtSearchUtils.ArbitrationResult(serve, stale, deleted);
  }

  /**
   * Driver-side finalist arbiter: batched RLI lookup over the distinct finalist keys, then
   * {@link #arbitrateMaterializedFinalists(List, Map)}. Used by the exact-rerank plan path, which
   * already materializes finalists to the driver, so no distributed shuffle is incurred.
   */
  static VectorIndexMdtSearchUtils.ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable,
      List<ScoredVectorPostingMatch> finalists) {
    return arbitrateMaterializedFinalists(metadataTable, finalists, false);
  }

  static VectorIndexMdtSearchUtils.ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable,
      List<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    if (finalists.isEmpty()) {
      return new VectorIndexMdtSearchUtils.ArbitrationResult(Collections.emptyList(), Collections.emptyList(), 0L);
    }
    if (partitionedRecordIndex) {
      List<ScoredVectorPostingMatch> serve = new ArrayList<>();
      List<ScoredVectorPostingMatch> stale = new ArrayList<>();
      long deleted = 0L;
      Map<String, List<ScoredVectorPostingMatch>> byPartition = new HashMap<>();
      for (ScoredVectorPostingMatch candidate : finalists) {
        byPartition.computeIfAbsent(candidate.getPartitionPath(), p -> new ArrayList<>()).add(candidate);
      }
      Map<String, Map<String, HoodieRecordGlobalLocation>> locations = readPartitionedRecordIndexLocations(
          metadataTable, groupKeysByPartition(finalists.stream()
              .map(candidate -> Pair.of(candidate.getPartitionPath(), candidate.getRecordKey()))
              .collect(Collectors.toList())));
      for (Map.Entry<String, List<ScoredVectorPostingMatch>> entry : byPartition.entrySet()) {
        VectorIndexMdtSearchUtils.ArbitrationResult result = arbitrateMaterializedFinalists(
            entry.getValue(), locations.getOrDefault(entry.getKey(), Collections.emptyMap()));
        serve.addAll(result.serve());
        stale.addAll(result.stale());
        deleted += result.deletedCount();
      }
      return new VectorIndexMdtSearchUtils.ArbitrationResult(serve, stale, deleted);
    }
    Set<String> distinctKeys = new HashSet<>();
    for (ScoredVectorPostingMatch candidate : finalists) {
      distinctKeys.add(candidate.getRecordKey());
    }
    Map<String, HoodieRecordGlobalLocation> currentLocations = new HashMap<>();
    metadataTable.readRecordIndexLocationsWithKeys(HoodieListData.eager(new ArrayList<>(distinctKeys)))
        .collectAsList()
        .forEach(pair -> currentLocations.put(pair.getKey(), pair.getValue()));
    return arbitrateMaterializedFinalists(finalists, currentLocations);
  }

  private static Map<String, Set<String>> groupKeysByPartition(List<Pair<String, String>> partitionKeys) {
    Map<String, Set<String>> keysByPartition = new HashMap<>();
    partitionKeys.forEach(pk -> keysByPartition.computeIfAbsent(pk.getLeft(), p -> new HashSet<>()).add(pk.getRight()));
    return keysByPartition;
  }

  /**
   * Partitioned-RLI lookup: data partition -> (record key -> current location).
   *
   * <p>{@code readRecordIndexLocationsWithKeys(keys, partition)} reads the single RLI file group the
   * first key hashes to, so every batch must hold keys of one partition AND one RLI file group.
   * Keys are therefore bucketed with {@link HoodieTableMetadataUtil#mapRecordKeyToFileGroupIndex}
   * over the partition's file groups, the same grouping {@code SparkMetadataTableRecordLevelIndex}
   * and {@code PartitionedRecordLevelIndexSupport} use. A partition without RLI file groups has no
   * indexed keys, so its candidates resolve to an RLI miss (DELETED).
   */
  static Map<String, Map<String, HoodieRecordGlobalLocation>> readPartitionedRecordIndexLocations(
      HoodieTableMetadata metadataTable,
      Map<String, Set<String>> keysByPartition) {
    Map<String, Map<String, HoodieRecordGlobalLocation>> locations = new HashMap<>();
    if (keysByPartition.isEmpty()) {
      return locations;
    }
    Map<String, List<FileSlice>> fileGroupsByPartition =
        metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
    keysByPartition.forEach((partition, keys) -> {
      int numFileGroups = fileGroupsByPartition.getOrDefault(partition, Collections.emptyList()).size();
      if (numFileGroups == 0 || keys.isEmpty()) {
        return;
      }
      Map<String, HoodieRecordGlobalLocation> partitionLocations = locations.computeIfAbsent(partition, p -> new HashMap<>());
      keys.stream()
          .collect(Collectors.groupingBy(key -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, numFileGroups)))
          .values()
          .forEach(fileGroupKeys -> metadataTable
              .readRecordIndexLocationsWithKeys(HoodieListData.eager(fileGroupKeys), Option.of(partition))
              .collectAsList()
              .forEach(pair -> partitionLocations.put(pair.getKey(), pair.getValue())));
    });
    return locations;
  }
}
