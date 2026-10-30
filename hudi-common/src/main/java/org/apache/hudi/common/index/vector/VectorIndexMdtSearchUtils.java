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

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shared helper for MDT-native vector posting lookup and approximate candidate reduction.
 *
 * <p>This provides the missing second-stage building block after coarse IVF cluster pruning:
 * exact HFile lookups of cluster metadata, prefix scans over {@code P|gen|cluster|shard|},
 * approximate RaBitQ scoring, and direct file-slice targeting from posting payload metadata.
 */
public final class VectorIndexMdtSearchUtils {

  private VectorIndexMdtSearchUtils() {
  }

  public static List<VectorPostingPrefixRawKey> buildPostingPrefixes(
      int generationId,
      Map<Integer, Integer> clusterShardCounts) {
    return VectorPostingReader.buildPostingPrefixes(generationId, clusterShardCounts);
  }

  public static HoodieData<VectorPostingMatch> readPostingMatches(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      boolean shouldLoadInMemory) {
    return VectorPostingReader.readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory);
  }

  public static Map<Integer, Set<String>> collectClusterToFileGroups(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      Collection<String> partitionPaths,
      boolean shouldLoadInMemory) {
    return VectorPostingReader.collectClusterToFileGroups(
        metadataTable,
        indexPartition,
        generationId,
        clusterShardCounts,
        partitionPaths,
        shouldLoadInMemory);
  }

  /**
   * Reads all posting records from the vector index partition and returns a mapping
   * from record key to IVF cluster ID. This supports vector-aware clustering.
   */
  public static HoodiePairData<String, Integer> readClusterAssignments(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      boolean shouldLoadInMemory) {
    return VectorPostingReader.readClusterAssignments(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory);
  }

  public static HoodieData<ScoredVectorPostingMatch> scoreSymmetric1Bit(HoodieData<VectorPostingMatch> postingMatches,
                                                                  float[] queryVector,
                                                                  int dimension,
                                                                  long randomSeed,
                                                                  boolean assumeNormalized) {
    return scoreConfiguredPostings(postingMatches, queryVector, dimension, randomSeed, 1, assumeNormalized,
        VectorDistanceMetric.COSINE, false);
  }

  /**
   * Metric-aware approximate scoring of posting matches (RFC-109 L2 fix).
   *
   * <p>The binary code captures direction only; magnitude lives in the posting {@code scalar}.
   * We recover a cosine estimate (binary-symmetric Hamming or, when {@code asymmetric}, the
   * lower-variance float-vs-binary dot using the rotated query) and reconstruct the configured
   * {@code metric} via {@code ||q||} and {@code ||x||}. Passing the metric is what makes L2/dot
   * candidate selection consistent with the index definition.
   */
  public static HoodieData<ScoredVectorPostingMatch> scoreConfigured1Bit(HoodieData<VectorPostingMatch> postingMatches,
                                                                  float[] queryVector,
                                                                  int dimension,
                                                                  long randomSeed,
                                                                  boolean assumeNormalized,
                                                                  VectorDistanceMetric metric,
                                                                  boolean asymmetric) {
    return scoreConfiguredPostings(postingMatches, queryVector, dimension, randomSeed, 1, assumeNormalized,
        metric, asymmetric);
  }

  public static HoodieData<ScoredVectorPostingMatch> scoreConfiguredPostings(HoodieData<VectorPostingMatch> postingMatches,
                                                                      float[] queryVector,
                                                                      int dimension,
                                                                      long randomSeed,
                                                                      int rabitqBits,
                                                                      boolean assumeNormalized,
                                                                      VectorDistanceMetric metric,
                                                                      boolean asymmetric) {
    return scoreConfiguredPostings(postingMatches, queryVector, dimension, randomSeed, rabitqBits, assumeNormalized,
        metric, asymmetric, false, null);
  }

  /**
   * Metric-aware approximate scoring of posting matches with optional residual-mode query prep.
   *
   * <p>When {@code residualEncoding} is enabled, posting codes/scalars represent {@code x - c}
   * for the posting's IVF cluster centroid {@code c}. The query must be transformed per posting
   * cluster to {@code q - c} before RaBitQ scoring; otherwise L2 ranking inside a cluster remains
   * fundamentally misaligned with the encoded payload.
   */
  public static HoodieData<ScoredVectorPostingMatch> scoreResidual1Bit(HoodieData<VectorPostingMatch> postingMatches,
                                                                float[] queryVector,
                                                                int dimension,
                                                                long randomSeed,
                                                                boolean assumeNormalized,
                                                                VectorDistanceMetric metric,
                                                                boolean asymmetric,
                                                                boolean residualEncoding,
                                                                float[][] centroids) {
    return scoreConfiguredPostings(postingMatches, queryVector, dimension, randomSeed, 1, assumeNormalized,
        metric, asymmetric, residualEncoding, centroids);
  }

  public static HoodieData<ScoredVectorPostingMatch> scoreConfiguredPostings(HoodieData<VectorPostingMatch> postingMatches,
                                                                      float[] queryVector,
                                                                      int dimension,
                                                                      long randomSeed,
                                                                      int rabitqBits,
                                                                      boolean assumeNormalized,
                                                                      VectorDistanceMetric metric,
                                                                      boolean asymmetric,
                                                                      boolean residualEncoding,
                                                                      float[][] centroids) {
    return VectorPostingScorer.score(
        postingMatches,
        queryVector,
        dimension,
        randomSeed,
        rabitqBits,
        assumeNormalized,
        metric,
        asymmetric,
        residualEncoding,
        centroids);
  }

  /**
   * Scans MDT vector postings and returns the best approximate candidates held by
   * the candidate heap. {@code candidateHeapSize} is a precomputed candidate-set
   * size, not the user-facing result K; callers should pass the refine set size
   * they intend to rerank, including any overlay slack reserved by the plan.
   */
  public static HoodieData<ScoredVectorPostingMatch> scanPostingCandidates(HoodieTableMetadata metadataTable,
                                                                     String indexPartition,
                                                                     int generationId,
                                                                     Map<Integer, Integer> clusterShardCounts,
                                                                     float[] queryVector,
                                                                     int dimension,
                                                                     long randomSeed,
                                                                     int rabitqBits,
                                                                     boolean assumeNormalized,
                                                                     VectorDistanceMetric metric,
                                                                     boolean asymmetric,
                                                                     boolean residualEncoding,
                                                                     float[][] centroids,
                                                                     int candidateHeapSize) {
    return scanPostingCandidates(
        metadataTable,
        indexPartition,
        generationId,
        clusterShardCounts,
        queryVector,
        dimension,
        randomSeed,
        rabitqBits,
        assumeNormalized,
        metric,
        asymmetric,
        residualEncoding,
        centroids,
        candidateHeapSize,
        selectPostingScanMode(rabitqBits, metric, asymmetric, residualEncoding));
  }

  static HoodieData<ScoredVectorPostingMatch> scanPostingCandidates(HoodieTableMetadata metadataTable,
                                                             String indexPartition,
                                                             int generationId,
                                                             Map<Integer, Integer> clusterShardCounts,
                                                             float[] queryVector,
                                                             int dimension,
                                                             long randomSeed,
                                                             int rabitqBits,
                                                             boolean assumeNormalized,
                                                             VectorDistanceMetric metric,
                                                             boolean asymmetric,
                                                             boolean residualEncoding,
                                                             float[][] centroids,
                                                             int candidateHeapSize,
                                                             PostingScanMode scanMode) {
    switch (scanMode) {
      case POSTING_MATCHES:
        return scanPostingMatchCandidates(
            metadataTable,
            indexPartition,
            generationId,
            clusterShardCounts,
            queryVector,
            dimension,
            randomSeed,
            rabitqBits,
            assumeNormalized,
            metric,
            asymmetric,
            residualEncoding,
            centroids,
            candidateHeapSize);
      case PACKED_BLOCKS:
        return scanPackedPostingBlockCandidates(
            metadataTable,
            indexPartition,
            generationId,
            clusterShardCounts,
            queryVector,
            dimension,
            randomSeed,
            rabitqBits,
            assumeNormalized,
            metric,
            centroids,
            candidateHeapSize);
      default:
        throw new IllegalArgumentException("Unsupported vector posting scan mode: " + scanMode);
    }
  }

  static PostingScanMode selectPostingScanMode(int rabitqBits,
                                               VectorDistanceMetric metric,
                                               boolean asymmetric,
                                               boolean residualEncoding) {
    if (rabitqBits > 1 && residualEncoding && !asymmetric) {
      return PostingScanMode.PACKED_BLOCKS;
    }
    if (rabitqBits == 1) {
      return PostingScanMode.POSTING_MATCHES;
    }
    throw new IllegalArgumentException(
        "Unsupported vector posting scan configuration: bits=" + rabitqBits
            + ", metric=" + metric
            + ", asymmetric=" + asymmetric
            + ", residualEncoding=" + residualEncoding);
  }

  private static HoodieData<ScoredVectorPostingMatch> scanPostingMatchCandidates(HoodieTableMetadata metadataTable,
                                                                          String indexPartition,
                                                                          int generationId,
                                                                          Map<Integer, Integer> clusterShardCounts,
                                                                          float[] queryVector,
                                                                          int dimension,
                                                                          long randomSeed,
                                                                          int rabitqBits,
                                                                          boolean assumeNormalized,
                                                                          VectorDistanceMetric metric,
                                                                          boolean asymmetric,
                                                                          boolean residualEncoding,
                                                                          float[][] centroids,
                                                                          int candidateHeapSize) {
    HoodieData<ScoredVectorPostingMatch> scored = scoreConfiguredPostings(
        readPostingMatches(metadataTable, indexPartition, generationId, clusterShardCounts, false),
        queryVector,
        dimension,
        randomSeed,
        rabitqBits,
        assumeNormalized,
        metric,
        asymmetric,
        residualEncoding,
        centroids);
    return selectTopK(overlayByRecordKey(scored), candidateHeapSize);
  }

  private static HoodieData<ScoredVectorPostingMatch> scanPackedPostingBlockCandidates(HoodieTableMetadata metadataTable,
                                                                                String indexPartition,
                                                                                int generationId,
                                                                                Map<Integer, Integer> clusterShardCounts,
                                                                                float[] queryVector,
                                                                                int dimension,
                                                                                long randomSeed,
                                                                                int rabitqBits,
                                                                                boolean assumeNormalized,
                                                                                VectorDistanceMetric metric,
                                                                                float[][] centroids,
                                                                                int candidateHeapSize) {
    return VectorPostingBlockScanner.scan(
        metadataTable,
        indexPartition,
        generationId,
        clusterShardCounts,
        queryVector,
        dimension,
        randomSeed,
        rabitqBits,
        assumeNormalized,
        metric,
        centroids,
        candidateHeapSize);
  }

  enum PostingScanMode {
    POSTING_MATCHES,
    PACKED_BLOCKS
  }

  static Iterator<ScoredVectorPostingMatch> scanBlockPartition(
      Iterator<HoodieRecord<HoodieMetadataPayload>> records,
      MetricQueryState metricQueryState,
      float[][] centroids,
      int dimension,
      int rabitqBits,
      int heapSize) {
    return VectorPostingBlockScanner.scanBlockPartition(
        records, metricQueryState, centroids, dimension, rabitqBits, heapSize);
  }

  static HoodieData<ScoredVectorPostingMatch> overlayByRecordKey(HoodieData<ScoredVectorPostingMatch> candidates) {
    return candidates
        .mapToPair(candidate -> Pair.of(candidate.getRecordKey(), candidate))
        .reduceByKey(VectorIndexMdtSearchUtils::chooseOverlayCandidate, 1)
        .values()
        .mapPartitions(iterator -> {
          List<ScoredVectorPostingMatch> live = new ArrayList<>();
          while (iterator.hasNext()) {
            ScoredVectorPostingMatch candidate = iterator.next();
            if (!candidate.isDeleted()) {
              live.add(candidate);
            }
          }
          return live.iterator();
        }, true);
  }

  static ScoredVectorPostingMatch chooseOverlayCandidate(ScoredVectorPostingMatch left, ScoredVectorPostingMatch right) {
    if (left.isDeleted() || right.isDeleted()) {
      return left.isDeleted() ? left : right;
    }
    if (left.isDelta() != right.isDelta()) {
      return left.isDelta() ? left : right;
    }
    int instantCompare = comparePostingInstant(left.getBaseInstantTime(), right.getBaseInstantTime());
    if (instantCompare < 0) {
      return right;
    }
    if (instantCompare > 0) {
      return left;
    }
    return VectorCandidateReducer.compare(left, right) <= 0 ? left : right;
  }

  private static int comparePostingInstant(String left, String right) {
    String safeLeft = left == null ? "" : left;
    String safeRight = right == null ? "" : right;
    return safeLeft.compareTo(safeRight);
  }

  static MetricQueryState.ClusterQuery clusterQuery(
      MetricQueryState metricQueryState,
      float[][] centroids,
      Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache,
      int clusterId) {
    return clusterQueryCache.computeIfAbsent(clusterId, id -> {
      if (id < 0 || id >= centroids.length || centroids[id] == null) {
        throw new IllegalArgumentException("Missing centroid for residual clusterId=" + id);
      }
      // Rotate the centroid once per distinct cluster (cached); wRot = qRot - cRot in forRotatedCentroid.
      return metricQueryState.forRotatedCentroid(metricQueryState.rotateCentroid(centroids[id]));
    });
  }

  static byte[] repackExtendedLevels(PostingBlockView view,
                                     int vectorIndex,
                                     int dimension,
                                     byte[] scratch) {
    return VectorPostingBlockScanner.repackExtendedLevels(view, vectorIndex, dimension, scratch);
  }

  public static HoodieData<ScoredVectorPostingMatch> attachRecordLocations(HoodieTableMetadata metadataTable,
                                                                     HoodieData<ScoredVectorPostingMatch> scoredVectorPostingMatches) {
    HoodiePairData<String, ScoredVectorPostingMatch> scoredByRecordKey =
        scoredVectorPostingMatches.mapToPair(candidate -> Pair.of(candidate.getRecordKey(), candidate));
    HoodiePairData<String, HoodieRecordGlobalLocation> locations =
        metadataTable.readRecordIndexLocationsWithKeys(scoredByRecordKey.keys().distinct());

    return scoredByRecordKey.leftOuterJoin(locations)
        .flatMapValues(joined -> {
          if (!joined.getRight().isPresent()) {
            return Collections.<ScoredVectorPostingMatch>emptyIterator();
          }
          HoodieRecordGlobalLocation location = joined.getLeft().getLocation() == null
              ? joined.getRight().get()
              : joined.getLeft().getLocation();
          return Collections.singletonList(joined.getLeft().withLocation(location)).iterator();
        })
        .values();
  }

  public static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(
      HoodieTableMetadata metadataTable, HoodieData<ScoredVectorPostingMatch> finalists) {
    return VectorIndexRliArbitrator.arbitrateFinalists(metadataTable, finalists);
  }

  public static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(
      HoodieTableMetadata metadataTable,
      HoodieData<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    return VectorIndexRliArbitrator.arbitrateFinalists(metadataTable, finalists, partitionedRecordIndex);
  }

  /**
   * Bucketed outcome of driver-side finalist arbitration. {@link #serve()} and {@link #stale()}
   * carry the tagged candidates (each already {@code withArbiterVerdict}); DELETED candidates are
   * dropped and only counted. Callers apply the mode-specific action:
   *
   * <ul>
   *   <li>Approximate mode: keep {@link #serve()}, discard {@link #stale()} and deleted.</li>
   *   <li>Exact mode: positional-fetch {@link #serve()}, key-fallback-fetch {@link #stale()}
   *       (or throw under {@code stale.policy=fail}), discard deleted.</li>
   * </ul>
   *
   * <p>{@link #staleCount()} equals {@code stale().size()}; both are surfaced so log lines can
   * emit {@code arbiterExclusions{stale=..,deleted=..}} without re-counting.
   */
  public static final class ArbitrationResult {
    private final List<ScoredVectorPostingMatch> serve;
    private final List<ScoredVectorPostingMatch> stale;
    private final long deletedCount;

    ArbitrationResult(List<ScoredVectorPostingMatch> serve, List<ScoredVectorPostingMatch> stale, long deletedCount) {
      this.serve = serve;
      this.stale = stale;
      this.deletedCount = deletedCount;
    }

    public List<ScoredVectorPostingMatch> serve() {
      return serve;
    }

    public List<ScoredVectorPostingMatch> stale() {
      return stale;
    }

    public long staleCount() {
      return stale.size();
    }

    public long deletedCount() {
      return deletedCount;
    }
  }

  public static ArbitrationResult arbitrateMaterializedFinalists(
      List<ScoredVectorPostingMatch> finalists,
      Map<String, HoodieRecordGlobalLocation> currentLocations) {
    return VectorIndexRliArbitrator.arbitrateMaterializedFinalists(finalists, currentLocations);
  }

  public static ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable, List<ScoredVectorPostingMatch> finalists) {
    return VectorIndexRliArbitrator.arbitrateMaterializedFinalists(metadataTable, finalists);
  }

  public static ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable,
      List<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    return VectorIndexRliArbitrator.arbitrateMaterializedFinalists(
        metadataTable, finalists, partitionedRecordIndex);
  }

  public static HoodieData<ScoredVectorPostingMatch> selectTopK(
      HoodieData<ScoredVectorPostingMatch> candidates, int topK) {
    return VectorCandidateReducer.selectTopK(candidates, topK);
  }

  public static List<ScoredVectorPostingMatch> collectCandidateHeapWithLocations(HoodieTableMetadata metadataTable,
                                                                          String indexPartition,
                                                                          int generationId,
                                                                          Map<Integer, Integer> clusterShardCounts,
                                                                          float[] queryVector,
                                                                          int dimension,
                                                                          long randomSeed,
                                                                          int rabitqBits,
                                                                          boolean assumeNormalized,
                                                                          VectorDistanceMetric metric,
                                                                          boolean asymmetric,
                                                                          boolean residualEncoding,
                                                                          float[][] centroids,
                                                                          int topK,
                                                                          int refineFactor) {
    int candidateHeapSize = Math.max(topK, topK * Math.max(1, refineFactor));
    HoodieData<ScoredVectorPostingMatch> scoredTopK = scanPostingCandidates(
        metadataTable,
        indexPartition,
        generationId,
        clusterShardCounts,
        queryVector,
        dimension,
        randomSeed,
        rabitqBits,
        assumeNormalized,
        metric,
        asymmetric,
        residualEncoding,
        centroids,
        candidateHeapSize);
    HoodieData<ScoredVectorPostingMatch> topKData = attachRecordLocations(metadataTable, scoredTopK);

    try {
      return topKData.collectAsList();
    } finally {
      topKData.unpersistWithDependencies();
    }
  }

  public static HoodieVectorIndexPostingBlock asPostingBlock(Object info) {
    return VectorPostingSerde.asPostingBlock(info);
  }

}
