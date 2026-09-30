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
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Shared helper for MDT-native vector posting lookup and approximate candidate reduction.
 *
 * <p>This provides the missing second-stage building block after coarse IVF cluster pruning:
 * exact HFile lookups of cluster metadata, prefix scans over {@code P|gen|cluster|shard|},
 * approximate RaBitQ scoring, and direct file-slice targeting from posting payload metadata.
 */
public final class VectorIndexMdtSearchUtils {

  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMdtSearchUtils.class);
  private static final int TOP_K_REDUCER_KEY = 0;
  private static final Comparator<ScoredVectorPostingMatch> BEST_FIRST = Comparator
      .comparingDouble(ScoredVectorPostingMatch::getApproxDistance)
      .thenComparing(ScoredVectorPostingMatch::getRecordKey);

  private VectorIndexMdtSearchUtils() {
  }

  public static Map<Integer, Integer> readClusterShardCounts(HoodieTableMetadata metadataTable,
                                                             String indexPartition,
                                                             int generationId,
                                                             Collection<Integer> clusterIds) {
    if (clusterIds == null || clusterIds.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<Integer, Integer> shardCounts = new HashMap<>();
    for (Integer clusterId : clusterIds) {
      shardCounts.put(clusterId, 1);
    }
    return shardCounts;
  }

  public static Map<Integer, Set<String>> readClusterFileGroups(HoodieTableMetadata metadataTable,
                                                                 String indexPartition,
                                                                 int generationId,
                                                                 Collection<Integer> clusterIds,
                                                                 Collection<String> partitionPaths,
                                                                 boolean shouldLoadInMemory) {
    if (clusterIds == null || clusterIds.isEmpty()) {
      return Collections.emptyMap();
    }

    return Collections.emptyMap();
  }

  public static List<VectorPostingPrefixRawKey> buildPostingPrefixes(int generationId,
                                                                     Map<Integer, Integer> clusterShardCounts) {
    if (clusterShardCounts == null || clusterShardCounts.isEmpty()) {
      return Collections.emptyList();
    }

    List<Map.Entry<Integer, Integer>> clusters = new ArrayList<>(clusterShardCounts.entrySet());
    clusters.sort(Map.Entry.comparingByKey());

    List<VectorPostingPrefixRawKey> prefixes = new ArrayList<>();
    for (Map.Entry<Integer, Integer> entry : clusters) {
      int clusterId = entry.getKey();
      int shardCount = Math.max(1, entry.getValue());
      for (int shardId = 0; shardId < shardCount; shardId++) {
        prefixes.add(new VectorPostingPrefixRawKey(generationId, clusterId, shardId));
      }
    }
    return prefixes;
  }

  public static HoodieData<VectorPostingMatch> readPostingMatches(HoodieTableMetadata metadataTable,
                                                            String indexPartition,
                                                            int generationId,
                                                            Map<Integer, Integer> clusterShardCounts,
                                                            boolean shouldLoadInMemory) {
    List<VectorPostingPrefixRawKey> postingPrefixes = buildPostingPrefixes(generationId, clusterShardCounts);
    if (postingPrefixes.isEmpty()) {
      return HoodieListData.eager(Collections.emptyList());
    }

    List<RawKey> rawKeys = new ArrayList<>(postingPrefixes);
    HoodieData<VectorPostingMatch> matches = metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(rawKeys), indexPartition, shouldLoadInMemory)
        .flatMap(record -> {
          Option<Object> infoOpt = getVectorInfo(record);
          if (!infoOpt.isPresent()) {
            return Collections.<VectorPostingMatch>emptyIterator();
          }

          if (record.getData().isDeleted()) {
            String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
            return deletedRecordKey == null
                ? Collections.<VectorPostingMatch>emptyIterator()
                : Collections.singletonList(VectorPostingMatch.tombstone(deletedRecordKey, VectorPostingSerde.parsePostingKey(record.getRecordKey()))).iterator();
          }

          Object info = infoOpt.get();
          int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
          if (VectorPostingSerde.isPostingTombstoneInfo(info)) {
            String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
            return deletedRecordKey == null
                ? Collections.<VectorPostingMatch>emptyIterator()
                : Collections.singletonList(VectorPostingMatch.tombstone(deletedRecordKey, keyComponents)).iterator();
          }
          if (VectorPostingSerde.isPostingBlockInfo(info)) {
            PostingBlockView view = new PostingBlockView(asPostingBlock(info));
            List<VectorPostingMatch> blockMatches = new ArrayList<>(view.numVectors());
            for (int vectorIndex = 0; vectorIndex < view.numVectors(); vectorIndex++) {
              PostingBlockView.RowLocator rowLocator = view.rowLocator(vectorIndex);
              blockMatches.add(new VectorPostingMatch(
                  view.recordKey(vectorIndex),
                  keyComponents[0],
                  keyComponents[1],
                  rowLocator.getFileGroupId(),
                  rowLocator.getPartitionPath(),
                  rowLocator.getInstantTime(),
                  rowLocator.getRowPosition(),
                  VectorPostingSerde.copyBuffer(view.signPlaneRow(vectorIndex)),
                  VectorPostingSerde.packExtendedLevels(view, vectorIndex),
                  view.scalarFactor(PostingBlockView.ScalarFactor.RESIDUAL_NORM, vectorIndex),
                  view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_EX, vectorIndex),
                  view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_EX, vectorIndex),
                  view.hasVectorNorm() ? view.scalarFactor(PostingBlockView.ScalarFactor.VECTOR_NORM, vectorIndex) : null));
            }
            return blockMatches.iterator();
          }

          if (!VectorPostingSerde.isPostingDeltaInfo(info) || VectorPostingSerde.getDeltaBinaryCode(info) == null) {
            return Collections.<VectorPostingMatch>emptyIterator();
          }

          VectorPostingMatch match = VectorPostingMatch.delta(
              VectorPostingSerde.getDeltaRecordKey(info),
              keyComponents[0],
              keyComponents[1],
              VectorPostingSerde.getDeltaFileGroupId(info),
              VectorPostingSerde.getDeltaPartitionPath(info),
              VectorPostingSerde.getDeltaBaseInstantTime(info),
              VectorPostingSerde.getDeltaRowPosition(info),
              VectorPostingSerde.copyBuffer(VectorPostingSerde.getDeltaBinaryCode(info)),
              null,
              VectorPostingSerde.getDeltaResidualNorm(info),
              VectorPostingSerde.getDeltaFAddEx(info),
              VectorPostingSerde.getDeltaFRescaleEx(info),
              VectorPostingSerde.getDeltaVectorNormOrNull(info));
          return Collections.singletonList(match).iterator();
        });
    return matches.mapPartitions(iterator -> {
      long startMs = System.currentTimeMillis();
      List<VectorPostingMatch> partitionMatches = new ArrayList<>();
      long binaryCodeBytes = 0L;
      long extendedCodeBytes = 0L;
      Set<Integer> clusters = new HashSet<>();
      Set<String> fileGroups = new HashSet<>();
      while (iterator.hasNext()) {
        VectorPostingMatch match = iterator.next();
        partitionMatches.add(match);
        if (match.getBinaryCode() != null) {
          binaryCodeBytes += match.getBinaryCode().length;
        }
        if (match.getExtendedCode() != null) {
          extendedCodeBytes += match.getExtendedCode().length;
        }
        clusters.add(match.getClusterId());
        if (match.getFileGroupId() != null) {
          fileGroups.add(match.getFileGroupId());
        }
      }
      LOG.info("[vector_search][stage][read_postings] prefixes={} shouldLoadInMemory={} matches={} codeBytes={} extendedCodeBytes={} distinctClusters={} distinctFileGroups={} elapsedMs={}",
          postingPrefixes.size(),
          shouldLoadInMemory,
          partitionMatches.size(),
          binaryCodeBytes,
          extendedCodeBytes,
          clusters.size(),
          fileGroups.size(),
          System.currentTimeMillis() - startMs);
      return partitionMatches.iterator();
    }, true);
  }

  public static Map<Integer, Set<String>> collectClusterToFileGroups(HoodieTableMetadata metadataTable,
                                                                     String indexPartition,
                                                                     int generationId,
                                                                     Map<Integer, Integer> clusterShardCounts,
                                                                     Collection<String> partitionPaths,
                                                                     boolean shouldLoadInMemory) {
    if (clusterShardCounts == null || clusterShardCounts.isEmpty()) {
      return Collections.emptyMap();
    }

    Set<String> partitionFilter =
        partitionPaths == null || partitionPaths.isEmpty() ? Collections.emptySet() : new HashSet<>(partitionPaths);
    List<VectorPostingMatch> postingMatches = readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory).collectAsList();
    Map<Integer, Set<String>> clusterToFileGroups = new HashMap<>();
    for (VectorPostingMatch match : postingMatches) {
      if (match.getFileGroupId() == null) {
        continue;
      }
      if (!partitionFilter.isEmpty()
          && match.getPartitionPath() != null
          && !partitionFilter.contains(match.getPartitionPath())) {
        continue;
      }
      clusterToFileGroups
          .computeIfAbsent(match.getClusterId(), ignored -> new HashSet<>())
          .add(match.getFileGroupId());
    }
    return clusterToFileGroups;
  }

  /**
   * Reads all posting records from the vector index partition and returns a mapping
   * from record key to IVF cluster ID.  This is used for vector-aware clustering:
   * by sorting main table records by their cluster assignment during Hudi clustering,
   * records from the same IVF cluster are co-located in the same file groups, which
   * dramatically reduces I/O during the exact-read phase of vector search.
   */
  public static HoodiePairData<String, Integer> readClusterAssignments(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      boolean shouldLoadInMemory) {
    HoodieData<VectorPostingMatch> postings = readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory);
    return postings.mapToPair(p -> Pair.of(p.getRecordKey(), p.getClusterId()));
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
    long queryPrepStartMs = System.currentTimeMillis();
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, rabitqBits, randomSeed, assumeNormalized);
    final VectorDistanceMetric scoringMetric = metric;
    final boolean useAsymmetric = asymmetric;
    final boolean useResidualEncoding = residualEncoding;
    final float[][] residualCentroids = centroids;
    final int effectiveBits = Math.max(1, rabitqBits);
    final boolean useMultibitEstimator = effectiveBits > 1;

    if (useMultibitEstimator && useAsymmetric) {
      throw new IllegalArgumentException("Multibit RaBitQ does not support asymmetric scoring.");
    }
    if (useResidualEncoding && (residualCentroids == null || residualCentroids.length == 0)) {
      throw new IllegalArgumentException("Residual RaBitQ scoring requires IVF centroids.");
    }

    final RaBitQQueryState sharedQueryState;
    if (useMultibitEstimator || useResidualEncoding) {
      sharedQueryState = null;
    } else {
      sharedQueryState = (RaBitQQueryState) encoder.encodeQuery(queryVector);
    }
    final MetricQueryState multibitMetricQueryState = useMultibitEstimator
        ? MetricQueryState.create(scoringMetric, encoder::rotateVector, queryVector, assumeNormalized)
        : null;
    final MetricQueryState.ClusterQuery originClusterQuery = useMultibitEstimator && !useResidualEncoding
        ? multibitMetricQueryState.forRotatedCentroid(new float[queryVector.length])
        : null;

    long queryPrepMs = System.currentTimeMillis() - queryPrepStartMs;
    LOG.info("[vector_search][stage][score_postings_setup] queryDim={} queryCodeBytes={} metric={} bits={} asymmetric={} residualEncoding={} elapsedMs={}",
        dimension,
        sharedQueryState == null ? Math.max(1, (dimension + 7) / 8) : sharedQueryState.getBinaryCode().length,
        scoringMetric,
        effectiveBits,
        useAsymmetric,
        useResidualEncoding,
        queryPrepMs);
    return postingMatches.mapPartitions(iterator -> {
      long partitionStartMs = System.currentTimeMillis();
      if (!iterator.hasNext()) {
        LOG.info("[vector_search][stage][score_postings] input=0 output=0 queryDim={} residualEncoding={} residualQueryStates=0 computeMs=0 elapsedMs={}",
            dimension,
            useResidualEncoding,
            System.currentTimeMillis() - partitionStartMs);
        return Collections.<ScoredVectorPostingMatch>emptyIterator();
      }

      List<ScoredVectorPostingMatch> scored = new ArrayList<>();
      Map<Integer, RaBitQQueryState> residualQueryStates = (useResidualEncoding && !useMultibitEstimator) ? new HashMap<>() : null;
      Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache =
          useResidualEncoding && useMultibitEstimator ? new HashMap<>() : null;
      float minDistance = Float.POSITIVE_INFINITY;
      float maxDistance = Float.NEGATIVE_INFINITY;
      long computeStartMs = System.currentTimeMillis();
      while (iterator.hasNext()) {
        VectorPostingMatch match = iterator.next();
        if (match.isDeleted()) {
          scored.add(new ScoredVectorPostingMatch(match, Float.POSITIVE_INFINITY, null));
          continue;
        }
        RaBitQQueryState activeQueryState = sharedQueryState;
        if (useResidualEncoding && !useMultibitEstimator) {
          activeQueryState = residualQueryStates.computeIfAbsent(match.getClusterId(), clusterId -> {
            if (clusterId < 0 || clusterId >= residualCentroids.length || residualCentroids[clusterId] == null) {
              throw new IllegalArgumentException("Missing centroid for residual clusterId=" + clusterId);
            }
            return (RaBitQQueryState) encoder.encodeQuery(VectorPostingSerde.subtract(queryVector, residualCentroids[clusterId]));
          });
        }
        final float approxDistance;
        if (useMultibitEstimator) {
          MetricQueryState.ClusterQuery clusterQuery = useResidualEncoding
              ? clusterQuery(
                  multibitMetricQueryState,
                  residualCentroids,
                  clusterQueryCache,
                  match.getClusterId())
              : originClusterQuery;
          float dotTerm = RaBitQEncoder.multibitDotTerm(
              clusterQuery.rotatedQuery,
              clusterQuery.querySum,
              match.getBinaryCode(),
              match.getExtendedCode(),
              dimension,
              effectiveBits);
          double rip = (match.getRescaleFactor() == null ? 0.0d : match.getRescaleFactor()) * (double) dotTerm;
          float centerRip = match.getAdditiveFactor() != null ? match.getAdditiveFactor() : 0.0f;
          float residualNorm = match.getScalar() != null ? match.getScalar() : 0.0f;
          float vectorNorm = match.getVectorNorm() != null ? match.getVectorNorm() : Float.NaN;
          approxDistance = (float) multibitMetricQueryState.rankingDistance(rip, centerRip, residualNorm, vectorNorm, clusterQuery);
        } else {
          float effectiveScalar = match.getScalar() != null ? match.getScalar() : 1.0f;
          approxDistance = encoder.estimateDistance(
              activeQueryState,
              new QuantizedVector(match.getBinaryCode(), effectiveScalar),
              scoringMetric,
              useAsymmetric);
        }
        scored.add(new ScoredVectorPostingMatch(match, approxDistance, match.toLocation().orElse(null)));
        minDistance = Math.min(minDistance, approxDistance);
        maxDistance = Math.max(maxDistance, approxDistance);
      }
      long computeMs = System.currentTimeMillis() - computeStartMs;
      LOG.info("[vector_search][stage][score_postings] input={} output={} queryDim={} bits={} residualEncoding={} residualQueryStates={} minDist={} maxDist={} computeMs={} elapsedMs={}",
          scored.size(),
          scored.size(),
          dimension,
          effectiveBits,
          useResidualEncoding,
          residualQueryStates == null ? (clusterQueryCache == null ? 0 : clusterQueryCache.size()) : residualQueryStates.size(),
          scored.isEmpty() ? "n/a" : minDistance,
          scored.isEmpty() ? "n/a" : maxDistance,
          computeMs,
          System.currentTimeMillis() - partitionStartMs);
      return scored.iterator();
    }, true);
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
    return BEST_FIRST.compare(left, right) <= 0 ? left : right;
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

  public static HoodieData<ScoredVectorPostingMatch> selectTopK(HoodieData<ScoredVectorPostingMatch> candidates, int topK) {
    if (topK <= 0) {
      return HoodieListData.eager(Collections.emptyList());
    }

    HoodiePairData<Integer, List<ScoredVectorPostingMatch>> partialTopK = candidates
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          PriorityQueue<ScoredVectorPostingMatch> localTopK = new PriorityQueue<>(topK, BEST_FIRST.reversed());
          int inputCount = 0;
          while (iterator.hasNext()) {
            inputCount++;
            offerTopK(localTopK, iterator.next(), topK);
          }
          List<ScoredVectorPostingMatch> kept = new ArrayList<>(localTopK);
          trimTopK(kept, topK);
          LOG.info("[vector_search][stage][select_topk_local] input={} kept={} topK={} elapsedMs={}",
              inputCount,
              kept.size(),
              topK,
              System.currentTimeMillis() - startMs);
          if (kept.isEmpty()) {
            return Collections.<Pair<Integer, List<ScoredVectorPostingMatch>>>emptyIterator();
          }
          return Collections.singletonList(Pair.of(TOP_K_REDUCER_KEY, kept)).iterator();
        }, true)
        .mapToPair(pair -> Pair.of(pair.getLeft(), pair.getRight()));

    return partialTopK
        .reduceByKey((left, right) -> mergeTopK(left, right, topK), 1)
        .values()
        .flatMap(List::iterator)
        .mapPartitions(iterator -> {
          long startMs = System.currentTimeMillis();
          List<ScoredVectorPostingMatch> finalTopK = new ArrayList<>();
          while (iterator.hasNext()) {
            finalTopK.add(iterator.next());
          }
          LOG.info("[vector_search][stage][select_topk_final] kept={} topK={} elapsedMs={}",
              finalTopK.size(),
              topK,
              System.currentTimeMillis() - startMs);
          return finalTopK.iterator();
        }, true);
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

  private static Option<Object> getVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    return record.getData().getVectorIndexMetadata();
  }

  public static HoodieVectorIndexPostingBlock asPostingBlock(Object info) {
    return VectorPostingSerde.asPostingBlock(info);
  }

  static List<ScoredVectorPostingMatch> mergeTopK(List<ScoredVectorPostingMatch> left,
                                            List<ScoredVectorPostingMatch> right,
                                            int topK) {
    List<ScoredVectorPostingMatch> merged = new ArrayList<>(left.size() + right.size());
    merged.addAll(left);
    merged.addAll(right);
    trimTopK(merged, topK);
    return merged;
  }

  private static void trimTopK(List<ScoredVectorPostingMatch> candidates, int topK) {
    candidates.sort(BEST_FIRST);
    if (candidates.size() > topK) {
      candidates.subList(topK, candidates.size()).clear();
    }
  }

  private static void offerTopK(PriorityQueue<ScoredVectorPostingMatch> heap,
                                ScoredVectorPostingMatch candidate,
                                int topK) {
    if (heap.size() < topK) {
      heap.offer(candidate);
    } else if (BEST_FIRST.compare(candidate, heap.peek()) < 0) {
      heap.poll();
      heap.offer(candidate);
    }
  }

}
