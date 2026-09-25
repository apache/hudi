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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

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

  public static HoodieData<VectorPostingMatch> readVectorPostingMatches(HoodieTableMetadata metadataTable,
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
    List<VectorPostingMatch> postingMatches = readVectorPostingMatches(
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
    HoodieData<VectorPostingMatch> postings = readVectorPostingMatches(
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
        readVectorPostingMatches(metadataTable, indexPartition, generationId, clusterShardCounts, false),
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
    List<VectorPostingPrefixRawKey> postingPrefixes = buildPostingPrefixes(generationId, clusterShardCounts);
    if (candidateHeapSize <= 0 || postingPrefixes.isEmpty()) {
      return HoodieListData.eager(Collections.emptyList());
    }
    if (rabitqBits <= 1) {
      throw new IllegalArgumentException("Packed block scanner requires multibit RaBitQ postings.");
    }
    if (centroids == null || centroids.length == 0) {
      throw new IllegalArgumentException("Packed block scanner requires IVF centroids for residual scoring.");
    }

    long setupStartMs = System.currentTimeMillis();
    RaBitQEncoder encoder = new RaBitQEncoder(dimension, rabitqBits, randomSeed, assumeNormalized);
    MetricQueryState metricQueryState = MetricQueryState.create(metric, encoder::rotateVector, queryVector, assumeNormalized);
    int effectiveBits = Math.max(1, rabitqBits);
    LOG.info("[vector_search][stage][scan_posting_blocks_setup] prefixes={} queryDim={} metric={} bits={} assumeNormalized={} candidateHeapSize={} elapsedMs={}",
        postingPrefixes.size(),
        dimension,
        metric,
        effectiveBits,
        assumeNormalized,
        candidateHeapSize,
        System.currentTimeMillis() - setupStartMs);

    List<RawKey> rawKeys = new ArrayList<>(postingPrefixes);
    HoodieData<ScoredVectorPostingMatch> localTopK = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(rawKeys), indexPartition, false)
        .mapPartitions(iterator -> scanBlockPartition(iterator, metricQueryState, centroids, dimension, effectiveBits, candidateHeapSize), true);
    return selectTopK(overlayByRecordKey(localTopK), candidateHeapSize);
  }

  enum PostingScanMode {
    POSTING_MATCHES,
    PACKED_BLOCKS
  }

  static Iterator<ScoredVectorPostingMatch> scanBlockPartition(Iterator<HoodieRecord<HoodieMetadataPayload>> records,
                                                         MetricQueryState metricQueryState,
                                                         float[][] centroids,
                                                         int dimension,
                                                         int rabitqBits,
                                                         int heapSize) {
    long partitionStartMs = System.currentTimeMillis();
    if (heapSize <= 0) {
      return Collections.emptyIterator();
    }

    VectorCandidateHeap heap = new VectorCandidateHeap(heapSize);
    List<PostingBlockView> blocks = new ArrayList<>();
    List<int[]> blockKeyComponents = new ArrayList<>();
    List<ScoredVectorPostingMatch> deltaMatches = new ArrayList<>();
    Map<String, ScoredVectorPostingMatch> deltaByRecordKey = new HashMap<>();
    Set<String> suppressedPostingKeys = new HashSet<>();
    Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache = new HashMap<>();
    Map<Integer, RaBitQByteLutScorer> lutScorerCache = new HashMap<>();
    int blocksRead = 0;
    int deltasRead = 0;
    long vectorsSeen = 0L;
    long boundSurvivors = 0L;
    // §5A fix 5 (deepened): split the conflated scan wall time. iterNs = time pulling records
    // (HFile block read + Avro decode, lazy in next()); kernelNs = scoring loop (what the LUT
    // touches); gcMs delta = allocation pressure. This isolates whether the straggler is I/O or CPU.
    long iterNs = 0L;
    long kernelNs = 0L;
    long gcStartMs = totalGcMillis();

    while (true) {
      long iterStart = System.nanoTime();
      boolean hasNext = records.hasNext();
      HoodieRecord<HoodieMetadataPayload> record = hasNext ? records.next() : null;
      iterNs += System.nanoTime() - iterStart;
      if (!hasNext) {
        break;
      }
      if (record.getData().isDeleted()) {
        String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
        int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
        String postingKey = postingIdentity(keyComponents, deletedRecordKey);
        if (deletedRecordKey != null && suppressedPostingKeys.add(postingKey)) {
          removeSuppressedDelta(deltaByRecordKey, deletedRecordKey, keyComponents);
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        continue;
      }
      Option<Object> infoOpt = getVectorInfo(record);
      if (!infoOpt.isPresent()) {
        continue;
      }

      Object info = infoOpt.get();
      if (VectorPostingSerde.isPostingTombstoneInfo(info)) {
        String deletedRecordKey = VectorIndexMetadataKey.postingRecordKey(record.getRecordKey());
        int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
        String postingKey = postingIdentity(keyComponents, deletedRecordKey);
        if (deletedRecordKey != null && suppressedPostingKeys.add(postingKey)) {
          removeSuppressedDelta(deltaByRecordKey, deletedRecordKey, keyComponents);
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        continue;
      }
      int[] keyComponents = VectorPostingSerde.parsePostingKey(record.getRecordKey());
      if (VectorPostingSerde.isPostingBlockInfo(info)) {
        PostingBlockView view = new PostingBlockView(asPostingBlock(info));
        validatePackedBlock(view, rabitqBits);
        int blockOrdinal = blocks.size();
        blocks.add(view);
        blockKeyComponents.add(keyComponents);
        blocksRead++;
        ByteBuffer signPlane = view.signPlaneBuffer();
        ByteBuffer exPlanes = view.exPlanesBuffer();
        MetricQueryState.ClusterQuery clusterQuery = clusterQuery(
            metricQueryState,
            centroids,
            clusterQueryCache,
            keyComponents[0]);
        // Byte-LUT scorer built once per probed cluster (RFC-109 v3 §5A fixes 1-2): preserves
        // exact float-query semantics, replaces the per-dimension sign loop and the per-survivor
        // repack/allocate pass-2 path with table lookups and zero scoring-loop allocation.
        RaBitQByteLutScorer scorer = lutScorer(
            lutScorerCache, clusterQuery, keyComponents[0], dimension, view.codeRowBytes());
        int exBits = rabitqBits - 1;
        long kernelStart = System.nanoTime();
        for (int vectorIndex = 0; vectorIndex < view.numVectors(); vectorIndex++) {
          vectorsSeen++;
          int signOffset = view.signPlaneOffset(vectorIndex);
          double signDot = scorer.planeDot(signPlane, signOffset);
          float dotTerm1 = scorer.pass1FromDot(signDot);
          float centerRip = view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_1, vectorIndex);
          float residualNorm = view.scalarFactor(PostingBlockView.ScalarFactor.RESIDUAL_NORM, vectorIndex);
          float vectorNorm = view.vectorNormOrNaN(vectorIndex);
          double rip1 = view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_1, vectorIndex) * (double) dotTerm1;
          float bound = (float) metricQueryState.optimisticRankingDistance(
              rip1,
              view.scalarFactor(PostingBlockView.ScalarFactor.ERR_1, vectorIndex),
              centerRip,
              residualNorm,
              vectorNorm,
              clusterQuery);
          if (!heap.wouldAdmit(bound)) {
            continue;
          }

          boundSurvivors++;
          // Pass 2 scores the extended planes in place from the same LUT; no signRow copy,
          // no repackExtendedLevels, no per-survivor byte[] allocation.
          float dotTerm = scorer.pass2(signDot, view, exPlanes, vectorIndex, exBits, rabitqBits);
          double rip = view.scalarFactor(PostingBlockView.ScalarFactor.F_RESCALE_EX, vectorIndex) * (double) dotTerm;
          float approxDistance = (float) metricQueryState.rankingDistance(
              rip,
              view.scalarFactor(PostingBlockView.ScalarFactor.F_ADD_EX, vectorIndex),
              residualNorm,
              vectorNorm,
              clusterQuery);
          heap.offer(packBlockKey(blockOrdinal, vectorIndex), approxDistance);
        }
        kernelNs += System.nanoTime() - kernelStart;
      } else if (VectorPostingSerde.isPostingDeltaInfo(info) && VectorPostingSerde.getDeltaBinaryCode(info) != null) {
        deltasRead++;
        ScoredVectorPostingMatch scoredDelta = scoreDeltaMatch(
            info,
            keyComponents,
            metricQueryState,
            centroids,
            clusterQueryCache,
            dimension,
            rabitqBits);
        if (suppressedPostingKeys.contains(postingIdentity(keyComponents, scoredDelta.getRecordKey()))) {
          continue;
        }
        boolean newOverlayKey = !deltaByRecordKey.containsKey(scoredDelta.getRecordKey());
        deltaByRecordKey.merge(scoredDelta.getRecordKey(), scoredDelta, VectorIndexMdtSearchUtils::chooseOverlayCandidate);
        if (newOverlayKey) {
          heap.growTo(heapSize + deltaByRecordKey.size() + suppressedPostingKeys.size());
        }
        if (heap.wouldAdmit(scoredDelta.getApproxDistance())) {
          int deltaOrdinal = deltaMatches.size();
          deltaMatches.add(scoredDelta);
          heap.offer(packDeltaKey(deltaOrdinal), scoredDelta.getApproxDistance());
        }
      } else {
        throw new IllegalArgumentException("Unexpected vector posting metadata record: " + info.getClass().getName());
      }
    }

    List<ScoredVectorPostingMatch> scored = new ArrayList<>(heap.size());
    Set<String> emittedDeltaRecordKeys = new HashSet<>();
    for (VectorCandidateHeap.Entry entry : heap.entriesBestFirst()) {
      long key = entry.key;
      if (isDeltaKey(key)) {
        ScoredVectorPostingMatch delta = deltaMatches.get(unpackDeltaOrdinal(key));
        int[] keyComponents = {delta.getClusterId(), delta.getShardId()};
        if (!suppressedPostingKeys.contains(postingIdentity(keyComponents, delta.getRecordKey()))
            && delta == deltaByRecordKey.get(delta.getRecordKey())) {
          scored.add(delta);
          emittedDeltaRecordKeys.add(delta.getRecordKey());
        }
        continue;
      }
      int blockOrdinal = unpackBlockOrdinal(key);
      int vectorIndex = unpackVectorIndex(key);
      PostingBlockView view = blocks.get(blockOrdinal);
      int[] keyComponents = blockKeyComponents.get(blockOrdinal);
      String recordKey = view.recordKey(vectorIndex);
      if (suppressedPostingKeys.contains(postingIdentity(keyComponents, recordKey))) {
        continue;
      }
      ScoredVectorPostingMatch overlayDelta = deltaByRecordKey.get(recordKey);
      if (overlayDelta != null) {
        if (emittedDeltaRecordKeys.add(recordKey)) {
          scored.add(overlayDelta);
        }
        continue;
      }
      PostingBlockView.RowLocator rowLocator = view.rowLocator(vectorIndex);
      VectorPostingMatch match = new VectorPostingMatch(
          recordKey,
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
          view.hasVectorNorm() ? view.scalarFactor(PostingBlockView.ScalarFactor.VECTOR_NORM, vectorIndex) : null);
      scored.add(new ScoredVectorPostingMatch(match, entry.score, match.toLocation().orElse(null)));
    }
    long partitionElapsedMs = System.currentTimeMillis() - partitionStartMs;
    // RFC-109 v3 §5A fix 5: task-normalized throughput so the next investigation needs no log
    // archaeology. vectorsPerMs exposes per-partition scan speed; survivorsPerVector exposes
    // pass-1 selectivity (skew shows up as low vectorsPerMs on a high-vectors partition).
    double vectorsPerMs = partitionElapsedMs > 0 ? (double) vectorsSeen / partitionElapsedMs : (double) vectorsSeen;
    double survivorsPerVector = vectorsSeen > 0 ? (double) boundSurvivors / vectorsSeen : 0.0;
    long iterMs = iterNs / 1_000_000L;
    long kernelMs = kernelNs / 1_000_000L;
    long gcMs = totalGcMillis() - gcStartMs;
    LOG.info("[vector_search][stage][scan_posting_blocks] blocks={} deltas={} vectors={} boundSurvivors={} kept={} clusterQueries={} "
            + "vectorsPerMs={} survivorsPerVector={} iterMs={} kernelMs={} gcMs={} elapsedMs={}",
        blocksRead,
        deltasRead,
        vectorsSeen,
        boundSurvivors,
        scored.size(),
        clusterQueryCache.size(),
        String.format("%.1f", vectorsPerMs),
        String.format("%.4f", survivorsPerVector),
        iterMs,
        kernelMs,
        gcMs,
        partitionElapsedMs);
    return scored.iterator();
  }

  private static HoodieData<ScoredVectorPostingMatch> overlayByRecordKey(HoodieData<ScoredVectorPostingMatch> candidates) {
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

  private static ScoredVectorPostingMatch chooseOverlayCandidate(ScoredVectorPostingMatch left, ScoredVectorPostingMatch right) {
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

  private static void validatePackedBlock(PostingBlockView view, int rabitqBits) {
    int expectedExPlanes = Math.max(0, rabitqBits - 1);
    if (view.numExPlanes() != expectedExPlanes) {
      throw new IllegalArgumentException(
          "Posting block bit-plane mismatch: expected exPlanes=" + expectedExPlanes
              + ", actual=" + view.numExPlanes());
    }
  }

  private static MetricQueryState.ClusterQuery clusterQuery(
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

  private static RaBitQByteLutScorer lutScorer(
      Map<Integer, RaBitQByteLutScorer> lutScorerCache,
      MetricQueryState.ClusterQuery clusterQuery,
      int clusterId,
      int dimension,
      int codeRowBytes) {
    return lutScorerCache.computeIfAbsent(clusterId, id ->
        RaBitQByteLutScorer.forQuery(clusterQuery.rotatedQuery, clusterQuery.querySum, dimension, codeRowBytes));
  }

  /** Cumulative JVM GC time (ms) across all collectors; delta over a scan isolates allocation stalls. */
  private static long totalGcMillis() {
    long total = 0L;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      long t = bean.getCollectionTime();
      if (t > 0) {
        total += t;
      }
    }
    return total;
  }

  private static ScoredVectorPostingMatch scoreDeltaMatch(Object info,
                                                    int[] keyComponents,
                                                    MetricQueryState metricQueryState,
                                                    float[][] centroids,
                                                    Map<Integer, MetricQueryState.ClusterQuery> clusterQueryCache,
                                                    int dimension,
                                                    int rabitqBits) {
    CodeRows codeRows = splitDeltaCode(VectorPostingSerde.getDeltaBinaryCode(info), dimension, rabitqBits);
    MetricQueryState.ClusterQuery clusterQuery = clusterQuery(
        metricQueryState,
        centroids,
        clusterQueryCache,
        keyComponents[0]);
    float dotTerm = RaBitQEncoder.multibitDotTerm(
        clusterQuery.rotatedQuery,
        clusterQuery.querySum,
        codeRows.sign,
        codeRows.extended,
        dimension,
        rabitqBits);
    VectorPostingMatch match = VectorPostingMatch.delta(
        VectorPostingSerde.getDeltaRecordKey(info),
        keyComponents[0],
        keyComponents[1],
        VectorPostingSerde.getDeltaFileGroupId(info),
        VectorPostingSerde.getDeltaPartitionPath(info),
        VectorPostingSerde.getDeltaBaseInstantTime(info),
        VectorPostingSerde.getDeltaRowPosition(info),
        codeRows.sign,
        codeRows.extended,
        VectorPostingSerde.getDeltaResidualNorm(info),
        VectorPostingSerde.getDeltaFAddEx(info),
        VectorPostingSerde.getDeltaFRescaleEx(info),
        VectorPostingSerde.getDeltaVectorNormOrNull(info));
    double rip = VectorPostingSerde.getDeltaFRescaleEx(info) * (double) dotTerm;
    float approxDistance = (float) metricQueryState.rankingDistance(
        rip,
        VectorPostingSerde.getDeltaFAddEx(info),
        VectorPostingSerde.getDeltaResidualNorm(info),
        VectorPostingSerde.getDeltaVectorNormOrNaN(info),
        clusterQuery);
    return new ScoredVectorPostingMatch(match, approxDistance, match.toLocation().orElse(null));
  }

  private static CodeRows splitDeltaCode(ByteBuffer codeBuffer, int dimension, int rabitqBits) {
    int signBytes = (dimension + 7) / 8;
    int extendedBytes = (dimension * Math.max(0, rabitqBits - 1) + 7) / 8;
    int expectedBytes = signBytes + extendedBytes;
    ByteBuffer duplicate = codeBuffer.duplicate();
    if (duplicate.remaining() != expectedBytes) {
      throw new IllegalArgumentException(
          "Posting delta code length mismatch: expected=" + expectedBytes
              + ", actual=" + duplicate.remaining());
    }
    byte[] sign = new byte[signBytes];
    byte[] extended = new byte[extendedBytes];
    duplicate.get(sign);
    duplicate.get(extended);
    return new CodeRows(sign, extended);
  }

  static byte[] repackExtendedLevels(PostingBlockView view,
                                     int vectorIndex,
                                     int dimension,
                                     byte[] scratch) {
    int exBits = view.numExPlanes();
    if (exBits <= 0) {
      return new byte[0];
    }
    int packedBytes = (dimension * exBits + 7) / 8;
    Arrays.fill(scratch, 0, packedBytes, (byte) 0);
    ByteBuffer exPlanes = view.exPlanesBuffer();
    int bitOffset = 0;
    for (int dim = 0; dim < dimension; dim++) {
      for (int bit = 0; bit < exBits; bit++) {
        int plane = bit;
        int planeOffset = view.exPlaneOffset(vectorIndex, plane) + (dim >> 3);
        if ((exPlanes.get(planeOffset) & (1 << (dim & 7))) != 0) {
          int absoluteBit = bitOffset + bit;
          scratch[absoluteBit >> 3] |= (byte) (1 << (absoluteBit & 7));
        }
      }
      bitOffset += exBits;
    }
    return Arrays.copyOf(scratch, packedBytes);
  }

  private static long packBlockKey(int blockOrdinal, int vectorIndex) {
    return ((long) blockOrdinal << Integer.SIZE) | (vectorIndex & 0xFFFFFFFFL);
  }

  private static long packDeltaKey(int deltaOrdinal) {
    return Long.MIN_VALUE | (deltaOrdinal & 0x7FFFFFFFL);
  }

  private static boolean isDeltaKey(long key) {
    return key < 0;
  }

  private static int unpackDeltaOrdinal(long key) {
    return (int) (key & 0x7FFFFFFFL);
  }

  private static int unpackBlockOrdinal(long key) {
    return (int) (key >>> Integer.SIZE);
  }

  private static int unpackVectorIndex(long key) {
    return (int) key;
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

  /**
   * The RFC-109 RLI finalist arbiter. Resolves each finalist's current location from the
   * record-level index (one batched {@code readRecordIndexLocationsWithKeys} over the distinct
   * finalist keys) and tags it with a {@link VectorIndexArbiter.Decision} plus the resolved
   * location.
   *
   * <p>Unlike {@link #attachRecordLocations}, this does <em>not</em> drop candidates: it tags all
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
  public static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(HoodieTableMetadata metadataTable,
                                                                  HoodieData<ScoredVectorPostingMatch> finalists) {
    return arbitrateFinalists(metadataTable, finalists, false);
  }

  public static HoodieData<ScoredVectorPostingMatch> arbitrateFinalists(
      HoodieTableMetadata metadataTable,
      HoodieData<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    if (!partitionedRecordIndex) {
      return arbitrateFinalistsForPartition(metadataTable, finalists, Option.empty());
    }
    List<String> partitions = finalists.map(ScoredVectorPostingMatch::getPartitionPath)
        .distinct()
        .collectAsList();
    HoodieData<ScoredVectorPostingMatch> arbitrated = null;
    for (String partition : partitions) {
      HoodieData<ScoredVectorPostingMatch> partitionFinalists = finalists
          .filter(candidate -> Objects.equals(partition, candidate.getPartitionPath()));
      HoodieData<ScoredVectorPostingMatch> partitionResult = arbitrateFinalistsForPartition(
          metadataTable, partitionFinalists, Option.ofNullable(partition));
      arbitrated = arbitrated == null ? partitionResult : arbitrated.union(partitionResult);
    }
    return arbitrated == null ? HoodieListData.eager(Collections.emptyList()) : arbitrated;
  }

  private static HoodieData<ScoredVectorPostingMatch> arbitrateFinalistsForPartition(
      HoodieTableMetadata metadataTable,
      HoodieData<ScoredVectorPostingMatch> finalists,
      Option<String> dataTablePartition) {
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
      metadataTable.readRecordIndexLocationsWithKeys(
              HoodieListData.eager(distinctKeys), dataTablePartition)
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
  public static ArbitrationResult arbitrateMaterializedFinalists(
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
    return new ArbitrationResult(serve, stale, deleted);
  }

  /**
   * Driver-side finalist arbiter: batched RLI lookup over the distinct finalist keys, then
   * {@link #arbitrateMaterializedFinalists(List, Map)}. Used by the exact-rerank plan path, which
   * already materializes finalists to the driver, so no distributed shuffle is incurred.
   */
  public static ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable,
      List<ScoredVectorPostingMatch> finalists) {
    return arbitrateMaterializedFinalists(metadataTable, finalists, false);
  }

  public static ArbitrationResult arbitrateMaterializedFinalists(
      HoodieTableMetadata metadataTable,
      List<ScoredVectorPostingMatch> finalists,
      boolean partitionedRecordIndex) {
    if (finalists.isEmpty()) {
      return new ArbitrationResult(Collections.emptyList(), Collections.emptyList(), 0L);
    }
    if (partitionedRecordIndex) {
      List<ScoredVectorPostingMatch> serve = new ArrayList<>();
      List<ScoredVectorPostingMatch> stale = new ArrayList<>();
      long deleted = 0L;
      Map<String, List<ScoredVectorPostingMatch>> byPartition = finalists.stream()
          .collect(Collectors.groupingBy(ScoredVectorPostingMatch::getPartitionPath));
      for (Map.Entry<String, List<ScoredVectorPostingMatch>> entry : byPartition.entrySet()) {
        List<ScoredVectorPostingMatch> partitionFinalists = entry.getValue();
        Set<String> partitionKeys = partitionFinalists.stream()
            .map(ScoredVectorPostingMatch::getRecordKey)
            .collect(Collectors.toSet());
        Map<String, HoodieRecordGlobalLocation> partitionLocations = new HashMap<>();
        metadataTable.readRecordIndexLocationsWithKeys(
                HoodieListData.eager(new ArrayList<>(partitionKeys)), Option.of(entry.getKey()))
            .collectAsList()
            .forEach(pair -> partitionLocations.put(pair.getKey(), pair.getValue()));
        ArbitrationResult result = arbitrateMaterializedFinalists(partitionFinalists, partitionLocations);
        serve.addAll(result.serve());
        stale.addAll(result.stale());
        deleted += result.deletedCount();
      }
      return new ArbitrationResult(serve, stale, deleted);
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

  private static String postingIdentity(int[] keyComponents, String recordKey) {
    return keyComponents[0] + "\u0000" + keyComponents[1] + "\u0000" + recordKey;
  }

  private static void removeSuppressedDelta(
      Map<String, ScoredVectorPostingMatch> deltaByRecordKey,
      String recordKey,
      int[] keyComponents) {
    ScoredVectorPostingMatch delta = deltaByRecordKey.get(recordKey);
    if (delta != null
        && delta.getClusterId() == keyComponents[0]
        && delta.getShardId() == keyComponents[1]) {
      deltaByRecordKey.remove(recordKey);
    }
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

  private static final class CodeRows {
    private final byte[] sign;
    private final byte[] extended;

    private CodeRows(byte[] sign, byte[] extended) {
      this.sign = sign;
      this.extended = extended;
    }
  }

}
