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

import org.apache.hudi.avro.model.HoodieVectorIndexPostingBlock;
import org.apache.hudi.avro.model.HoodieVectorIndexPostingDelta;
import org.apache.hudi.avro.model.HoodieVectorIndexTombstone;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.index.vector.MetricQueryState;
import org.apache.hudi.common.index.vector.RaBitQEncoder;
import org.apache.hudi.common.index.vector.VectorDistanceMetric;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorIndexMetadataKey;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Concrete snapshot-generation MDT prefix scanner for packed blocks, deltas, and tombstones. */
public final class MdtVectorPostingScanner implements VectorPostingScanner {

  private static final long serialVersionUID = 1L;
  private static final Comparator<VectorCandidate> CANDIDATE_ORDER = Comparator
      .comparingDouble(VectorCandidate::getApproximateDistance)
      .thenComparing(VectorCandidate::getRecordKey);

  private final HoodieTableMetadata metadataTable;
  private final String indexPartition;
  private final float[][] centroids;
  private final Map<Integer, Integer> clusterShardCounts;
  private final int dimension;
  private final int rabitqBits;
  private final long randomSeed;
  private final boolean assumeNormalized;

  public MdtVectorPostingScanner(HoodieTableMetadata metadataTable,
                                 String indexPartition,
                                 float[][] centroids,
                                 Map<Integer, Integer> clusterShardCounts,
                                 int dimension,
                                 int rabitqBits,
                                 long randomSeed,
                                 boolean assumeNormalized) {
    this.metadataTable = Objects.requireNonNull(metadataTable, "metadataTable");
    this.indexPartition = Objects.requireNonNull(indexPartition, "indexPartition");
    this.centroids = copyCentroids(centroids, dimension);
    this.clusterShardCounts = Collections.unmodifiableMap(new HashMap<>(clusterShardCounts));
    this.dimension = dimension;
    this.rabitqBits = rabitqBits;
    this.randomSeed = randomSeed;
    this.assumeNormalized = assumeNormalized;
  }

  @Override
  public VectorPostingScanResult scan(VectorSearchPlan plan,
                                      HoodieEngineContext engineContext,
                                      int packedCandidateLimit) {
    int generationId = plan.getSnapshot().getVectorIndex().getGenerationId();
    float[] query = plan.getRequest().getQueryVector();
    List<Integer> clusterIds = selectClusters(
        query, plan.getRequest().getMetric(), plan.getRequest().getNprobe());
    List<RawKey> prefixes = postingPrefixes(generationId, clusterIds);
    if (prefixes.isEmpty() || packedCandidateLimit <= 0) {
      return new VectorPostingScanResult(
          Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
    }

    RaBitQEncoder encoder = new RaBitQEncoder(dimension, rabitqBits, randomSeed, assumeNormalized);
    MetricQueryState queryState = MetricQueryState.create(
        plan.getRequest().getMetric(), encoder::rotateVector, query, assumeNormalized);
    List<PartitionScanResult> partitions = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(prefixes), indexPartition, false)
        .mapPartitions(records -> Collections.singletonList(scanPartition(
            records, queryState, generationId, packedCandidateLimit)).iterator(), true)
        .collectAsList();

    List<VectorCandidate> packed = new ArrayList<>();
    List<VectorCandidate> deltas = new ArrayList<>();
    List<VectorPostingKey> tombstones = new ArrayList<>();
    for (PartitionScanResult partition : partitions) {
      packed.addAll(partition.packed);
      deltas.addAll(partition.deltas);
      tombstones.addAll(partition.tombstones);
    }
    packed.sort(CANDIDATE_ORDER);
    if (packed.size() > packedCandidateLimit) {
      packed.subList(packedCandidateLimit, packed.size()).clear();
    }
    return new VectorPostingScanResult(packed, deltas, tombstones);
  }

  private PartitionScanResult scanPartition(
      Iterator<HoodieRecord<HoodieMetadataPayload>> records,
      MetricQueryState queryState,
      int generationId,
      int packedCandidateLimit) {
    List<VectorCandidate> packed = new ArrayList<>();
    List<VectorCandidate> deltas = new ArrayList<>();
    List<VectorPostingKey> tombstones = new ArrayList<>();
    while (records.hasNext()) {
      HoodieRecord<HoodieMetadataPayload> record = records.next();
      Option<Object> metadata = record.getData().getVectorIndexMetadata();
      if (!metadata.isPresent()) {
        continue;
      }
      String metadataKey = record.getRecordKey();
      int clusterId = requireCoordinate(
          VectorIndexMetadataKey.postingClusterId(metadataKey), "cluster", metadataKey);
      int shardId = requireCoordinate(
          VectorIndexMetadataKey.postingShard(metadataKey), "shard", metadataKey);
      Object value = metadata.get();
      if (value instanceof HoodieVectorIndexTombstone) {
        String recordKey = VectorIndexMetadataKey.postingRecordKey(metadataKey);
        if (recordKey == null) {
          throw new IllegalArgumentException("Vector tombstone is not keyed as a posting delta");
        }
        tombstones.add(new VectorPostingKey(clusterId, shardId, recordKey));
      } else if (value instanceof HoodieVectorIndexPostingDelta) {
        deltas.add(PostingDeltaScorer.score(
            (HoodieVectorIndexPostingDelta) value, queryState, centroids[clusterId],
            dimension, rabitqBits, generationId, clusterId, shardId));
      } else if (value instanceof HoodieVectorIndexPostingBlock) {
        long blockId = VectorIndexMetadataKey.postingBlockId(metadataKey);
        packed.addAll(PackedPostingBlockScorer.score(
            (HoodieVectorIndexPostingBlock) value, queryState, centroids[clusterId],
            dimension, rabitqBits, packedCandidateLimit,
            generationId, clusterId, shardId, blockId));
      } else {
        throw new IllegalArgumentException(
            "Unexpected vector posting payload: " + value.getClass().getName());
      }
    }
    packed.sort(CANDIDATE_ORDER);
    if (packed.size() > packedCandidateLimit) {
      packed.subList(packedCandidateLimit, packed.size()).clear();
    }
    return new PartitionScanResult(packed, deltas, tombstones);
  }

  private List<Integer> selectClusters(float[] query, VectorDistanceMetric metric, int nprobe) {
    if (query.length != dimension) {
      throw new IllegalArgumentException(
          "Query dimension mismatch: expected=" + dimension + ", actual=" + query.length);
    }
    List<Integer> ids = new ArrayList<>(centroids.length);
    for (int clusterId = 0; clusterId < centroids.length; clusterId++) {
      ids.add(clusterId);
    }
    DefaultExactVectorScorer scorer = new DefaultExactVectorScorer();
    ids.sort(Comparator
        .comparingDouble((Integer id) -> scorer.distance(query, centroids[id], metric))
        .thenComparingInt(Integer::intValue));
    return ids.subList(0, Math.min(Math.max(1, nprobe), ids.size()));
  }

  private List<RawKey> postingPrefixes(int generationId, List<Integer> clusterIds) {
    List<RawKey> prefixes = new ArrayList<>();
    for (Integer clusterId : clusterIds) {
      int shards = Math.max(1, clusterShardCounts.getOrDefault(clusterId, 1));
      for (int shardId = 0; shardId < shards; shardId++) {
        prefixes.add(new VectorPostingPrefixRawKey(generationId, clusterId, shardId));
      }
    }
    return prefixes;
  }

  private static int requireCoordinate(int value, String name, String key) {
    if (value < 0) {
      throw new IllegalArgumentException("Invalid posting " + name + " in metadata key: " + key);
    }
    return value;
  }

  private static float[][] copyCentroids(float[][] values, int dimension) {
    Objects.requireNonNull(values, "centroids");
    if (values.length == 0 || dimension <= 0) {
      throw new IllegalArgumentException("centroids and dimension must be non-empty");
    }
    float[][] copy = new float[values.length][];
    for (int i = 0; i < values.length; i++) {
      if (values[i] == null || values[i].length != dimension) {
        throw new IllegalArgumentException("Centroid dimension mismatch at cluster " + i);
      }
      copy[i] = Arrays.copyOf(values[i], dimension);
    }
    return copy;
  }

  private static final class PartitionScanResult {
    private final List<VectorCandidate> packed;
    private final List<VectorCandidate> deltas;
    private final List<VectorPostingKey> tombstones;

    private PartitionScanResult(List<VectorCandidate> packed,
                                List<VectorCandidate> deltas,
                                List<VectorPostingKey> tombstones) {
      this.packed = packed;
      this.deltas = deltas;
      this.tombstones = tombstones;
    }
  }
}
