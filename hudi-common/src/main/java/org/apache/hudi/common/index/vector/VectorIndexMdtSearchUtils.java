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

import org.apache.hudi.avro.model.HoodieVectorIndexInfo;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.RawKey;
import org.apache.hudi.metadata.VectorClusterRawKey;
import org.apache.hudi.metadata.VectorPostingPrefixRawKey;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared helper for MDT-native vector posting lookup and approximate candidate reduction.
 *
 * <p>This provides the missing second-stage building block after coarse IVF cluster pruning:
 * exact HFile lookups of cluster metadata, prefix scans over {@code P|gen|cluster|shard|},
 * approximate RaBitQ scoring, and optional map-back through the record index.
 */
public final class VectorIndexMdtSearchUtils {

  private static final int TOP_K_REDUCER_KEY = 0;

  private VectorIndexMdtSearchUtils() {
  }

  public static Map<Integer, Integer> readClusterShardCounts(HoodieTableMetadata metadataTable,
                                                             String indexPartition,
                                                             int generationId,
                                                             Collection<Integer> clusterIds) {
    if (clusterIds == null || clusterIds.isEmpty()) {
      return Collections.emptyMap();
    }

    List<RawKey> clusterKeys = new ArrayList<>(clusterIds.size());
    for (Integer clusterId : clusterIds) {
      clusterKeys.add(new VectorClusterRawKey(generationId, clusterId));
    }

    List<HoodieRecord<HoodieMetadataPayload>> records = metadataTable
        .getRecordsByKeyPrefixes(HoodieListData.eager(clusterKeys), indexPartition, true)
        .collectAsList();

    Map<Integer, Integer> shardCounts = new HashMap<>();
    for (HoodieRecord<HoodieMetadataPayload> record : records) {
      Option<HoodieVectorIndexInfo> infoOpt = getVectorInfo(record);
      if (!infoOpt.isPresent()) {
        continue;
      }

      HoodieVectorIndexInfo info = infoOpt.get();
      if (!HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_CLUSTER.equals(info.getEntryType())) {
        continue;
      }
      shardCounts.put(info.getClusterId(), Math.max(1, info.getShardCount()));
    }
    return shardCounts;
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

  public static HoodieData<PostingMatch> readPostingMatches(HoodieTableMetadata metadataTable,
                                                            String indexPartition,
                                                            int generationId,
                                                            Map<Integer, Integer> clusterShardCounts,
                                                            boolean shouldLoadInMemory) {
    List<VectorPostingPrefixRawKey> postingPrefixes = buildPostingPrefixes(generationId, clusterShardCounts);
    if (postingPrefixes.isEmpty()) {
      return HoodieListData.eager(Collections.emptyList());
    }

    List<RawKey> rawKeys = new ArrayList<>(postingPrefixes);
    return metadataTable.getRecordsByKeyPrefixes(HoodieListData.eager(rawKeys), indexPartition, shouldLoadInMemory)
        .flatMap(record -> {
          Option<HoodieVectorIndexInfo> infoOpt = getVectorInfo(record);
          if (!infoOpt.isPresent()) {
            return Collections.<PostingMatch>emptyIterator();
          }

          HoodieVectorIndexInfo info = infoOpt.get();
          if (!HoodieMetadataPayload.VECTOR_INDEX_ENTRY_TYPE_POSTING.equals(info.getEntryType())
              || record.getData().isDeleted()
              || info.getBinaryCode() == null) {
            return Collections.<PostingMatch>emptyIterator();
          }

          String recordKey = HoodieTableMetadataUtil.getVectorIndexPostingRecordKey(record.getRecordKey());
          if (recordKey == null) {
            return Collections.<PostingMatch>emptyIterator();
          }

          ByteBuffer duplicated = info.getBinaryCode().duplicate();
          byte[] binaryCode = new byte[duplicated.remaining()];
          duplicated.get(binaryCode);
          PostingMatch match = new PostingMatch(
              recordKey,
              info.getClusterId(),
              info.getShardId(),
              info.getFileGroupId(),
              info.getPartitionPath(),
              binaryCode,
              info.getScalar());
          return Collections.singletonList(match).iterator();
        });
  }

  public static HoodieData<ScoredPostingMatch> scorePostingMatches(HoodieData<PostingMatch> postingMatches,
                                                                   float[] queryVector,
                                                                   int dimension,
                                                                   long randomSeed,
                                                                   boolean assumeNormalized) {
    return postingMatches.mapPartitions(iterator -> {
      if (!iterator.hasNext()) {
        return Collections.<ScoredPostingMatch>emptyIterator();
      }

      RaBitQEncoder encoder = new RaBitQEncoder(dimension, randomSeed, assumeNormalized);
      VectorQuantizer.QueryState queryState = encoder.encodeQuery(queryVector);
      List<ScoredPostingMatch> scored = new ArrayList<>();
      while (iterator.hasNext()) {
        PostingMatch match = iterator.next();
        float effectiveScalar = match.getScalar() != null ? match.getScalar() : 1.0f;
        float approxDistance = encoder.estimateDistance(
            queryState,
            new VectorQuantizer.QuantizedVector(match.getBinaryCode(), effectiveScalar));
        scored.add(new ScoredPostingMatch(match, approxDistance, null));
      }
      return scored.iterator();
    }, true);
  }

  public static HoodieData<ScoredPostingMatch> attachRecordLocations(HoodieTableMetadata metadataTable,
                                                                     HoodieData<ScoredPostingMatch> scoredPostingMatches) {
    HoodiePairData<String, ScoredPostingMatch> scoredByRecordKey =
        scoredPostingMatches.mapToPair(candidate -> Pair.of(candidate.getRecordKey(), candidate));
    HoodiePairData<String, HoodieRecordGlobalLocation> locations =
        metadataTable.readRecordIndexLocationsWithKeys(scoredByRecordKey.keys().distinct());

    return scoredByRecordKey.leftOuterJoin(locations)
        .flatMapValues(joined -> {
          if (!joined.getRight().isPresent()) {
            return Collections.<ScoredPostingMatch>emptyIterator();
          }
          return Collections.singletonList(joined.getLeft().withLocation(joined.getRight().get())).iterator();
        })
        .values();
  }

  public static HoodieData<ScoredPostingMatch> selectTopK(HoodieData<ScoredPostingMatch> candidates, int topK) {
    if (topK <= 0) {
      return HoodieListData.eager(Collections.emptyList());
    }

    HoodiePairData<Integer, List<ScoredPostingMatch>> partialTopK = candidates
        .mapPartitions(iterator -> {
          List<ScoredPostingMatch> localTopK = new ArrayList<>();
          while (iterator.hasNext()) {
            localTopK.add(iterator.next());
          }
          trimTopK(localTopK, topK);
          if (localTopK.isEmpty()) {
            return Collections.<Pair<Integer, List<ScoredPostingMatch>>>emptyIterator();
          }
          return Collections.singletonList(Pair.of(TOP_K_REDUCER_KEY, localTopK)).iterator();
        }, true)
        .mapToPair(pair -> Pair.of(pair.getLeft(), pair.getRight()));

    return partialTopK
        .reduceByKey((left, right) -> mergeTopK(left, right, topK), 1)
        .values()
        .flatMap(List::iterator);
  }

  public static List<ScoredPostingMatch> collectTopKWithLocations(HoodieTableMetadata metadataTable,
                                                                  String indexPartition,
                                                                  int generationId,
                                                                  Map<Integer, Integer> clusterShardCounts,
                                                                  float[] queryVector,
                                                                  int dimension,
                                                                  long randomSeed,
                                                                  boolean assumeNormalized,
                                                                  int topK) {
    HoodieData<ScoredPostingMatch> topKData = selectTopK(
        attachRecordLocations(
            metadataTable,
            scorePostingMatches(
                readPostingMatches(metadataTable, indexPartition, generationId, clusterShardCounts, false),
                queryVector,
                dimension,
                randomSeed,
                assumeNormalized)),
        topK);

    try {
      return topKData.collectAsList();
    } finally {
      topKData.unpersistWithDependencies();
    }
  }

  private static Option<HoodieVectorIndexInfo> getVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    return record.getData().getVectorIndexMetadata();
  }

  static List<ScoredPostingMatch> mergeTopK(List<ScoredPostingMatch> left,
                                            List<ScoredPostingMatch> right,
                                            int topK) {
    List<ScoredPostingMatch> merged = new ArrayList<>(left.size() + right.size());
    merged.addAll(left);
    merged.addAll(right);
    trimTopK(merged, topK);
    return merged;
  }

  private static void trimTopK(List<ScoredPostingMatch> candidates, int topK) {
    candidates.sort(Comparator
        .comparingDouble(ScoredPostingMatch::getApproxDistance)
        .thenComparing(ScoredPostingMatch::getRecordKey));
    if (candidates.size() > topK) {
      candidates.subList(topK, candidates.size()).clear();
    }
  }

  public static class PostingMatch implements Serializable {
    private final String recordKey;
    private final int clusterId;
    private final int shardId;
    private final String fileGroupId;
    private final String partitionPath;
    private final byte[] binaryCode;
    private final Float scalar;

    public PostingMatch(String recordKey,
                        int clusterId,
                        int shardId,
                        String fileGroupId,
                        String partitionPath,
                        byte[] binaryCode,
                        Float scalar) {
      this.recordKey = recordKey;
      this.clusterId = clusterId;
      this.shardId = shardId;
      this.fileGroupId = fileGroupId;
      this.partitionPath = partitionPath;
      this.binaryCode = binaryCode;
      this.scalar = scalar;
    }

    public String getRecordKey() {
      return recordKey;
    }

    public int getClusterId() {
      return clusterId;
    }

    public int getShardId() {
      return shardId;
    }

    public String getFileGroupId() {
      return fileGroupId;
    }

    public String getPartitionPath() {
      return partitionPath;
    }

    public byte[] getBinaryCode() {
      return binaryCode;
    }

    public Float getScalar() {
      return scalar;
    }
  }

  public static final class ScoredPostingMatch extends PostingMatch {
    private final float approxDistance;
    private final HoodieRecordGlobalLocation location;

    public ScoredPostingMatch(PostingMatch match, float approxDistance, HoodieRecordGlobalLocation location) {
      super(
          match.getRecordKey(),
          match.getClusterId(),
          match.getShardId(),
          match.getFileGroupId(),
          match.getPartitionPath(),
          match.getBinaryCode(),
          match.getScalar());
      this.approxDistance = approxDistance;
      this.location = location;
    }

    public float getApproxDistance() {
      return approxDistance;
    }

    public HoodieRecordGlobalLocation getLocation() {
      return location;
    }

    public ScoredPostingMatch withLocation(HoodieRecordGlobalLocation newLocation) {
      return new ScoredPostingMatch(this, approxDistance, newLocation);
    }
  }
}
