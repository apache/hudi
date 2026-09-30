/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
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
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.HoodieRecord;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Reads and decodes vector posting records from the metadata table. */
final class VectorPostingReader {

  // Preserve the established log category while moving implementation code mechanically.
  private static final Logger LOG = LoggerFactory.getLogger(VectorIndexMdtSearchUtils.class);

  private VectorPostingReader() {
  }

  static Map<Integer, Integer> readClusterShardCounts(
      HoodieTableMetadata metadataTable,
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

  static Map<Integer, Set<String>> readClusterFileGroups(
      HoodieTableMetadata metadataTable,
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

  static List<VectorPostingPrefixRawKey> buildPostingPrefixes(
      int generationId,
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

  static HoodieData<VectorPostingMatch> readPostingMatches(
      HoodieTableMetadata metadataTable,
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
            PostingBlockView view = new PostingBlockView(VectorIndexMdtSearchUtils.asPostingBlock(info));
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

  static Map<Integer, Set<String>> collectClusterToFileGroups(
      HoodieTableMetadata metadataTable,
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
  static HoodiePairData<String, Integer> readClusterAssignments(
      HoodieTableMetadata metadataTable,
      String indexPartition,
      int generationId,
      Map<Integer, Integer> clusterShardCounts,
      boolean shouldLoadInMemory) {
    HoodieData<VectorPostingMatch> postings = readPostingMatches(
        metadataTable, indexPartition, generationId, clusterShardCounts, shouldLoadInMemory);
    return postings.mapToPair(p -> Pair.of(p.getRecordKey(), p.getClusterId()));
  }

  private static Option<Object> getVectorInfo(HoodieRecord<HoodieMetadataPayload> record) {
    return record.getData().getVectorIndexMetadata();
  }

}
