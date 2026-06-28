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

package org.apache.hudi.source.stats;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.metadata.MetadataPartitionType;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An index support implementation that leverages partitioned Record Level Index to prune file slices.
 *
 * <p>Unlike the global RLI, record keys in partitioned RLI are only unique within a data-table partition.
 * This implementation therefore scopes metadata-table lookups by candidate data partition. To avoid reading
 * every RLI shard for each candidate partition, it first groups filter-derived record keys by the shard they
 * hash to within each partition, then performs one metadata lookup per {@code (partition, shard)} group.
 *
 * <p>The grouped lookups use bounded local parallelism through {@link HoodieFlinkEngineContext#map(List,
 * org.apache.hudi.common.function.SerializableFunction, int)}. This is intentionally not implemented with
 * {@code HoodieFlinkEngineContext#parallelize}, because the Flink context backs {@code parallelize} with
 * in-memory {@code HoodieListData}, not distributed Flink tasks.
 */
@Slf4j
public class RecordLevelIndex extends BaseRecordLevelIndex {
  private static final long serialVersionUID = 1L;

  /**
   * Upper bound on the number of candidate data-table partitions eligible for a partitioned RLI lookup.
   *
   * <p>Unlike the global RLI (a single lookup over all keys), the partitioned variant performs one metadata-table
   * read per candidate partition. When a query does not filter on the partition column the candidate set can span
   * many partitions, and fanning out a lookup to each one can add latency that outweighs the skipping benefit.
   * Once the candidate partition count exceeds this threshold, pruning is skipped.
   */
  private static final int MAX_PARTITIONS = 3;

  RecordLevelIndex(
      String basePath,
      Configuration conf,
      HoodieTableMetaClient metaClient,
      List<String> hoodieKeysFromFilter) {
    super(basePath, conf, metaClient, hoodieKeysFromFilter);
  }

  /**
   * Finds candidate data file groups by querying partitioned RLI for the record keys extracted from filters.
   *
   * <p>The method fails open through {@link BaseRecordLevelIndex#computeCandidateFileSlices(List)} if metadata
   * lookup fails. Returning {@link Option#empty()} is reserved for cases where pruning should be skipped without
   * treating it as an error, such as exceeding the configured candidate partition threshold.
   */
  @Override
  protected Option<Set<HoodieFileGroupId>> lookupCandidateFileGroupIds(List<FileSlice> fileSlices) {
    Set<String> partitions = fileSlices.stream()
        .map(FileSlice::getPartitionPath)
        .collect(Collectors.toSet());
    if (partitions.isEmpty()) {
      return Option.empty();
    }
    if (partitions.size() > MAX_PARTITIONS) {
      log.info("The number of candidate partitions {} exceeds the partitioned record level index lookup threshold {}. Skipping pruning.",
          partitions.size(), MAX_PARTITIONS);
      return Option.empty();
    }

    Set<HoodieFileGroupId> fileGroupIds = new HashSet<>();
    HoodieTableMetadata metadataTable = getMetadataTable();
    Map<String, List<FileSlice>> fileGroupsByDataPartition =
        metadataTable.getBucketizedFileGroupsForPartitionedRLI(MetadataPartitionType.RECORD_INDEX);
    // Build one lookup group per RLI shard within a data partition so each metadata-table lookup
    // only needs to resolve keys targeting that shard.
    List<PartitionShardKeys> lookupGroups = groupKeysByPartitionAndShard(partitions, fileGroupsByDataPartition)
        .entrySet().stream()
        .flatMap(partitionEntry -> partitionEntry.getValue().values().stream()
            .map(keysInSingleShard -> new PartitionShardKeys(partitionEntry.getKey(), keysInSingleShard)))
        .collect(Collectors.toList());
    // HoodieFlinkEngineContext#parallelize returns in-memory HoodieListData; use map(...) for bounded
    // local parallelism over the already bucketed (partition, shard) lookup groups.
    HoodieFlinkEngineContext.DEFAULT.map(
        lookupGroups,
        lookupGroup -> lookupFileGroupIds(metadataTable, lookupGroup),
        getLookupParallelism(lookupGroups.size()))
        .forEach(fileGroupIds::addAll);
    return Option.of(fileGroupIds);
  }

  /**
   * Groups filter-derived record keys by data-table partition and RLI shard.
   *
   * <p>The lookup API is partition-scoped. This helper additionally splits keys by RLI shard from the
   * bucketized partitioned-RLI file groups returned by the metadata table.
   */
  private Map<String, Map<Integer, List<String>>> groupKeysByPartitionAndShard(
      Set<String> partitions,
      Map<String, List<FileSlice>> fileGroupsByDataPartition) {
    return partitions.stream().collect(Collectors.toMap(
        partition -> partition,
        partition -> {
          List<FileSlice> fileGroups = fileGroupsByDataPartition.get(partition);
          ValidationUtils.checkState(fileGroups != null && !fileGroups.isEmpty(),
              "No record index file groups found for data partition: " + partition);
          return hoodieKeysFromFilter.stream().collect(Collectors.groupingBy(
              key -> HoodieTableMetadataUtil.mapRecordKeyToFileGroupIndex(key, fileGroups.size()),
              Collectors.toCollection(ArrayList::new)));
        }));
  }

  /**
   * Looks up one pre-bucketed {@code (partition, shard)} group and converts matched RLI locations to file group IDs.
   */
  private static Set<HoodieFileGroupId> lookupFileGroupIds(
      HoodieTableMetadata metadataTable,
      PartitionShardKeys lookupGroup) {
    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
        metadataTable.readRecordIndexLocationsWithKeys(
            HoodieListData.eager(lookupGroup.keysInSingleShard), Option.of(lookupGroup.partition));
    return getFileGroupIds(recordIndexData);
  }

  /**
   * Caps local lookup parallelism by available processors and the number of non-empty lookup groups.
   */
  private static int getLookupParallelism(int lookupGroupCount) {
    return Math.max(1, Math.min(lookupGroupCount, Runtime.getRuntime().availableProcessors()));
  }

  /**
   * Keys that hash to one RLI shard within one data-table partition.
   */
  private static class PartitionShardKeys {
    private final String partition;
    private final List<String> keysInSingleShard;

    private PartitionShardKeys(String partition, List<String> keysInSingleShard) {
      this.partition = partition;
      this.keysInSingleShard = keysInSingleShard;
    }
  }
}
