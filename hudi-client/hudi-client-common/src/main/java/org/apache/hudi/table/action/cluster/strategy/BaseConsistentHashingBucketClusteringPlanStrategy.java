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

package org.apache.hudi.table.action.cluster.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieConsistentBucketIndex;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Clustering plan strategy specifically for consistent bucket index.
 */
public abstract class BaseConsistentHashingBucketClusteringPlanStrategy<T extends HoodieRecordPayload, I, K, O>
    extends PartitionAwareClusteringPlanStrategy<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseConsistentHashingBucketClusteringPlanStrategy.class);

  public static final String METADATA_PARTITION_KEY = "clustering.group.partition";
  public static final String METADATA_CHILD_NODE_KEY = "clustering.group.child.node";
  public static final String METADATA_SEQUENCE_NUMBER_KEY = "clustering.group.sequence.no";

  public BaseConsistentHashingBucketClusteringPlanStrategy(
      HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    ValidationUtils.checkArgument(
        getHoodieTable().getIndex() instanceof HoodieConsistentBucketIndex,
        this.getClass().getName() + " is only applicable to table with consistent hash index.");
  }

  /**
   * TODO maybe add force config to schedule the clustering. It could allow clustering on partitions that are not doing write operation.
   * Block clustering if there is any ongoing concurrent writers
   *
   * @return true if the schedule can proceed
   */
  @Override
  public boolean checkPrecondition() {
    HoodieTimeline timeline = getHoodieTable().getActiveTimeline().getDeltaCommitTimeline().filterInflightsAndRequested();
    if (!timeline.empty()) {
      LOG.warn("When using consistent bucket, clustering cannot be scheduled async if there are concurrent writers. "
          + "Writer instant: {}.", timeline.getInstants());
      return false;
    }
    return true;
  }

  /**
   * Generate cluster group based on split, merge and sort rules
   */
  @Override
  protected Pair<Stream<HoodieClusteringGroup>, Boolean> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    Option<HoodieConsistentHashingMetadata> metadata = ConsistentBucketIndexUtils.loadMetadata(getHoodieTable(), partitionPath);
    ValidationUtils.checkArgument(metadata.isPresent(), "Metadata is empty for partition: " + partitionPath);
    ConsistentBucketIdentifier identifier = new ConsistentBucketIdentifier(metadata.get());

    // Apply split rule
    int splitSlot = getWriteConfig().getBucketIndexMaxNumBuckets() - identifier.getNumBuckets();
    Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> splitResult =
        buildSplitClusteringGroups(identifier, fileSlices, splitSlot);
    List<HoodieClusteringGroup> ret = new ArrayList<>(splitResult.getLeft());

    List<FileSlice> remainedSlices = splitResult.getRight();
    if (isBucketClusteringMergeEnabled()) {
      // Apply merge rule
      int mergeSlot = identifier.getNumBuckets() - getWriteConfig().getBucketIndexMinNumBuckets() + splitResult.getMiddle();
      Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> mergeResult =
          buildMergeClusteringGroup(identifier, remainedSlices, mergeSlot);
      ret.addAll(mergeResult.getLeft());
      remainedSlices = mergeResult.getRight();
    }
    if (isBucketClusteringSortEnabled()) {
      // Apply sort only to the remaining file groups
      ret.addAll(remainedSlices.stream().map(fs -> {
        ConsistentHashingNode oldNode = identifier.getBucketByFileId(fs.getFileId());
        ConsistentHashingNode newNode = new ConsistentHashingNode(oldNode.getValue(), FSUtils.createNewFileIdPfx(), ConsistentHashingNode.NodeTag.REPLACE);
        return HoodieClusteringGroup.newBuilder()
            .setSlices(getFileSliceInfo(Collections.singletonList(fs)))
            .setNumOutputFileGroups(1)
            .setMetrics(buildMetrics(Collections.singletonList(fs)))
            .setExtraMetadata(constructExtraMetadata(fs.getPartitionPath(), Collections.singletonList(newNode), identifier.getMetadata().getSeqNo()))
            .build();
      }).collect(Collectors.toList()));
    }
    return Pair.of(ret.stream(), true);
  }

  /**
   * Whether enable buckets merged when using consistent hashing bucket index.
   *
   * @return true if bucket merge is enabled, false otherwise.
   */
  protected boolean isBucketClusteringMergeEnabled() {
    return true;
  }

  /**
   * Whether generate regular sort clustering plans for buckets that are not involved in merge or split.
   *
   * @return true if generate regular sort clustering plans for buckets that are not involved in merge or split, false otherwise.
   */
  protected boolean isBucketClusteringSortEnabled() {
    return true;
  }

  /**
   * Generate candidate clustering file slices of the given partition.
   * If there is inflight / requested clustering working on the partition, then return empty list
   * to ensure serialized update to the hashing metadata.
   *
   * @return candidate file slices to be clustered (i.e., sort, bucket split or merge)
   */
  @Override
  protected Stream<FileSlice> getFileSlicesEligibleForClustering(String partition) {
    TableFileSystemView fileSystemView = getHoodieTable().getFileSystemView();
    boolean isPartitionInClustering = fileSystemView.getFileGroupsInPendingClustering().anyMatch(p -> p.getLeft().getPartitionPath().equals(partition));
    if (isPartitionInClustering) {
      LOG.info("Partition {} is already in clustering, skip.", partition);
      return Stream.empty();
    }

    return super.getFileSlicesEligibleForClustering(partition);
  }

  @Override
  protected Map<String, String> getStrategyParams() {
    Map<String, String> params = new HashMap<>();
    if (!StringUtils.isNullOrEmpty(getWriteConfig().getClusteringSortColumns())) {
      params.put(HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS.key(), getWriteConfig().getClusteringSortColumns());
    }
    return params;
  }

  /**
   * Generate clustering groups according to split rules.
   * Currently, we always split bucket into two sub-buckets.
   *
   * @param identifier bucket identifier
   * @param fileSlices file slice candidate to be built as split clustering groups
   * @param splitSlot  number of new bucket allowed to produce, in order to constrain the upper bound of the total number of bucket
   * @return list of clustering group, number of new buckets generated, remaining file slice (that does not split)
   */
  protected Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> buildSplitClusteringGroups(
      ConsistentBucketIdentifier identifier, List<FileSlice> fileSlices, int splitSlot) {
    List<HoodieClusteringGroup> retGroup = new ArrayList<>();
    List<FileSlice> fsUntouched = new ArrayList<>();
    long splitSize = getSplitSize();
    int remainingSplitSlot = splitSlot;
    for (FileSlice fs : fileSlices) {
      boolean needSplit = fs.getTotalFileSize() > splitSize;
      if (!needSplit || remainingSplitSlot == 0) {
        fsUntouched.add(fs);
        continue;
      }

      Option<List<ConsistentHashingNode>> nodes = identifier.splitBucket(fs.getFileId());

      // Bucket cannot be split
      if (!nodes.isPresent()) {
        fsUntouched.add(fs);
        continue;
      }

      remainingSplitSlot--;
      List<FileSlice> fsList = Collections.singletonList(fs);
      retGroup.add(HoodieClusteringGroup.newBuilder()
          .setSlices(getFileSliceInfo(fsList))
          .setNumOutputFileGroups(2)
          .setMetrics(buildMetrics(fsList))
          .setExtraMetadata(constructExtraMetadata(fs.getPartitionPath(), nodes.get(), identifier.getMetadata().getSeqNo()))
          .build());
    }
    return Triple.of(retGroup, splitSlot - remainingSplitSlot, fsUntouched);
  }

  /**
   * Generate clustering group according to merge rules
   *
   * @param identifier bucket identifier
   * @param fileSlices file slice candidates to be built as merge clustering groups
   * @param mergeSlot  number of bucket allowed to be merged, in order to guarantee the lower bound of the total number of bucket
   * @return list of clustering group, number of buckets merged (removed), remaining file slice (that does not be merged)
   */
  protected Triple<List<HoodieClusteringGroup>, Integer, List<FileSlice>> buildMergeClusteringGroup(
      ConsistentBucketIdentifier identifier, List<FileSlice> fileSlices, int mergeSlot) {
    if (fileSlices.size() <= 1) {
      return Triple.of(Collections.emptyList(), 0, fileSlices);
    }

    long mergeSize = getMergeSize();
    int remainingMergeSlot = mergeSlot;
    List<HoodieClusteringGroup> groups = new ArrayList<>();
    boolean[] added = new boolean[fileSlices.size()];

    fileSlices.sort(Comparator.comparingInt(a -> identifier.getBucketByFileId(a.getFileId()).getValue()));
    // In each round, we check if the ith file slice can be merged with its predecessors and successors
    for (int i = 0; i < fileSlices.size(); ++i) {
      if (added[i] || fileSlices.get(i).getTotalFileSize() > mergeSize) {
        continue;
      }

      // 0: startIdx, 1: endIdx
      int[] rangeIdx = {i, i};
      long totalSize = fileSlices.get(i).getTotalFileSize();
      // Do backward check first (k == 0), and then forward check (k == 1)
      for (int k = 0; k < 2; ++k) {
        boolean forward = k == 1;
        do {
          int nextIdx = forward ? (rangeIdx[k] + 1 < fileSlices.size() ? rangeIdx[k] + 1 : 0) : (rangeIdx[k] >= 1 ? rangeIdx[k] - 1 : fileSlices.size() - 1);
          ConsistentHashingNode bucketOfNextFile = identifier.getBucketByFileId(fileSlices.get(nextIdx).getFileId());
          ConsistentHashingNode nextBucket = forward ? identifier.getLatterBucket(fileSlices.get(rangeIdx[k]).getFileId()) : identifier.getFormerBucket(fileSlices.get(rangeIdx[k]).getFileId());
          boolean isNeighbour = bucketOfNextFile == nextBucket;
          /**
           * Merge condition:
           * 1. there is still slot to merge bucket
           * 2. the previous file slices is not merged
           * 3. the previous file slice and current file slice are neighbour in the hash ring
           * 4. Both the total file size up to now and the previous file slice size are smaller than merge size threshold
           */
          if (remainingMergeSlot == 0 || added[nextIdx] || !isNeighbour || totalSize > mergeSize || fileSlices.get(nextIdx).getTotalFileSize() > mergeSize
              || nextIdx == rangeIdx[1 - k] // if start equal to end after update range
          ) {
            break;
          }

          // Mark preIdx as merge candidate
          totalSize += fileSlices.get(nextIdx).getTotalFileSize();
          rangeIdx[k] = nextIdx;
          remainingMergeSlot--;
        } while (rangeIdx[k] != i);
      }

      int startIdx = rangeIdx[0];
      int endIdx = rangeIdx[1];
      if (endIdx == i && startIdx == i) {
        continue;
      }

      // Construct merge group if there is at least two file slices
      List<FileSlice> fs = new ArrayList<>();
      while (true) {
        added[startIdx] = true;
        fs.add(fileSlices.get(startIdx));
        if (startIdx == endIdx) {
          break;
        }
        startIdx = startIdx + 1 < fileSlices.size() ? startIdx + 1 : 0;
      }

      groups.add(HoodieClusteringGroup.newBuilder()
          .setSlices(getFileSliceInfo(fs))
          .setNumOutputFileGroups(1)
          .setMetrics(buildMetrics(fs))
          .setExtraMetadata(
              constructExtraMetadata(
                  fs.get(0).getPartitionPath(),
                  identifier.mergeBucket(fs.stream().map(FileSlice::getFileId).collect(Collectors.toList())),
                  identifier.getMetadata().getSeqNo()))
          .build());
    }

    // Collect file slices that are not involved in merge
    List<FileSlice> fsUntouched = IntStream.range(0, fileSlices.size()).filter(i -> !added[i])
        .mapToObj(fileSlices::get).collect(Collectors.toList());

    return Triple.of(groups, mergeSlot - remainingMergeSlot, fsUntouched);
  }

  /**
   * Construct extra metadata for clustering group
   */
  private Map<String, String> constructExtraMetadata(String partition, List<ConsistentHashingNode> nodes, int seqNo) {
    Map<String, String> extraMetadata = new HashMap<>();
    try {
      extraMetadata.put(METADATA_PARTITION_KEY, partition);
      extraMetadata.put(METADATA_CHILD_NODE_KEY, ConsistentHashingNode.toJsonString(nodes));
      extraMetadata.put(METADATA_SEQUENCE_NUMBER_KEY, Integer.toString(seqNo));
    } catch (IOException e) {
      LOG.error("Failed to construct extra metadata, partition: {}, nodes:{}", partition, nodes);
      throw new HoodieClusteringException("Failed to construct extra metadata, partition: " + partition + ", nodes:" + nodes);
    }
    return extraMetadata;
  }

  private long getSplitSize() {
    HoodieFileFormat format = getHoodieTable().getBaseFileFormat();
    return (long) (getWriteConfig().getMaxFileSize(format) * getWriteConfig().getBucketSplitThreshold());
  }

  private long getMergeSize() {
    HoodieFileFormat format = getHoodieTable().getBaseFileFormat();
    return (long) (getWriteConfig().getMaxFileSize(format) * getWriteConfig().getBucketMergeThreshold());
  }
}
