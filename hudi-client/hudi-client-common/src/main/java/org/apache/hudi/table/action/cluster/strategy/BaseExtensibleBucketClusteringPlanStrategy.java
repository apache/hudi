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
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ExtensibleBucketResizingOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.MathUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.index.bucket.ExtensibleBucketIdentifier;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieExtensibleBucketIndex;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils.constructExtensibleExtraMetadata;

/**
 * Clustering plan strategy specifically for extensible bucket index.
 */
public abstract class BaseExtensibleBucketClusteringPlanStrategy<T extends HoodieRecordPayload, I, K, O> extends PartitionAwareClusteringPlanStrategy<T, I, K, O> {

  private static final Logger LOG = LoggerFactory.getLogger(BaseExtensibleBucketClusteringPlanStrategy.class);

  public static final String CLUSTERING_PLAN_TYPE_KEY = "clustering.plan.type";
  public static final String BUCKET_RESIZING_PLAN = "bucket.resizing";

  private final ExtensibleBucketResizingPlanStrategy resizingPlanStrategy;

  public BaseExtensibleBucketClusteringPlanStrategy(HoodieTable table, HoodieEngineContext engineContext,
                                                    HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
    ValidationUtils.checkArgument(getHoodieTable().getIndex() instanceof HoodieExtensibleBucketIndex,
        this.getClass().getName() + " is only applicable for extensible bucket index");
    this.resizingPlanStrategy = buildResizingPlanStrategy();
  }

  private ExtensibleBucketResizingPlanStrategy buildResizingPlanStrategy() {
    switch (getWriteConfig().getBucketResizingPlanMode()) {
      case FORCE:
        return new ForceExtensibleBucketResizingPlanStrategy();
      case SIZE_BASED:
        // TODO: support size based bucket resizing
        return new SizeBasedExtensibleBucketResizingPlanStrategy();
      default:
        throw new HoodieNotSupportedException("Unsupported bucket resizing plan mode: " + getWriteConfig().getBucketResizingPlanMode());
    }
  }

  public enum ExtensibleBucketResizingPlanMode {
    FORCE,
    SIZE_BASED
  }

  interface ExtensibleBucketResizingPlanStrategy extends Serializable {
    Pair<ExtensibleBucketResizingOperation/*resizing operation*/, Integer/*merge or split num for each operation*/> generateResizingPlan(HoodieExtensibleBucketMetadata currentMetadata,
                                                                                                                                         String partitionPath, List<FileSlice> fileSlices);
  }

  class ForceExtensibleBucketResizingPlanStrategy implements ExtensibleBucketResizingPlanStrategy {

    @Override
    public Pair<ExtensibleBucketResizingOperation, Integer> generateResizingPlan(HoodieExtensibleBucketMetadata currentMetadata,
                                                                                 String partitionPath, List<FileSlice> fileSlices) {
      int targetNum = getWriteConfig().getBucketResizingTargetNum();
      ValidationUtils.checkArgument(MathUtils.isPowerOf2(targetNum), "Bucket-resizing target number of buckets should be power of 2");
      int currentNum = currentMetadata.getBucketNum();
      return currentNum > targetNum ? Pair.of(ExtensibleBucketResizingOperation.MERGE, currentNum / targetNum) :
          Pair.of(ExtensibleBucketResizingOperation.SPLIT, targetNum / currentNum);
    }
  }

  class SizeBasedExtensibleBucketResizingPlanStrategy implements ExtensibleBucketResizingPlanStrategy {

    @Override
    public Pair<ExtensibleBucketResizingOperation, Integer> generateResizingPlan(HoodieExtensibleBucketMetadata currentMetadata,
                                                                                 String partitionPath, List<FileSlice> fileSlices) {
      throw new HoodieNotSupportedException("Size based extensible bucket resizing is not supported yet");
    }

  }

  /**
   * Block bucket resizing in these scenarios.
   * - inflight writes
   * - inflight compactions
   * - inflight clustering
   *
   * @return
   */
  @Override
  public boolean checkPrecondition() {
    HoodieTimeline timeline = getHoodieTable().getActiveTimeline().getCommitsAndCompactionTimeline().filterInflightsAndRequested();
    if (!timeline.empty()) {
      LOG.warn("Found inflight commits/compactions/clustering :{}. Skipping clustering of extensible bucket resizing for now", timeline.getInstants());
      return false;
    }
    return true;
  }

  /**
   * Generate candidate bucket resizing file slices of the given partition.
   * Filter out the partition's all file slices that are in the inflight clustering plan.
   *
   * @param partition partition path
   * @return file slices eligible for clustering
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

  /**
   * Bucket resizing should be done for all groups in the partition.
   *
   * @return unlimited number of groups
   */
  @Override
  protected long clusteringMaxNumGroups() {
    return Long.MAX_VALUE;
  }

  @Override
  protected Stream<HoodieClusteringGroup> buildClusteringGroupsForPartition(String partitionPath, List<FileSlice> fileSlices) {
    Option<HoodieExtensibleBucketMetadata> metadata = ExtensibleBucketIndexUtils.loadMetadata(getHoodieTable(), partitionPath);
    ValidationUtils.checkState(metadata.isPresent(), "Extensible bucket metadata not found for partition: " + partitionPath);
    int currentBucketNum = metadata.get().getBucketNum();
    ValidationUtils.checkState(MathUtils.isPowerOf2(currentBucketNum),
        "Performs bucket-resizing should make sure that current number of buckets should be power of 2, but got " + currentBucketNum);

    Pair<ExtensibleBucketResizingOperation, Integer> opAndNum = this.resizingPlanStrategy.generateResizingPlan(metadata.get(), partitionPath, fileSlices);
    switch (opAndNum.getKey()) {
      case MERGE:
        // Merge operation, combine `num` buckets into one
        return buildMergeClusteringGroup(metadata.get(), partitionPath, fileSlices, opAndNum.getValue()).stream();
      case SPLIT:
        // Split operation, split one bucket into `num` buckets
        return buildSplitClusteringGroup(metadata.get(), partitionPath, fileSlices, opAndNum.getValue()).stream();
      default:
        throw new HoodieNotSupportedException("Unsupported bucket resizing operation: " + opAndNum.getKey());
    }
  }

  /**
   * Mark it as extensible-bucket-resizing plan.
   *
   * @return
   */
  @Override
  protected Map<String, String> getExtraMetadata() {
    HashMap<String, String> map = new HashMap<>();
    map.put(CLUSTERING_PLAN_TYPE_KEY, BUCKET_RESIZING_PLAN);
    return map;
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
   * Build merge clustering group.
   *
   * @param metadata      current metadata before bucket resizing
   * @param partitionPath partition path
   * @param fileSlices    file slices to be merged
   * @param mergeNum      number of buckets to be merged to one bucket
   * @return merge clustering group
   */
  protected List<HoodieClusteringGroup> buildMergeClusteringGroup(HoodieExtensibleBucketMetadata metadata, String partitionPath, List<FileSlice> fileSlices, int mergeNum) {
    int currentBucketNum = metadata.getBucketNum();
    int newBucketNum = currentBucketNum / mergeNum;
    // for not exist file group for the current version bucket layout, we should also include them in the ClusteringGroup
    ExtensibleBucketIdentifier identifier = new ExtensibleBucketIdentifier(metadata);
    List<HoodieSliceInfo> fileSliceInfos = generateFileSliceInfos(partitionPath, fileSlices, identifier);
    List<HoodieClusteringGroup> groups = Collections.singletonList(HoodieClusteringGroup.newBuilder()
        .setSlices(fileSliceInfos)
        .setNumOutputFileGroups(newBucketNum)
        .setMetrics(buildMetrics(fileSlices))
        .setExtraMetadata(constructExtensibleExtraMetadata(partitionPath, metadata, newBucketNum))
        .build());
    return groups;
  }

  private List<HoodieSliceInfo> generateFileSliceInfos(String partitionPath, List<FileSlice> fileSlices, ExtensibleBucketIdentifier identifier) {
    Set<String> fileSliceInfo = new HashSet<>();
    List<HoodieSliceInfo> existSliceInfos = getFileSliceInfo(fileSlices);
    for (HoodieSliceInfo hoodieSliceInfo : existSliceInfos) {
      fileSliceInfo.add(hoodieSliceInfo.getFileId());
    }
    // for not exist file group for the current version bucket layout, we should also include them in the ClusteringGroup
    Stream<HoodieSliceInfo> notExistFileSliceInfo = identifier.generateLogicalRecordLocationForAllBuckets()
        .filter(loc -> !fileSliceInfo.contains(loc.getFileId()))
        .map(loc -> HoodieSliceInfo.newBuilder()
            .setPartitionPath(partitionPath)
            .setFileId(loc.getFileId())
            .setDataFilePath(StringUtils.EMPTY_STRING)
            .setDeltaFilePaths(Collections.emptyList())
            .setBootstrapFilePath(StringUtils.EMPTY_STRING)
            .build());
    return Stream.concat(existSliceInfos.stream(), notExistFileSliceInfo).collect(Collectors.toList());
  }

  /**
   * Build split clustering group.
   *
   * @param metadata      current metadata before bucket resizing
   * @param partitionPath partition path
   * @param fileSlices    file slices to be split
   * @param splitNum      one bucket is split into `splitNum` buckets
   * @return split clustering group
   */
  protected List<HoodieClusteringGroup> buildSplitClusteringGroup(HoodieExtensibleBucketMetadata metadata, String partitionPath, List<FileSlice> fileSlices, int splitNum) {
    int currentBucketNum = metadata.getBucketNum();
    int newBucketNum = currentBucketNum * splitNum;
    ExtensibleBucketIdentifier identifier = new ExtensibleBucketIdentifier(metadata);
    List<HoodieSliceInfo> fileSliceInfos = generateFileSliceInfos(partitionPath, fileSlices, identifier);
    List<HoodieClusteringGroup> groups = Collections.singletonList(HoodieClusteringGroup.newBuilder()
        .setSlices(fileSliceInfos)
        .setNumOutputFileGroups(newBucketNum)
        .setMetrics(buildMetrics(fileSlices))
        .setExtraMetadata(constructExtensibleExtraMetadata(partitionPath, metadata, newBucketNum))
        .build());
    return groups;
  }

}
