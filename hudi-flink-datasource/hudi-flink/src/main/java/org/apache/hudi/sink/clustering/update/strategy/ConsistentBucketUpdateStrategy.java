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

package org.apache.hudi.sink.clustering.update.strategy;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.sink.clustering.update.strategy.ConsistentBucketUpdateStrategy.BucketRecords;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;
import org.apache.hudi.table.action.cluster.util.ConsistentHashingUpdateStrategyUtils;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;

import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Update strategy for consistent hashing bucket index. If updates to file groups that are under clustering are identified,
 * then the current batch of records will route to both old and new file groups, i.e., dual write.
 */
public class ConsistentBucketUpdateStrategy<T> extends UpdateStrategy<T, List<BucketRecords>> {

  private static final Logger LOG = LoggerFactory.getLogger(ConsistentBucketUpdateStrategy.class);

  private boolean initialized = false;
  private final List<String> indexKeyFields;
  private final Map<String, Pair<String, ConsistentBucketIdentifier>> partitionToIdentifier;
  private String lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;

  public ConsistentBucketUpdateStrategy(
      HoodieFlinkWriteClient writeClient, List<String> indexKeyFields) {
    super(writeClient.getEngineContext(), writeClient.getHoodieTable(), Collections.emptySet());

    this.indexKeyFields = indexKeyFields;
    this.partitionToIdentifier = new HashMap<>();
  }

  public void initialize(HoodieFlinkWriteClient writeClient) {
    if (initialized) {
      return;
    }
    HoodieFlinkTable table = writeClient.getHoodieTable();
    List<HoodieInstant> instants = ClusteringUtils.getPendingClusteringInstantTimes(table.getMetaClient());
    if (!instants.isEmpty()) {
      HoodieInstant latestPendingReplaceInstant = instants.get(instants.size() - 1);
      if (latestPendingReplaceInstant.requestedTime().compareTo(lastRefreshInstant) > 0) {
        LOG.info("Found new pending replacement commit. Last pending replacement commit is {}.", latestPendingReplaceInstant);
        this.table = table;
        this.fileGroupsInPendingClustering = table.getFileSystemView().getFileGroupsInPendingClustering()
            .map(Pair::getKey).collect(Collectors.toSet());
        // TODO throw exception if exists bucket merge plan
        this.lastRefreshInstant = latestPendingReplaceInstant.requestedTime();
        this.partitionToIdentifier.clear();
      }
    }
    this.initialized = true;
  }

  public void reset() {
    initialized = false;
  }

  @Override
  public Pair<List<BucketRecords>, Set<HoodieFileGroupId>> handleUpdate(List<BucketRecords> bucketRecordsList) {
    ValidationUtils.checkArgument(initialized, "Strategy has not been initialized");
    ValidationUtils.checkArgument(bucketRecordsList.size() == 1);

    BucketRecords bucketRecords = bucketRecordsList.get(0);
    BucketInfo bucketInfo = bucketRecords.getBucketInfo();
    HoodieFileGroupId fileId = new HoodieFileGroupId(bucketInfo.getPartitionPath(), bucketInfo.getFileIdPrefix());
    if (!needDualWrite(fileId)) {
      return Pair.of(bucketRecordsList, Collections.singleton(fileId));
    }
    return doHandleUpdate(fileId, bucketRecords);
  }

  public boolean needDualWrite(HoodieFileGroupId fileId) {
    return !fileGroupsInPendingClustering.isEmpty() && fileGroupsInPendingClustering.contains(fileId);
  }

  private Pair<List<BucketRecords>, Set<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, BucketRecords bucketRecords) {
    Pair<String, ConsistentBucketIdentifier> bucketIdentifierPair = getBucketIdentifierOfPartition(fileId.getPartitionPath());
    String clusteringInstant = bucketIdentifierPair.getLeft();
    ConsistentBucketIdentifier identifier = bucketIdentifierPair.getRight();

    List<HoodieRecord> records = new ArrayList<>();
    bucketRecords.getRecordItr().forEachRemaining(records::add);
    // Construct records list routing to new file groups according the new bucket identifier
    Map<String, List<HoodieRecord>> fileIdToRecords = records.stream().map(HoodieRecord::newInstance)
        .collect(Collectors.groupingBy(r -> identifier.getBucket(r.getKey(), indexKeyFields).getFileIdPrefix()));

    // Tag first record with the corresponding fileId & clusteringInstantTime
    List<BucketRecords> bucketRecordsList = new ArrayList<>();
    Set<HoodieFileGroupId> fgs = new LinkedHashSet<>();
    for (Map.Entry<String, List<HoodieRecord>> e : fileIdToRecords.entrySet()) {
      String newFileId = FSUtils.createNewFileId(e.getKey(), 0);
      BucketInfo bucketInfo = new BucketInfo(BucketType.UPDATE, newFileId, bucketRecords.getBucketInfo().getPartitionPath());
      bucketRecordsList.add(BucketRecords.of(e.getValue().iterator(), bucketInfo, clusteringInstant));
      fgs.add(new HoodieFileGroupId(fileId.getPartitionPath(), newFileId));
    }
    LOG.info("Apply duplicate update for FileGroup {}, routing records to: {}.", fileId, String.join(",", fileIdToRecords.keySet()));
    // TODO add option to skip dual update, i.e., write updates only to the new file group
    // `recordItr` inside `bucketRecords` is based on MutableObjectIterator, which is one-time iterator and cannot be reused.
    bucketRecordsList.add(BucketRecords.of(records.iterator(), bucketRecords.getBucketInfo(), bucketRecords.getInstant()));
    fgs.add(fileId);
    return Pair.of(bucketRecordsList, fgs);
  }

  private Pair<String, ConsistentBucketIdentifier> getBucketIdentifierOfPartition(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(Collections.singleton(p), table).get(p)
    );
  }

  public static class BucketRecords {
    @Getter
    private final Iterator<HoodieRecord> recordItr;
    @Getter
    private final BucketInfo bucketInfo;
    @Getter
    private final String instant;

    private BucketRecords(Iterator<HoodieRecord> recordItr, BucketInfo bucketInfo, String instant) {
      this.recordItr = recordItr;
      this.bucketInfo = bucketInfo;
      this.instant = instant;
    }

    public static BucketRecords of(Iterator<HoodieRecord> recordItr, BucketInfo bucketInfo, String instant) {
      return new BucketRecords(recordItr, bucketInfo, instant);
    }
  }
}
