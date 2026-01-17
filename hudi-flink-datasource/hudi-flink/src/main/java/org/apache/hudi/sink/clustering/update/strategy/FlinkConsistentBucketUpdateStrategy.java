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
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.HoodieFlinkTable;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;
import org.apache.hudi.table.action.cluster.util.ConsistentHashingUpdateStrategyUtils;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Update strategy for consistent hashing bucket index. If updates to file groups that are under clustering are identified,
 * then the current batch of records will route to both old and new file groups
 * (i.e., dual write).
 *
 * TODO: remove this class when RowData mode writing is supported for COW.
 */
@Slf4j
public class FlinkConsistentBucketUpdateStrategy<T extends HoodieRecordPayload> extends UpdateStrategy<T, List<Pair<List<HoodieRecord>, String>>> {

  private boolean initialized = false;
  private final List<String> indexKeyFields;
  private final Map<String, Pair<String, ConsistentBucketIdentifier>> partitionToIdentifier;
  private String lastRefreshInstant = HoodieTimeline.INIT_INSTANT_TS;

  public FlinkConsistentBucketUpdateStrategy(HoodieFlinkWriteClient writeClient, List<String> indexKeyFields) {
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
        log.info("Found new pending replacement commit. Last pending replacement commit is {}.", latestPendingReplaceInstant);
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
  public Pair<List<Pair<List<HoodieRecord>, String>>, Set<HoodieFileGroupId>> handleUpdate(List<Pair<List<HoodieRecord>, String>> recordsList) {
    ValidationUtils.checkArgument(initialized, "Strategy has not been initialized");
    ValidationUtils.checkArgument(recordsList.size() == 1);

    Pair<List<HoodieRecord>, String> recordsInstantPair = recordsList.get(0);
    HoodieRecord sampleRecord = recordsInstantPair.getLeft().get(0);
    HoodieFileGroupId fileId = new HoodieFileGroupId(sampleRecord.getPartitionPath(), sampleRecord.getCurrentLocation().getFileId());
    if (!needDualWrite(fileId)) {
      return Pair.of(recordsList, Collections.singleton(fileId));
    }

    return doHandleUpdate(fileId, recordsInstantPair);
  }

  public boolean needDualWrite(HoodieFileGroupId fileId) {
    return !fileGroupsInPendingClustering.isEmpty() && fileGroupsInPendingClustering.contains(fileId);
  }

  private Pair<List<Pair<List<HoodieRecord>, String>>, Set<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, Pair<List<HoodieRecord>, String> recordsInstantPair) {
    Pair<String, ConsistentBucketIdentifier> bucketIdentifierPair = getBucketIdentifierOfPartition(fileId.getPartitionPath());
    String clusteringInstant = bucketIdentifierPair.getLeft();
    ConsistentBucketIdentifier identifier = bucketIdentifierPair.getRight();

    // Construct records list routing to new file groups according the new bucket identifier
    Map<String, List<HoodieRecord>> fileIdToRecords = recordsInstantPair.getLeft().stream().map(HoodieRecord::newInstance)
        .collect(Collectors.groupingBy(r -> identifier.getBucket(r.getKey(), indexKeyFields).getFileIdPrefix()));

    // Tag first record with the corresponding fileId & clusteringInstantTime
    List<Pair<List<HoodieRecord>, String>> recordsList = new ArrayList<>();
    Set<HoodieFileGroupId> fgs = new LinkedHashSet<>();
    for (Map.Entry<String, List<HoodieRecord>> e : fileIdToRecords.entrySet()) {
      String newFileId = FSUtils.createNewFileId(e.getKey(), 0);
      patchFileIdToRecords(e.getValue(), newFileId);
      recordsList.add(Pair.of(e.getValue(), clusteringInstant));
      fgs.add(new HoodieFileGroupId(fileId.getPartitionPath(), newFileId));
    }
    log.info("Apply duplicate update for FileGroup {}, routing records to: {}.", fileId, String.join(",", fileIdToRecords.keySet()));
    // TODO add option to skip dual update, i.e., write updates only to the new file group
    recordsList.add(recordsInstantPair);
    fgs.add(fileId);
    return Pair.of(recordsList, fgs);
  }

  private Pair<String, ConsistentBucketIdentifier> getBucketIdentifierOfPartition(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(Collections.singleton(p), table).get(p)
    );
  }

  /**
   * Rewrite the first record with given fileID
   */
  private void patchFileIdToRecords(List<HoodieRecord> records, String fileId) {
    HoodieRecord first = records.get(0);
    HoodieRecord record = new HoodieAvroRecord<>(first.getKey(), (HoodieRecordPayload) first.getData(), first.getOperation());
    HoodieRecordLocation newLoc = new HoodieRecordLocation("U", fileId);
    record.setCurrentLocation(newLoc);
    records.set(0, record);
  }
}
