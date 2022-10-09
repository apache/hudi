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

package org.apache.hudi.sink.clustering.update.strategy;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.action.cluster.update.strategy.ConsistentHashingUpdateStrategyUtils;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Update strategy for (consistent hashing) bucket index
 * If updates to file groups that are under clustering are identified, then the current batch
 * of records will route to both old and new file groups (i.e., dual write)
 */
public class FlinkConsistentBucketDuplicateUpdateStrategy<T extends HoodieRecordPayload> extends BaseFlinkUpdateStrategy<T> {

  private static final Logger LOG = LogManager.getLogger(FlinkConsistentBucketDuplicateUpdateStrategy.class);

  private List<String> indexKeyFields;
  private Map<String, Pair<String, ConsistentBucketIdentifier>> partitionToIdentifier;

  public FlinkConsistentBucketDuplicateUpdateStrategy(HoodieEngineContext engineContext) {
    super(engineContext);
    this.indexKeyFields = null;
    this.partitionToIdentifier = new HashMap<>();
  }

  @Override
  public void initialize(HoodieFlinkWriteClient writeClient) {
    super.initialize(writeClient);
    if (indexKeyFields == null) {
      indexKeyFields = Arrays.asList(writeClient.getHoodieTable().getConfig().getBucketIndexHashField().split(","));
    }
  }

  @Override
  public void reset() {
    super.reset();

    // Reset the identifier cache
    this.partitionToIdentifier = new HashMap<>();
  }

  @Override
  protected Pair<List<RecordsInstantPair>, List<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, RecordsInstantPair recordsInstantPair) {
    Map<String, List<HoodieRecord>> fileIdToRecords = new HashMap<>(4);

    Pair<String, ConsistentBucketIdentifier> identifierPair = getIdentifierOfPartition(fileId.getPartitionPath());
    String instant = identifierPair.getLeft();
    ConsistentBucketIdentifier identifier = identifierPair.getRight();
    // TODO maybe handle bucket split & merge differently. Bucket merge does not need rehashing, just routing all records to the new bucket.
    recordsInstantPair.records.forEach(r -> {
      ConsistentHashingNode node = identifier.getBucket(r.getKey(), this.indexKeyFields);
      fileIdToRecords.computeIfAbsent(node.getFileIdPrefix(), n -> new ArrayList<>()).add(r.newInstance());
    });

    Set<Map.Entry<String, List<HoodieRecord>>> fileIdToRecordsEntry = fileIdToRecords.entrySet();
    Stream<RecordsInstantPair> remappedRecordsInstantPair = fileIdToRecordsEntry.stream().map(e -> RecordsInstantPair.of(e.getValue(), instant));
    Stream<HoodieFileGroupId> remappedFileGroupIds = fileIdToRecordsEntry.stream().map(e -> new HoodieFileGroupId(fileId.getPartitionPath(), e.getKey()));

    // TODO add option to skip dual update, i.e., write updates only to the new file group
    LOG.info("Apply duplicate update for FileGroup" + fileId + ", routing records to: " + String.join(",", fileIdToRecords.keySet()));
    return Pair.of(Stream.concat(remappedRecordsInstantPair, Stream.of(recordsInstantPair)).collect(Collectors.toList()),
        Stream.concat(remappedFileGroupIds, Stream.of(fileId)).collect(Collectors.toList()));
  }

  private Pair<String, ConsistentBucketIdentifier> getIdentifierOfPartition(String partition) {
    return partitionToIdentifier.computeIfAbsent(partition, p -> {
          return ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(Collections.singleton(p), table.getMetaClient()).get(p);
        }
    );
  }
}
