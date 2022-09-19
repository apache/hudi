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

  public FlinkConsistentBucketDuplicateUpdateStrategy(HoodieEngineContext engineContext) {
    super(engineContext);
    indexKeyFields = null;
  }

  @Override
  public void initialize(HoodieFlinkWriteClient writeClient) {
    super.initialize(writeClient);
    indexKeyFields = Arrays.asList(writeClient.getHoodieTable().getConfig().getBucketIndexHashField().split(","));
  }

  @Override
  protected Pair<List<RecordsInstantPair>, List<HoodieFileGroupId>> doHandleUpdate(HoodieFileGroupId fileId, RecordsInstantPair recordsInstantPair) {
    Map<String, List<HoodieRecord>> fileIdToRecords = new HashMap<>();

    Map<String, Pair<String, ConsistentBucketIdentifier>> partitionToIdentifier =
        ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(Collections.singleton(fileId.getPartitionPath()), table.getMetaClient());
    String instant = partitionToIdentifier.get(fileId.getPartitionPath()).getLeft();
    ConsistentBucketIdentifier identifier = partitionToIdentifier.get(fileId.getPartitionPath()).getRight();
    recordsInstantPair.records.forEach(r -> {
      ConsistentHashingNode node = identifier.getBucket(r.getKey(), this.indexKeyFields);
      fileIdToRecords.computeIfAbsent(node.getFileIdPrefix(), n -> new ArrayList<>()).add(r);
    });

    Stream<RecordsInstantPair> remappedRecordsInstantPair = fileIdToRecords.values().stream().map(r -> RecordsInstantPair.of(r, instant));
    Stream<HoodieFileGroupId> remappedFileGroupIds = fileIdToRecords.keySet().stream().map(fg -> new HoodieFileGroupId(fileId.getPartitionPath(), fg));

    // TODO add option to skip dual update
    return Pair.of(Stream.concat(remappedRecordsInstantPair, Stream.of(recordsInstantPair)).collect(Collectors.toList()),
        Stream.concat(remappedFileGroupIds, Stream.of(fileId)).collect(Collectors.toList()));
  }
}
