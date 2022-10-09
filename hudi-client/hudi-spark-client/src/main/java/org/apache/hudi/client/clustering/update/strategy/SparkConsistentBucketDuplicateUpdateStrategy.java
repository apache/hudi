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

package org.apache.hudi.client.clustering.update.strategy;

import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.update.strategy.ConsistentHashingUpdateStrategyUtils;
import org.apache.hudi.table.action.cluster.update.strategy.UpdateStrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.index.HoodieIndexUtils.getTaggedRecord;

/**
 * Update strategy for (consistent hashing) bucket index
 * If updates to file groups that are under clustering are identified, then generate
 * two same records for each update, routing to both old and new file groups
 */
public class SparkConsistentBucketDuplicateUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, HoodieData<HoodieRecord<T>>> {

  private static final Logger LOG = LogManager.getLogger(SparkConsistentBucketDuplicateUpdateStrategy.class);

  public SparkConsistentBucketDuplicateUpdateStrategy(HoodieEngineContext engineContext, HoodieTable table, Set<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, table, fileGroupsInPendingClustering);
  }

  @Override
  public Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> handleUpdate(HoodieData<HoodieRecord<T>> taggedRecordsRDD) {
    if (fileGroupsInPendingClustering.isEmpty()) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    HoodieData<HoodieRecord<T>> filteredRecordsRDD = taggedRecordsRDD.filter(r -> {
      checkState(r.getCurrentLocation() != null);
      return fileGroupsInPendingClustering.contains(new HoodieFileGroupId(r.getPartitionPath(), r.getCurrentLocation().getFileId()));
    });

    if (filteredRecordsRDD.count() == 0) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    final Set<String> partitions = new HashSet<>(filteredRecordsRDD.map(HoodieRecord::getPartitionPath).distinct().collectAsList());
    Map<String, Pair<String, ConsistentBucketIdentifier>> partitionToIdentifier =
        ConsistentHashingUpdateStrategyUtils.constructPartitionToIdentifier(partitions, table.getMetaClient());

    // Produce records tagged with new record location
    List<String> indexKeyFields = Arrays.asList(table.getConfig().getBucketIndexHashField().split(","));
    HoodieData<HoodieRecord<T>> redirectedRecordsRDD = filteredRecordsRDD.map(r -> {
      Pair<String, ConsistentBucketIdentifier> identifierPair = partitionToIdentifier.get(r.getPartitionPath());
      ConsistentHashingNode node = identifierPair.getValue().getBucket(r.getKey(), indexKeyFields);
      return getTaggedRecord(new HoodieAvroRecord(r.getKey(), r.getData(), r.getOperation()),
          Option.ofNullable(new HoodieRecordLocation(identifierPair.getKey(), FSUtils.createNewFileId(node.getFileIdPrefix(), 0))));
    });

    // Return combined iterator (the original and records with new location)
    return Pair.of(taggedRecordsRDD.union(redirectedRecordsRDD), Collections.emptySet());
  }
}
