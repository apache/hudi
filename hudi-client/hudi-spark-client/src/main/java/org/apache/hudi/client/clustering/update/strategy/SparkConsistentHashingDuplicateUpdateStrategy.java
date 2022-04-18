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

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.HoodieIndexUtils;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.UpdateStrategy;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Update strategy for (consistent hashing) bucket index
 * If updates to file groups that are under clustering are identified, then generate
 * two same records for each update, routing to both old and new file groups
 */
public class SparkConsistentHashingDuplicateUpdateStrategy<T extends HoodieRecordPayload<T>> extends UpdateStrategy<T, HoodieData<HoodieRecord<T>>> {

  private static final Logger LOG = LogManager.getLogger(SparkConsistentHashingDuplicateUpdateStrategy.class);

  public SparkConsistentHashingDuplicateUpdateStrategy(HoodieEngineContext engineContext, HoodieTable table, Set<HoodieFileGroupId> fileGroupsInPendingClustering) {
    super(engineContext, table, fileGroupsInPendingClustering);
  }

  @Override
  public Pair<HoodieData<HoodieRecord<T>>, Set<HoodieFileGroupId>> handleUpdate(HoodieData<HoodieRecord<T>> taggedRecordsRDD) {
    if (fileGroupsInPendingClustering.isEmpty()) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    HoodieData<HoodieRecord<T>> filteredRecordsRDD = taggedRecordsRDD.filter(r -> {
      ValidationUtils.checkState(r.getCurrentLocation() != null);
      return fileGroupsInPendingClustering.contains(new HoodieFileGroupId(r.getPartitionPath(), r.getCurrentLocation().getFileId()));
    });

    if (filteredRecordsRDD.count() == 0) {
      return Pair.of(taggedRecordsRDD, Collections.emptySet());
    }

    // Read all pending/ongoing clustering plans
    List<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPairs =
        table.getMetaClient().getActiveTimeline().filterInflightsAndRequested().filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).getInstants()
            .map(instant -> ClusteringUtils.getClusteringPlan(table.getMetaClient(), instant))
            .flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
            .collect(Collectors.toList());

    // Construct child node for each partition & build the bucket identifier
    final Set<String> partitions = new HashSet<>(filteredRecordsRDD.map(HoodieRecord::getPartitionPath).distinct().collectAsList());
    Map<String, HoodieConsistentHashingMetadata> partitionToHashingMeta = new HashMap<>();
    Map<String, String> partitionToInstant = new HashMap<>();
    for (Pair<HoodieInstant, HoodieClusteringPlan> pair : instantPlanPairs) {
      String instant = pair.getLeft().getTimestamp();
      HoodieClusteringPlan plan = pair.getRight();
      extractHashingMetadataFromClusteringPlan(instant, plan, partitions, partitionToHashingMeta, partitionToInstant);
    }
    Map<String, ConsistentBucketIdentifier> partitionToIdentifier = partitionToHashingMeta.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> new ConsistentBucketIdentifier(e.getValue())));

    // Produce records tagged with new record location
    List<String> indexKeyFields = Arrays.asList(table.getConfig().getBucketIndexHashField().split(","));
    HoodieData<HoodieRecord<T>> redirectedRecordsRDD = filteredRecordsRDD.map(r -> {
      ConsistentHashingNode node = partitionToIdentifier.get(r.getPartitionPath()).getBucket(r.getKey(), indexKeyFields);
      return HoodieIndexUtils.getTaggedRecord(new HoodieAvroRecord(r.getKey(), r.getData(), r.getOperation()),
          Option.ofNullable(new HoodieRecordLocation(partitionToInstant.get(r.getPartitionPath()), FSUtils.createNewFileId(node.getFileIdPrefix(), 0))));
    });

    // Return combined iterator (the original and records with new location)
    return Pair.of(taggedRecordsRDD.union(redirectedRecordsRDD), Collections.emptySet());
  }

  private void extractHashingMetadataFromClusteringPlan(String instant, HoodieClusteringPlan plan, final Set<String> recordPartitions,
                                                        Map<String, HoodieConsistentHashingMetadata> partitionToHashingMeta, Map<String, String> partitionToInstant) {
    for (HoodieClusteringGroup group : plan.getInputGroups()) {
      Map<String, String> groupMeta = group.getExtraMetadata();
      String p = groupMeta.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_PARTITION_KEY);
      // Skip unrelated clustering group
      if (!recordPartitions.contains(p)) {
        return;
      }

      String preInstant = partitionToInstant.putIfAbsent(p, instant);
      ValidationUtils.checkState(preInstant == null || preInstant.equals(instant), "Find a partition: " + p + " with two clustering instants");
      if (!partitionToHashingMeta.containsKey(p)) {
        partitionToHashingMeta.put(p, HoodieSparkConsistentBucketIndex.loadMetadata(table, p));
      }

      try {
        String nodeJson = group.getExtraMetadata().get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
        List<ConsistentHashingNode> nodes = ConsistentHashingNode.fromJsonString(nodeJson);
        partitionToHashingMeta.get(p).getChildrenNodes().addAll(nodes);
      } catch (Exception e) {
        LOG.error("Failed to parse child nodes in clustering plan", e);
        throw new HoodieException("Failed to parse child nodes in clustering plan, partition: " + p + ", cluster group: " + group, e);
      }
    }
  }

}
