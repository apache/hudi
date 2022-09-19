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

package org.apache.hudi.table.action.cluster.update.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

public class ConsistentHashingUpdateStrategyUtils {

  private static final Logger LOG = LogManager.getLogger(ConsistentHashingUpdateStrategyUtils.class);

  /**
   * Construct identifier for the given partitions that are under concurrent resizing (i.e., clustering).
   * @return map from partition to pair<instant, identifier>, where instant is the clustering instant.
   */
  public static Map<String, Pair<String, ConsistentBucketIdentifier>> constructPartitionToIdentifier(Set<String> partitions, HoodieTableMetaClient metaClient) {
    // Read all pending/ongoing clustering plans
    List<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPairs =
        metaClient.getActiveTimeline().filterInflightsAndRequested().filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)).getInstants()
            .map(instant -> ClusteringUtils.getClusteringPlan(metaClient, instant))
            .flatMap(o -> o.isPresent() ? Stream.of(o.get()) : Stream.empty())
            .collect(Collectors.toList());

    // Construct child node for each partition & build the bucket identifier
    Map<String, HoodieConsistentHashingMetadata> partitionToHashingMeta = new HashMap<>();
    Map<String, String> partitionToInstant = new HashMap<>();
    for (Pair<HoodieInstant, HoodieClusteringPlan> pair : instantPlanPairs) {
      String instant = pair.getLeft().getTimestamp();
      HoodieClusteringPlan plan = pair.getRight();
      ConsistentHashingUpdateStrategyUtils.extractHashingMetadataFromClusteringPlan(instant, plan, metaClient, partitions, partitionToHashingMeta, partitionToInstant);
    }
    return partitionToHashingMeta.entrySet().stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> Pair.of(partitionToInstant.get(e.getKey()), new ConsistentBucketIdentifier(e.getValue()))));
  }

  public static void extractHashingMetadataFromClusteringPlan(String instant, HoodieClusteringPlan plan, HoodieTableMetaClient metaClient, final Set<String> recordPartitions,
                                                              Map<String, HoodieConsistentHashingMetadata> partitionToHashingMeta, Map<String, String> partitionToInstant) {
    for (HoodieClusteringGroup group : plan.getInputGroups()) {
      Map<String, String> groupMeta = group.getExtraMetadata();
      String p = groupMeta.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_PARTITION_KEY);
      checkState(p != null, "Clustering plan does not has partition info, plan: " + plan);
      // Skip unrelated clustering group
      if (!recordPartitions.contains(p)) {
        return;
      }

      String preInstant = partitionToInstant.putIfAbsent(p, instant);
      checkState(preInstant == null || preInstant.equals(instant), "Find a partition: " + p + " with two clustering instants");
      if (!partitionToHashingMeta.containsKey(p)) {
        Option<HoodieConsistentHashingMetadata> metadataOption = ConsistentBucketIndexUtils.loadMetadata(metaClient, p);
        checkState(metadataOption.isPresent(), "Failed to load consistent hashing metadata for partition: " + p);
        partitionToHashingMeta.put(p, metadataOption.get());
      }

      try {
        String nodeJson = group.getExtraMetadata().get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
        List<ConsistentHashingNode> nodes = ConsistentHashingNode.fromJsonString(nodeJson);
        partitionToHashingMeta.get(p).getChildrenNodes().addAll(nodes);
      } catch (Exception e) {
        LOG.error("Failed to parse child nodes in clustering plan", e);
        throw new HoodieException("Failed to parse child nodes in clustering plan, partition: " + p + ", cluster group: " + group, e);
      }
    }
  }
}
