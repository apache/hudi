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

package org.apache.hudi.index.bucket;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

/**
 * Consistent hashing bucket index implementation, with auto-adjust bucket number.
 * NOTE: bucket resizing is triggered by clustering.
 */
public class HoodieSparkConsistentBucketIndex extends HoodieConsistentBucketIndex {

  public HoodieSparkConsistentBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Persist hashing metadata to storage. Only clustering operations will modify the metadata.
   * For example, splitting & merging buckets, or just sorting and producing a new bucket.
   */
  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable,
                                                String instantTime)
      throws HoodieIndexException {
    HoodieInstant instant = hoodieTable.getMetaClient().getActiveTimeline().findInstantsAfterOrEquals(instantTime, 1).firstInstant().get();
    ValidationUtils.checkState(instant.requestedTime().equals(instantTime), "Cannot get the same instant, instantTime: " + instantTime);
    if (!ClusteringUtils.isClusteringOrReplaceCommitAction(instant.getAction())) {
      return writeStatuses;
    }

    // Double-check if it is a clustering operation by trying to obtain the clustering plan
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPair =
        ClusteringUtils.getClusteringPlan(hoodieTable.getMetaClient(), instant);
    if (!instantPlanPair.isPresent()) {
      return writeStatuses;
    }

    HoodieClusteringPlan plan = instantPlanPair.get().getRight();
    HoodieJavaRDD.getJavaRDD(context.parallelize(plan.getInputGroups().stream().map(HoodieClusteringGroup::getExtraMetadata).collect(Collectors.toList())))
        .mapToPair(m -> new Tuple2<>(m.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_PARTITION_KEY), m)
    ).groupByKey().foreach((input) -> {
      // Process each partition
      String partition = input._1();
      List<ConsistentHashingNode> childNodes = new ArrayList<>();
      int seqNo = 0;
      for (Map<String, String> m: input._2()) {
        String nodesJson = m.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
        childNodes.addAll(ConsistentHashingNode.fromJsonString(nodesJson));
        seqNo = Integer.parseInt(m.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_SEQUENCE_NUMBER_KEY));
      }

      Option<HoodieConsistentHashingMetadata> metadataOption = ConsistentBucketIndexUtils.loadMetadata(hoodieTable, partition);
      ValidationUtils.checkState(metadataOption.isPresent(), "Failed to load metadata for partition: " + partition);
      HoodieConsistentHashingMetadata meta = metadataOption.get();
      ValidationUtils.checkState(meta.getSeqNo() == seqNo,
          "Non serialized update to hashing metadata, old seq: " + meta.getSeqNo() + ", new seq: " + seqNo);

      // Get new metadata and save
      meta.setChildrenNodes(childNodes);
      List<ConsistentHashingNode> newNodes = (new ConsistentBucketIdentifier(meta)).getNodes().stream()
          .map(n -> new ConsistentHashingNode(n.getValue(), n.getFileIdPrefix(), ConsistentHashingNode.NodeTag.NORMAL))
          .collect(Collectors.toList());
      HoodieConsistentHashingMetadata newMeta = new HoodieConsistentHashingMetadata(meta.getVersion(), meta.getPartitionPath(),
          instantTime, meta.getNumBuckets(), seqNo + 1, newNodes);
      if (!ConsistentBucketIndexUtils.saveMetadata(hoodieTable, newMeta)) {
        throw new HoodieIndexException("Failed to save metadata for partition: " + partition);
      }
    });

    return writeStatuses;
  }
}
