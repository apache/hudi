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
import org.apache.hudi.client.clustering.plan.strategy.SparkConsistentBucketClusteringPlanStrategy;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.table.HoodieTable;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Tuple2;

import static org.apache.hudi.index.bucket.ConsistentBucketIndexUtils.loadMetadata;
import static org.apache.hudi.index.bucket.ConsistentBucketIndexUtils.saveMetadata;

/**
 * Consistent hashing bucket index implementation, with auto-adjust bucket number.
 * NOTE: bucket resizing is triggered by clustering.
 */
public class HoodieSparkConsistentBucketIndex extends HoodieBucketIndex {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkConsistentBucketIndex.class);

  public HoodieSparkConsistentBucketIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieData<WriteStatus> updateLocation(HoodieData<WriteStatus> writeStatuses,
                                                HoodieEngineContext context,
                                                HoodieTable hoodieTable)
      throws HoodieIndexException {
    throw new HoodieIndexException("Consistent hashing index does not support update location without the instant parameter");
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
    ValidationUtils.checkState(instant.getTimestamp().equals(instantTime), "Cannot get the same instant, instantTime: " + instantTime);
    if (!instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
      return writeStatuses;
    }

    // Double-check if it is a clustering operation by trying to obtain the clustering plan
    Option<Pair<HoodieInstant, HoodieClusteringPlan>> instantPlanPair =
        ClusteringUtils.getClusteringPlan(hoodieTable.getMetaClient(), HoodieTimeline.getReplaceCommitRequestedInstant(instantTime));
    if (!instantPlanPair.isPresent()) {
      return writeStatuses;
    }

    HoodieClusteringPlan plan = instantPlanPair.get().getRight();
    HoodieJavaRDD.getJavaRDD(context.parallelize(plan.getInputGroups().stream().map(HoodieClusteringGroup::getExtraMetadata).collect(Collectors.toList())))
        .mapToPair(m -> new Tuple2<>(m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_PARTITION_KEY), m)
    ).groupByKey().foreach((input) -> {
      // Process each partition
      String partition = input._1();
      List<ConsistentHashingNode> childNodes = new ArrayList<>();
      int seqNo = 0;
      for (Map<String, String> m: input._2()) {
        String nodesJson = m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY);
        childNodes.addAll(ConsistentHashingNode.fromJsonString(nodesJson));
        seqNo = Integer.parseInt(m.get(SparkConsistentBucketClusteringPlanStrategy.METADATA_SEQUENCE_NUMBER_KEY));
      }

      Option<HoodieConsistentHashingMetadata> metadataOption = loadMetadata(hoodieTable.getMetaClient(), partition);
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
      saveMetadata(hoodieTable.getMetaClient(), newMeta);
    });

    return writeStatuses;
  }

  /**
   * Do nothing.
   * A failed write may create a hashing metadata for a partition. In this case, we still do nothing when rolling back
   * the failed write. Because the hashing metadata created by a writer must have 00000000000000 timestamp and can be viewed
   * as the initialization of a partition rather than as a part of the failed write.
   */
  @Override
  public boolean rollbackCommit(String instantTime) {
    return true;
  }

  @Override
  protected BucketIndexLocationMapper getLocationMapper(HoodieTable table, List<String> partitionPath) {
    return new ConsistentBucketIndexLocationMapper(table, partitionPath);
  }

  public class ConsistentBucketIndexLocationMapper implements BucketIndexLocationMapper {

    /**
     * Mapping from partitionPath -> bucket identifier
     */
    private final Map<String, ConsistentBucketIdentifier> partitionToIdentifier;

    public ConsistentBucketIndexLocationMapper(HoodieTable table, List<String> partitions) {
      // TODO maybe parallel
      partitionToIdentifier = partitions.stream().collect(Collectors.toMap(p -> p, p -> {
        HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(table.getMetaClient(), p, getNumBuckets());
        return new ConsistentBucketIdentifier(metadata);
      }));
    }

    @Override
    public Option<HoodieRecordLocation> getRecordLocation(HoodieKey key) {
      String partitionPath = key.getPartitionPath();
      ConsistentHashingNode node = partitionToIdentifier.get(partitionPath).getBucket(key, indexKeyFields);
      if (!StringUtils.isNullOrEmpty(node.getFileIdPrefix())) {
        /**
         * Dynamic Bucket Index doesn't need the instant time of the latest file group.
         * We add suffix 0 here to the file uuid, following the naming convention, i.e., fileId = [uuid]_[numWrites]
         */
        return Option.of(new HoodieRecordLocation(null, FSUtils.createNewFileId(node.getFileIdPrefix(), 0)));
      }

      LOG.error("Consistent hashing node has no file group, partition: " + partitionPath + ", meta: "
          + partitionToIdentifier.get(partitionPath).getMetadata().getFilename() + ", record_key: " + key.toString());
      throw new HoodieIndexException("Failed to getBucket as hashing node has no file group");
    }
  }
}
