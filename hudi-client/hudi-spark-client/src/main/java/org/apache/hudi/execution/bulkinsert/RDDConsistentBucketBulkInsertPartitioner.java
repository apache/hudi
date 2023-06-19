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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieConsistentHashingMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.index.bucket.ConsistentBucketIdentifier;
import org.apache.hudi.index.bucket.ConsistentBucketIndexUtils;
import org.apache.hudi.index.bucket.HoodieSparkConsistentBucketIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * A partitioner for (consistent hashing) bucket index used in bulk_insert
 */
public class RDDConsistentBucketBulkInsertPartitioner<T> extends RDDBucketIndexPartitioner<T> {

  private final Map<String, List<ConsistentHashingNode>> hashingChildrenNodes;

  public RDDConsistentBucketBulkInsertPartitioner(HoodieTable table,
                                                  Map<String, String> strategyParams,
                                                  boolean preserveHoodieMetadata) {
    super(table,
        strategyParams.getOrDefault(PLAN_STRATEGY_SORT_COLUMNS.key(), null),
        preserveHoodieMetadata);
    ValidationUtils.checkArgument(table.getMetaClient().getTableType().equals(HoodieTableType.MERGE_ON_READ),
        "Consistent hash bucket index doesn't support CoW table");
    this.hashingChildrenNodes = new HashMap<>();
  }

  public RDDConsistentBucketBulkInsertPartitioner(HoodieTable table) {
    this(table, Collections.emptyMap(), false);
    ValidationUtils.checkArgument(table.getIndex() instanceof HoodieSparkConsistentBucketIndex,
        "RDDConsistentBucketPartitioner can only be used together with consistent hashing bucket index");
  }

  /**
   * Repartition the records to conform the bucket index storage layout constraints.
   * Specifically, partition the records based on consistent bucket index, which is computed
   * using hashing metadata and records' key.
   *
   * @param records               Input Hoodie records
   * @param outputSparkPartitions Not used, the actual parallelism is determined by the bucket number
   * @return partitioned records, each partition of data corresponds to a bucket (i.e., file group)
   */
  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
    Map<String, ConsistentBucketIdentifier> partitionToIdentifier = initializeBucketIdentifier(records);
    Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap = generateFileIdPfx(partitionToIdentifier);
    return doPartition(records, new Partitioner() {
      @Override
      public int numPartitions() {
        return fileIdPfxList.size();
      }

      @Override
      public int getPartition(Object key) {
        HoodieKey hoodieKey = (HoodieKey) key;
        String partition = hoodieKey.getPartitionPath();
        ConsistentHashingNode node = partitionToIdentifier.get(partition).getBucket(hoodieKey, indexKeyFields);
        return partitionToFileIdPfxIdxMap.get(partition).get(node.getFileIdPrefix());
      }
    });
  }

  public void addHashingChildrenNodes(String partition, List<ConsistentHashingNode> nodes) {
    ValidationUtils.checkState(nodes.stream().noneMatch(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL), "children nodes should not be tagged as NORMAL");
    hashingChildrenNodes.put(partition, nodes);
  }

  /**
   * Get (construct) the bucket identifier of the given partition
   */
  private ConsistentBucketIdentifier getBucketIdentifier(String partition) {
    HoodieSparkConsistentBucketIndex index = (HoodieSparkConsistentBucketIndex) table.getIndex();
    HoodieConsistentHashingMetadata metadata = ConsistentBucketIndexUtils.loadOrCreateMetadata(this.table, partition, index.getNumBuckets());
    if (hashingChildrenNodes.containsKey(partition)) {
      metadata.setChildrenNodes(hashingChildrenNodes.get(partition));
    }
    return new ConsistentBucketIdentifier(metadata);
  }

  /**
   * Initialize hashing metadata of input records. The metadata of all related partitions will be loaded, and
   * the mapping from partition to its bucket identifier is constructed.
   */
  private Map<String, ConsistentBucketIdentifier> initializeBucketIdentifier(JavaRDD<HoodieRecord<T>> records) {
    return records.map(HoodieRecord::getPartitionPath).distinct().collect().stream()
        .collect(Collectors.toMap(p -> p, p -> getBucketIdentifier(p)));
  }

  /**
   * Initialize fileIdPfx for each data partition. Specifically, the following fields is constructed:
   * - fileIdPfxList: the Nth element corresponds to the Nth data partition, indicating its fileIdPfx
   * - doAppend: represents if the Nth data partition should use AppendHandler
   * - partitionToFileIdPfxIdxMap (return value): (table partition) -> (fileIdPfx -> idx) mapping
   *
   * @param partitionToIdentifier Mapping from table partition to bucket identifier
   */
  private Map<String, Map<String, Integer>> generateFileIdPfx(Map<String, ConsistentBucketIdentifier> partitionToIdentifier) {
    Map<String, Map<String, Integer>> partitionToFileIdPfxIdxMap = new HashMap(partitionToIdentifier.size() * 2);
    int count = 0;
    for (ConsistentBucketIdentifier identifier : partitionToIdentifier.values()) {
      Map<String, Integer> fileIdPfxToIdx = new HashMap();
      for (ConsistentHashingNode node : identifier.getNodes()) {
        fileIdPfxToIdx.put(node.getFileIdPrefix(), count++);
      }
      fileIdPfxList.addAll(identifier.getNodes().stream().map(ConsistentHashingNode::getFileIdPrefix).collect(Collectors.toList()));
      if (identifier.getMetadata().isFirstCreated()) {
        // Create new file group when the hashing metadata is new (i.e., first write to the partition)
        doAppend.addAll(Collections.nCopies(identifier.getNodes().size(), false));
      } else {
        // Child node requires generating a fresh new base file, rather than log file
        doAppend.addAll(identifier.getNodes().stream().map(n -> n.getTag() == ConsistentHashingNode.NodeTag.NORMAL).collect(Collectors.toList()));
      }
      partitionToFileIdPfxIdxMap.put(identifier.getMetadata().getPartitionPath(), fileIdPfxToIdx);
    }

    ValidationUtils.checkState(fileIdPfxList.size() == partitionToIdentifier.values().stream().mapToInt(ConsistentBucketIdentifier::getNumBuckets).sum(),
        "Error state after constructing fileId & idx mapping");
    return partitionToFileIdPfxIdxMap;
  }
}
