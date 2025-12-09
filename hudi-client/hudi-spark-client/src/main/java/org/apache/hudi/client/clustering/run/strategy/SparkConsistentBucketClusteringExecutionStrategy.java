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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.HoodieDatasetBulkInsertHelper;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.ConsistentHashingNode;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.execution.bulkinsert.ConsistentBucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.RDDConsistentBucketBulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.cluster.strategy.BaseConsistentHashingBucketClusteringPlanStrategy;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Clustering execution strategy specifically for consistent hashing index
 */
public class SparkConsistentBucketClusteringExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends MultipleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkConsistentBucketClusteringExecutionStrategy.class);

  public SparkConsistentBucketClusteringExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext,
                                                          HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsAsRow(Dataset<Row> inputRecords,
                                                                   int numOutputGroups,
                                                                   String instantTime,
                                                                   Map<String, String> strategyParams,
                                                                   HoodieSchema schema,
                                                                   List<HoodieFileGroupId> fileGroupIdList,
                                                                   boolean shouldPreserveHoodieMetadata,
                                                                   Map<String, String> extraMetadata) {
    LOG.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);
    Properties props = getWriteConfig().getProps();

    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();

    Pair<String, List<ConsistentHashingNode>> childNodesPair = extractChildNodes(extraMetadata);
    ConsistentBucketIndexBulkInsertPartitionerWithRows partitioner = new ConsistentBucketIndexBulkInsertPartitionerWithRows(getHoodieTable(), strategyParams, shouldPreserveHoodieMetadata,
        Collections.singletonMap(childNodesPair.getKey(), childNodesPair.getValue()));

    Dataset<Row> repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups);

    return HoodieDatasetBulkInsertHelper.bulkInsert(repartitionedRecords, instantTime, getHoodieTable(), newConfig,
        partitioner.arePartitionRecordsSorted(), shouldPreserveHoodieMetadata);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsRDD(HoodieData<HoodieRecord<T>> inputRecords, int numOutputGroups, String instantTime,
                                                                 Map<String, String> strategyParams, Schema schema, List<HoodieFileGroupId> fileGroupIdList,
                                                                 boolean preserveHoodieMetadata, Map<String, String> extraMetadata) {

    LOG.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);
    Properties props = getWriteConfig().getProps();
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();

    Pair<String, List<ConsistentHashingNode>> childNodesPair = extractChildNodes(extraMetadata);
    RDDConsistentBucketBulkInsertPartitioner<T> partitioner = new RDDConsistentBucketBulkInsertPartitioner<>(getHoodieTable(), strategyParams, preserveHoodieMetadata,
        Collections.singletonMap(childNodesPair.getKey(), childNodesPair.getValue()));

    return (HoodieData<WriteStatus>) SparkBulkInsertHelper.newInstance()
        .bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig, false, partitioner, true, numOutputGroups);
  }

  private Pair<String/*partition*/, List<ConsistentHashingNode>> extractChildNodes(Map<String, String> extraMetadata) {
    try {
      List<ConsistentHashingNode> nodes = ConsistentHashingNode.fromJsonString(extraMetadata.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_CHILD_NODE_KEY));
      return Pair.of(extraMetadata.get(BaseConsistentHashingBucketClusteringPlanStrategy.METADATA_PARTITION_KEY), nodes);
    } catch (Exception e) {
      LOG.error("Failed to extract hashing children nodes", e);
      throw new HoodieClusteringException("Failed to extract hashing children nodes", e);
    }
  }
}
