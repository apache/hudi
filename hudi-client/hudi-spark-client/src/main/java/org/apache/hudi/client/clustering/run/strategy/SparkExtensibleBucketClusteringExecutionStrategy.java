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
import org.apache.hudi.common.model.HoodieExtensibleBucketMetadata;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.ExtensibleBucketIndexBulkInsertPartitionerWithRows;
import org.apache.hudi.execution.bulkinsert.RDDExtensibleBucketBulkInsertPartitioner;
import org.apache.hudi.index.bucket.ExtensibleBucketIndexUtils;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Clustering execution strategy specifically for consistent bucket index
 */
public class SparkExtensibleBucketClusteringExecutionStrategy<T extends HoodieRecordPayload<T>> extends MultipleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LoggerFactory.getLogger(SparkExtensibleBucketClusteringExecutionStrategy.class);

  public SparkExtensibleBucketClusteringExecutionStrategy(HoodieTable table, HoodieEngineContext engineContext,
                                                          HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsAsRow(Dataset<Row> inputRecords, int numOutputGroups, String instantTime, Map<String, String> strategyParams,
                                                                   Schema schema, List<HoodieFileGroupId> fileGroupIdList, boolean shouldPreserveHoodieMetadata,
                                                                   Map<String, String> extraMetadata) {
    LOG.info("Performing SparkExtensibleBucketClusteringExecutionStrategy with row-writer for fileGroupIdList: {} with numOutputGroups: {}", fileGroupIdList, numOutputGroups);
    Properties props = getWriteConfig().getProps();

    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();
    ExtensibleBucketIndexBulkInsertPartitionerWithRows partitioner = new ExtensibleBucketIndexBulkInsertPartitionerWithRows(getHoodieTable(), strategyParams, shouldPreserveHoodieMetadata);

    // add pending bucket resizing related extensible-bucket-index metadata
    HoodieExtensibleBucketMetadata pendingMetadata = ExtensibleBucketIndexUtils.deconstructExtensibleExtraMetadata(extraMetadata, instantTime);
    partitioner.addPendingExtensibleBucketMetadata(pendingMetadata);

    Dataset<Row> repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups);
    return HoodieDatasetBulkInsertHelper.bulkInsert(repartitionedRecords, instantTime, getHoodieTable(), newConfig,
        partitioner.arePartitionRecordsSorted(), shouldPreserveHoodieMetadata);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsRDD(HoodieData<HoodieRecord<T>> inputRecords, int numOutputGroups, String instantTime,
                                                                 Map<String, String> strategyParams, Schema schema, List<HoodieFileGroupId> fileGroupIdList,
                                                                 boolean shouldPreserveHoodieMetadata, Map<String, String> extraMetadata) {
    LOG.info("Performing SparkExtensibleBucketClusteringExecutionStrategy for fileGroupIdList: {} with numOutputGroups: {}", fileGroupIdList, numOutputGroups);
    Properties props = getWriteConfig().getProps();
    // We are calling another action executor - disable auto commit. Strategy is only expected to write data in new files.
    props.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), Boolean.FALSE.toString());
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();

    RDDExtensibleBucketBulkInsertPartitioner<T> partitioner = new RDDExtensibleBucketBulkInsertPartitioner<>(getHoodieTable(), strategyParams, shouldPreserveHoodieMetadata);
    // add pending bucket resizing related extensible-bucket-index metadata
    HoodieExtensibleBucketMetadata pendingMetadata = ExtensibleBucketIndexUtils.deconstructExtensibleExtraMetadata(extraMetadata, instantTime);
    partitioner.addPendingExtensibleBucketMetadata(pendingMetadata);

    return (HoodieData<WriteStatus>) SparkBulkInsertHelper.newInstance()
        .bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig, false, partitioner, true, numOutputGroups);
  }

}
