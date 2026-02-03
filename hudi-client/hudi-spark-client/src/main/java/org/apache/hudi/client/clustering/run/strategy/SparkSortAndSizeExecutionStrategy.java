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
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;
import java.util.Map;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Uses bulk_insert to write data into new files.
 */
@Slf4j
public class SparkSortAndSizeExecutionStrategy<T>
    extends MultipleSparkJobExecutionStrategy<T> {

  public SparkSortAndSizeExecutionStrategy(HoodieTable table,
                                           HoodieEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsAsRow(Dataset<Row> inputRecords,
                                                                   int numOutputGroups,
                                                                   String instantTime, Map<String, String> strategyParams,
                                                                   HoodieSchema schema,
                                                                   List<HoodieFileGroupId> fileGroupIdList,
                                                                   boolean shouldPreserveHoodieMetadata,
                                                                   Map<String, String> extraMetadata) {
    log.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withBulkInsertParallelism(numOutputGroups)
        .withProps(getWriteConfig().getProps()).build();

    newConfig.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));

    BulkInsertPartitioner<Dataset<Row>> partitioner = getRowPartitioner(strategyParams, schema);
    Dataset<Row> repartitionedRecords = partitioner.repartitionRecords(inputRecords, numOutputGroups);

    return HoodieDatasetBulkInsertHelper.bulkInsert(repartitionedRecords, instantTime, getHoodieTable(), newConfig,
        partitioner.arePartitionRecordsSorted(), shouldPreserveHoodieMetadata);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsRDD(final HoodieData<HoodieRecord<T>> inputRecords,
                                                                 final int numOutputGroups,
                                                                 final String instantTime,
                                                                 final Map<String, String> strategyParams,
                                                                 final HoodieSchema schema,
                                                                 final List<HoodieFileGroupId> fileGroupIdList,
                                                                 final boolean shouldPreserveHoodieMetadata,
                                                                 final Map<String, String> extraMetadata) {
    log.info("Starting clustering for a group, parallelism:{} commit:{}", numOutputGroups, instantTime);

    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withBulkInsertParallelism(numOutputGroups)
        .withProps(getWriteConfig().getProps()).build();

    newConfig.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));

    return (HoodieData<WriteStatus>) SparkBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, getHoodieTable(),
        newConfig, false, getRDDPartitioner(strategyParams, schema), true, numOutputGroups, new CreateHandleFactory(shouldPreserveHoodieMetadata));
  }
}
