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

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.CreateHandleFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Uses bulk_insert to write data into new files.
 */
public class SparkSortAndSizeExecutionStrategy<T>
    extends MultipleSparkJobExecutionStrategy<T> {
  private static final Logger LOG = LogManager.getLogger(SparkSortAndSizeExecutionStrategy.class);

  public SparkSortAndSizeExecutionStrategy(HoodieTable table,
                                           HoodieEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public HoodieData<WriteStatus> performClusteringWithRecordsRDD(final HoodieData<HoodieRecord<T>> inputRecords, final int numOutputGroups,
                                                              final String instantTime, final Map<String, String> strategyParams, final Schema schema,
                                                              final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata) {
    LOG.info("Starting clustering for a group, parallelism:" + numOutputGroups + " commit:" + instantTime);

    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder()
        .withBulkInsertParallelism(numOutputGroups)
        .withProps(getWriteConfig().getProps()).build();
    newConfig.setValue(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE, String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));
    return (HoodieData<WriteStatus>) SparkBulkInsertHelper.newInstance()
        .bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig, false, getPartitioner(strategyParams, schema), true, numOutputGroups, new CreateHandleFactory(preserveHoodieMetadata));
  }
}
