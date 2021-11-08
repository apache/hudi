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

package org.apache.hudi.client.clustering.run.strategy;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;
import org.apache.hudi.io.SingleFileHandleCreateFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * This strategy is similar to {@link SparkSortAndSizeExecutionStrategy} with the difference being that
 * there should be only one large file group per clustering group.
 */
public class SparkSingleFileSortExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends MultipleSparkJobExecutionStrategy<T> {

  private static final Logger LOG = LogManager.getLogger(SparkSingleFileSortExecutionStrategy.class);

  public SparkSingleFileSortExecutionStrategy(HoodieTable table,
                                              HoodieEngineContext engineContext,
                                              HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public JavaRDD<WriteStatus> performClusteringWithRecordsRDD(JavaRDD<HoodieRecord<T>> inputRecords,
                                                              int numOutputGroups,
                                                              String instantTime,
                                                              Map<String, String> strategyParams,
                                                              Schema schema,
                                                              List<HoodieFileGroupId> fileGroupIdList,
                                                              boolean preserveHoodieMetadata) {
    if (numOutputGroups != 1 || fileGroupIdList.size() != 1) {
      throw new HoodieClusteringException("Expect only one file group for strategy: " + getClass().getName());
    }
    LOG.info("Starting clustering for a group, parallelism:" + numOutputGroups + " commit:" + instantTime);
    Properties props = getWriteConfig().getProps();
    props.put(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key(), String.valueOf(numOutputGroups));
    // We are calling another action executor - disable auto commit. Strategy is only expected to write data in new files.
    props.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), Boolean.FALSE.toString());
    // Since clustering will write to single file group using HoodieUnboundedCreateHandle, set max file size to a large value.
    props.put(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), String.valueOf(Long.MAX_VALUE));
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();
    return (JavaRDD<WriteStatus>) SparkBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig,
        false, getPartitioner(strategyParams, schema), true, numOutputGroups, new SingleFileHandleCreateFactory(fileGroupIdList.get(0).getFileId(), preserveHoodieMetadata));
  }
}
