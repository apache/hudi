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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.execution.bulkinsert.RDDSpatialCurveOptimizationSortPartitioner;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;

import org.apache.avro.Schema;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.config.HoodieClusteringConfig.PLAN_STRATEGY_SORT_COLUMNS;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Uses bulk_insert to write data into new files.
 */
public class SparkSortAndSizeExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends MultipleSparkJobExecutionStrategy<T> {
  private static final Logger LOG = LogManager.getLogger(SparkSortAndSizeExecutionStrategy.class);

  public SparkSortAndSizeExecutionStrategy(HoodieTable table,
                                           HoodieEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public JavaRDD<WriteStatus> performClusteringWithRecordsRDD(final JavaRDD<HoodieRecord<T>> inputRecords, final int numOutputGroups,
                                                final String instantTime, final Map<String, String> strategyParams, final Schema schema,
                                                final List<HoodieFileGroupId> fileGroupIdList, final boolean preserveHoodieMetadata) {
    LOG.info("Starting clustering for a group, parallelism:" + numOutputGroups + " commit:" + instantTime);
    Properties props = getWriteConfig().getProps();
    props.put(HoodieWriteConfig.BULKINSERT_PARALLELISM_VALUE.key(), String.valueOf(numOutputGroups));
    // We are calling another action executor - disable auto commit. Strategy is only expected to write data in new files.
    props.put(HoodieWriteConfig.AUTO_COMMIT_ENABLE.key(), Boolean.FALSE.toString());
    props.put(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), String.valueOf(getWriteConfig().getClusteringTargetFileMaxBytes()));
    HoodieWriteConfig newConfig = HoodieWriteConfig.newBuilder().withProps(props).build();
    return (JavaRDD<WriteStatus>) SparkBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig,
        false, getPartitioner(strategyParams, schema), true, numOutputGroups, preserveHoodieMetadata);
  }

  /**
   * Create BulkInsertPartitioner based on strategy params.
   */
  protected Option<BulkInsertPartitioner<T>> getPartitioner(Map<String, String> strategyParams, Schema schema) {
    if (getWriteConfig().getSpaceFillingCurveDataOptimizeEnable()) {
      // sort input records by z-order/hilbert
      return Option.of(new RDDSpatialCurveOptimizationSortPartitioner((HoodieSparkEngineContext) getEngineContext(),
          getWriteConfig(), HoodieAvroUtils.addMetadataFields(schema)));
    } else if (strategyParams.containsKey(PLAN_STRATEGY_SORT_COLUMNS.key())) {
      return Option.of(new RDDCustomColumnsSortPartitioner(strategyParams.get(PLAN_STRATEGY_SORT_COLUMNS.key()).split(","),
          HoodieAvroUtils.addMetadataFields(schema)));
    } else {
      return Option.empty();
    }
  }
}
