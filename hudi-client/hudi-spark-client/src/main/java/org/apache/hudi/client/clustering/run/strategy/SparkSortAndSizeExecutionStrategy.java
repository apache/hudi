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

import org.apache.avro.Schema;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.execution.bulkinsert.RDDCustomColumnsSortPartitioner;
import org.apache.hudi.table.BulkInsertPartitioner;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.hudi.table.action.cluster.strategy.ClusteringExecutionStrategy;
import org.apache.hudi.table.action.commit.SparkBulkInsertHelper;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.config.HoodieClusteringConfig.CLUSTERING_SORT_COLUMNS_PROPERTY;

/**
 * Clustering Strategy based on following.
 * 1) Spark execution engine.
 * 2) Uses bulk_insert to write data into new files.
 */
public class SparkSortAndSizeExecutionStrategy<T extends HoodieRecordPayload<T>>
    extends ClusteringExecutionStrategy<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>> {
  private static final Logger LOG = LogManager.getLogger(SparkSortAndSizeExecutionStrategy.class);

  public SparkSortAndSizeExecutionStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                           HoodieSparkEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkSortAndSizeExecutionStrategy(HoodieSparkMergeOnReadTable<T> table,
                                           HoodieSparkEngineContext engineContext,
                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  public JavaRDD<WriteStatus> performClustering(final JavaRDD<HoodieRecord<T>> inputRecords, final int numOutputGroups,
                                                final String instantTime, final Map<String, String> strategyParams, final Schema schema) {
    Properties props = getWriteConfig().getProps();
    props.put(HoodieWriteConfig.BULKINSERT_PARALLELISM, String.valueOf(numOutputGroups));
    // We are calling another action executor - disable auto commit. Strategy is only expected to write data in new files.
    props.put(HoodieWriteConfig.HOODIE_AUTO_COMMIT_PROP, Boolean.FALSE.toString());
    HoodieWriteConfig newConfig =  HoodieWriteConfig.newBuilder().withProps(props).build();
    return (JavaRDD<WriteStatus>) SparkBulkInsertHelper.newInstance().bulkInsert(inputRecords, instantTime, getHoodieTable(), newConfig,
        false, getPartitioner(strategyParams, schema), true, numOutputGroups);
  }

  /**
   * Create BulkInsertPartitioner based on strategy params.
   */
  protected Option<BulkInsertPartitioner<T>> getPartitioner(Map<String, String> strategyParams, Schema schema) {
    if (strategyParams.containsKey(CLUSTERING_SORT_COLUMNS_PROPERTY)) {
      return Option.of(new RDDCustomColumnsSortPartitioner(strategyParams.get(CLUSTERING_SORT_COLUMNS_PROPERTY).split(","),
          HoodieAvroUtils.addMetadataFields(schema)));
    } else {
      return Option.empty();
    }
  }
}
