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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;

import java.util.Properties;

/**
 * In this strategy, clustering group for each partition is built in the same way as {@link SparkSizeBasedClusteringPlanStrategy}.
 * The difference is that {@link HoodieClusteringConfig#PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP} is set to a small value so that
 * each file is considered as separate clustering group. This ensures that each clustering group has just one file group.
 * Since file could be huge as data will be sorted and clustered in the single file group, so
 * {@link HoodieStorageConfig#PARQUET_MAX_FILE_SIZE} is set to a large value.
 */
public class SparkSingleFileSortPlanStrategy<T extends HoodieRecordPayload<T>>
    extends SparkSizeBasedClusteringPlanStrategy<T> {

  public SparkSingleFileSortPlanStrategy(HoodieSparkCopyOnWriteTable<T> table, HoodieSparkEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkSingleFileSortPlanStrategy(HoodieSparkMergeOnReadTable<T> table, HoodieSparkEngineContext engineContext, HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  protected HoodieWriteConfig getWriteConfig() {
    Properties props = super.getWriteConfig().getProps();
    props.put(HoodieClusteringConfig.PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP.key(), String.valueOf(0));
    props.put(HoodieStorageConfig.PARQUET_MAX_FILE_SIZE.key(), String.valueOf(Long.MAX_VALUE));
    return HoodieWriteConfig.newBuilder().withProps(props).build();
  }
}
