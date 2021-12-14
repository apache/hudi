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

package org.apache.hudi.client.clustering.plan.strategy;

import static org.apache.hudi.config.HoodieClusteringConfig.CLUSTERING_STRATEGY_PARAM_PREFIX;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;
import org.apache.hudi.table.HoodieSparkMergeOnReadTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Clustering Strategy to filter just specified partitions from regex pattern.
 */
public class SparkRegexMatchPartitionsClusteringPlanStrategy<T extends HoodieRecordPayload<T>>
    extends SparkSizeBasedClusteringPlanStrategy<T> {
  private static final Logger LOG = LogManager.getLogger(SparkRegexMatchPartitionsClusteringPlanStrategy.class);

  public static final String CONF_REGEX_PATTERN = CLUSTERING_STRATEGY_PARAM_PREFIX + "cluster.partition.regex.pattern";

  public SparkRegexMatchPartitionsClusteringPlanStrategy(HoodieSparkCopyOnWriteTable<T> table,
                                                           HoodieSparkEngineContext engineContext,
                                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  public SparkRegexMatchPartitionsClusteringPlanStrategy(HoodieSparkMergeOnReadTable<T> table,
                                                           HoodieSparkEngineContext engineContext,
                                                           HoodieWriteConfig writeConfig) {
    super(table, engineContext, writeConfig);
  }

  @Override
  protected List<String> filterPartitionPaths(List<String> partitionPaths) {
    String partitionRegexPattern = getWriteConfig().getProps().getProperty(CONF_REGEX_PATTERN);

    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(partitionRegexPattern),
        "Please set " + CONF_REGEX_PATTERN + " when using " + this.getClass().getName());
    List<String> filteredPartitions = partitionPaths.stream()
        .filter(partition -> Pattern.matches(partitionRegexPattern, partition))
        .collect(Collectors.toList());
    LOG.info("Filtered to the following partitions: " + filteredPartitions);
    return filteredPartitions;
  }
}
