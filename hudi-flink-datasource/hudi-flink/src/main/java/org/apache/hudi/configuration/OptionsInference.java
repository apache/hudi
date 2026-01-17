/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.configuration;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.common.model.PartitionBucketIndexHashingConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.util.ClientIds;
import org.apache.hudi.util.FlinkWriteClients;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.FlinkVersion;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.SchedulerExecutionMode;

/**
 * Tool helping to infer the flink options {@link FlinkOptions}.
 */
@Slf4j
public class OptionsInference {

  private static final String FLINK2_VERSION = "2.0";

  /**
   * Sets up the default source task parallelism if it is not specified.
   *
   * @param conf     The configuration
   * @param envTasks The parallelism of the execution env
   */
  public static void setupSourceTasks(Configuration conf, int envTasks) {
    if (!conf.contains(FlinkOptions.READ_TASKS)) {
      conf.set(FlinkOptions.READ_TASKS, envTasks);
    }
  }

  /**
   * Sets up the default sink tasks parallelism if it is not specified.
   *
   * @param conf     The configuration
   * @param envTasks The parallelism of the execution env
   */
  public static void setupSinkTasks(Configuration conf, int envTasks) {
    // write task number, default same as execution env tasks
    if (!conf.contains(FlinkOptions.WRITE_TASKS)) {
      conf.set(FlinkOptions.WRITE_TASKS, envTasks);
    }
    int writeTasks = conf.get(FlinkOptions.WRITE_TASKS);
    // bucket assign tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.BUCKET_ASSIGN_TASKS)) {
      conf.set(FlinkOptions.BUCKET_ASSIGN_TASKS, writeTasks);
    }
    // compaction tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.COMPACTION_TASKS)) {
      conf.set(FlinkOptions.COMPACTION_TASKS, writeTasks);
    }
    // clustering tasks, default same as write tasks
    if (!conf.contains(FlinkOptions.CLUSTERING_TASKS)) {
      conf.set(FlinkOptions.CLUSTERING_TASKS, writeTasks);
    }
  }

  /**
   * Utilities that help to auto generate the client id for multi-writer scenarios.
   * It basically handles two cases:
   *
   * <ul>
   *   <li>find the next client id for the new job;</li>
   *   <li>clean the existing inactive client heartbeat files.</li>
   * </ul>
   *
   * @see ClientIds
   */
  public static void setupClientId(Configuration conf) {
    if (OptionsResolver.isMultiWriter(conf)) {
      // explicit client id always has higher priority
      if (!conf.contains(FlinkOptions.WRITE_CLIENT_ID)) {
        try (ClientIds clientIds = ClientIds.builder().conf(conf).build()) {
          String clientId = clientIds.nextId(conf);
          conf.set(FlinkOptions.WRITE_CLIENT_ID, clientId);
        }
      }
    }
  }

  /**
   * Set up Flink runtime scheduler configurations, which is used to check whether the flink job graph is
   * generated incrementally.
   */
  public static void setupRuntimeConfigs(Configuration conf, ReadableConfig runtimeConf) {
    if (FlinkVersion.current().toString().compareTo(FLINK2_VERSION) >= 0
        && runtimeConf.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.BATCH
        && getSchedulerType(conf) == JobManagerOptions.SchedulerType.AdaptiveBatch) {
      conf.set(FlinkOptions.WRITE_INCREMENTAL_JOB_GRAPH_GENERATION, true);
    }
  }

  /**
   * Get the scheduler type for the flink job.
   *
   * <p>NOTE: referred from Flink {@code DefaultSlotPoolServiceSchedulerFactory#getSchedulerType}.
   */
  private static JobManagerOptions.SchedulerType getSchedulerType(ReadableConfig configuration) {
    if (configuration.get(JobManagerOptions.SCHEDULER_MODE) == SchedulerExecutionMode.REACTIVE
        || configuration.get(JobManagerOptions.SCHEDULER) == JobManagerOptions.SchedulerType.Adaptive) {
      // overwrite
      return JobManagerOptions.SchedulerType.AdaptiveBatch;
    } else {
      boolean isDynamicGraph = configuration.getOptional(JobManagerOptions.SCHEDULER).orElse(
          JobManagerOptions.SchedulerType.AdaptiveBatch) == JobManagerOptions.SchedulerType.AdaptiveBatch;
      return configuration.getOptional(JobManagerOptions.SCHEDULER).orElse(
          isDynamicGraph ? JobManagerOptions.SchedulerType.AdaptiveBatch : JobManagerOptions.SchedulerType.Default);
    }
  }

  /**
   * Set up Index related configs.
   * For now we will add partition level bucket index related expressions and bucket number during start-up
   * instant of loading hashing config from dfs everywhere.
   */
  public static void setupIndexConfigs(Configuration conf) {
    if (OptionsResolver.isPartitionLevelSimpleBucketIndex(conf)) {
      try (HoodieFlinkWriteClient writeClient = FlinkWriteClients.createWriteClient(conf, false, false)) {
        HoodieTableMetaClient metaClient = writeClient.getHoodieTable().getMetaClient();
        PartitionBucketIndexHashingConfig hashingConfig = PartitionBucketIndexHashingConfig.loadingLatestHashingConfig(metaClient);
        conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_EXPRESSIONS, hashingConfig.getExpressions());
        conf.set(FlinkOptions.BUCKET_INDEX_PARTITION_RULE, hashingConfig.getRule());
        conf.set(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS, hashingConfig.getDefaultBucketNumber());
        log.info("Loaded Latest Hashing Config " + hashingConfig
            + ". Reset hoodie.bucket.index.num.buckets to " + hashingConfig.getDefaultBucketNumber());
      }
    }
  }
}
