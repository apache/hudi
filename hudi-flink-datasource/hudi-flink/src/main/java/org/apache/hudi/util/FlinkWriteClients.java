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

package org.apache.hudi.util;

import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Triple;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Locale;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;
import static org.apache.hudi.util.StreamerUtil.getLockConfig;
import static org.apache.hudi.util.StreamerUtil.getPayloadConfig;
import static org.apache.hudi.util.StreamerUtil.getSourceSchema;

/**
 * Utilities for {@link org.apache.hudi.client.HoodieFlinkWriteClient}.
 */
public class FlinkWriteClients {

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by the driver, the client can then send requests for files view.
   *
   * <p>The task context supplier is a constant: the write token is always '0-1-0'.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf) throws IOException {
    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, true, false);
    // build the write client to start the embedded timeline server
    final HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient<>(new HoodieFlinkEngineContext(HadoopConfigurations.getHadoopConf(conf)), writeConfig);
    writeClient.setOperationType(WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION)));
    // create the filesystem view storage properties for client
    final FileSystemViewStorageConfig viewStorageConfig = writeConfig.getViewStorageConfig();
    // rebuild the view storage config with simplified options.
    FileSystemViewStorageConfig rebuilt = FileSystemViewStorageConfig.newBuilder()
        .withStorageType(viewStorageConfig.getStorageType())
        .withRemoteServerHost(viewStorageConfig.getRemoteViewServerHost())
        .withRemoteServerPort(viewStorageConfig.getRemoteViewServerPort())
        .withRemoteTimelineClientTimeoutSecs(viewStorageConfig.getRemoteTimelineClientTimeoutSecs())
        .withRemoteTimelineClientRetry(viewStorageConfig.isRemoteTimelineClientRetryEnabled())
        .withRemoteTimelineClientMaxRetryNumbers(viewStorageConfig.getRemoteTimelineClientMaxRetryNumbers())
        .withRemoteTimelineInitialRetryIntervalMs(viewStorageConfig.getRemoteTimelineInitialRetryIntervalMs())
        .withRemoteTimelineClientMaxRetryIntervalMs(viewStorageConfig.getRemoteTimelineClientMaxRetryIntervalMs())
        .withRemoteTimelineClientRetryExceptions(viewStorageConfig.getRemoteTimelineClientRetryExceptions())
        .build();
    ViewStorageProperties.createProperties(conf.get(FlinkOptions.PATH), rebuilt, conf);
    return writeClient;
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by the driver, the client can then send requests for files view.
   *
   * <p>The task context supplier is a constant: the write token is always '0-1-0'.
   *
   * <p>Note: different with {@link #createWriteClient}, the fs view storage options are set into the given
   * configuration {@code conf}.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClientV2(Configuration conf) {
    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, true, false);
    // build the write client to start the embedded timeline server
    final HoodieFlinkWriteClient writeClient = new HoodieFlinkWriteClient<>(new HoodieFlinkEngineContext(HadoopConfigurations.getHadoopConf(conf)), writeConfig);
    writeClient.setOperationType(WriteOperationType.fromValue(conf.get(FlinkOptions.OPERATION)));
    // create the filesystem view storage properties for client
    final FileSystemViewStorageConfig viewStorageConfig = writeConfig.getViewStorageConfig();
    conf.setString(FileSystemViewStorageConfig.VIEW_TYPE.key(), viewStorageConfig.getStorageType().name());
    conf.setString(FileSystemViewStorageConfig.REMOTE_HOST_NAME.key(), viewStorageConfig.getRemoteViewServerHost());
    conf.setString(FileSystemViewStorageConfig.REMOTE_PORT_NUM.key(), viewStorageConfig.getRemoteViewServerPort() + "");
    return writeClient;
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by client, the driver should start an embedded timeline server.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext) {
    return createWriteClient(conf, runtimeContext, true);
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by client, set flag {@code loadFsViewStorageConfig} to use
   * remote filesystem view storage config, or an in-memory filesystem view storage is used.
   */
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, boolean enableEmbeddedTimelineService, boolean loadFsViewStorageConfig) {
    HoodieFlinkEngineContext context = new HoodieFlinkEngineContext(HadoopConfigurations.getHadoopConf(conf));
    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, enableEmbeddedTimelineService, loadFsViewStorageConfig);
    return new HoodieFlinkWriteClient<>(context, writeConfig);
  }

  /**
   * Creates the Flink write client.
   *
   * <p>This expects to be used by client, set flag {@code loadFsViewStorageConfig} to use
   * remote filesystem view storage config, or an in-memory filesystem view storage is used.
   */
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext, boolean loadFsViewStorageConfig) {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            HadoopFSUtils.getStorageConf(HadoopConfigurations.getHadoopConf(conf)),
            new FlinkTaskContextSupplier(runtimeContext));

    HoodieWriteConfig writeConfig = getHoodieClientConfig(conf, loadFsViewStorageConfig);
    return new HoodieFlinkWriteClient<>(context, writeConfig);
  }

  /**
   * Mainly used for tests.
   */
  public static HoodieWriteConfig getHoodieClientConfig(Configuration conf) {
    return getHoodieClientConfig(conf, false, false);
  }

  public static HoodieWriteConfig getHoodieClientConfig(Configuration conf, boolean loadFsViewStorageConfig) {
    return getHoodieClientConfig(conf, false, loadFsViewStorageConfig);
  }

  public static HoodieWriteConfig getHoodieClientConfig(
      Configuration conf,
      boolean enableEmbeddedTimelineService,
      boolean loadFsViewStorageConfig) {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withEngineType(EngineType.FLINK)
            .withPath(conf.get(FlinkOptions.PATH))
            .combineInput(conf.get(FlinkOptions.PRE_COMBINE), true)
            .withWriteTableVersion(conf.get(FlinkOptions.WRITE_TABLE_VERSION))
            .withMergeAllowDuplicateOnInserts(OptionsResolver.insertClustering(conf))
            .withClusteringConfig(
                HoodieClusteringConfig.newBuilder()
                    .withClusteringPlanStrategyClass(conf.getString(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS.key(), OptionsResolver.getDefaultPlanStrategyClassName(conf)))
                    .withClusteringPlanPartitionFilterMode(
                        ClusteringPlanPartitionFilterMode.valueOf(conf.get(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME)))
                    .withClusteringTargetPartitions(conf.get(FlinkOptions.CLUSTERING_TARGET_PARTITIONS))
                    .withClusteringMaxNumGroups(conf.get(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS))
                    .withClusteringTargetFileMaxBytes(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES))
                    .withClusteringPlanSmallFileLimit(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT) * 1024 * 1024L)
                    .withClusteringSkipPartitionsFromLatest(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST))
                    .withClusteringPartitionFilterBeginPartition(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION))
                    .withClusteringPartitionFilterEndPartition(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION))
                    .withClusteringPartitionRegexPattern(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN))
                    .withClusteringPartitionSelected(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_SELECTED))
                    .withAsyncClusteringMaxCommits(conf.get(FlinkOptions.CLUSTERING_DELTA_COMMITS))
                    .withClusteringSortColumns(conf.get(FlinkOptions.CLUSTERING_SORT_COLUMNS))
                    .withScheduleInlineClustering(conf.get(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED))
                    .withAsyncClustering(conf.get(FlinkOptions.CLUSTERING_ASYNC_ENABLED))
                    .build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withAsyncClean(conf.get(FlinkOptions.CLEAN_ASYNC_ENABLED))
                .retainCommits(conf.get(FlinkOptions.CLEAN_RETAIN_COMMITS))
                .cleanerNumHoursRetained(conf.get(FlinkOptions.CLEAN_RETAIN_HOURS))
                .retainFileVersions(conf.get(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS))
                // override and hardcode to 20,
                // actually Flink cleaning is always with parallelism 1 now
                .withCleanerParallelism(20)
                .withCleanerPolicy(HoodieCleaningPolicy.valueOf(conf.get(FlinkOptions.CLEAN_POLICY)))
                .build())
            .withArchivalConfig(HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(conf.get(FlinkOptions.ARCHIVE_MIN_COMMITS), conf.get(FlinkOptions.ARCHIVE_MAX_COMMITS))
                .build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withTargetIOPerCompactionInMB(conf.get(FlinkOptions.COMPACTION_TARGET_IO))
                .withInlineCompactionTriggerStrategy(
                    CompactionTriggerStrategy.valueOf(conf.get(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toUpperCase(Locale.ROOT)))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.get(FlinkOptions.COMPACTION_DELTA_COMMITS))
                .withMaxDeltaSecondsBeforeCompaction(conf.get(FlinkOptions.COMPACTION_DELTA_SECONDS))
                .build())
            .withMemoryConfig(
                HoodieMemoryConfig.newBuilder()
                    .withMaxMemoryMaxSize(
                        conf.get(FlinkOptions.WRITE_MERGE_MAX_MEMORY) * 1024 * 1024L,
                        conf.get(FlinkOptions.COMPACTION_MAX_MEMORY) * 1024 * 1024L
                    ).build())
            .forTable(conf.get(FlinkOptions.TABLE_NAME))
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                .logFileDataBlockMaxSize((long) conf.get(FlinkOptions.WRITE_LOG_BLOCK_SIZE) * 1024 * 1024)
                .logFileMaxSize(conf.get(FlinkOptions.WRITE_LOG_MAX_SIZE) * 1024 * 1024)
                .parquetBlockSize(conf.get(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE) * 1024 * 1024)
                .parquetPageSize(conf.get(FlinkOptions.WRITE_PARQUET_PAGE_SIZE) * 1024 * 1024)
                .parquetMaxFileSize(conf.get(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE) * 1024 * 1024L)
                .withWriteUtcTimezone(conf.get(FlinkOptions.WRITE_UTC_TIMEZONE))
                .build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                .withEngineType(EngineType.FLINK) // this affects the default value inference
                .enable(conf.get(FlinkOptions.METADATA_ENABLED))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.get(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS))
                .build())
            .withIndexConfig(StreamerUtil.getIndexConfig(conf))
            .withPayloadConfig(getPayloadConfig(conf))
            .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineService)
            .withEmbeddedTimelineServerReuseEnabled(true) // make write client embedded timeline service singleton
            .withAllowOperationMetadataField(conf.get(FlinkOptions.CHANGELOG_ENABLED))
            .withProps(flinkConf2TypedProperties(conf))
            .withSchema(getSourceSchema(conf).toString());

    // <merge_mode, payload_class, merge_strategy_id>
    Triple<RecordMergeMode, String, String> mergingBehavior = StreamerUtil.inferMergingBehavior(conf);
    builder.withRecordMergeMode(mergingBehavior.getLeft())
        .withRecordMergeImplClasses(StreamerUtil.getMergerClasses(conf, mergingBehavior.getLeft(), mergingBehavior.getMiddle()));
    if (mergingBehavior.getRight() != null) {
      builder.withRecordMergeStrategyId(mergingBehavior.getRight());
    }

    Option<HoodieLockConfig> lockConfig = getLockConfig(conf);
    if (lockConfig.isPresent()) {
      builder.withLockConfig(lockConfig.get());
    }

    HoodieWriteConfig writeConfig = builder.build();
    if (!OptionsResolver.isBlockingInstantGeneration(conf)) {
      // always LAZY for non-blocking instant time generation.
      writeConfig.setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(),
          HoodieFailedWritesCleaningPolicy.LAZY.name());
    }
    if (loadFsViewStorageConfig && !conf.containsKey(FileSystemViewStorageConfig.REMOTE_HOST_NAME.key())) {
      // do not use the builder to give a change for recovering the original fs view storage config
      FileSystemViewStorageConfig viewStorageConfig = ViewStorageProperties.loadFromProperties(conf.get(FlinkOptions.PATH), conf);
      writeConfig.setViewStorageConfig(viewStorageConfig);
    }
    return writeConfig;
  }
}
