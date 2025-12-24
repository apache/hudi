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

import org.apache.hudi.client.FlinkTaskContextSupplier;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieMemoryConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.sink.clustering.LSMClusteringScheduleMode;
import org.apache.hudi.table.action.cluster.ClusteringPlanPartitionFilterMode;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.Locale;

import static org.apache.hudi.util.StreamerUtil.flinkConf2TypedProperties;
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
    writeClient.setOperationType(WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
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
    ViewStorageProperties.createProperties(conf.getString(FlinkOptions.PATH), rebuilt, conf);
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
    writeClient.setOperationType(WriteOperationType.fromValue(conf.getString(FlinkOptions.OPERATION)));
    // create the filesystem view storage properties for client
    final FileSystemViewStorageConfig viewStorageConfig = writeConfig.getViewStorageConfig();
    conf.setString(FileSystemViewStorageConfig.VIEW_TYPE.key(), viewStorageConfig.getStorageType().name());
    conf.setString(FileSystemViewStorageConfig.REMOTE_HOST_NAME.key(), viewStorageConfig.getRemoteViewServerHost());
    conf.setInteger(FileSystemViewStorageConfig.REMOTE_PORT_NUM.key(), viewStorageConfig.getRemoteViewServerPort());
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
  @SuppressWarnings("rawtypes")
  public static HoodieFlinkWriteClient createWriteClient(Configuration conf, RuntimeContext runtimeContext, boolean loadFsViewStorageConfig) {
    HoodieFlinkEngineContext context =
        new HoodieFlinkEngineContext(
            new SerializableConfiguration(HadoopConfigurations.getHadoopConf(conf)),
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
            .withPath(conf.getString(FlinkOptions.PATH))
            .withRecordMergerImpls(conf.getString(FlinkOptions.RECORD_MERGER_IMPLS))
            .withRecordMergerStrategy(conf.getString(FlinkOptions.RECORD_MERGER_STRATEGY))
            .withFlushConcurrency(conf.getInteger(FlinkOptions.WRITE_BULK_INSERT_FLUSH_CONCURRENCY))
            .combineInput(conf.getBoolean(FlinkOptions.PRE_COMBINE), true)
            .withMergeAllowDuplicateOnInserts(OptionsResolver.insertClustering(conf))
            .withOperation(conf.getString(FlinkOptions.OPERATION))
            .withStorageStrategyClass(conf.getString(FlinkOptions.STORAGE_STRATEGY))
            .withStoragePath(conf.getString(FlinkOptions.STORAGE_PATH))
            .withClusteringConfig(
                HoodieClusteringConfig.newBuilder()
                    // clustering conf for LSM
                    .withLsmClusteringPlanStrategyClass(conf.getString(FlinkOptions.LSM_CLUSTERING_PLAN_STRATEGY_CLASS))
                    .withLsmClusteringPlanNumSortRunTrigger(conf.getInteger(FlinkOptions.NUM_RUN_CLUSTERING_TRIGGER))
                    .withLsmClusteringPlanMaxSizeAmp(conf.getInteger(FlinkOptions.MAX_SIZE_AMP))
                    .withLsmClusteringReadFooterEnabled(conf.getBoolean(FlinkOptions.READFOOTER_ENABLED))
                    .withLsmAsyncClusteringSchedule(conf.getString(FlinkOptions.LSM_CLUSTERING_SCHEDULE_MODE).equalsIgnoreCase(LSMClusteringScheduleMode.ASYNC.name()))
                    .withLsmAsyncClusteringMaxCommits(conf.getInteger(FlinkOptions.LSM_CLUSTERING_DELTA_COMMITS))
                    .withLsmMaxOfPendingClustering(conf.getInteger(FlinkOptions.LSM_CLUSTERING_PENDING_MAX_NUM))
                    .withLsmClusteringPlanSmallFileLimit(conf.getLong(FlinkOptions.LSM_CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT))
                    .withLsmClusteringMaxBytesInGroup(conf.getLong(FlinkOptions.LSM_PLAN_STRATEGY_MAX_BYTES_PER_OUTPUT_FILEGROUP))
                    .withLsmClusteringMinNumGroups(conf.getInteger(FlinkOptions.LSM_CLUSTERING_MIN_NUM_GROUPS))
                    // clustreing conf before
                    .withAsyncClustering(conf.getBoolean(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED))
                    .withClusteringPlanStrategyClass(conf.getString(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLASS))
                    .withClusteringPlanPartitionFilterMode(
                        ClusteringPlanPartitionFilterMode.valueOf(conf.getString(FlinkOptions.CLUSTERING_PLAN_PARTITION_FILTER_MODE_NAME)))
                    .withClusteringTargetPartitions(conf.getInteger(FlinkOptions.CLUSTERING_TARGET_PARTITIONS))
                    .withClusteringMaxNumGroups(conf.getInteger(FlinkOptions.CLUSTERING_MAX_NUM_GROUPS))
                    .withClusteringTargetFileMaxBytes(conf.getLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_TARGET_FILE_MAX_BYTES))
                    .withClusteringPlanSmallFileLimit(conf.getLong(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SMALL_FILE_LIMIT) * 1024 * 1024L)
                    .withClusteringSkipPartitionsFromLatest(conf.getInteger(FlinkOptions.CLUSTERING_PLAN_STRATEGY_SKIP_PARTITIONS_FROM_LATEST))
                    .withClusteringPartitionFilterBeginPartition(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_BEGIN_PARTITION))
                    .withClusteringPartitionFilterEndPartition(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_CLUSTER_END_PARTITION))
                    .withClusteringPartitionRegexPattern(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_REGEX_PATTERN))
                    .withClusteringPartitionSelected(conf.get(FlinkOptions.CLUSTERING_PLAN_STRATEGY_PARTITION_SELECTED))
                    .withAsyncClusteringMaxCommits(conf.getInteger(FlinkOptions.CLUSTERING_DELTA_COMMITS))
                    .withClusteringSortColumns(conf.getString(FlinkOptions.CLUSTERING_SORT_COLUMNS))
                    .withMaxOfPendingClustering(conf.getInteger(FlinkOptions.CLUSTERING_PENDING_MAX_NUM))
                    .build())
            .withCleanConfig(HoodieCleanConfig.newBuilder()
                .withAsyncClean(conf.getBoolean(FlinkOptions.CLEAN_ASYNC_ENABLED))
                .retainCommits(conf.getInteger(FlinkOptions.CLEAN_RETAIN_COMMITS))
                .cleanerNumHoursRetained(conf.getInteger(FlinkOptions.CLEAN_RETAIN_HOURS))
                .retainFileVersions(conf.getInteger(FlinkOptions.CLEAN_RETAIN_FILE_VERSIONS))
                // override and hardcode to 20,
                // actually Flink cleaning is always with parallelism 1 now
                .withCleanerParallelism(20)
                .withCleanerPolicy(HoodieCleaningPolicy.valueOf(conf.getString(FlinkOptions.CLEAN_POLICY)))
                .build())
            .withAysncRollback(conf.getBoolean(FlinkOptions.ROLLBACK_ASYNC_ENABLE))
            .withArchivalConfig(HoodieArchivalConfig.newBuilder()
                .archiveCommitsWith(conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS), conf.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS))
                .archiveCleanCommitsWith(conf.getInteger(FlinkOptions.ARCHIVE_MIN_COMMITS_CLEAN), conf.getInteger(FlinkOptions.ARCHIVE_MAX_COMMITS_CLEAN))
                .build())
            .withCompactionConfig(HoodieCompactionConfig.newBuilder()
                .withTargetIOPerCompactionInMB(conf.getLong(FlinkOptions.COMPACTION_TARGET_IO))
                .withInlineCompactionTriggerStrategy(
                    CompactionTriggerStrategy.valueOf(conf.getString(FlinkOptions.COMPACTION_TRIGGER_STRATEGY).toUpperCase(Locale.ROOT)))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_COMMITS))
                .withMaxDeltaSecondsBeforeCompaction(conf.getInteger(FlinkOptions.COMPACTION_DELTA_SECONDS))
                .withMaxOfPendingCompaction(conf.getInteger(FlinkOptions.COMPACTION_PENDING_MAX_NUM))
                .withIncrementalPartitions(conf.getBoolean(FlinkOptions.COMPACTION_SCHEDULE_INCREMENTAL_PARTITIONS))
                .build())
            .withMemoryConfig(
                HoodieMemoryConfig.newBuilder()
                    .withMaxMemoryMaxSize(
                        conf.getInteger(FlinkOptions.WRITE_MERGE_MAX_MEMORY) * 1024 * 1024L,
                        conf.getInteger(FlinkOptions.COMPACTION_MAX_MEMORY) * 1024 * 1024L
                    ).build())
            .forTable(conf.getString(FlinkOptions.TABLE_NAME))
            .withStorageConfig(HoodieStorageConfig.newBuilder()
                .logFileDataBlockMaxSize(conf.getInteger(FlinkOptions.WRITE_LOG_BLOCK_SIZE) * 1024 * 1024)
                .logFileMaxSize(conf.getLong(FlinkOptions.WRITE_LOG_MAX_SIZE) * 1024 * 1024)
                .logFileRolloverDirectly(conf.getBoolean(FlinkOptions.WRITE_LOG_ROLLOVER_DIRECTLY))
                .parquetBlockSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_BLOCK_SIZE) * 1024 * 1024)
                .parquetPageSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_PAGE_SIZE) * 1024 * 1024)
                .parquetMaxFileSize(conf.getInteger(FlinkOptions.WRITE_PARQUET_MAX_FILE_SIZE) * 1024 * 1024L)
                .dataSketchEnabled(conf.getBoolean(FlinkOptions.DATASKETCH_ENABLED))
                .parquetRecordKeyBloomFilterEnabled(conf.getBoolean(FlinkOptions.PARQUET_RECORDKEY_BLOOM_FILTER_ENABLED))
                .parquetRecordKeyClusteringBloomFilterEnabled(conf.getBoolean(FlinkOptions.PARQUET_RECORDKEY_CLUSTERING_BLOOM_FILTER_ENABLED))
                .parquetRecordKeyCompactionBloomFilterEnabled(conf.getBoolean(FlinkOptions.PARQUET_RECORDKEY_COMPACTION_BLOOM_FILTER_ENABLED))
                .build())
            .withMetadataConfig(HoodieMetadataConfig.newBuilder()
                .enable(conf.getBoolean(FlinkOptions.METADATA_ENABLED))
                .withMaxNumDeltaCommitsBeforeCompaction(conf.getInteger(FlinkOptions.METADATA_COMPACTION_DELTA_COMMITS))
                .build())
            .withPayloadConfig(getPayloadConfig(conf))
            .withEmbeddedTimelineServerEnabled(enableEmbeddedTimelineService)
            .withEmbeddedTimelineServerReuseEnabled(true) // make write client embedded timeline service singleton
            .withAutoCommit(false)
            .withAllowOperationMetadataField(conf.getBoolean(FlinkOptions.CHANGELOG_ENABLED))
            .withProps(flinkConf2TypedProperties(conf))
            .withSchema(getSourceSchema(conf).toString())
            .withWriteIgnoreFailed(conf.get(FlinkOptions.IGNORE_FAILED));

    if (conf.getString(FlinkOptions.INDEX_TYPE).equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name())) {
      builder.withIndexConfig(HoodieIndexConfig.newBuilder()
          .withIndexType(HoodieIndex.IndexType.BUCKET)
          .withIndexKeyField(conf.getString(FlinkOptions.INDEX_KEY_FIELD))
          .withIndexRecordKeyField(conf.getString(FlinkOptions.RECORD_KEY_FIELD))
          .withBucketNum(String.valueOf(conf.getInteger(FlinkOptions.BUCKET_INDEX_NUM_BUCKETS)))
          .withBucketIndexAtPartitionLevel(conf.getBoolean(FlinkOptions.BUCKET_INDEX_PARTITION_LEVEL))
          .withPartitionBucketExpr(conf.getString(FlinkOptions.BUCKET_INDEX_PARTITION_BUCKET_EXPR))
          .build());
    }

    if (conf.getBoolean(FlinkOptions.METADATA_ENABLED)) {
      builder.withWriteConcurrencyMode(WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL);
      if (!conf.containsKey(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key())) {
        builder.withLockConfig(HoodieLockConfig.newBuilder()
            .withLockProvider(FileSystemBasedLockProvider.class)
            .withLockWaitTimeInMillis(2000L) // 2s
            .withFileSystemLockExpire(1) // 1 minute
            .withClientNumRetries(30)
            .withFileSystemLockPath(StreamerUtil.getAuxiliaryPath(conf))
            .build());
      }
    }

    // do not configure cleaning strategy as LAZY until multi-writers is supported.
    HoodieWriteConfig writeConfig = builder.build();
    if (loadFsViewStorageConfig && !conf.containsKey(FileSystemViewStorageConfig.REMOTE_HOST_NAME.key())) {
      // do not use the builder to give a change for recovering the original fs view storage config
      FileSystemViewStorageConfig viewStorageConfig = ViewStorageProperties.loadFromProperties(conf.getString(FlinkOptions.PATH), conf);
      writeConfig.setViewStorageConfig(viewStorageConfig);
    }
    return writeConfig;
  }
}
