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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.FailOnFirstErrorWriteStatus;
import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SpillableMapBasedFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.config.metrics.HoodieMetricsM3Config;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.util.Lazy;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ASYNC_CLEAN;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.StringUtils.isNullOrEmpty;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_PARTITION_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToBloomFilterRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToColumnStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToPartitionStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToRecordIndexRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.generateColumnStatsKeys;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getColumnsToIndex;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.translateWriteStatToFileStats;

/**
 * Metadata table write utils.
 */
public class HoodieMetadataWriteUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieMetadataWriteUtils.class);
  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  public static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  // MDT writes are always prepped. Hence, insert and upsert shuffle parallelism are not important to be configured. Same for delete
  // parallelism as deletes are not used.
  // The finalize, cleaner and rollback tasks will operate on each fileGroup so their parallelism should be as large as the total file groups.
  // But it's not possible to accurately get the file group count here so keeping these values large enough. This parallelism would
  // any ways be limited by the executor counts.
  private static final int MDT_DEFAULT_PARALLELISM = 512;

  // File groups in each partition are fixed at creation time and we do not want them to be split into multiple files
  // ever. Hence, we use a very large basefile size in metadata table. The actual size of the HFiles created will
  // eventually depend on the number of file groups selected for each partition (See estimateFileGroupCount function)
  private static final long MDT_MAX_HFILE_SIZE_BYTES = 10 * 1024 * 1024 * 1024L; // 10GB

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   *
   * @param writeConfig                {@code HoodieWriteConfig} of the main dataset writer
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   */
  @VisibleForTesting
  public static HoodieWriteConfig createMetadataWriteConfig(
      HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy,
      HoodieTableVersion datatableVersion) {
    String tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
    boolean isStreamingWritesToMetadataEnabled = writeConfig.isMetadataStreamingWritesEnabled(datatableVersion);
    WriteConcurrencyMode concurrencyMode = isStreamingWritesToMetadataEnabled
        ? WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL : WriteConcurrencyMode.SINGLE_WRITER;
    HoodieLockConfig lockConfig = isStreamingWritesToMetadataEnabled
        ? HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build() : HoodieLockConfig.newBuilder().build();
    // HUDI-9407 tracks adding support for separate lock configuration for MDT. Until then, all writes to MDT will happen within data table lock.

    if (isStreamingWritesToMetadataEnabled) {
      failedWritesCleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
    }

    final long maxLogFileSizeBytes = writeConfig.getMetadataConfig().getMaxLogFileSize();
    // Borrow the cleaner policy from the main table and adjust the cleaner policy based on the main table's cleaner policy
    HoodieCleaningPolicy dataTableCleaningPolicy = writeConfig.getCleanerPolicy();
    HoodieCleanConfig.Builder cleanConfigBuilder = HoodieCleanConfig.newBuilder()
        .withAsyncClean(DEFAULT_METADATA_ASYNC_CLEAN)
        .withAutoClean(false)
        .withCleanerParallelism(MDT_DEFAULT_PARALLELISM)
        .withFailedWritesCleaningPolicy(failedWritesCleaningPolicy)
        .withCleanerPolicy(dataTableCleaningPolicy);

    if (HoodieCleaningPolicy.KEEP_LATEST_COMMITS.equals(dataTableCleaningPolicy)) {
      int retainCommits = (int) Math.max(DEFAULT_METADATA_CLEANER_COMMITS_RETAINED, writeConfig.getCleanerCommitsRetained() * 1.2);
      cleanConfigBuilder.retainCommits(retainCommits);
    } else if (HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS.equals(dataTableCleaningPolicy)) {
      int retainFileVersions = (int) Math.ceil(writeConfig.getCleanerFileVersionsRetained() * 1.2);
      cleanConfigBuilder.retainFileVersions(retainFileVersions);
    } else if (HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS.equals(dataTableCleaningPolicy)) {
      int numHoursRetained = (int) Math.ceil(writeConfig.getCleanerHoursRetained() * 1.2);
      cleanConfigBuilder.cleanerNumHoursRetained(numHoursRetained);
    }

    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withEngineType(writeConfig.getEngineType())
        .withWriteTableVersion(writeConfig.getWriteVersion().versionCode())
        .withMergeAllowDuplicateOnInserts(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false)
            .withFileListingParallelism(writeConfig.getFileListingParallelism()).build())
        .withAvroSchemaValidate(false)
        .withEmbeddedTimelineServerEnabled(false)
        .withMarkersType(MarkerType.DIRECT.name())
        .withRollbackUsingMarkers(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        // we will trigger cleaning manually, to control the instant times
        .withCleanConfig(cleanConfigBuilder.build())
        // we will trigger archive manually, to ensure only regular writer invokes it
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(
                writeConfig.getMinCommitsToKeep() + 1, writeConfig.getMaxCommitsToKeep() + 1)
            .withAutoArchive(false)
            .build())
        // we will trigger compaction manually, to control the instant times
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax())
            .withEnableOptimizedLogBlocksScan(String.valueOf(writeConfig.enableOptimizedLogBlocksScan()))
            // Compaction on metadata table is used as a barrier for archiving on main dataset and for validating the
            // deltacommits having corresponding completed commits. Therefore, we need to compact all fileslices of all
            // partitions together requiring UnBoundedCompactionStrategy.
            .withCompactionStrategy(new UnBoundedCompactionStrategy())
            // Check if log compaction is enabled, this is needed for tables with a lot of records.
            .withLogCompactionEnabled(writeConfig.isLogCompactionEnabledOnMetadata())
            // Below config is only used if isLogCompactionEnabled is set.
            .withLogCompactionBlocksThreshold(writeConfig.getMetadataLogCompactBlocksThreshold())
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(MDT_MAX_HFILE_SIZE_BYTES)
            .logFileMaxSize(maxLogFileSizeBytes)
            // Keeping the log blocks as large as the log files themselves reduces the number of HFile blocks to be checked for
            // presence of keys
            .logFileDataBlockMaxSize(maxLogFileSizeBytes)
                               .withBloomFilterType(writeConfig.getMetadataConfig().getBloomFilterType())
                               .withBloomFilterNumEntries(writeConfig.getMetadataConfig().getBloomFilterNumEntries())
                               .withBloomFilterFpp(writeConfig.getMetadataConfig().getBloomFilterFpp())
                               .withBloomFilterDynamicMaxEntries(writeConfig.getMetadataConfig().getDynamicBloomFilterMaxNumEntries())
                               .build())
        .withRollbackParallelism(MDT_DEFAULT_PARALLELISM)
        .withFinalizeWriteParallelism(MDT_DEFAULT_PARALLELISM)
        .withKeyGenerator(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .withPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS)
        .withWriteStatusClass(FailOnFirstErrorWriteStatus.class)
        .withReleaseResourceEnabled(writeConfig.areReleaseResourceEnabled())
        .withRecordMergeMode(RecordMergeMode.CUSTOM)
        .withRecordMergeStrategyId(HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID)
        .withPayloadConfig(HoodiePayloadConfig.newBuilder()
            .withPayloadClass(HoodieMetadataPayload.class.getCanonicalName()).build())
        .withRecordMergeImplClasses(HoodieAvroRecordMerger.class.getCanonicalName())
        .withWriteRecordPositionsEnabled(false)
        .withWriteConcurrencyMode(concurrencyMode)
        .withLockConfig(lockConfig);

    // RecordKey properties are needed for the metadata table records
    final Properties properties = new Properties();
    properties.put(HoodieTableConfig.TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), RECORD_KEY_FIELD_NAME);
    properties.put("hoodie.datasource.write.recordkey.field", RECORD_KEY_FIELD_NAME);
    if (nonEmpty(writeConfig.getMetricReporterMetricsNamePrefix())) {
      properties.put(HoodieMetricsConfig.METRICS_REPORTER_PREFIX.key(),
          writeConfig.getMetricReporterMetricsNamePrefix() + METADATA_TABLE_NAME_SUFFIX);
    }
    // HFile caching properties
    properties.put(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED.key(),
        writeConfig.getBooleanOrDefault(HoodieReaderConfig.HFILE_BLOCK_CACHE_ENABLED));
    properties.put(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE.key(),
        writeConfig.getIntOrDefault(HoodieReaderConfig.HFILE_BLOCK_CACHE_SIZE));
    properties.put(HoodieReaderConfig.HFILE_BLOCK_CACHE_TTL_MINUTES.key(),
        writeConfig.getIntOrDefault(HoodieReaderConfig.HFILE_BLOCK_CACHE_TTL_MINUTES));
    builder.withProperties(properties);

    if (writeConfig.isMetricsOn()) {
      // Table Name is needed for metric reporters prefix
      Properties commonProperties = new Properties();
      commonProperties.put(HoodieWriteConfig.TBL_NAME.key(), tableName);

      builder.withMetricsConfig(HoodieMetricsConfig.newBuilder()
          .fromProperties(commonProperties)
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .withMetricsReporterMetricNamePrefix(writeConfig.getMetricReporterMetricsNamePrefix() + "_" + HoodieTableMetaClient.METADATA_STR)
          .on(true).build());
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          builder.withMetricsGraphiteConfig(HoodieMetricsGraphiteConfig.newBuilder()
              .onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix()).build());
          break;
        case JMX:
          builder.withMetricsJmxConfig(HoodieMetricsJmxConfig.newBuilder()
              .onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost())
              .build());
          break;
        case PROMETHEUS_PUSHGATEWAY:
          HoodieMetricsPrometheusConfig pushGatewayConfig = HoodieMetricsPrometheusConfig.newBuilder()
              .withPushgatewayJobname(writeConfig.getPushGatewayJobName())
              .withPushgatewayRandomJobnameSuffix(writeConfig.getPushGatewayRandomJobNameSuffix())
              .withPushgatewayLabels(writeConfig.getPushGatewayLabels())
              .withPushgatewayReportPeriodInSeconds(String.valueOf(writeConfig.getPushGatewayReportPeriodSeconds()))
              .withPushgatewayHostName(writeConfig.getPushGatewayHost())
              .withPushgatewayPortNum(writeConfig.getPushGatewayPort()).build();
          builder.withProperties(pushGatewayConfig.getProps());
          break;
        case M3:
          HoodieMetricsM3Config m3Config = HoodieMetricsM3Config.newBuilder()
              .onM3Port(writeConfig.getM3ServerPort())
              .toM3Host(writeConfig.getM3ServerHost())
              .useM3Tags(writeConfig.getM3Tags())
              .useM3Service(writeConfig.getM3Service())
              .useM3Env(writeConfig.getM3Env()).build();
          builder.withProperties(m3Config.getProps());
          break;
        case DATADOG:
          HoodieMetricsDatadogConfig.Builder datadogConfig = HoodieMetricsDatadogConfig.newBuilder()
                  .withDatadogApiKey(writeConfig.getDatadogApiKey())
                  .withDatadogApiKeySkipValidation(writeConfig.getDatadogApiKeySkipValidation())
                  .withDatadogPrefix(writeConfig.getDatadogMetricPrefix())
                  .withDatadogReportPeriodSeconds(writeConfig.getDatadogReportPeriodSeconds())
                  .withDatadogTags(String.join(",", writeConfig.getDatadogMetricTags()))
                  .withDatadogApiTimeoutSeconds(writeConfig.getDatadogApiTimeoutSeconds());
          if (writeConfig.getDatadogMetricHost() != null) {
            datadogConfig = datadogConfig.withDatadogHost(writeConfig.getDatadogMetricHost());
          }
          if (writeConfig.getDatadogApiSite() != null) {
            datadogConfig = datadogConfig.withDatadogApiSite(writeConfig.getDatadogApiSite().name());
          }

          builder.withProperties(datadogConfig.build().getProps());
          break;
        case PROMETHEUS:
          HoodieMetricsPrometheusConfig prometheusConfig = HoodieMetricsPrometheusConfig.newBuilder()
              .withPushgatewayLabels(writeConfig.getPushGatewayLabels())
              .withPrometheusPortNum(writeConfig.getPrometheusPort()).build();
          builder.withProperties(prometheusConfig.getProps());
          break;
        case CONSOLE:
        case INMEMORY:
        case CLOUDWATCH:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }
    }

    HoodieWriteConfig metadataWriteConfig = builder.build();

    // Inline compaction and auto clean is required as we do not expose this table outside
    ValidationUtils.checkArgument(!metadataWriteConfig.isAutoClean(), "Cleaning is controlled internally for Metadata table.");
    ValidationUtils.checkArgument(!metadataWriteConfig.inlineCompactionEnabled(), "Compaction is controlled internally for metadata table.");
    ValidationUtils.checkArgument(metadataWriteConfig.getWriteStatusClassName().equals(FailOnFirstErrorWriteStatus.class.getName()),
        "MDT should use " + FailOnFirstErrorWriteStatus.class.getName());
    // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
    ValidationUtils.checkArgument(!metadataWriteConfig.isMetadataTableEnabled(), "File listing cannot be used for Metadata Table");

    return metadataWriteConfig;
  }

  /**
   * Convert commit action to metadata records for the enabled partition types.
   *
   * @param context                     - Engine context to use
   * @param dataWriteConfig             - Hudi configs
   * @param commitMetadata              - Commit action metadata
   * @param instantTime                 - Action instant time
   * @param dataMetaClient              - HoodieTableMetaClient for data
   * @param tableMetadata
   * @param metadataConfig              - HoodieMetadataConfig
   * @param enabledPartitionTypes       - Set of enabled MDT partitions to update
   * @param bloomFilterType             - Type of generated bloom filter records
   * @param bloomIndexParallelism       - Parallelism for bloom filter record generation
   * @param enableOptimizeLogBlocksScan - flag used to enable scanInternalV2 for log blocks in data table
   * @return Map of partition to metadata records for the commit action
   */
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext context, HoodieWriteConfig dataWriteConfig, HoodieCommitMetadata commitMetadata,
                                                                               String instantTime, HoodieTableMetaClient dataMetaClient, HoodieTableMetadata tableMetadata,
                                                                               HoodieMetadataConfig metadataConfig, Set<String> enabledPartitionTypes, String bloomFilterType,
                                                                               int bloomIndexParallelism, int writesFileIdEncoding, EngineType engineType,
                                                                               Option<HoodieRecord.HoodieRecordType> recordTypeOpt, boolean enableOptimizeLogBlocksScan) {
    final Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = context.parallelize(
        convertMetadataToFilesPartitionRecords(commitMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES.getPartitionPath(), filesPartitionRecordsRDD);

    if (enabledPartitionTypes.contains(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath())) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecords = convertMetadataToBloomFilterRecords(
          context, dataWriteConfig, commitMetadata, instantTime, dataMetaClient, bloomFilterType, bloomIndexParallelism);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), metadataBloomFilterRecords);
    }

    if (enabledPartitionTypes.contains(MetadataPartitionType.COLUMN_STATS.getPartitionPath())) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD = convertMetadataToColumnStatsRecords(commitMetadata, context,
          dataMetaClient, metadataConfig, recordTypeOpt);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.PARTITION_STATS.getPartitionPath())) {
      checkState(MetadataPartitionType.COLUMN_STATS.isMetadataPartitionAvailable(dataMetaClient),
          "Column stats partition must be enabled to generate partition stats. Please enable: " + HoodieMetadataConfig.ENABLE_METADATA_INDEX_COLUMN_STATS.key());
      // Generate Hoodie Pair data of partition name and list of column range metadata for all the files in that partition
      boolean isDeletePartition = commitMetadata.getOperationType().equals(WriteOperationType.DELETE_PARTITION);
      final HoodieData<HoodieRecord> partitionStatsRDD = convertMetadataToPartitionStatRecords(
          commitMetadata, instantTime, context, dataWriteConfig, dataMetaClient, tableMetadata, metadataConfig, recordTypeOpt, isDeletePartition);
      partitionToRecordsMap.put(MetadataPartitionType.PARTITION_STATS.getPartitionPath(), partitionStatsRDD);
    }
    if (enabledPartitionTypes.contains(MetadataPartitionType.RECORD_INDEX.getPartitionPath())) {
      partitionToRecordsMap.put(MetadataPartitionType.RECORD_INDEX.getPartitionPath(), convertMetadataToRecordIndexRecords(context, commitMetadata, metadataConfig,
          dataMetaClient, writesFileIdEncoding, instantTime, engineType, enableOptimizeLogBlocksScan));
    }
    return partitionToRecordsMap;
  }

  public static HoodieData<HoodieRecord> convertMetadataToPartitionStatRecords(HoodieCommitMetadata commitMetadata, String instantTime,
                                                                               HoodieEngineContext engineContext, HoodieWriteConfig dataWriteConfig,
                                                                               HoodieTableMetaClient dataMetaClient,
                                                                               HoodieTableMetadata tableMetadata, HoodieMetadataConfig metadataConfig,
                                                                               Option<HoodieRecord.HoodieRecordType> recordTypeOpt, boolean isDeletePartition) {
    try {
      Option<Schema> writerSchema =
          Option.ofNullable(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY))
              .flatMap(writerSchemaStr ->
                  isNullOrEmpty(writerSchemaStr)
                      ? Option.empty()
                      : Option.of(new Schema.Parser().parse(writerSchemaStr)));
      HoodieTableConfig tableConfig = dataMetaClient.getTableConfig();
      Option<HoodieSchema> tableSchema = writerSchema.map(schema -> tableConfig.populateMetaFields() ? addMetadataFields(schema) : schema).map(HoodieSchema::fromAvroSchema);
      if (tableSchema.isEmpty()) {
        return engineContext.emptyHoodieData();
      }
      HoodieIndexVersion partitionStatsIndexVersion = existingIndexVersionOrDefault(PARTITION_NAME_PARTITION_STATS, dataMetaClient);
      Lazy<Option<HoodieSchema>> writerSchemaOpt = Lazy.eagerly(tableSchema);
      Map<String, HoodieSchema> columnsToIndexSchemaMap = getColumnsToIndex(dataMetaClient.getTableConfig(), metadataConfig, writerSchemaOpt, false, recordTypeOpt, partitionStatsIndexVersion);
      if (columnsToIndexSchemaMap.isEmpty()) {
        return engineContext.emptyHoodieData();
      }

      // if this is DELETE_PARTITION, then create delete metadata payload for all columns for partition_stats
      if (isDeletePartition) {
        HoodieReplaceCommitMetadata replaceCommitMetadata = (HoodieReplaceCommitMetadata) commitMetadata;
        Map<String, List<String>> partitionToReplaceFileIds = replaceCommitMetadata.getPartitionToReplaceFileIds();
        List<String> partitionsToDelete = new ArrayList<>(partitionToReplaceFileIds.keySet());
        if (partitionToReplaceFileIds.isEmpty()) {
          return engineContext.emptyHoodieData();
        }
        return engineContext.parallelize(partitionsToDelete, partitionsToDelete.size()).flatMap(partition -> {
          Stream<HoodieRecord> columnRangeMetadata = columnsToIndexSchemaMap.keySet().stream()
              .flatMap(column -> HoodieMetadataPayload.createPartitionStatsRecords(
                  partition,
                  Collections.singletonList(HoodieColumnRangeMetadata.stub("", column, partitionStatsIndexVersion)),
                  true, true, Option.empty()));
          return columnRangeMetadata.iterator();
        });
      }

      // In this function we fetch column range metadata for all new files part of commit metadata along with all the other files
      // of the affected partitions. The column range metadata is grouped by partition name to generate HoodiePairData of partition name
      // and list of column range metadata for that partition files. This pair data is then used to generate partition stat records.
      List<HoodieWriteStat> allWriteStats = commitMetadata.getPartitionToWriteStats().values().stream()
          .flatMap(Collection::stream).collect(Collectors.toList());
      if (allWriteStats.isEmpty()) {
        return engineContext.emptyHoodieData();
      }

      List<String> colsToIndex = new ArrayList<>(columnsToIndexSchemaMap.keySet());
      LOG.debug("Indexing following columns for partition stats index: {}", columnsToIndexSchemaMap.keySet());
      // Group by partitionPath and then gather write stats lists,
      // where each inner list contains HoodieWriteStat objects that have the same partitionPath.
      List<List<HoodieWriteStat>> partitionedWriteStats = new ArrayList<>(allWriteStats.stream()
          .collect(Collectors.groupingBy(HoodieWriteStat::getPartitionPath))
          .values());
      Map<String, Set<String>> fileGroupIdsToReplaceMap = (commitMetadata instanceof HoodieReplaceCommitMetadata)
          ? ((HoodieReplaceCommitMetadata) commitMetadata).getPartitionToReplaceFileIds()
          .entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new HashSet<>(e.getValue())))
          : Collections.emptyMap();

      int parallelism = Math.max(Math.min(partitionedWriteStats.size(), metadataConfig.getPartitionStatsIndexParallelism()), 1);
      String maxInstantTime = getMaxInstantTime(dataMetaClient, instantTime);
      HoodiePairData<String, List<HoodieColumnRangeMetadata<Comparable>>> columnRangeMetadata =
          engineContext.parallelize(partitionedWriteStats, parallelism).mapToPair(partitionedWriteStat -> {
            final String partitionName = partitionedWriteStat.get(0).getPartitionPath();
            checkState(tableMetadata != null, "tableMetadata should not be null when scanning metadata table");

            // Collect column metadata for each file part of the latest merged file slice before the current instant time
            List<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata = partitionedWriteStat.stream()
                .flatMap(writeStat -> translateWriteStatToFileStats(writeStat, dataMetaClient, colsToIndex, partitionStatsIndexVersion).stream()).collect(toList());
            // Collect column metadata of each file that does not have column stats provided by the write stat in the commit metadata
            Set<String> filesToFetchColumnStats = getFilesToFetchColumnStats(partitionedWriteStat, dataMetaClient, tableMetadata, dataWriteConfig, partitionName, maxInstantTime,
                instantTime, fileGroupIdsToReplaceMap, colsToIndex, partitionStatsIndexVersion);
            // Fetch metadata table COLUMN_STATS partition records for the above files
            List<HoodieColumnRangeMetadata<Comparable>> partitionColumnMetadata = tableMetadata
                .getRecordsByKeyPrefixes(
                    HoodieListData.lazy(generateColumnStatsKeys(colsToIndex, partitionName)),
                    MetadataPartitionType.COLUMN_STATS.getPartitionPath(), false)
                // schema and properties are ignored in getInsertValue, so simply pass as null
                .map(record -> ((HoodieMetadataPayload) record.getData()).getColumnStatMetadata())
                .filter(Option::isPresent)
                .map(colStatsOpt -> colStatsOpt.get())
                .filter(stats -> filesToFetchColumnStats.contains(stats.getFileName()))
                .map(HoodieColumnRangeMetadata::fromColumnStats).collectAsList();
            // fileColumnMetadata already contains stats for the files from the current inflight commit.
            // Here it adds the stats for the commited files part of the latest merged file slices
            fileColumnMetadata.addAll(partitionColumnMetadata);
            return Pair.of(partitionName, fileColumnMetadata);
          });

      return convertMetadataToPartitionStatsRecords(columnRangeMetadata, dataMetaClient, columnsToIndexSchemaMap, partitionStatsIndexVersion);
    } catch (Exception e) {
      throw new HoodieException("Failed to generate column stats records for metadata table", e);
    }
  }

  private static StoragePathInfo getBaseFileStoragePathInfo(HoodieBaseFile baseFile) {
    StoragePathInfo pathInfo = baseFile.getPathInfo();
    if (pathInfo != null) {
      return pathInfo;
    }
    return new StoragePathInfo(baseFile.getStoragePath(), baseFile.getFileLen(), false, (short) 0, 0, 0);
  }

  private static StoragePathInfo getLogFileStoragePathInfo(HoodieLogFile logFile) {
    StoragePathInfo pathInfo = logFile.getPathInfo();
    if (pathInfo != null) {
      return pathInfo;
    }
    return new StoragePathInfo(logFile.getPath(), logFile.getFileSize(), false, (short) 0, 0, 0);
  }

  public static String getMaxInstantTime(HoodieTableMetaClient dataMetaClient, String instantTime) {
    Option<String> lastCompletedInstant = dataMetaClient.getActiveTimeline().filterCompletedInstants()
        .lastInstant()
        .map(HoodieInstant::requestedTime);
    return lastCompletedInstant.map(lastCompletedInstantTime ->
        lastCompletedInstantTime.compareTo(instantTime) > 0 ? lastCompletedInstantTime : instantTime).orElse(instantTime);
  }

  /**
   * Collect column metadata of each file that does not have column stats provided by the write stat in the commit metadata
   */
  public static Set<String> getFilesToFetchColumnStats(List<HoodieWriteStat> partitionedWriteStat,
                                                       HoodieTableMetaClient dataMetaClient,
                                                       HoodieTableMetadata tableMetadata,
                                                       HoodieWriteConfig dataWriteConfig,
                                                       String partitionName,
                                                       String maxInstantTime,
                                                       String instantTime,
                                                       Map<String, Set<String>> fileGroupIdsToReplaceMap,
                                                       List<String> colsToIndex,
                                                       HoodieIndexVersion partitionStatsIndexVersion) {
    // Get the latest merged file slices based on the commited files part of the latest snapshot and the new files of the current commit metadata
    List<StoragePathInfo> consolidatedPathInfos = new ArrayList<>();
    partitionedWriteStat.forEach(
        stat -> consolidatedPathInfos.add(
            new StoragePathInfo(new StoragePath(dataMetaClient.getBasePath(), stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0)));
    SyncableFileSystemView fileSystemViewForCommitedFiles =
        FileSystemViewManager.createViewManager(new HoodieLocalEngineContext(dataMetaClient.getStorageConf()),
            dataWriteConfig.getMetadataConfig(), dataWriteConfig.getViewStorageConfig(), dataWriteConfig.getCommonConfig(),
            unused -> tableMetadata).getFileSystemView(dataMetaClient);
    fileSystemViewForCommitedFiles.getLatestMergedFileSlicesBeforeOrOn(partitionName, maxInstantTime)
        .forEach(fileSlice -> {
          if (fileSlice.getBaseFile().isPresent()) {
            consolidatedPathInfos.add(getBaseFileStoragePathInfo(fileSlice.getBaseFile().get()));
          }
          fileSlice.getLogFiles().forEach(logFile -> consolidatedPathInfos.add(getLogFileStoragePathInfo(logFile)));
        });
    SpillableMapBasedFileSystemView consolidatedFileSystemView = new SpillableMapBasedFileSystemView(
        tableMetadata, dataMetaClient, dataMetaClient.getActiveTimeline(),
        consolidatedPathInfos, dataWriteConfig.getViewStorageConfig(), dataWriteConfig.getCommonConfig());

    // Collect column metadata for each file part of the latest merged file slice before the current instant time
    List<HoodieColumnRangeMetadata<Comparable>> fileColumnMetadata = partitionedWriteStat.stream()
        .flatMap(writeStat -> translateWriteStatToFileStats(writeStat, dataMetaClient, colsToIndex, partitionStatsIndexVersion).stream()).collect(toList());
    Set<String> fileGroupIdsToReplace = fileGroupIdsToReplaceMap.getOrDefault(partitionName, Collections.emptySet());
    Set<String> filesWithColumnStats = partitionedWriteStat.stream()
        .map(stat -> new StoragePath(stat.getPath()).getName()).collect(Collectors.toSet());
    // Collect column metadata of each file that does not have column stats provided by the write stat in the commit metadata
    return consolidatedFileSystemView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionName, maxInstantTime, instantTime)
        .flatMap(fileSlice -> Stream.concat(
            Stream.of(fileSlice.getBaseFile().map(HoodieBaseFile::getFileName).orElse(null)),
            fileSlice.getLogFiles().map(HoodieLogFile::getFileName)))
        .filter(e -> Objects.nonNull(e) && !filesWithColumnStats.contains(e) && !fileGroupIdsToReplace.contains(e))
        .collect(Collectors.toSet());
  }
}
