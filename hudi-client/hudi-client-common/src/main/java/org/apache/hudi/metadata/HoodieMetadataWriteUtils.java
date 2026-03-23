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
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.HoodieTableServiceManagerConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.common.config.metrics.HoodieMetricsM3Config;
import org.apache.hudi.common.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteConcurrencyMode;
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
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.transaction.lock.InProcessLockProvider;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ASYNC_CLEAN;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.common.util.ValidationUtils.checkState;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.translateWriteStatToFileStats;

/**
 * Metadata table write utils.
 */
@Slf4j
public class HoodieMetadataWriteUtils {
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
    WriteConcurrencyMode metadataWriteConcurrencyMode =
        WriteConcurrencyMode.valueOf(writeConfig.getMetadataConfig().getWriteConcurrencyMode());
    // Multi-writer on MDT is for separate table service execution; streaming writes are not compatible,
    // so force it off to avoid confusing config mismatch errors.
    boolean isStreamingWritesToMetadataEnabled = !metadataWriteConcurrencyMode.supportsMultiWriter()
        && writeConfig.isMetadataStreamingWritesEnabled(datatableVersion);

    WriteConcurrencyMode concurrencyMode;
    HoodieLockConfig lockConfig;
    if (metadataWriteConcurrencyMode.supportsMultiWriter()) {
      checkState(metadataWriteConcurrencyMode == writeConfig.getWriteConcurrencyMode(),
          "If multiwriter is used on metadata table, its concurrency mode (" + metadataWriteConcurrencyMode
              + ") must match the data table concurrency mode (" + writeConfig.getWriteConcurrencyMode() + ")");
      String lockProviderClass = writeConfig.getLockProviderClass();
      checkState(lockProviderClass != null,
          "Lock provider class must be set for data table to enable async executions of table services in metadata table");
      checkState(!InProcessLockProvider.isInProcessLockProvider(lockProviderClass),
          "InProcessLockProvider cannot be used for metadata table multi-writer mode as it does not support cross-process locking. "
              + "Configure a distributed lock provider on the data table.");
      concurrencyMode = metadataWriteConcurrencyMode;
      failedWritesCleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
      lockConfig = HoodieLockConfig.deriveLockConfigForDifferentTable(lockProviderClass, writeConfig);
    } else {
      if (isStreamingWritesToMetadataEnabled) {
        concurrencyMode = WriteConcurrencyMode.NON_BLOCKING_CONCURRENCY_CONTROL;
        lockConfig = HoodieLockConfig.newBuilder().withLockProvider(InProcessLockProvider.class).build();
        failedWritesCleaningPolicy = HoodieFailedWritesCleaningPolicy.LAZY;
      } else {
        concurrencyMode = WriteConcurrencyMode.SINGLE_WRITER;
        lockConfig = HoodieLockConfig.newBuilder().build();
      }
    }

    final long maxLogFileSizeBytes = writeConfig.getMetadataConfig().getMaxLogFileSize();
    // Borrow the cleaner policy from the main table and adjust the cleaner policy based on the main table's cleaner policy
    boolean shouldDeriveFromDataTableCleanPolicy = writeConfig.getMetadataConfig().shouldDeriveFromDataTableCleanPolicy();
    HoodieCleanConfig.Builder cleanConfigBuilder = HoodieCleanConfig.newBuilder()
        .withAsyncClean(DEFAULT_METADATA_ASYNC_CLEAN)
        .withAutoClean(false)
        .withCleanerParallelism(MDT_DEFAULT_PARALLELISM)
        .withFailedWritesCleaningPolicy(failedWritesCleaningPolicy)
        .withMaxCommitsBeforeCleaning(writeConfig.getCleanTriggerMaxCommits())
        .withCleanOptimizationWithLocalEngineEnabled(
            writeConfig.isCleanOptimizationWithLocalEngineEnabled());

    if (shouldDeriveFromDataTableCleanPolicy) {
      HoodieCleaningPolicy dataTableCleaningPolicy = writeConfig.getCleanerPolicy();
      cleanConfigBuilder.withCleanerPolicy(dataTableCleaningPolicy);
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
    } else {
      cleanConfigBuilder.withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_FILE_VERSIONS);
      cleanConfigBuilder.retainFileVersions(2);
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
        .withEmbeddedTimelineServerEnabled(writeConfig.isEmbeddedTimelineServerEnabled())
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
            // Compaction on metadata table is used as a barrier for archiving on main dataset and for validating the
            // deltacommits having corresponding completed commits. Therefore, we need to compact all fileslices of all
            // partitions together requiring UnBoundedCompactionStrategy.
            .withCompactionStrategy(new UnBoundedCompactionStrategy())
            // Check if log compaction is enabled, this is needed for tables with a lot of records.
            .withLogCompactionEnabled(writeConfig.isLogCompactionEnabledOnMetadata())
            // Below config is only used if isLogCompactionEnabled is set.
            .withLogCompactionBlocksThreshold(writeConfig.getMetadataLogCompactBlocksThreshold())
            .withInlineCompactionTriggerStrategy(CompactionTriggerStrategy.valueOf(writeConfig.getMetadataCompactionTriggerStrategy()))
            .withMaxDeltaSecondsBeforeCompaction(writeConfig.getMetadataMaxDeltaSecondsBeforeCompaction())
            .build())
        .withStorageConfig(HoodieStorageConfig.newBuilder().hfileMaxFileSize(MDT_MAX_HFILE_SIZE_BYTES)
            .allowDuplicatesWithHfileWrites(writeConfig.allowDuplicatesWithHfileWrites())
            .logFileMaxSize(maxLogFileSizeBytes)
            // Keeping the log blocks as large as the log files themselves reduces the number of HFile blocks to be checked for
            // presence of keys
            .logFileDataBlockMaxSize(maxLogFileSizeBytes)
            .hfileBloomFilterEnable(writeConfig.hfileBloomFilterEnabled())
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
    // Pass table service manager config for MDT
    properties.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ENABLED.key(),
        String.valueOf(writeConfig.getMetadataConfig().isTableServiceManagerEnabled()));
    properties.put(HoodieTableServiceManagerConfig.TABLE_SERVICE_MANAGER_ACTIONS.key(),
        writeConfig.getMetadataConfig().getTableServiceManagerActions());
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

  private static StoragePathInfo getBaseFileStoragePathInfo(HoodieBaseFile baseFile) {
    StoragePathInfo pathInfo = baseFile.getPathInfo();
    if (pathInfo != null) {
      return pathInfo;
    }
    return new StoragePathInfo(baseFile.getStoragePath(), baseFile.getFileSize(), false, (short) 0, 0, 0);
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
    // Get the latest merged file slices based on the committed files part of the latest snapshot and the new files of the current commit metadata
    List<StoragePathInfo> consolidatedPathInfos = new ArrayList<>();
    partitionedWriteStat.forEach(
        stat -> consolidatedPathInfos.add(
            new StoragePathInfo(new StoragePath(dataMetaClient.getBasePath(), stat.getPath()), stat.getFileSizeInBytes(), false, (short) 0, 0, 0)));
    SyncableFileSystemView fileSystemViewForCommittedFiles =
        FileSystemViewManager.createViewManager(new HoodieLocalEngineContext(dataMetaClient.getStorageConf()),
            dataWriteConfig.getMetadataConfig(), dataWriteConfig.getViewStorageConfig(), dataWriteConfig.getCommonConfig(),
            unused -> tableMetadata).getFileSystemView(dataMetaClient);
    fileSystemViewForCommittedFiles.getLatestMergedFileSlicesBeforeOrOn(partitionName, maxInstantTime)
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
