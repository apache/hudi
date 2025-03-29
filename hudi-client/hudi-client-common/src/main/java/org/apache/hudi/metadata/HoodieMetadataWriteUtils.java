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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.client.FailOnFirstErrorWriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsDatadogConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.config.metrics.HoodieMetricsM3Config;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.metadata.index.IndexBuilder;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ASYNC_CLEAN;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.StringUtils.nonEmpty;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToBloomFilterRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToColumnStatsRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToExpressionIndexRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToFilesPartitionRecords;

/**
 * Metadata table write utils.
 */
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
      HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy) {
    String tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;

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
        .withWriteConcurrencyMode(WriteConcurrencyMode.SINGLE_WRITER)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).withFileListingParallelism(writeConfig.getFileListingParallelism()).build())
        .withAutoCommit(true)
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
            .logFileDataBlockMaxSize(maxLogFileSizeBytes).build())
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
        .withWriteRecordPositionsEnabled(false);

    // RecordKey properties are needed for the metadata table records
    final Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), RECORD_KEY_FIELD_NAME);
    properties.put("hoodie.datasource.write.recordkey.field", RECORD_KEY_FIELD_NAME);
    if (nonEmpty(writeConfig.getMetricReporterMetricsNamePrefix())) {
      properties.put(HoodieMetricsConfig.METRICS_REPORTER_PREFIX.key(),
          writeConfig.getMetricReporterMetricsNamePrefix() + METADATA_TABLE_NAME_SUFFIX);
    }
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
    // Auto commit is required
    ValidationUtils.checkArgument(metadataWriteConfig.shouldAutoCommit(), "Auto commit is required for Metadata Table");
    ValidationUtils.checkArgument(metadataWriteConfig.getWriteStatusClassName().equals(FailOnFirstErrorWriteStatus.class.getName()),
        "MDT should use " + FailOnFirstErrorWriteStatus.class.getName());
    // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
    ValidationUtils.checkArgument(!metadataWriteConfig.isMetadataTableEnabled(), "File listing cannot be used for Metadata Table");

    return metadataWriteConfig;
  }

  /**
   * Convert the clean action to metadata records.
   */
  // TODO(yihua): this is only used by writers, why is this in hudi-common?
  public static Map<String, HoodieData<HoodieRecord>> convertMetadataToRecords(HoodieEngineContext engineContext,
                                                                               HoodieCleanMetadata cleanMetadata,
                                                                               String instantTime,
                                                                               HoodieTableMetaClient dataMetaClient,
                                                                               HoodieMetadataConfig metadataConfig,
                                                                               Map<MetadataPartitionType, IndexBuilder> enabledIndexBuilderMap,
                                                                               int bloomIndexParallelism,
                                                                               Option<HoodieRecord.HoodieRecordType> recordTypeOpt) {
    final Map<String, HoodieData<HoodieRecord>> partitionToRecordsMap = new HashMap<>();
    final HoodieData<HoodieRecord> filesPartitionRecordsRDD = engineContext.parallelize(
        convertMetadataToFilesPartitionRecords(cleanMetadata, instantTime), 1);
    partitionToRecordsMap.put(MetadataPartitionType.FILES.getPartitionPath(), filesPartitionRecordsRDD);
    if (enabledIndexBuilderMap.containsKey(MetadataPartitionType.BLOOM_FILTERS)) {
      final HoodieData<HoodieRecord> metadataBloomFilterRecordsRDD =
          convertMetadataToBloomFilterRecords(cleanMetadata, engineContext, instantTime, bloomIndexParallelism);
      partitionToRecordsMap.put(MetadataPartitionType.BLOOM_FILTERS.getPartitionPath(), metadataBloomFilterRecordsRDD);
    }

    if (enabledIndexBuilderMap.containsKey(MetadataPartitionType.COLUMN_STATS)) {
      final HoodieData<HoodieRecord> metadataColumnStatsRDD =
          convertMetadataToColumnStatsRecords(cleanMetadata, engineContext,
              dataMetaClient, metadataConfig, recordTypeOpt);
      partitionToRecordsMap.put(MetadataPartitionType.COLUMN_STATS.getPartitionPath(), metadataColumnStatsRDD);
    }
    if (enabledIndexBuilderMap.containsKey(MetadataPartitionType.EXPRESSION_INDEX)) {
      convertMetadataToExpressionIndexRecords(engineContext, cleanMetadata, instantTime, dataMetaClient, metadataConfig, bloomIndexParallelism, partitionToRecordsMap,
          recordTypeOpt);
    }

    return partitionToRecordsMap;
  }

  public static HoodieIndexDefinition getIndexDefinition(HoodieTableMetaClient dataTableMetaClient,
                                                         String indexName) {
    return HoodieTableMetadataUtil.getHoodieIndexDefinition(indexName, dataTableMetaClient);
  }

  public static List<Pair<String, FileSlice>> getPartitionFileSlicePairs(HoodieTableMetaClient dataTableMetaClient,
                                                                         HoodieTableMetadata metadata,
                                                                         HoodieTableFileSystemView fsView)
      throws IOException {
    String latestInstant = dataTableMetaClient.getActiveTimeline().filterCompletedAndCompactionInstants().lastInstant()
        .map(HoodieInstant::requestedTime).orElse(SOLO_COMMIT_TIMESTAMP);
    // TODO(yihua): originally this uses try-catch on fsView so it's not reused.
    // now we need to rely on outside caller to pass fsView and close the fsView.
    // also see if we can reuse fsView across indexes.
    // Collect the list of latest file slices present in each partition
    List<String> partitions = metadata.getAllPartitionPaths();
    fsView.loadAllPartitions();
    List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
    partitions.forEach(partition -> fsView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant)
        .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs))));
    return partitionFileSlicePairs;
  }
}
