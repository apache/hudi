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
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsConfig;
import org.apache.hudi.config.metrics.HoodieMetricsGraphiteConfig;
import org.apache.hudi.config.metrics.HoodieMetricsJmxConfig;
import org.apache.hudi.config.metrics.HoodieMetricsPrometheusConfig;
import org.apache.hudi.exception.HoodieMetadataException;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;

import java.util.Properties;

import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_ASYNC_CLEAN;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.common.config.HoodieMetadataConfig.DEFAULT_METADATA_POPULATE_META_FIELDS;
import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;

/**
 * Metadata table write utils.
 */
public class HoodieMetadataWriteUtils {
  // Virtual keys support for metadata table. This Field is
  // from the metadata payload schema.
  public static final String RECORD_KEY_FIELD_NAME = HoodieMetadataPayload.KEY_FIELD_NAME;

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.  This is used by async
   * indexer only.
   *
   * @param writeConfig                {@code HoodieWriteConfig} of the main dataset writer
   * @param failedWritesCleaningPolicy Cleaning policy on failed writes
   */
  public static HoodieWriteConfig createMetadataWriteConfig(
      HoodieWriteConfig writeConfig, HoodieFailedWritesCleaningPolicy failedWritesCleaningPolicy) {
    String tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
    int parallelism = writeConfig.getMetadataInsertParallelism();

    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withEngineType(writeConfig.getEngineType())
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
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
        .withCleanConfig(HoodieCleanConfig.newBuilder()
            .withAsyncClean(DEFAULT_METADATA_ASYNC_CLEAN)
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .withFailedWritesCleaningPolicy(failedWritesCleaningPolicy)
            .retainCommits(Math.min(writeConfig.getCleanerCommitsRetained(), DEFAULT_METADATA_CLEANER_COMMITS_RETAINED))
            .build())
        // we will trigger archive manually, to ensure only regular writer invokes it
        .withArchivalConfig(HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(
                writeConfig.getMinCommitsToKeep(), writeConfig.getMaxCommitsToKeep())
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
            // Check if log compaction is enabled, this is needed for tables with lot of records.
            .withLogCompactionEnabled(writeConfig.isLogCompactionEnabledOnMetadata())
            // Below config is only used if isLogCompactionEnabled is set.
            .withLogCompactionBlocksThreshold(writeConfig.getMetadataLogCompactBlocksThreshold())
            .build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism)
        .withAllowMultiWriteOnSameInstant(true)
        .withKeyGenerator(HoodieTableMetadataKeyGenerator.class.getCanonicalName())
        .withPopulateMetaFields(DEFAULT_METADATA_POPULATE_META_FIELDS)
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileWriterToAllowDuplicates(writeConfig.hfileWriterToAllowDuplicates())
            .build())
        .withWriteStatusClass(FailOnFirstErrorWriteStatus.class)
        .withReleaseResourceEnabled(writeConfig.areReleaseResourceEnabled());

    // RecordKey properties are needed for the metadata table records
    final Properties properties = new Properties();
    properties.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), RECORD_KEY_FIELD_NAME);
    properties.put("hoodie.datasource.write.recordkey.field", RECORD_KEY_FIELD_NAME);
    builder.withProperties(properties);

    if (writeConfig.isMetricsOn()) {
      // Table Name is needed for metric reporters prefix
      Properties commonProperties = new Properties();
      commonProperties.put(HoodieWriteConfig.TBL_NAME.key(), tableName);

      builder.withMetricsConfig(HoodieMetricsConfig.newBuilder()
          .fromProperties(commonProperties)
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
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
          HoodieMetricsPrometheusConfig prometheusConfig = HoodieMetricsPrometheusConfig.newBuilder()
              .withPushgatewayJobname(writeConfig.getPushGatewayJobName())
              .withPushgatewayRandomJobnameSuffix(writeConfig.getPushGatewayRandomJobNameSuffix())
              .withPushgatewayLabels(writeConfig.getPushGatewayLabels())
              .withPushgatewayReportPeriodInSeconds(String.valueOf(writeConfig.getPushGatewayReportPeriodSeconds()))
              .withPushgatewayHostName(writeConfig.getPushGatewayHost())
              .withPushgatewayPortNum(writeConfig.getPushGatewayPort()).build();
          builder.withProperties(prometheusConfig.getProps());
          break;
        case DATADOG:
        case PROMETHEUS:
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
}
