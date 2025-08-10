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

package org.apache.hudi.table.upgrade;

import org.apache.hudi.client.timeline.TimelineArchivers;
import org.apache.hudi.client.timeline.versioning.v1.TimelineArchiverV1;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.ArchivedTimelineLoader;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.MetadataConversionUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.ArchivedTimelineLoaderV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieInstantWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.HoodieTableConfig.TABLE_METADATA_PARTITIONS;
import static org.apache.hudi.common.table.timeline.HoodieInstant.UNDERSCORE;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V1;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V2;
import static org.apache.hudi.metadata.MetadataPartitionType.BLOOM_FILTERS;
import static org.apache.hudi.metadata.MetadataPartitionType.COLUMN_STATS;
import static org.apache.hudi.metadata.MetadataPartitionType.FILES;
import static org.apache.hudi.metadata.MetadataPartitionType.RECORD_INDEX;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.EIGHT_TO_SIX_TIMELINE_ACTION_MAP;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.convertCompletionTimeToEpoch;

/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class EightToSevenDowngradeHandler implements DowngradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(EightToSevenDowngradeHandler.class);
  private static final Set<String> SUPPORTED_METADATA_PARTITION_PATHS = getSupportedMetadataPartitionPaths();

  @Override
  public UpgradeDowngrade.TableConfigChangeSet downgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder().setConf(context.getStorageConf().newInstance()).setBasePath(config.getBasePath()).build();
    // Handle timeline downgrade:
    //  - Rename instants in active timeline to old format for table version 6
    //  - Convert LSM timeline format to archived timeline for table version 6
    List<HoodieInstant> instants;
    try {
      // We need to move all the instants - not just completed ones.
      instants = metaClient.scanHoodieInstantsFromFileSystem(metaClient.getTimelinePath(),
          ActiveTimelineV2.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    } catch (IOException ioe) {
      LOG.error("Failed to get instants from filesystem", ioe);
      throw new HoodieIOException("Failed to get instants from filesystem", ioe);
    }

    if (!instants.isEmpty()) {
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
      CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
      ActiveTimelineV1 activeTimelineV1 = new ActiveTimelineV1(metaClient);
      context.map(instants, instant -> {
        String originalFileName = instantFileNameGenerator.getFileName(instant);
        return downgradeActiveTimelineInstant(instant, originalFileName, metaClient, commitMetadataSerDeV1, activeTimelineV1);
      }, instants.size());
    }
    downgradeFromLSMTimeline(table, config);

    // downgrade table properties
    downgradePartitionFields(config, metaClient.getTableConfig(), tablePropsToAdd);
    unsetInitialVersion(metaClient.getTableConfig(), tablePropsToAdd);
    List<ConfigProperty> tablePropsToRemove = new ArrayList<>();
    tablePropsToRemove.addAll(unsetRecordMergeMode(config, metaClient.getTableConfig(), tablePropsToAdd));
    tablePropsToRemove.add(HoodieTableConfig.RECORD_MERGE_STRATEGY_ID);
    downgradeKeyGeneratorType(metaClient.getTableConfig(), tablePropsToAdd);
    downgradeBootstrapIndexType(metaClient.getTableConfig(), tablePropsToAdd);

    // Prepare parameters.
    if (metaClient.getTableConfig().isMetadataTableAvailable()) {
      // Delete unsupported metadata partitions in table version 7.
      downgradeMetadataPartitions(context, metaClient.getStorage(), metaClient, tablePropsToAdd);
      UpgradeDowngradeUtils.updateMetadataTableVersion(context, HoodieTableVersion.SEVEN, metaClient);
    }
    return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, tablePropsToRemove);
  }

  static void downgradePartitionFields(HoodieWriteConfig config,
                                       HoodieTableConfig tableConfig,
                                       Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    String partitionPathField = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (keyGenerator != null && partitionPathField != null
        && (keyGenerator.equals(KeyGeneratorType.CUSTOM.getClassName()) || keyGenerator.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName()))) {
      tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, tableConfig.getPartitionFieldProp());
    }
  }

  static void unsetInitialVersion(HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    tableConfig.getProps().remove(HoodieTableConfig.INITIAL_VERSION.key());
  }

  static List<ConfigProperty> unsetRecordMergeMode(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String payloadClass = tableConfig.getPayloadClass();
    if (StringUtils.isNullOrEmpty(payloadClass)) {
      RecordMergeMode mergeMode = tableConfig.getRecordMergeMode();
      switch (mergeMode) {
        case EVENT_TIME_ORDERING:
          tablePropsToAdd.put(HoodieTableConfig.PAYLOAD_CLASS_NAME, DefaultHoodieRecordPayload.class.getName());
          break;
        case COMMIT_TIME_ORDERING:
          tablePropsToAdd.put(HoodieTableConfig.PAYLOAD_CLASS_NAME, OverwriteWithLatestAvroPayload.class.getName());
          break;
        case CUSTOM:
          throw new HoodieUpgradeDowngradeException("Custom payload class must be available for downgrading custom merge mode");
        default:
          throw new HoodieUpgradeDowngradeException("Downgrade is not handled for " + mergeMode);
      }
    }
    return Collections.singletonList(HoodieTableConfig.RECORD_MERGE_MODE);
  }

  static void downgradeBootstrapIndexType(HoodieTableConfig tableConfig,
                                          Map<ConfigProperty, String> tablePropsToAdd) {
    if (tableConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME) || tableConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE)) {
      String bootstrapIndexClassName = BootstrapIndexType.getBootstrapIndexClassName(tableConfig);
      if (StringUtils.nonEmpty(bootstrapIndexClassName)) {
        tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME, bootstrapIndexClassName);
      }
    }
    tableConfig.getProps().remove(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE.key());
  }

  static void downgradeKeyGeneratorType(HoodieTableConfig tableConfig,
                                        Map<ConfigProperty, String> tablePropsToAdd) {
    if (tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME) || tableConfig.contains(HoodieTableConfig.KEY_GENERATOR_TYPE)) {
      String keyGenerator = KeyGeneratorType.getKeyGeneratorClassName(tableConfig);
      if (StringUtils.nonEmpty(keyGenerator)) {
        tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGenerator);
      }
    }
    tableConfig.getProps().remove(HoodieTableConfig.KEY_GENERATOR_TYPE.key());
  }

  @SuppressWarnings("rawtypes, unchecked")
  static void downgradeFromLSMTimeline(HoodieTable table, HoodieWriteConfig config) {
    // if timeline layout version is present in the Option then check if it is LAYOUT_VERSION_2
    table.getMetaClient().getTableConfig().getTimelineLayoutVersion().ifPresent(
        timelineLayoutVersion -> ValidationUtils.checkState(TimelineLayoutVersion.LAYOUT_VERSION_2.equals(timelineLayoutVersion),
            "Downgrade from LSM timeline is only supported for layout version 2. Given version: " + timelineLayoutVersion));

    try {
      TimelineArchiverV1 archiver = (TimelineArchiverV1) TimelineArchivers.getInstance(TimelineLayoutVersion.LAYOUT_VERSION_1, config, table);
      int batchSize = config.getCommitArchivalBatchSize();
      StoragePath archivePath = new StoragePath(table.getMetaClient().getMetaPath(), "archived");
      try (ArchiveEntryFlusher flusher = new ArchiveEntryFlusher(table.getMetaClient(), archiver, batchSize, archivePath)) {
        // Load and process instants in the batch
        ArchivedTimelineLoader timelineLoader = new ArchivedTimelineLoaderV2();
        timelineLoader.loadInstants(
            table.getMetaClient(),
            null,
            HoodieArchivedTimeline.LoadMode.FULL,
            record -> true,
            flusher);
      }
    } catch (Exception e) {
      LOG.warn("Failed to downgrade LSM timeline to old archived format");
      if (config.isFailOnTimelineArchivingEnabled()) {
        throw new HoodieException("Failed to downgrade LSM timeline to old archived format", e);
      }
    }
  }

  /**
   * Consumer for flushing the archive records.
   */
  private static class ArchiveEntryFlusher implements BiConsumer<String, GenericRecord>, AutoCloseable {
    private final TimelineArchiverV1 archiverV1;
    private final List<GenericRecord> buffer;
    private final int batchSize;
    private final StoragePath archivePath;
    private final HoodieTableMetaClient metaClient;

    public ArchiveEntryFlusher(HoodieTableMetaClient metaClient, TimelineArchiverV1 archiverV1, int batchSize, StoragePath archivePath) {
      this.metaClient = metaClient;
      this.archiverV1 = archiverV1;
      this.batchSize = batchSize;
      this.buffer = new ArrayList<>();
      this.archivePath = archivePath;
    }

    @Override
    public void accept(String s, GenericRecord archiveEntry) {
      if (buffer.size() >= batchSize) {
        archiverV1.flushArchiveEntries(new ArrayList<>(buffer), archivePath);
        buffer.clear();
      } else {
        try {
          GenericRecord legacyArchiveEntry = MetadataConversionUtils.createMetaWrapper(metaClient, archiveEntry);
          buffer.add(legacyArchiveEntry);
        } catch (IOException e) {
          throw new HoodieException("Convert lsm archive entry to legacy error", e);
        }
      }
    }

    @Override
    public void close() {
      if (!buffer.isEmpty()) {
        archiverV1.flushArchiveEntries(new ArrayList<>(buffer), this.archivePath);
        buffer.clear();
      }
    }
  }

  static boolean downgradeActiveTimelineInstant(HoodieInstant instant, String originalFileName, HoodieTableMetaClient metaClient,
                                                CommitMetadataSerDeV1 commitMetadataSerDeV1, ActiveTimelineV1 activeTimelineV1) {
    String replacedFileName = originalFileName;
    boolean isCompleted = instant.isCompleted();
    // Rename the metadata file name from the ${instant_time}_${completion_time}.action[.state] format in version 1.x
    // to the ${instant_time}.action[.state] format in version 0.x.
    if (isCompleted) {
      replacedFileName = replacedFileName.replaceAll(UNDERSCORE + "\\d+", "");
    }
    // Rename the action if necessary (e.g., CLUSTERING_ACTION to REPLACE_COMMIT_ACTION).
    // NOTE: New action names were only applied for pending instants. Completed instants do not have any change in action names.
    if (EIGHT_TO_SIX_TIMELINE_ACTION_MAP.containsKey(instant.getAction()) && !isCompleted) {
      replacedFileName = replacedFileName.replace(instant.getAction(), EIGHT_TO_SIX_TIMELINE_ACTION_MAP.get(instant.getAction()));
    }
    try {
      return rewriteTimelineV2InstantFileToV1Format(instant, metaClient, originalFileName, replacedFileName, commitMetadataSerDeV1, activeTimelineV1);
    } catch (IOException e) {
      LOG.error("Can not to complete the downgrade from version eight to version seven. The reason for failure is {}", e.getMessage());
      throw new HoodieException(e);
    }
  }

  static boolean rewriteTimelineV2InstantFileToV1Format(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName, String replacedFileName,
                                                        CommitMetadataSerDeV1 commitMetadataSerDeV1, ActiveTimelineV1 activeTimelineV1)
      throws IOException {
    StoragePath fromPath = new StoragePath(TIMELINE_LAYOUT_V2.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), originalFileName);
    long modificationTime = instant.isCompleted() ? convertCompletionTimeToEpoch(instant) : -1;
    StoragePath toPath = new StoragePath(TIMELINE_LAYOUT_V1.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), replacedFileName);
    boolean success = true;
    if (instant.getAction().equals(COMMIT_ACTION) || instant.getAction().equals(DELTA_COMMIT_ACTION)
        || ((instant.getAction().equals(REPLACE_COMMIT_ACTION) || instant.getAction().equals(CLUSTERING_ACTION)) && instant.isCompleted())) {
      Option<HoodieInstantWriter> instantWriterOption;
      if (instant.getAction().equals(REPLACE_COMMIT_ACTION) || instant.getAction().equals(CLUSTERING_ACTION)) {
        instantWriterOption = commitMetadataSerDeV1.getInstantWriter(metaClient.getActiveTimeline().readReplaceCommitMetadata(instant));
      } else {
        instantWriterOption = commitMetadataSerDeV1.getInstantWriter(metaClient.getActiveTimeline().readCommitMetadata(instant));
      }
      String toPathStr = toPath.toUri().toString();
      activeTimelineV1.createFileInMetaPath(toPathStr, instantWriterOption, true);
      /*
        When we downgrade the table from 1.0 to 0.x, it is important to set the modification
        timestamp of the 0.x completed instant to match the completion time of the
        corresponding 1.x instant. Otherwise,  log files in previous file slices could
        be wrongly attributed to latest file slice for 1.0 readers.
        (see HoodieFileGroup.getBaseInstantTime)
       */
      if (modificationTime > 0) {
        metaClient.getStorage().setModificationTime(toPath, modificationTime);
      }
      metaClient.getStorage().deleteFile(fromPath);
    } else {
      success = metaClient.getStorage().rename(fromPath, toPath);
    }
    if (!success) {
      throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
    }
    return true;
  }

  static void downgradeMetadataPartitions(HoodieEngineContext context,
                                          HoodieStorage hoodieStorage,
                                          HoodieTableMetaClient metaClient,
                                          Map<ConfigProperty, String> tablePropsToAdd) {
    // Get base path for metadata table.
    StoragePath metadataTableBasePath =
        HoodieTableMetadata.getMetadataTableBasePath(metaClient.getBasePath());
    HoodieTableMetaClient metadataMetaClient =
        HoodieTableMetaClient.builder()
            .setBasePath(metadataTableBasePath.toUri().toString())
            .setConf(hoodieStorage.getConf())
            .build();

    // Fetch metadata partition paths.
    List<String> metadataPartitions = FSUtils.getAllPartitionPaths(context, metadataMetaClient, false);

    // Delete partitions.
    List<String> validPartitionPaths = deleteMetadataPartition(context, metaClient, metadataPartitions);

    // Clean the configuration.
    tablePropsToAdd.put(TABLE_METADATA_PARTITIONS, String.join(",", validPartitionPaths));
  }

  static List<String> deleteMetadataPartition(HoodieEngineContext context,
                                              HoodieTableMetaClient metaClient,
                                              List<String> metadataPartitions) {
    metadataPartitions.stream()
        .filter(metadataPath -> !SUPPORTED_METADATA_PARTITION_PATHS.contains(metadataPath))
        .forEach(metadataPath ->
            HoodieTableMetadataUtil.deleteMetadataTablePartition(
                metaClient, context, metadataPath, true)
        );

    return metadataPartitions.stream()
        .filter(SUPPORTED_METADATA_PARTITION_PATHS::contains)
        .collect(Collectors.toList());
  }

  private static Set<String> getSupportedMetadataPartitionPaths() {
    Set<String> supportedPartitionPaths = new HashSet<>();
    supportedPartitionPaths.add(BLOOM_FILTERS.getPartitionPath());
    supportedPartitionPaths.add(COLUMN_STATS.getPartitionPath());
    supportedPartitionPaths.add(FILES.getPartitionPath());
    supportedPartitionPaths.add(RECORD_INDEX.getPartitionPath());
    return supportedPartitionPaths;
  }
}
