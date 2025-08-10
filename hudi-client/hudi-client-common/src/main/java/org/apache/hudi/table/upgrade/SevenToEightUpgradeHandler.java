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

import org.apache.hudi.client.timeline.versioning.v2.LSMTimelineWriter;
import org.apache.hudi.client.utils.LegacyArchivedMetaEntryReader;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.BootstrapIndexType;
import org.apache.hudi.common.model.DefaultHoodieRecordPayload;
import org.apache.hudi.common.model.EventTimeAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.ActiveAction;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.timeline.versioning.v1.ActiveTimelineV1;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieInstant.UNDERSCORE;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V1;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V2;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.SIX_TO_EIGHT_TIMELINE_ACTION_MAP;
import static org.apache.hudi.table.upgrade.UpgradeDowngradeUtils.checkAndHandleMetadataTable;

/**
 * Version 7 is going to be placeholder version for bridge release 0.16.0.
 * Version 8 is the placeholder version to track 1.x.
 */
public class SevenToEightUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(SevenToEightUpgradeHandler.class);

  @Override
  public UpgradeDowngrade.TableConfigChangeSet upgrade(HoodieWriteConfig config,
                                                                         HoodieEngineContext context,
                                                                         String instantTime,
                                                                         SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    Map<ConfigProperty, String> tablePropsToAdd = new HashMap<>();
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTableConfig tableConfig = metaClient.getTableConfig();
    // If auto upgrade is disabled, set writer version to 6 and return
    if (!config.autoUpgrade()) {
      config.setValue(HoodieWriteConfig.WRITE_TABLE_VERSION, String.valueOf(HoodieTableVersion.SIX.versionCode()));
      return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, Collections.emptyList());
    }

    // If metadata is enabled for the data table, and existing metadata table is behind the data table, then delete it
    checkAndHandleMetadataTable(context, table, config, metaClient, true);

    try {
      HoodieTableMetaClient.createTableLayoutOnStorage(context.getStorageConf(), new StoragePath(config.getBasePath()), config.getProps(), TimelineLayoutVersion.VERSION_2, false);
    } catch (IOException e) {
      LOG.error("Failed to create table layout on storage for timeline layout version {}", TimelineLayoutVersion.VERSION_2, e);
      throw new HoodieIOException("Failed to create table layout on storage", e);
    }

    // handle table properties upgrade
    tablePropsToAdd.put(HoodieTableConfig.TIMELINE_PATH, HoodieTableConfig.TIMELINE_PATH.defaultValue());
    upgradePartitionFields(config, tableConfig, tablePropsToAdd);
    upgradeMergeMode(tableConfig, tablePropsToAdd);
    setInitialVersion(tableConfig, tablePropsToAdd);
    upgradeKeyGeneratorType(tableConfig, tablePropsToAdd);
    upgradeBootstrapIndexType(tableConfig, tablePropsToAdd);

    // Handle timeline upgrade:
    //  - Rewrite instants in active timeline to new format
    //  - Convert archived timeline to new LSM timeline format
    List<HoodieInstant> instants;
    try {
      // We need to move all the instants - not just completed ones.
      instants = metaClient.scanHoodieInstantsFromFileSystem(metaClient.getTimelinePath(),
          ActiveTimelineV1.VALID_EXTENSIONS_IN_ACTIVE_TIMELINE, false);
    } catch (IOException ioe) {
      LOG.error("Failed to get instants from filesystem", ioe);
      throw new HoodieIOException("Failed to get instants from filesystem", ioe);
    }

    if (!instants.isEmpty()) {
      InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
      CommitMetadataSerDeV2 commitMetadataSerDeV2 = new CommitMetadataSerDeV2();
      CommitMetadataSerDeV1 commitMetadataSerDeV1 = new CommitMetadataSerDeV1();
      ActiveTimelineV2 activeTimelineV2 = new ActiveTimelineV2(metaClient);
      context.map(instants, instant -> {
        String originalFileName = instantFileNameGenerator.getFileName(instant);
        return upgradeActiveTimelineInstant(instant, originalFileName, metaClient, commitMetadataSerDeV1, commitMetadataSerDeV2, activeTimelineV2);
      }, instants.size());
    }

    upgradeToLSMTimeline(table, context, config);

    return new UpgradeDowngrade.TableConfigChangeSet(tablePropsToAdd, Collections.emptyList());
  }

  static void upgradePartitionFields(HoodieWriteConfig config, HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    String partitionPathField = config.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key());
    if (keyGenerator != null && partitionPathField != null
        && (keyGenerator.equals(KeyGeneratorType.CUSTOM.getClassName()) || keyGenerator.equals(KeyGeneratorType.CUSTOM_AVRO.getClassName()))) {
      tablePropsToAdd.put(HoodieTableConfig.PARTITION_FIELDS, partitionPathField);
    }
  }

  static void upgradeMergeMode(HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String payloadClass = tableConfig.getPayloadClass();
    String preCombineFields = tableConfig.getPreCombineFieldsStr().orElse(null);
    if (isCustomPayloadClass(payloadClass)) {
      // This contains a special case: HoodieMetadataPayload.
      tablePropsToAdd.put(
          HoodieTableConfig.PAYLOAD_CLASS_NAME,
          payloadClass);
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_MODE,
          RecordMergeMode.CUSTOM.name());
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID,
          HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID);
    } else if (tableConfig.getTableType() == HoodieTableType.COPY_ON_WRITE) {
      setEventTimeOrCommitTimeBasedOnPayload(payloadClass, tablePropsToAdd);
    } else { // MOR table
      if (StringUtils.nonEmpty(preCombineFields)) {
        // This contains a special case: OverwriteWithLatestPayload with preCombine field.
        tablePropsToAdd.put(
            HoodieTableConfig.PAYLOAD_CLASS_NAME,
            DefaultHoodieRecordPayload.class.getName());
        tablePropsToAdd.put(
            HoodieTableConfig.RECORD_MERGE_MODE,
            RecordMergeMode.EVENT_TIME_ORDERING.name());
        tablePropsToAdd.put(
            HoodieTableConfig.RECORD_MERGE_STRATEGY_ID,
            HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
      } else {
        setEventTimeOrCommitTimeBasedOnPayload(payloadClass, tablePropsToAdd);
      }
    }
  }

  private static void setEventTimeOrCommitTimeBasedOnPayload(String payloadClass, Map<ConfigProperty, String> tablePropsToAdd) {
    // DefaultRecordPayload without preCombine Field.
    // This is unlikely to happen.
    if (useDefaultHoodieRecordPayload(payloadClass)) {
      tablePropsToAdd.put(
          HoodieTableConfig.PAYLOAD_CLASS_NAME,
          DefaultHoodieRecordPayload.class.getName());
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_MODE,
          RecordMergeMode.EVENT_TIME_ORDERING.name());
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID,
          HoodieRecordMerger.EVENT_TIME_BASED_MERGE_STRATEGY_UUID);
    } else {
      tablePropsToAdd.put(
          HoodieTableConfig.PAYLOAD_CLASS_NAME,
          OverwriteWithLatestAvroPayload.class.getName());
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_MODE,
          RecordMergeMode.COMMIT_TIME_ORDERING.name());
      tablePropsToAdd.put(
          HoodieTableConfig.RECORD_MERGE_STRATEGY_ID,
          HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
    }
  }

  static boolean useDefaultHoodieRecordPayload(String payloadClass) {
    return !StringUtils.isNullOrEmpty(payloadClass)
        && payloadClass.equals(DefaultHoodieRecordPayload.class.getName());
  }

  static boolean isCustomPayloadClass(String payloadClass) {
    return !StringUtils.isNullOrEmpty(payloadClass)
        && !payloadClass.equals(DefaultHoodieRecordPayload.class.getName())
        && !payloadClass.equals(EventTimeAvroPayload.class.getName())
        && !payloadClass.equals(OverwriteWithLatestAvroPayload.class.getName());
  }

  static void setInitialVersion(HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    if (tableConfig.contains(HoodieTableConfig.VERSION)) {
      tablePropsToAdd.put(HoodieTableConfig.INITIAL_VERSION, String.valueOf(tableConfig.getTableVersion().versionCode()));
    } else {
      tablePropsToAdd.put(HoodieTableConfig.INITIAL_VERSION, String.valueOf(HoodieTableVersion.SIX.versionCode()));
    }
  }

  static void upgradeBootstrapIndexType(HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    if (tableConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME) || tableConfig.contains(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE)) {
      String bootstrapIndexClass = BootstrapIndexType.getBootstrapIndexClassName(tableConfig);
      if (StringUtils.nonEmpty(bootstrapIndexClass)) {
        tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_CLASS_NAME, bootstrapIndexClass);
        tablePropsToAdd.put(HoodieTableConfig.BOOTSTRAP_INDEX_TYPE, BootstrapIndexType.fromClassName(bootstrapIndexClass).name());
      }
    }
  }

  static void upgradeKeyGeneratorType(HoodieTableConfig tableConfig, Map<ConfigProperty, String> tablePropsToAdd) {
    String keyGenerator = tableConfig.getKeyGeneratorClassName();
    if (StringUtils.nonEmpty(keyGenerator)) {
      tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_CLASS_NAME, keyGenerator);
      tablePropsToAdd.put(HoodieTableConfig.KEY_GENERATOR_TYPE, KeyGeneratorType.fromClassName(keyGenerator).name());
    }
  }

  static void upgradeToLSMTimeline(HoodieTable table, HoodieEngineContext engineContext, HoodieWriteConfig config) {
    table.getMetaClient().getTableConfig().getTimelineLayoutVersion().ifPresent(
        timelineLayoutVersion -> ValidationUtils.checkState(TimelineLayoutVersion.LAYOUT_VERSION_1.equals(timelineLayoutVersion),
            "Upgrade to LSM timeline is only supported for layout version 1. Given version: " + timelineLayoutVersion));
    try {
      LegacyArchivedMetaEntryReader reader = new LegacyArchivedMetaEntryReader(table.getMetaClient());
      StoragePath archivePath = new StoragePath(table.getMetaClient().getMetaPath(), "timeline/history");
      LSMTimelineWriter lsmTimelineWriter = LSMTimelineWriter.getInstance(config, table, Option.of(archivePath));
      int batchSize = config.getCommitArchivalBatchSize();
      List<ActiveAction> activeActionsBatch = new ArrayList<>(batchSize);
      try (ClosableIterator<ActiveAction> iterator = reader.getActiveActionsIterator()) {
        while (iterator.hasNext()) {
          activeActionsBatch.add(iterator.next());
          // If the batch is full, write it to the LSM timeline
          if (activeActionsBatch.size() == batchSize) {
            lsmTimelineWriter.write(new ArrayList<>(activeActionsBatch), Option.empty(), Option.empty());
            lsmTimelineWriter.compactAndClean(engineContext);
            activeActionsBatch.clear();
          }
        }

        // Write any remaining actions in the final batch
        if (!activeActionsBatch.isEmpty()) {
          lsmTimelineWriter.write(new ArrayList<>(activeActionsBatch), Option.empty(), Option.empty());
          lsmTimelineWriter.compactAndClean(engineContext);
        }
      }
    } catch (Exception e) {
      if (config.isFailOnTimelineArchivingEnabled()) {
        throw new HoodieException("Failed to upgrade to LSM timeline", e);
      } else {
        LOG.warn("Failed to upgrade to LSM timeline");
      }
    }
  }

  static boolean upgradeActiveTimelineInstant(HoodieInstant instant, String originalFileName, HoodieTableMetaClient metaClient, CommitMetadataSerDeV1 commitMetadataSerDeV1,
                                              CommitMetadataSerDeV2 commitMetadataSerDeV2, ActiveTimelineV2 activeTimelineV2) {
    String replacedFileName = originalFileName;
    boolean isCompleted = instant.isCompleted();
    // Rename the metadata file name from the ${instant_time}.action[.state] format in version 0.x
    // to the ${instant_time}_${completion_time}.action[.state] format in version 1.x.
    if (isCompleted) {
      String completionTime = instant.getCompletionTime(); // this is the file modification time
      String startTime = instant.requestedTime();
      replacedFileName = replacedFileName.replace(startTime, startTime + UNDERSCORE + completionTime);
    }
    // Rename the action if necessary (e.g., REPLACE_COMMIT_ACTION to CLUSTERING_ACTION).
    // NOTE: New action names were only applied for pending instants. Completed instants do not have any change in action names.
    if (SIX_TO_EIGHT_TIMELINE_ACTION_MAP.containsKey(instant.getAction()) && !isCompleted) {
      replacedFileName = replacedFileName.replace(instant.getAction(), SIX_TO_EIGHT_TIMELINE_ACTION_MAP.get(instant.getAction()));
    }
    try {
      return rewriteTimelineV1InstantFileToV2Format(instant, metaClient, originalFileName, replacedFileName, commitMetadataSerDeV1, commitMetadataSerDeV2, activeTimelineV2);
    } catch (IOException e) {
      LOG.warn("Can not to complete the upgrade from version seven to version eight. The reason for failure is {}", e.getMessage());
    }
    return false;
  }

  static boolean rewriteTimelineV1InstantFileToV2Format(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName, String replacedFileName,
                                                        CommitMetadataSerDeV1 commitMetadataSerDeV1, CommitMetadataSerDeV2 commitMetadataSerDeV2, ActiveTimelineV2 activeTimelineV2)
      throws IOException {
    StoragePath fromPath = new StoragePath(TIMELINE_LAYOUT_V1.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), originalFileName);
    StoragePath toPath = new StoragePath(TIMELINE_LAYOUT_V2.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), replacedFileName);
    boolean success = true;
    if (instant.getAction().equals(COMMIT_ACTION) || instant.getAction().equals(DELTA_COMMIT_ACTION) || (instant.getAction().equals(REPLACE_COMMIT_ACTION) && instant.isCompleted())) {
      Class<? extends HoodieCommitMetadata> clazz = instant.getAction().equals(REPLACE_COMMIT_ACTION) ? HoodieReplaceCommitMetadata.class : HoodieCommitMetadata.class;
      HoodieCommitMetadata commitMetadata = metaClient.getActiveTimeline().readInstantContent(instant, clazz);
      String toPathStr = toPath.toUri().toString();
      activeTimelineV2.createFileInMetaPath(toPathStr, Option.of(commitMetadata), true);
      metaClient.getStorage().deleteFile(fromPath);
    } else {
      success = metaClient.getStorage().rename(fromPath, toPath);
    }
    if (!success) {
      throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
    }
    return true;
  }
}
