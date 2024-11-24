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

import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.timeline.versioning.v1.CommitMetadataSerDeV1;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.CommitMetadataSerDeV2;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;
import org.apache.hudi.table.action.compact.strategy.UnBoundedCompactionStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.CLUSTERING_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.REPLACE_COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V1;
import static org.apache.hudi.common.table.timeline.TimelineLayout.TIMELINE_LAYOUT_V2;

public class UpgradeDowngradeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDowngradeUtils.class);

  // Map of actions that were renamed in table version 8
  static final Map<String, String> SIX_TO_EIGHT_TIMELINE_ACTION_MAP = CollectionUtils.createImmutableMap(
      Pair.of(REPLACE_COMMIT_ACTION, CLUSTERING_ACTION)
  );

  /**
   * Utility method to run compaction for MOR table as part of downgrade step.
   */
  public static void runCompaction(HoodieTable table, HoodieEngineContext context, HoodieWriteConfig config,
                                   SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    try {
      if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
        // set required configs for scheduling compaction.
        HoodieInstantTimeGenerator.setCommitTimeZone(table.getMetaClient().getTableConfig().getTimelineTimezone());
        HoodieWriteConfig compactionConfig = HoodieWriteConfig.newBuilder().withProps(config.getProps()).build();
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
        compactionConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), CompactionTriggerStrategy.NUM_COMMITS.name());
        compactionConfig.setValue(HoodieCompactionConfig.COMPACTION_STRATEGY.key(), UnBoundedCompactionStrategy.class.getName());
        compactionConfig.setValue(HoodieMetadataConfig.ENABLE.key(), "false");
        try (BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(compactionConfig, context)) {
          Option<String> compactionInstantOpt = writeClient.scheduleCompaction(Option.empty());
          if (compactionInstantOpt.isPresent()) {
            writeClient.compact(compactionInstantOpt.get());
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  /**
   * See HUDI-6040.
   */
  public static void syncCompactionRequestedFileToAuxiliaryFolder(HoodieTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    TimelineFactory timelineFactory = metaClient.getTimelineLayout().getTimelineFactory();
    InstantFileNameGenerator instantFileNameGenerator = metaClient.getInstantFileNameGenerator();
    HoodieTimeline compactionTimeline = timelineFactory.createActiveTimeline(metaClient, false).filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    compactionTimeline.getInstantsAsStream().forEach(instant -> {
      String fileName = instantFileNameGenerator.getFileName(instant);
      try {
        if (!metaClient.getStorage().exists(new StoragePath(metaClient.getMetaAuxiliaryPath(), fileName))) {
          FileIOUtils.copy(metaClient.getStorage(),
              new StoragePath(metaClient.getTimelinePath(), fileName),
              new StoragePath(metaClient.getMetaAuxiliaryPath(), fileName));
        }
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
    });
  }

  static void updateMetadataTableVersion(HoodieEngineContext context, HoodieTableVersion toVersion, HoodieTableMetaClient dataMetaClient) throws HoodieIOException {
    try {
      StoragePath metadataBasePath = new StoragePath(dataMetaClient.getBasePath(), HoodieTableMetaClient.METADATA_TABLE_FOLDER_PATH);
      if (dataMetaClient.getStorage().exists(metadataBasePath)) {
        HoodieTableMetaClient metaClient = HoodieTableMetaClient.builder()
            .setConf(context.getStorageConf().newInstance())
            .setBasePath(metadataBasePath)
            .build();
        metaClient.getTableConfig().setTableVersion(toVersion);
        HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Error while updating metadata table version", e);
    }
  }

  static boolean renameTimelineV1InstantFileToV2Format(HoodieInstant instant, HoodieTableMetaClient metaClient, String originalFileName, String replacedFileName,
                                                       CommitMetadataSerDeV1 commitMetadataSerDeV1, CommitMetadataSerDeV2 commitMetadataSerDeV2, ActiveTimelineV2 activeTimelineV2)
      throws IOException {
    StoragePath fromPath = new StoragePath(TIMELINE_LAYOUT_V1.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), originalFileName);
    StoragePath toPath = new StoragePath(TIMELINE_LAYOUT_V2.getTimelinePathProvider().getTimelinePath(metaClient.getTableConfig(), metaClient.getBasePath()), replacedFileName);
    boolean success = true;
    if (instant.getAction().equals(HoodieTimeline.COMMIT_ACTION) || instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION)) {
      HoodieCommitMetadata commitMetadata =
          commitMetadataSerDeV1.deserialize(instant, metaClient.getActiveTimeline().getInstantDetails(instant).get(), HoodieCommitMetadata.class);
      Option<byte[]> data = commitMetadataSerDeV2.serialize(commitMetadata);
      String toPathStr = toPath.toUri().toString();
      activeTimelineV2.createFileInMetaPath(toPathStr, data, true);
      metaClient.getStorage().deleteFile(fromPath);
    } else {
      success = metaClient.getStorage().rename(fromPath, toPath);
    }
    if (!success) {
      throw new HoodieIOException("an error that occurred while renaming " + fromPath + " to: " + toPath);
    }
    return true;
  }

  /**
   * Extract Epoch time from completion time string
   *
   * @param instant : HoodieInstant
   * @return
   */
  public static long convertCompletionTimeToEpoch(HoodieInstant instant) {
    try {
      String completionTime = instant.getCompletionTime();
      // In Java 8, no direct API to convert to epoch in millis.
      // Strip off millis
      String completionTimeInSecs = Long.parseLong(completionTime) / 1000 + "";
      DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
      ZoneId zoneId = ZoneId.systemDefault();
      LocalDateTime ldtInSecs = LocalDateTime.parse(completionTimeInSecs, inputFormatter);
      long millis = Long.parseLong(completionTime.substring(completionTime.length() - 3));
      return ldtInSecs.atZone(zoneId).toEpochSecond() * 1000 + millis;
    } catch (Exception e) {
      LOG.warn("Failed to parse completion time string for instant " + instant, e);
      return -1;
    }
  }
}
