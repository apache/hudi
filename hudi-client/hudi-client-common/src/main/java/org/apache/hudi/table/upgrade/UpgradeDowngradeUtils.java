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
import org.apache.hudi.client.transaction.lock.NoopLockProvider;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.HoodieTimeGeneratorConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantFileNameGenerator;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieLockConfig;
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

public class UpgradeDowngradeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(UpgradeDowngradeUtils.class);

  // Map of actions that were renamed in table version 8
  static final Map<String, String> SIX_TO_EIGHT_TIMELINE_ACTION_MAP = CollectionUtils.createImmutableMap(
      Pair.of(REPLACE_COMMIT_ACTION, CLUSTERING_ACTION)
  );
  static final Map<String, String> EIGHT_TO_SIX_TIMELINE_ACTION_MAP = CollectionUtils.reverseMap(SIX_TO_EIGHT_TIMELINE_ACTION_MAP);

  /**
   * Utility method to run compaction for MOR table as part of downgrade step.
   *
   * @Deprecated Use {@link UpgradeDowngradeUtils#rollbackFailedWritesAndCompact(HoodieTable, HoodieEngineContext, HoodieWriteConfig, SupportsUpgradeDowngrade, boolean, HoodieTableVersion)} instead.
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
      LOG.warn("Failed to parse completion time string for instant {}", instant, e);
      return -1;
    }
  }

  static void rollbackFailedWritesAndCompact(HoodieTable table, HoodieEngineContext context, HoodieWriteConfig config,
                                             SupportsUpgradeDowngrade upgradeDowngradeHelper, boolean shouldCompact, HoodieTableVersion tableVersion) {
    try {
      // set required configs for rollback
      HoodieInstantTimeGenerator.setCommitTimeZone(table.getMetaClient().getTableConfig().getTimelineTimezone());
      // NOTE: at this stage rollback should use the current writer version and disable auto upgrade/downgrade
      TypedProperties properties = new TypedProperties();
      properties.putAll(config.getProps());
      // TimeGenerators are cached and re-used based on table base path. Since here we are changing the lock configurations, avoiding the cache use
      // for upgrade code block.
      properties.put(HoodieTimeGeneratorConfig.TIME_GENERATOR_REUSE_ENABLE.key(),"false");
      // override w/ NoopLock Provider to avoid re-entrant locking. already upgrade is happening within the table level lock.
      // Below we do trigger rollback and compaction which might again try to acquire the lock. So, here we are explicitly overriding to
      // NoopLockProvider for just the upgrade code block.
      properties.put(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(), NoopLockProvider.class.getName());
      // if auto adjust it not disabled, chances that InProcessLockProvider will get overridden for single writer use-cases.
      properties.put(HoodieWriteConfig.AUTO_ADJUST_LOCK_CONFIGS.key(),"false");
      HoodieWriteConfig rollbackWriteConfig = HoodieWriteConfig.newBuilder()
          .withProps(properties)
          .withWriteTableVersion(tableVersion.versionCode())
          .withAutoUpgradeVersion(false)
          .build();
      // set eager cleaning
      rollbackWriteConfig.setValue(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), HoodieFailedWritesCleaningPolicy.EAGER.name());
      rollbackWriteConfig.setValue(HoodieWriteConfig.ROLLBACK_USING_MARKERS_ENABLE.key(), String.valueOf(config.shouldRollbackUsingMarkers()));
      // set compaction configs
      if (shouldCompact) {
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "true");
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), "1");
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), CompactionTriggerStrategy.NUM_COMMITS.name());
        rollbackWriteConfig.setValue(HoodieCompactionConfig.COMPACTION_STRATEGY.key(), UnBoundedCompactionStrategy.class.getName());
      } else {
        rollbackWriteConfig.setValue(HoodieCompactionConfig.INLINE_COMPACT.key(), "false");
      }
      rollbackWriteConfig.setValue(HoodieMetadataConfig.ENABLE.key(), "false");

      try (BaseHoodieWriteClient writeClient = upgradeDowngradeHelper.getWriteClient(rollbackWriteConfig, context)) {
        writeClient.rollbackFailedWrites();
        if (shouldCompact) {
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
}
