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

package org.apache.hudi.common.util;

import java.util.stream.Collectors;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanPartitionMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.model.CleanFileInfo;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataMigrator;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataV1MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanMetadataV2MigrationHandler;
import org.apache.hudi.common.table.timeline.versioning.clean.CleanPlanMigrator;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;

public class CleanerUtils {

  private static final Logger LOG = LogManager.getLogger(CleanerUtils.class);

  public static final Integer CLEAN_METADATA_VERSION_1 = CleanMetadataV1MigrationHandler.VERSION;
  public static final Integer CLEAN_METADATA_VERSION_2 = CleanMetadataV2MigrationHandler.VERSION;
  public static final Integer LATEST_CLEAN_METADATA_VERSION = CLEAN_METADATA_VERSION_2;

  public static HoodieCleanMetadata convertCleanMetadata(String startCleanTime,
                                                         Option<Long> durationInMs,
                                                         List<HoodieCleanStat> cleanStats) {
    Map<String, HoodieCleanPartitionMetadata> partitionMetadataMap = new HashMap<>();
    Map<String, HoodieCleanPartitionMetadata> partitionBootstrapMetadataMap = new HashMap<>();

    int totalDeleted = 0;
    String earliestCommitToRetain = null;
    for (HoodieCleanStat stat : cleanStats) {
      HoodieCleanPartitionMetadata metadata =
          new HoodieCleanPartitionMetadata(stat.getPartitionPath(), stat.getPolicy().name(),
              stat.getDeletePathPatterns(), stat.getSuccessDeleteFiles(), stat.getFailedDeleteFiles(), stat.isPartitionDeleted());
      partitionMetadataMap.put(stat.getPartitionPath(), metadata);
      if ((null != stat.getDeleteBootstrapBasePathPatterns())
          && (!stat.getDeleteBootstrapBasePathPatterns().isEmpty())) {
        HoodieCleanPartitionMetadata bootstrapMetadata = new HoodieCleanPartitionMetadata(stat.getPartitionPath(),
            stat.getPolicy().name(), stat.getDeleteBootstrapBasePathPatterns(), stat.getSuccessDeleteBootstrapBaseFiles(),
            stat.getFailedDeleteBootstrapBaseFiles(), stat.isPartitionDeleted());
        partitionBootstrapMetadataMap.put(stat.getPartitionPath(), bootstrapMetadata);
      }
      totalDeleted += stat.getSuccessDeleteFiles().size();
      if (earliestCommitToRetain == null) {
        // This will be the same for all partitions
        earliestCommitToRetain = stat.getEarliestCommitToRetain();
      }
    }

    return new HoodieCleanMetadata(startCleanTime, durationInMs.orElseGet(() -> -1L), totalDeleted,
      earliestCommitToRetain, partitionMetadataMap, CLEAN_METADATA_VERSION_2, partitionBootstrapMetadataMap);
  }

  /**
   * Get Latest Version of Hoodie Cleaner Metadata - Output of cleaner operation.
   * @param metaClient Hoodie Table Meta Client
   * @param cleanInstant Instant referring to clean action
   * @return Latest version of Clean metadata corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanMetadata getCleanerMetadata(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant)
      throws IOException {
    CleanMetadataMigrator metadataMigrator = new CleanMetadataMigrator(metaClient);
    HoodieCleanMetadata cleanMetadata = TimelineMetadataUtils.deserializeHoodieCleanMetadata(
        metaClient.getActiveTimeline().readCleanerInfoAsBytes(cleanInstant).get());
    return metadataMigrator.upgradeToLatest(cleanMetadata, cleanMetadata.getVersion());
  }

  /**
   * Get Latest version of cleaner plan corresponding to a clean instant.
   * @param metaClient  Hoodie Table Meta Client
   * @param cleanInstant Instant referring to clean action
   * @return Cleaner plan corresponding to clean instant
   * @throws IOException
   */
  public static HoodieCleanerPlan getCleanerPlan(HoodieTableMetaClient metaClient, HoodieInstant cleanInstant)
      throws IOException {
    CleanPlanMigrator cleanPlanMigrator = new CleanPlanMigrator(metaClient);
    HoodieCleanerPlan cleanerPlan = TimelineMetadataUtils.deserializeAvroMetadata(
        metaClient.getActiveTimeline().readCleanerInfoAsBytes(cleanInstant).get(), HoodieCleanerPlan.class);
    return cleanPlanMigrator.upgradeToLatest(cleanerPlan, cleanerPlan.getVersion());
  }

  /**
   * Convert list of cleanFileInfo instances to list of avro-generated HoodieCleanFileInfo instances.
   * @param cleanFileInfoList
   * @return
   */
  public static List<HoodieCleanFileInfo> convertToHoodieCleanFileInfoList(List<CleanFileInfo> cleanFileInfoList) {
    return cleanFileInfoList.stream().map(CleanFileInfo::toHoodieFileCleanInfo).collect(Collectors.toList());
  }

  /**
   * Execute {@link HoodieFailedWritesCleaningPolicy} to rollback failed writes for different actions.
   * @param cleaningPolicy
   * @param actionType
   * @param rollbackFailedWritesFunc
   */
  public static void rollbackFailedWrites(HoodieFailedWritesCleaningPolicy cleaningPolicy, String actionType,
                                          Functions.Function0<Boolean> rollbackFailedWritesFunc) {
    switch (actionType) {
      case HoodieTimeline.CLEAN_ACTION:
        if (cleaningPolicy.isEager()) {
          // No need to do any special cleanup for failed operations during clean
          return;
        } else if (cleaningPolicy.isLazy()) {
          LOG.info("Cleaned failed attempts if any");
          // Perform rollback of failed operations for all types of actions during clean
          rollbackFailedWritesFunc.apply();
          return;
        }
        // No action needed for cleaning policy NEVER
        break;
      case COMMIT_ACTION:
        // For any other actions, perform rollback of failed writes
        if (cleaningPolicy.isEager()) {
          LOG.info("Cleaned failed attempts if any");
          rollbackFailedWritesFunc.apply();
          return;
        }
        break;
      default:
        throw new IllegalArgumentException("Unsupported action type " + actionType);
    }
  }
}
