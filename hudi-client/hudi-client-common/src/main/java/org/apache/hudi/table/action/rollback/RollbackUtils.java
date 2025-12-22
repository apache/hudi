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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePathInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

public class RollbackUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RollbackUtils.class);

  /**
   * Get Latest version of Rollback plan corresponding to a clean instant.
   *
   * @param metaClient      Hoodie Table Meta Client
   * @param rollbackInstant Instant referring to rollback action
   * @return Rollback plan corresponding to rollback instant
   * @throws IOException
   */
  public static HoodieRollbackPlan getRollbackPlan(HoodieTableMetaClient metaClient, HoodieInstant rollbackInstant)
      throws IOException {
    // TODO: add upgrade step if required.
    final HoodieInstant requested = HoodieTimeline.getRollbackRequestedInstant(rollbackInstant);
    return TimelineMetadataUtils.deserializeAvroMetadata(
        metaClient.getActiveTimeline().readRollbackInfoAsBytes(requested).get(), HoodieRollbackPlan.class);
  }

  static Map<HoodieLogBlock.HeaderMetadataType, String> generateHeader(String instantToRollback, String rollbackInstantTime) {
    // generate metadata
    Map<HoodieLogBlock.HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HoodieLogBlock.HeaderMetadataType.INSTANT_TIME, rollbackInstantTime);
    header.put(HoodieLogBlock.HeaderMetadataType.TARGET_INSTANT_TIME, instantToRollback);
    header.put(HoodieLogBlock.HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return header;
  }

  /**
   * Helper to merge 2 rollback-stats for a given partition.
   *
   * @param stat1 HoodieRollbackStat
   * @param stat2 HoodieRollbackStat
   * @return Merged HoodieRollbackStat
   */
  static HoodieRollbackStat mergeRollbackStat(HoodieRollbackStat stat1, HoodieRollbackStat stat2) {
    checkArgument(stat1.getPartitionPath().equals(stat2.getPartitionPath()));
    final List<String> successDeleteFiles = new ArrayList<>();
    final List<String> failedDeleteFiles = new ArrayList<>();
    final Map<StoragePathInfo, Long> commandBlocksCount = new HashMap<>();
    final Map<String, Long> logFilesFromFailedCommit = new HashMap<>();
    Option.ofNullable(stat1.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat2.getSuccessDeleteFiles()).ifPresent(successDeleteFiles::addAll);
    Option.ofNullable(stat1.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat2.getFailedDeleteFiles()).ifPresent(failedDeleteFiles::addAll);
    Option.ofNullable(stat1.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat2.getCommandBlocksCount()).ifPresent(commandBlocksCount::putAll);
    Option.ofNullable(stat1.getLogFilesFromFailedCommit()).ifPresent(logFilesFromFailedCommit::putAll);
    Option.ofNullable(stat2.getLogFilesFromFailedCommit()).ifPresent(logFilesFromFailedCommit::putAll);
    return new HoodieRollbackStat(stat1.getPartitionPath(), successDeleteFiles, failedDeleteFiles, commandBlocksCount, logFilesFromFailedCommit);
  }

  static HoodieRollbackRequest mergeRollbackRequest(HoodieRollbackRequest rollbackRequest1, HoodieRollbackRequest rollbackRequest2) {
    checkArgument(rollbackRequest1.getPartitionPath().equals(rollbackRequest2.getPartitionPath()));
    checkArgument((rollbackRequest1.getFileId().equals(rollbackRequest2.getFileId())));
    checkArgument((rollbackRequest1.getLatestBaseInstant().equals(rollbackRequest2.getLatestBaseInstant())));
    final List<String> filesToBeDeleted = new ArrayList<>();
    final Map<String, Long> logBlocksToBeDeleted = new HashMap<>();
    Option.ofNullable(rollbackRequest1.getFilesToBeDeleted()).ifPresent(filesToBeDeleted::addAll);
    Option.ofNullable(rollbackRequest1.getLogBlocksToBeDeleted()).ifPresent(logBlocksToBeDeleted::putAll);
    Option.ofNullable(rollbackRequest2.getFilesToBeDeleted()).ifPresent(filesToBeDeleted::addAll);
    Option.ofNullable(rollbackRequest2.getLogBlocksToBeDeleted()).ifPresent(logBlocksToBeDeleted::putAll);

    return new HoodieRollbackRequest(rollbackRequest1.getPartitionPath(), rollbackRequest1.getFileId(), rollbackRequest1.getLatestBaseInstant(),
        filesToBeDeleted, logBlocksToBeDeleted);
  }
}
