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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieUpgradeDowngradeException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.compact.CompactHelpers;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 5 to 6.
 * Since we do not write/read compaction plan from .aux folder anyone, the
 * upgrade handler will delete compaction files from .aux folder.
 */
public class FiveToSixUpgradeHandler implements UpgradeHandler {

  private static final Logger LOG = LoggerFactory.getLogger(FiveToSixUpgradeHandler.class);

  @Override
  public Map<ConfigProperty, String> upgrade(HoodieWriteConfig config, HoodieEngineContext context, String instantTime, SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    final HoodieTable table = upgradeDowngradeHelper.getTable(config, context);

    deleteCompactionRequestedFileFromAuxiliaryFolder(table);
    checkUncompletedInstants(table);
    compactMetadataTableIfNeeded(table, context);

    return Collections.emptyMap();
  }

  /**
   * See HUDI-6040.
   */
  private void deleteCompactionRequestedFileFromAuxiliaryFolder(HoodieTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTimeline compactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline()
        .filter(instant -> instant.getState() == HoodieInstant.State.REQUESTED);
    compactionTimeline.getInstantsAsStream().forEach(
        deleteInstant -> {
          LOG.info("Deleting instant " + deleteInstant + " in auxiliary meta path " + metaClient.getMetaAuxiliaryPath());
          Path metaFile = new Path(metaClient.getMetaAuxiliaryPath(), deleteInstant.getFileName());
          try {
            if (metaClient.getFs().exists(metaFile)) {
              metaClient.getFs().delete(metaFile, false);
              LOG.info("Deleted instant file in auxiliary meta path : " + metaFile);
            }
          } catch (IOException e) {
            throw new HoodieUpgradeDowngradeException(HoodieTableVersion.FIVE.versionCode(), HoodieTableVersion.SIX.versionCode(), true, e);
          }
        }
    );
  }

  /**
   * When upgrading to {@link HoodieTableVersion#SIX}, it is required that pending actions in both
   * data table and metadata table should be either completed or rollback.
   * <p>
   * Note: this will be invoked for both data and metadata tables during upgrade/downgrade.
   *
   * @see UpgradeDowngrade#run(HoodieTableVersion, String)
   */
  static void checkUncompletedInstants(HoodieTable table) {
    try {
      List<HoodieInstant> uncompletedInstants = table.getActiveTimeline().filterInflightsAndRequested().getInstants();
      checkState(uncompletedInstants.isEmpty(), "Found uncompleted instants: "
          + uncompletedInstants.stream().map(HoodieInstant::getTimestamp).collect(Collectors.joining(",")));
    } catch (Exception e) {
      throw new HoodieUpgradeDowngradeException(
          "There are uncompleted instants in the table's timeline at '"
              + table.getConfig().getBasePath()
              + "'. Please complete the operations or perform rollback before upgrade.", e);
    }
  }

  private static void compactMetadataTableIfNeeded(HoodieTable table, HoodieEngineContext context) {
    if (!table.isMetadataTable()) {
      return;
    }
    Option<HoodieInstant> lastCommitInstantOpt = table.getActiveTimeline().getCommitsTimeline().lastInstant();
    if (!lastCommitInstantOpt.isPresent()) {
      return;
    }
    checkState(lastCommitInstantOpt.get().getState() == HoodieInstant.State.COMPLETED,
        "Found uncompleted instants: " + lastCommitInstantOpt.get());
    boolean shouldCompact = !lastCommitInstantOpt.get().getAction().equals(HoodieTimeline.COMMIT_ACTION);
    if (shouldCompact) {
      String compactionInstantTime = HoodieActiveTimeline.createNewInstantTime();
      table.scheduleCompaction(context, compactionInstantTime, Option.empty());
      HoodieCommitMetadata commitMetadata = (HoodieCommitMetadata) table
          .compact(context, compactionInstantTime).getCommitMetadata().get();
      CompactHelpers.getInstance().completeInflightCompaction(table, compactionInstantTime, commitMetadata);
    }
  }
}
