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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.BaseRollbackHelper;
import org.apache.hudi.table.action.rollback.ListingBasedRollbackStrategy;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Upgrade handle to assist in upgrading hoodie table from version 0 to 1.
 */
public class ZeroToOneUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(
      HoodieWriteConfig config, HoodieEngineContext context, String instantTime,
      SupportsUpgradeDowngrade upgradeDowngradeHelper) {
    // fetch pending commit info
    HoodieTable table = upgradeDowngradeHelper.getTable(config, context);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    if (commits.size() > 0 && instantTime != null) {
      // ignore the latest inflight commit since a new commit would have been started and we need to fix any pending commits from previous launch
      commits.remove(instantTime);
    }
    for (String commit : commits) {
      // for every pending commit, delete old markers and re-create markers in new format
      recreateMarkers(commit, table, context, config.getInt(HoodieWriteConfig.MARKERS_DELETE_PARALLELISM_VALUE));
    }
    return Collections.EMPTY_MAP;
  }

  /**
   * Recreate markers in new format.
   * Step1: Delete existing markers
   * Step2: Collect all rollback file info.
   * Step3: recreate markers for all interested files.
   *
   * @param commitInstantTime instant of interest for which markers need to be recreated.
   * @param table             instance of {@link HoodieTable} to use
   * @param context           instance of {@link HoodieEngineContext} to use
   * @throws HoodieRollbackException on any exception during upgrade.
   */
  protected void recreateMarkers(final String commitInstantTime,
                                 HoodieTable table,
                                 HoodieEngineContext context,
                                 int parallelism) throws HoodieRollbackException {
    try {
      // fetch hoodie instant
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        // delete existing markers
        WriteMarkers writeMarkers = WriteMarkersFactory.get(MarkerType.DIRECT, table, commitInstantTime);
        writeMarkers.quietDeleteMarkerDir(context, parallelism);

        // generate rollback stats
        List<HoodieRollbackStat> rollbackStats = getListBasedRollBackStats(table, context, commitInstantOpt);

        // recreate markers adhering to marker based rollback
        for (HoodieRollbackStat rollbackStat : rollbackStats) {
          for (String path : rollbackStat.getSuccessDeleteFiles()) {
            String dataFileName = path.substring(path.lastIndexOf("/") + 1);
            // not feasible to differentiate MERGE from CREATE. hence creating with MERGE IOType for all base files.
            writeMarkers.create(rollbackStat.getPartitionPath(), dataFileName, IOType.MERGE);
          }
          for (FileStatus fileStatus : rollbackStat.getCommandBlocksCount().keySet()) {
            writeMarkers.create(rollbackStat.getPartitionPath(), getFileNameForMarkerFromLogFile(fileStatus.getPath().toString(), table), IOType.APPEND);
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Exception thrown while upgrading Hoodie Table from version 0 to 1", e);
    }
  }

  List<HoodieRollbackStat> getListBasedRollBackStats(HoodieTable<?, ?, ?, ?> table, HoodieEngineContext context, Option<HoodieInstant> commitInstantOpt) {
    List<HoodieRollbackRequest> hoodieRollbackRequests =
        new ListingBasedRollbackStrategy(table, context, table.getConfig(), commitInstantOpt.get().getTimestamp())
            .getRollbackRequests(commitInstantOpt.get());
    return new BaseRollbackHelper(table.getMetaClient(), table.getConfig())
        .collectRollbackStats(context, commitInstantOpt.get(), hoodieRollbackRequests);
  }

  /**
   * Curates file name for marker from existing log file path.
   * log file format     : partitionpath/.fileid_baseInstant.log.writetoken
   * marker file format  : partitionpath/fileId_writetoken_baseinstant.basefileExtn.marker.APPEND
   *
   * @param logFilePath log file path for which marker file name needs to be generated.
   * @param table       {@link HoodieTable} instance to use
   * @return the marker file name thus curated.
   */
  private static String getFileNameForMarkerFromLogFile(String logFilePath, HoodieTable<?, ?, ?, ?> table) {
    Path logPath = new Path(table.getMetaClient().getBasePath(), logFilePath);
    String fileId = FSUtils.getFileIdFromLogPath(logPath);
    String baseInstant = FSUtils.getBaseCommitTimeFromLogPath(logPath);
    String writeToken = FSUtils.getWriteTokenFromLogPath(logPath);

    return FSUtils.makeBaseFileName(baseInstant, writeToken, fileId, table.getBaseFileFormat().getFileExtension());
  }
}
