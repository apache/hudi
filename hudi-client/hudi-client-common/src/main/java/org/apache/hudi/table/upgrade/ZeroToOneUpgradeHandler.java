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

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.BaseRollbackHelper;
import org.apache.hudi.table.action.rollback.ListingBasedRollbackHelper;
import org.apache.hudi.table.action.rollback.ListingBasedRollbackRequest;
import org.apache.hudi.table.action.rollback.RollbackUtils;
import org.apache.hudi.table.marker.WriteMarkers;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ZeroToOneUpgradeHandler implements UpgradeHandler {

  @Override
  public Map<ConfigProperty, String> upgrade(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context, String instantTime) {
    // fetch pending commit info
    HoodieTimeline inflightTimeline = metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    if (commits.size() > 0 && instantTime != null) {
      // ignore the latest inflight commit since a new commit would have been started and we need to fix any pending commits from previous launch
      commits.remove(instantTime);
    }
    for (String commit : commits) {
      // for every pending commit, delete old markers and re-create markers in new format
      recreateMarkers(commit, metaClient, config, context, config.getMarkersDeleteParallelism());
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
   * @param metaClient        instance of {@link HoodieTableMetaClient} to use
   * @param config            Write config
   * @param context           instance of {@link HoodieEngineContext} to use
   * @throws HoodieRollbackException on any exception during upgrade.
   */
  protected void recreateMarkers(final String commitInstantTime,
                                 HoodieTableMetaClient metaClient,
                                 HoodieWriteConfig config,
                                 HoodieEngineContext context,
                                 int parallelism) throws HoodieRollbackException {
    try {
      // fetch hoodie instant
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(metaClient.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        // delete existing markers
        WriteMarkers writeMarkers = WriteMarkersFactory.get(
            MarkerType.DIRECT, metaClient, config, context, commitInstantTime);
        writeMarkers.quietDeleteMarkerDir(context, parallelism);

        // generate rollback stats
        List<ListingBasedRollbackRequest> rollbackRequests;
        if (metaClient.getTableType() == HoodieTableType.COPY_ON_WRITE) {
          rollbackRequests = RollbackUtils.generateRollbackRequestsByListingCOW(context, metaClient.getBasePath(), config);
        } else {
          rollbackRequests = RollbackUtils.generateRollbackRequestsUsingFileListingMOR(
              commitInstantOpt.get(), metaClient, config, context,
              HoodieTable.createViewManager(context, config).getFileSystemView(metaClient));
        }
        List<HoodieRollbackStat> rollbackStats = getListBasedRollBackStats(
            metaClient, config, context, commitInstantOpt, rollbackRequests);

        // recreate markers adhering to marker based rollback
        for (HoodieRollbackStat rollbackStat : rollbackStats) {
          for (String path : rollbackStat.getSuccessDeleteFiles()) {
            String dataFileName = path.substring(path.lastIndexOf("/") + 1);
            // not feasible to differentiate MERGE from CREATE. hence creating with MERGE IOType for all base files.
            writeMarkers.create(rollbackStat.getPartitionPath(), dataFileName, IOType.MERGE);
          }
          for (FileStatus fileStatus : rollbackStat.getCommandBlocksCount().keySet()) {
            writeMarkers.create(rollbackStat.getPartitionPath(), getFileNameForMarkerFromLogFile(
                fileStatus.getPath().toString(), metaClient), IOType.APPEND);
          }
        }
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Exception thrown while upgrading Hoodie Table from version 0 to 1", e);
    }
  }

  List<HoodieRollbackStat> getListBasedRollBackStats(
      HoodieTableMetaClient metaClient, HoodieWriteConfig config, HoodieEngineContext context,
      Option<HoodieInstant> commitInstantOpt, List<ListingBasedRollbackRequest> rollbackRequests) {
    List<HoodieRollbackRequest> hoodieRollbackRequests = new ListingBasedRollbackHelper(metaClient, config)
        .getRollbackRequestsForRollbackPlan(context, commitInstantOpt.get(), rollbackRequests);
    return new BaseRollbackHelper(metaClient, config).collectRollbackStats(context, commitInstantOpt.get(), hoodieRollbackRequests);
  }

  /**
   * Curates file name for marker from existing log file path.
   * log file format     : partitionpath/.fileid_baseInstant.log.writetoken
   * marker file format  : partitionpath/fileId_writetoken_baseinstant.basefileExtn.marker.APPEND
   *
   * @param logFilePath log file path for which marker file name needs to be generated.
   * @param metaClient  {@link HoodieTableMetaClient} instance to use
   * @return the marker file name thus curated.
   */
  private static String getFileNameForMarkerFromLogFile(
      String logFilePath, HoodieTableMetaClient metaClient) {
    Path logPath = new Path(metaClient.getBasePath(), logFilePath);
    String fileId = FSUtils.getFileIdFromLogPath(logPath);
    String baseInstant = FSUtils.getBaseCommitTimeFromLogPath(logPath);
    String writeToken = FSUtils.getWriteTokenFromLogPath(logPath);

    return FSUtils.makeDataFileName(baseInstant, writeToken, fileId,
        metaClient.getTableConfig().getBaseFileFormat().getFileExtension());
  }
}
