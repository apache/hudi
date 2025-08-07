/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.GREATER_THAN;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Plans the restore action and add a restore.requested meta file to timeline.
 */
public class RestorePlanActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieRestorePlan>> {

  private static final Logger LOG = LoggerFactory.getLogger(RestorePlanActionExecutor.class);

  public static final Integer RESTORE_PLAN_VERSION_1 = 1;
  public static final Integer RESTORE_PLAN_VERSION_2 = 2;
  public static final Integer LATEST_RESTORE_PLAN_VERSION = RESTORE_PLAN_VERSION_2;
  private final String savepointToRestoreTimestamp;

  public RestorePlanActionExecutor(HoodieEngineContext context,
                                   HoodieWriteConfig config,
                                   HoodieTable<T, I, K, O> table,
                                   String instantTime,
                                   String savepointToRestoreTimestamp) {
    super(context, config, table, instantTime);
    this.savepointToRestoreTimestamp = savepointToRestoreTimestamp;
  }

  @Override
  public Option<HoodieRestorePlan> execute() {
    final HoodieInstant restoreInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.RESTORE_ACTION, instantTime);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    try (CompletionTimeQueryView completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(metaClient)) {
      // Get all the commits on the timeline after the provided commit time
      // rollback pending clustering instants first before other instants (See HUDI-3362)
      List<HoodieInstant> pendingClusteringInstantsToRollback = table.getActiveTimeline().filterPendingReplaceOrClusteringTimeline()
              // filter only clustering related replacecommits (Not insert_overwrite related commits)
              .filter(instant -> ClusteringUtils.isClusteringInstant(table.getActiveTimeline(), instant, instantGenerator))
              .getReverseOrderedInstants()
              .filter(instant -> GREATER_THAN.test(instant.requestedTime(), savepointToRestoreTimestamp))
              .collect(Collectors.toList());

      // Get all the commits on the timeline after the provided commit's completion time unless it is the SOLO_COMMIT_TIMESTAMP which indicates there are no commits for the table
      String completionTime = getCompletionTime(completionTimeQueryView);

      Predicate<HoodieInstant> instantFilter = constructInstantFilter(completionTime);
      List<HoodieInstant> commitInstantsToRollback = table.getActiveTimeline().getWriteTimeline()
              .getReverseOrderedInstantsByCompletionTime()
              .filter(instantFilter)
              .filter(instant -> !pendingClusteringInstantsToRollback.contains(instant))
              .collect(Collectors.toList());

      // Combine both lists - first rollback pending clustering and then rollback all other commits
      List<HoodieInstantInfo> instantsToRollback = Stream.concat(pendingClusteringInstantsToRollback.stream(), commitInstantsToRollback.stream())
              .map(entry -> new HoodieInstantInfo(entry.requestedTime(), entry.getAction()))
              .collect(Collectors.toList());

      HoodieRestorePlan restorePlan = new HoodieRestorePlan(instantsToRollback, LATEST_RESTORE_PLAN_VERSION, savepointToRestoreTimestamp);
      table.getActiveTimeline().saveToRestoreRequested(restoreInstant, restorePlan);
      table.getMetaClient().reloadActiveTimeline();
      LOG.info("Requesting Restore with instant time {}", restoreInstant);
      return Option.of(restorePlan);
    } catch (Exception e) {
      throw new HoodieException("Unable to restore to instant: " + savepointToRestoreTimestamp, e);
    }
  }

  /**
   * Finds the completion time for the restore action based off of the provided instant for the savepoint.
   * During the upgrade flow, the restore timestamp will be the SOLO_COMMIT_TIMESTAMP because there are no commits to the table and therefore no completion time.
   * In this case, we just return the input timestamp. In the case of the metadata table, it is possible that the table was deleted as part of the restore operation,
   * and therefore we will not be able to find the completion time for the commits and in that case we also return the input timestamp.
   * @param completionTimeQueryView
   * @return
   */
  private String getCompletionTime(CompletionTimeQueryView completionTimeQueryView) {
    if (savepointToRestoreTimestamp.equals(SOLO_COMMIT_TIMESTAMP)) {
      return savepointToRestoreTimestamp;
    }
    // if the table is a metadata table and only bootstrap commits exist, then return the current time as the instant to avoid operations on the reconstructed metadata table
    HoodieTableMetaClient metaClient = table.getMetaClient();
    if (metaClient.isMetadataTable() && metaClient.getActiveTimeline().lastInstant().map(instant -> instant.requestedTime().startsWith(SOLO_COMMIT_TIMESTAMP)).orElse(false)) {
      return HoodieInstantTimeGenerator.getCurrentInstantTimeStr();
    }
    return completionTimeQueryView.getCompletionTime(savepointToRestoreTimestamp)
        .orElseThrow(() -> new HoodieException("Unable to find completion time for instant: " + savepointToRestoreTimestamp));
  }

  private Predicate<HoodieInstant> constructInstantFilter(String completionTime) {
    HoodieInstant instantToRestoreTo = table.getMetaClient().getInstantGenerator()
        .createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.RESTORE_ACTION, savepointToRestoreTimestamp, completionTime);
    Comparator<HoodieInstant> comparator = RestoreInstantComparatorFactory.createComparator(table.getMetaClient());
    return instant -> comparator.compare(instant, instantToRestoreTo) > 0;
  }
}
