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
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    final HoodieInstant restoreInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.RESTORE_ACTION, instantTime);
    try {
      // Get all the commits on the timeline after the provided commit time
      // rollback pending clustering instants first before other instants (See HUDI-3362)
      List<HoodieInstant> pendingClusteringInstantsToRollback = table.getActiveTimeline().filterPendingReplaceTimeline()
              // filter only clustering related replacecommits (Not insert_overwrite related commits)
              .filter(instant -> ClusteringUtils.isClusteringInstant(table.getActiveTimeline(), instant))
              .getReverseOrderedInstants()
              .filter(instant -> HoodieActiveTimeline.GREATER_THAN.test(instant.getTimestamp(), savepointToRestoreTimestamp))
              .collect(Collectors.toList());

      // Get all the commits on the timeline after the provided commit time
      List<HoodieInstant> commitInstantsToRollback = table.getActiveTimeline().getWriteTimeline()
              .getReverseOrderedInstants()
              .filter(instant -> HoodieActiveTimeline.GREATER_THAN.test(instant.getTimestamp(), savepointToRestoreTimestamp))
              .filter(instant -> !pendingClusteringInstantsToRollback.contains(instant))
              .collect(Collectors.toList());

      // Combine both lists - first rollback pending clustering and then rollback all other commits
      List<HoodieInstantInfo> instantsToRollback = Stream.concat(pendingClusteringInstantsToRollback.stream(), commitInstantsToRollback.stream())
              .map(entry -> new HoodieInstantInfo(entry.getTimestamp(), entry.getAction()))
              .collect(Collectors.toList());

      HoodieRestorePlan restorePlan = new HoodieRestorePlan(instantsToRollback, LATEST_RESTORE_PLAN_VERSION, savepointToRestoreTimestamp);
      table.getActiveTimeline().saveToRestoreRequested(restoreInstant, TimelineMetadataUtils.serializeRestorePlan(restorePlan));
      table.getMetaClient().reloadActiveTimeline();
      LOG.info("Requesting Restore with instant time " + restoreInstant);
      return Option.of(restorePlan);
    } catch (IOException e) {
      LOG.error("Got exception when saving restore requested file", e);
      throw new HoodieIOException(e.getMessage(), e);
    }
  }
}
