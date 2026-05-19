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

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Base rollback plan action executor to assist in scheduling rollback requests. This phase serialized {@link HoodieRollbackPlan}
 * to rollback.requested instant.
 */
@Slf4j
public class BaseRollbackPlanActionExecutor<T, I, K, O> extends BaseActionExecutor<T, I, K, O, Option<HoodieRollbackPlan>> {

  protected final HoodieInstant instantToRollback;
  private final boolean skipTimelinePublish;
  private final boolean shouldRollbackUsingMarkers;
  protected final Boolean isRestore;

  public static final Integer ROLLBACK_PLAN_VERSION_1 = 1;
  public static final Integer LATEST_ROLLBACK_PLAN_VERSION = ROLLBACK_PLAN_VERSION_1;

  public BaseRollbackPlanActionExecutor(HoodieEngineContext context,
                                        HoodieWriteConfig config,
                                        HoodieTable<T, I, K, O> table,
                                        String instantTime,
                                        HoodieInstant instantToRollback,
                                        boolean skipTimelinePublish,
                                        boolean shouldRollbackUsingMarkers,
                                        boolean isRestore) {
    super(context, config, table, instantTime);
    this.instantToRollback = instantToRollback;
    this.skipTimelinePublish = skipTimelinePublish;
    this.shouldRollbackUsingMarkers = shouldRollbackUsingMarkers && !instantToRollback.isCompleted();
    this.isRestore = isRestore;
  }

  /**
   * Interface for RollbackStrategy. There are two types supported, listing based and marker based.
   */
  interface RollbackStrategy extends Serializable {

    /**
     * Fetch list of {@link HoodieRollbackRequest}s to be added to rollback plan.
     * @param instantToRollback instant to be rolled back.
     * @return list of {@link HoodieRollbackRequest}s to be added to rollback plan
     */
    List<HoodieRollbackRequest> getRollbackRequests(HoodieInstant instantToRollback);
  }

  /**
   * Fetch the Rollback strategy used.
   *
   * @return
   */
  private BaseRollbackPlanActionExecutor.RollbackStrategy getRollbackStrategy() {
    if (shouldRollbackUsingMarkers) {
      return new MarkerBasedRollbackStrategy(table, context, config, instantTime);
    } else {
      return new ListingBasedRollbackStrategy(table, context, config, instantTime, isRestore);
    }
  }

  /**
   * Creates a Rollback plan if there are files to be rolled back and stores them in instant file.
   * Rollback Plan contains absolute file paths.
   *
   * @param startRollbackTime Rollback Instant Time
   * @return Rollback Plan if generated
   */
  protected Option<HoodieRollbackPlan> requestRollback(String startRollbackTime) {
    final HoodieInstant rollbackInstant = instantGenerator.createNewInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime);
    try {
      List<HoodieRollbackRequest> rollbackRequests = new ArrayList<>();
      if (!instantToRollback.isRequested()) {
        rollbackRequests.addAll(getRollbackStrategy().getRollbackRequests(instantToRollback));
      }
      HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan(new HoodieInstantInfo(instantToRollback.requestedTime(),
          instantToRollback.getAction()), rollbackRequests, LATEST_ROLLBACK_PLAN_VERSION);
      if (!skipTimelinePublish) {
        if (table.getRollbackTimeline().filterInflightsAndRequested().containsInstant(rollbackInstant.requestedTime())) {
          log.info("Request Rollback found with instant time {}, hence skipping scheduling rollback", rollbackInstant);
        } else {
          table.getActiveTimeline().saveToRollbackRequested(rollbackInstant, rollbackPlan);
          log.info("Requesting Rollback with instant time {}", rollbackInstant);
        }
      }
      return Option.of(rollbackPlan);
    } catch (HoodieIOException e) {
      log.error("Got exception when saving rollback requested file", e);
      throw e;
    }
  }

  @Override
  public Option<HoodieRollbackPlan> execute() {
    // Plan a new rollback action
    return requestRollback(instantTime);
  }
}
