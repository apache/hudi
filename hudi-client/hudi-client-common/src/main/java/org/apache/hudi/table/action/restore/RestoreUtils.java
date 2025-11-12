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

package org.apache.hudi.table.action.restore;

import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.RestorePlanActionExecutor;

import java.io.IOException;

public class RestoreUtils {

  /**
   * Get Latest version of Restore plan corresponding to a restore instant.
   *
   * @param metaClient      Hoodie Table Meta Client
   * @param restoreInstant Instant referring to restore action
   * @return Rollback plan corresponding to rollback instant
   * @throws IOException
   */
  public static HoodieRestorePlan getRestorePlan(HoodieTableMetaClient metaClient, HoodieInstant restoreInstant)
      throws IOException {
    final HoodieInstant requested = metaClient.getInstantGenerator().getRollbackRequestedInstant(restoreInstant);
    return metaClient.getActiveTimeline().readRestorePlan(requested);
  }

  public static String getSavepointToRestoreTimestampV1Schema(HoodieTable table, HoodieRestorePlan plan) {
    //get earliest rollback
    String firstRollback = plan.getInstantsToRollback().get(plan.getInstantsToRollback().size() - 1).getCommitTime();
    //find last instant before first rollback
    Option<HoodieInstant> savepointInstance = table.getActiveTimeline().getSavePointTimeline().findInstantsBefore(firstRollback).lastInstant();
    return savepointInstance.isPresent() ? savepointInstance.get().requestedTime() : null;
  }

  /**
   * Get the savepoint timestamp that this restore instant is restoring
   * @param table          the HoodieTable
   * @param restoreInstant Instant referring to restore action
   * @return timestamp of the savepoint we are restoring
   * @throws IOException
   *
   * */
  public static String getSavepointToRestoreTimestamp(HoodieTable table, HoodieInstant restoreInstant) throws IOException {
    HoodieRestorePlan plan = getRestorePlan(table.getMetaClient(), restoreInstant);
    if (plan.getVersion().compareTo(RestorePlanActionExecutor.RESTORE_PLAN_VERSION_1) > 0) {
      return plan.getSavepointToRestoreTimestamp();
    }
    return getSavepointToRestoreTimestampV1Schema(table, plan);
  }
}
