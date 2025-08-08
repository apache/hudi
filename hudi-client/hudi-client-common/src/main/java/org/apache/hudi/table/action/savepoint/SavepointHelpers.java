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

package org.apache.hudi.table.action.savepoint;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.RestoreInstantComparatorFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

public class SavepointHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(SavepointHelpers.class);

  public static void deleteSavepoint(HoodieTable table, String savepointTime) {
    HoodieInstant savePoint = table.getMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      LOG.warn("No savepoint present {}", savepointTime);
      return;
    }

    table.getActiveTimeline().revertToInflight(savePoint);
    table.getActiveTimeline().deleteInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION,
        savepointTime));
    LOG.info("Savepoint {} deleted", savepointTime);
  }

  public static void validateSavepointRestore(HoodieTable table, String savepointTime) {
    // Make sure the restore was successful
    table.getMetaClient().reloadActiveTimeline();
    // Validate that the restore has returned the timeline to the anticipated state
    Comparator<HoodieInstant> instantComparator = RestoreInstantComparatorFactory.createComparator(table.getMetaClient());
    Option<HoodieInstant> lastInstant = Option.fromJavaOptional(table.getActiveTimeline()
        .getWriteTimeline()
        .filterCompletedAndCompactionInstants()
        .getInstantsAsStream()
        .max(instantComparator));
    ValidationUtils.checkArgument(lastInstant.isPresent());
    ValidationUtils.checkArgument(lastInstant.get().requestedTime().equals(savepointTime),
        () -> savepointTime + " is not the last commit after restoring to savepoint, last commit was "
            + lastInstant.get().requestedTime());
  }

  public static void validateSavepointPresence(HoodieTable table, String savepointTime) {
    HoodieInstant savePoint = table.getMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for instantTime " + savepointTime);
    }
  }
}
