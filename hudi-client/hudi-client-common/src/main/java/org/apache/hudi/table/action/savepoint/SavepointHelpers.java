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

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;

public class SavepointHelpers {

  private static final Logger LOG = LoggerFactory.getLogger(SavepointHelpers.class);

  public static void deleteSavepoint(HoodieTable table, String savepointTime) {
    HoodieInstant savePoint = table.getMetaClient().createNewInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      LOG.warn("No savepoint present " + savepointTime);
      return;
    }

    table.getActiveTimeline().revertToInflight(savePoint);
    table.getActiveTimeline().deleteInflight(table.getMetaClient().createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.SAVEPOINT_ACTION,
        savepointTime));
    LOG.info("Savepoint " + savepointTime + " deleted");
  }

  public static void validateSavepointRestore(HoodieTable table, String savepointTime) {
    // Make sure the restore was successful
    table.getMetaClient().reloadActiveTimeline();
    Option<HoodieInstant> lastInstant = Option.fromJavaOptional(table.getActiveTimeline()
        .getWriteTimeline()
        .filterCompletedAndCompactionInstants()
        .getInstantsAsStream()
        .max(new SavepointInstantComparator(table.getMetaClient().getTableConfig().getTableVersion().greaterThanOrEquals(HoodieTableVersion.EIGHT),
            table.getMetaClient().getTimelineLayout().getInstantComparator())));
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

  private static class SavepointInstantComparator implements Comparator<HoodieInstant> {
    private final boolean tableVersion8OrLater;
    private final InstantComparator instantComparator;

    public SavepointInstantComparator(boolean tableVersion8OrLater, InstantComparator instantComparator) {
      this.tableVersion8OrLater = tableVersion8OrLater;
      this.instantComparator = instantComparator;
    }

    @Override
    public int compare(HoodieInstant o1, HoodieInstant o2) {
      if (tableVersion8OrLater) {
        return instantComparator.completionTimeOrderedComparator().compare(o1, o2);
      } else {
        // Do to special handling of compaction instants, we need to use requested time based comparator for compaction instants but completion time based comparator for others
        if (o1.getAction().equals(HoodieTimeline.COMMIT_ACTION) || o2.getAction().equals(HoodieTimeline.COMMIT_ACTION)) {
          return instantComparator.requestedTimeOrderedComparator().compare(o1, o2);
        } else {
          return instantComparator.completionTimeOrderedComparator().compare(o1, o2);
        }
      }
    }
  }
}
