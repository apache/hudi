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

package org.apache.hudi.common.util;

import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A helper class used to diff timeline.
 */
public class TimelineDiffHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineDiffHelper.class);

  public static TimelineDiffResult getNewInstantsForIncrementalSync(HoodieTimeline oldTimeline,
      HoodieTimeline newTimeline) {

    HoodieTimeline oldT = oldTimeline.filterCompletedAndCompactionInstants();
    HoodieTimeline newT = newTimeline.filterCompletedAndCompactionInstants();

    Option<HoodieInstant> lastSeenInstant = oldT.lastInstant();
    Option<HoodieInstant> firstInstantInNewTimeline = newT.firstInstant();

    if (lastSeenInstant.isPresent() && firstInstantInNewTimeline.isPresent()) {
      if (HoodieTimeline.compareTimestamps(lastSeenInstant.get().getTimestamp(),
          firstInstantInNewTimeline.get().getTimestamp(), HoodieTimeline.LESSER)) {
        // The last seen instant is no longer in the timeline. Do not incrementally Sync.
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      Set<HoodieInstant> oldTimelineInstants = oldT.getInstants().collect(Collectors.toSet());

      List<HoodieInstant> newInstants = new ArrayList<>();

      // Check If any pending compaction is lost. If so, do not allow incremental timeline sync
      List<Pair<HoodieInstant, HoodieInstant>> compactionInstants = getPendingCompactionTransitions(oldT, newT);
      List<HoodieInstant> lostPendingCompactions = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue() == null).map(Pair::getKey).collect(Collectors.toList());
      if (!lostPendingCompactions.isEmpty()) {
        // If a compaction is unscheduled, fall back to complete refresh of fs view since some log files could have been
        // moved. Its unsafe to incrementally sync in that case.
        LOG.warn("Some pending compactions are no longer in new timeline (unscheduled ?). They are :{}", lostPendingCompactions);
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      List<HoodieInstant> finishedCompactionInstants = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue().getAction().equals(HoodieTimeline.COMMIT_ACTION)
              && instantPair.getValue().isCompleted())
          .map(Pair::getKey).collect(Collectors.toList());

      newT.getInstants().filter(instant -> !oldTimelineInstants.contains(instant)).forEach(newInstants::add);
      return new TimelineDiffResult(newInstants, finishedCompactionInstants, true);
    } else {
      // One or more timelines is empty
      LOG.warn("One or more timelines is empty");
      return TimelineDiffResult.UNSAFE_SYNC_RESULT;
    }
  }

  private static List<Pair<HoodieInstant, HoodieInstant>> getPendingCompactionTransitions(HoodieTimeline oldTimeline,
      HoodieTimeline newTimeline) {
    Set<HoodieInstant> newTimelineInstants = newTimeline.getInstants().collect(Collectors.toSet());

    return oldTimeline.filterPendingCompactionTimeline().getInstants().map(instant -> {
      if (newTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant compacted =
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(compacted)) {
          return Pair.of(instant, compacted);
        }
        return Pair.<HoodieInstant, HoodieInstant>of(instant, null);
      }
    }).collect(Collectors.toList());
  }

  /**
   * A diff result of timeline.
   */
  public static class TimelineDiffResult {

    private final List<HoodieInstant> newlySeenInstants;
    private final List<HoodieInstant> finishedCompactionInstants;
    private final boolean canSyncIncrementally;

    public static final TimelineDiffResult UNSAFE_SYNC_RESULT = new TimelineDiffResult(null, null, false);

    public TimelineDiffResult(List<HoodieInstant> newlySeenInstants, List<HoodieInstant> finishedCompactionInstants,
        boolean canSyncIncrementally) {
      this.newlySeenInstants = newlySeenInstants;
      this.finishedCompactionInstants = finishedCompactionInstants;
      this.canSyncIncrementally = canSyncIncrementally;
    }

    public List<HoodieInstant> getNewlySeenInstants() {
      return newlySeenInstants;
    }

    public List<HoodieInstant> getFinishedCompactionInstants() {
      return finishedCompactionInstants;
    }

    public boolean canSyncIncrementally() {
      return canSyncIncrementally;
    }

    @Override
    public String toString() {
      return "TimelineDiffResult{newlySeenInstants=" + newlySeenInstants + ", finishedCompactionInstants="
          + finishedCompactionInstants + ", canSyncIncrementally=" + canSyncIncrementally + '}';
    }
  }
}
