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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * A helper class used to diff timeline.
 */
public class TimelineDiffHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineDiffHelper.class);

  private TimelineDiffHelper() {
  }

  public static TimelineDiffResult getNewInstantsForIncrementalSync(HoodieTableMetaClient metaClient,
                                                                    HoodieTimeline oldTimeline,
                                                                    HoodieTimeline newTimeline) {

    HoodieTimeline oldT = oldTimeline.filterCompletedAndCompactionInstants();
    HoodieTimeline newT = newTimeline.filterCompletedAndCompactionInstants();

    Option<HoodieInstant> lastSeenInstant = oldT.lastInstant();
    Option<HoodieInstant> firstInstantInNewTimeline = newT.firstInstant();

    if (lastSeenInstant.isPresent() && firstInstantInNewTimeline.isPresent()) {
      if (compareTimestamps(lastSeenInstant.get().requestedTime(),
          LESSER_THAN, firstInstantInNewTimeline.get().requestedTime())) {
        // The last seen instant is no longer in the timeline. Do not incrementally Sync.
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      Set<HoodieInstant> oldTimelineInstants = oldT.getInstantsAsStream().collect(Collectors.toSet());

      List<HoodieInstant> newInstants = new ArrayList<>();

      // Check If any pending compaction is lost. If so, do not allow incremental timeline sync
      List<Pair<HoodieInstant, HoodieInstant>> compactionInstants = getPendingActionTransitions(metaClient, oldT.filterPendingCompactionTimeline(),
          newT, HoodieTimeline.COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION);
      List<HoodieInstant> lostPendingCompactions = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue() == null).map(Pair::getKey).collect(Collectors.toList());
      if (!lostPendingCompactions.isEmpty()) {
        // If a compaction is unscheduled, fall back to complete refresh of fs view since some log files could have been
        // moved. Its unsafe to incrementally sync in that case.
        LOG.warn("Some pending compactions are no longer in new timeline (unscheduled ?). They are: {}", lostPendingCompactions);
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      List<HoodieInstant> finishedCompactionInstants = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue().getAction().equals(HoodieTimeline.COMMIT_ACTION)
              && instantPair.getValue().isCompleted())
          .map(Pair::getKey).collect(Collectors.toList());

      newTimeline.getInstantsAsStream().filter(instant -> !oldTimelineInstants.contains(instant)).forEach(newInstants::add);

      List<Pair<HoodieInstant, HoodieInstant>> logCompactionInstants = getPendingActionTransitions(metaClient, oldTimeline.filterPendingLogCompactionTimeline(),
          newTimeline, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION);
      List<HoodieInstant> finishedOrRemovedLogCompactionInstants = logCompactionInstants.stream()
          .filter(instantPair -> !instantPair.getKey().isCompleted()
              && (instantPair.getValue() == null || instantPair.getValue().isCompleted()))
          .map(Pair::getKey).collect(Collectors.toList());
      return new TimelineDiffResult(newInstants, finishedCompactionInstants, finishedOrRemovedLogCompactionInstants, true);
    } else {
      // One or more timelines is empty
      LOG.warn("One or more timelines is empty");
      return TimelineDiffResult.UNSAFE_SYNC_RESULT;
    }
  }

  private static List<Pair<HoodieInstant, HoodieInstant>> getPendingActionTransitions(HoodieTableMetaClient metaClient,
                                                                                      HoodieTimeline pendingActionTimelineFromOld,
                                                                                      HoodieTimeline newTimeline,
                                                                                      String completedAction, String pendingAction) {
    Set<HoodieInstant> newTimelineInstants = newTimeline.getInstantsAsStream().collect(Collectors.toSet());
    InstantGenerator factory = metaClient.getInstantGenerator();

    return pendingActionTimelineFromOld.getInstantsAsStream().map(instant -> {
      if (newTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant completedInstant =
            factory.createNewInstant(State.COMPLETED, completedAction, instant.requestedTime());
        if (newTimelineInstants.contains(completedInstant)) {
          return Pair.of(instant, completedInstant);
        }
        HoodieInstant inflightInstant =
            factory.createNewInstant(State.INFLIGHT, pendingAction, instant.requestedTime());
        if (newTimelineInstants.contains(inflightInstant)) {
          return Pair.of(instant, inflightInstant);
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
    private final List<HoodieInstant> finishedOrRemovedLogCompactionInstants;
    private final boolean canSyncIncrementally;

    public static final TimelineDiffResult UNSAFE_SYNC_RESULT =
        new TimelineDiffResult(null, null, null, false);

    public TimelineDiffResult(List<HoodieInstant> newlySeenInstants, List<HoodieInstant> finishedCompactionInstants,
                              List<HoodieInstant> finishedOrRemovedLogCompactionInstants, boolean canSyncIncrementally) {
      this.newlySeenInstants = newlySeenInstants;
      this.finishedCompactionInstants = finishedCompactionInstants;
      this.finishedOrRemovedLogCompactionInstants = finishedOrRemovedLogCompactionInstants;
      this.canSyncIncrementally = canSyncIncrementally;
    }

    public List<HoodieInstant> getNewlySeenInstants() {
      return newlySeenInstants;
    }

    public List<HoodieInstant> getFinishedCompactionInstants() {
      return finishedCompactionInstants;
    }

    public List<HoodieInstant> getFinishedOrRemovedLogCompactionInstants() {
      return finishedOrRemovedLogCompactionInstants;
    }

    public boolean canSyncIncrementally() {
      return canSyncIncrementally;
    }

    @Override
    public String toString() {
      return "TimelineDiffResult{"
          + "newlySeenInstants=" + newlySeenInstants
          + ", finishedCompactionInstants=" + finishedCompactionInstants
          + ", finishedOrRemovedLogCompactionInstants=" + finishedOrRemovedLogCompactionInstants
          + ", canSyncIncrementally=" + canSyncIncrementally
          + '}';
    }
  }
}
