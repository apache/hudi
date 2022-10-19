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

import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
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

    HoodieTimeline oldT = oldTimeline.filterCompletedAndRewriteInstants();
    HoodieTimeline newT = newTimeline.filterCompletedAndRewriteInstants();

    Option<HoodieInstant> lastSeenInstant = oldT.lastInstant();
    Option<HoodieInstant> firstInstantInNewTimeline = newT.firstInstant();

    if (lastSeenInstant.isPresent() && firstInstantInNewTimeline.isPresent()) {
      if (HoodieTimeline.compareTimestamps(lastSeenInstant.get().getTimestamp(),
          HoodieTimeline.LESSER_THAN, firstInstantInNewTimeline.get().getTimestamp())) {
        // The last seen instant is no longer in the timeline. Do not incrementally Sync.
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      Set<HoodieInstant> oldTimelineInstants = oldT.getInstantsAsStream().collect(Collectors.toSet());

      List<HoodieInstant> newInstants = new ArrayList<>();

      // Check If any pending compaction is lost. If so, do not allow incremental timeline sync
      List<Pair<HoodieInstant, HoodieInstant>> compactionInstants = getPendingCompactionTransitions(oldT, newT);
      List<HoodieInstant> lostPendingCompactions = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue() == null).map(Pair::getKey).collect(Collectors.toList());
      if (!lostPendingCompactions.isEmpty()) {
        // If a compaction is unscheduled, fall back to complete refresh of fs view since some log files could have been
        // moved. Its unsafe to incrementally sync in that case.
        LOG.warn("Some pending compactions are no longer in new timeline (unscheduled ?). They are :"
            + lostPendingCompactions);
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      List<HoodieInstant> finishedCompactionInstants = compactionInstants.stream()
          .filter(instantPair -> instantPair.getValue().getAction().equals(HoodieTimeline.COMMIT_ACTION)
              && instantPair.getValue().isCompleted())
          .map(Pair::getKey).collect(Collectors.toList());

      newTimeline.getInstantsAsStream().filter(instant -> !oldTimelineInstants.contains(instant)).forEach(newInstants::add);

      // Check for log compaction commits completed or removed.
      List<Pair<HoodieInstant, HoodieInstant>> logCompactionInstants = getPendingLogCompactionTransitions(oldTimeline, newTimeline);
      List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants = logCompactionInstants.stream()
          .filter(instantPair -> !instantPair.getKey().isCompleted()
              && (instantPair.getValue() == null || instantPair.getValue().isCompleted()))
          .map(instantPair -> (instantPair.getValue() == null)
              ? Pair.of(instantPair.getKey(), false) : Pair.of(instantPair.getKey(), true))
          .collect(Collectors.toList());

      // Check for replace commits completed or removed.
      List<Pair<HoodieInstant, HoodieInstant>> replaceCommitInstants = getPendingReplaceCommitTransitions(oldTimeline, newTimeline);
      List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceCommitInstants = replaceCommitInstants.stream()
          .filter(instantPair -> !instantPair.getKey().isCompleted()
              && (instantPair.getValue() == null || instantPair.getValue().isCompleted()))
          .map(instantPair -> (instantPair.getValue() == null)
              ? Pair.of(instantPair.getKey(), false) : Pair.of(instantPair.getKey(), true))
          .collect(Collectors.toList());

      // New instants will contains instants that are newly completed commits or newly created pending rewrite commits
      // (i.e. compaction, logcompaciton, replacecommit)
      // Finished or removed rewrite commits are handled independently.
      return new TimelineDiffResult(newInstants, finishedCompactionInstants, finishedOrRemovedLogCompactionInstants,
          finishedOrRemovedReplaceCommitInstants, true);
    } else {
      // One or more timelines is empty
      LOG.warn("One or more timelines is empty");
      return TimelineDiffResult.UNSAFE_SYNC_RESULT;
    }
  }

  /**
   * Get pending replacecommit transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getPendingReplaceCommitTransitions(HoodieTimeline oldTimeline,
                                                                                             HoodieTimeline newTimeline) {
    Set<HoodieInstant> newTimelineInstants = newTimeline.getInstantsAsStream().collect(Collectors.toSet());

    return oldTimeline.filterPendingReplaceTimeline().getInstantsAsStream().map(instant -> {
      if (newTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant completedReplaceCommit =
            new HoodieInstant(State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(completedReplaceCommit)) {
          return Pair.of(instant, completedReplaceCommit);
        }
        HoodieInstant inflightRepaceCommit =
            new HoodieInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(inflightRepaceCommit)) {
          return Pair.of(instant, inflightRepaceCommit);
        }
        return Pair.<HoodieInstant, HoodieInstant>of(instant, null);
      }
    }).collect(Collectors.toList());
  }

  /**
   * Getting pending log compaction transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getPendingLogCompactionTransitions(HoodieTimeline oldTimeline,
                                                                                          HoodieTimeline newTimeline) {
    Set<HoodieInstant> newTimelineInstants = newTimeline.getInstantsAsStream().collect(Collectors.toSet());

    return oldTimeline.filterPendingLogCompactionTimeline().getInstantsAsStream().map(instant -> {
      if (newTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant logCompacted =
            new HoodieInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(logCompacted)) {
          return Pair.of(instant, logCompacted);
        }
        HoodieInstant inflightLogCompacted =
            new HoodieInstant(State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(inflightLogCompacted)) {
          return Pair.of(instant, inflightLogCompacted);
        }
        return Pair.<HoodieInstant, HoodieInstant>of(instant, null);
      }
    }).collect(Collectors.toList());
  }

  /**
   * Getting pending compaction transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getPendingCompactionTransitions(HoodieTimeline oldTimeline,
      HoodieTimeline newTimeline) {
    Set<HoodieInstant> newTimelineInstants = newTimeline.getInstantsAsStream().collect(Collectors.toSet());

    return oldTimeline.filterPendingCompactionTimeline().getInstantsAsStream().map(instant -> {
      if (newTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant compacted =
            new HoodieInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(compacted)) {
          return Pair.of(instant, compacted);
        }
        HoodieInstant inflightCompacted =
            new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, instant.getTimestamp());
        if (newTimelineInstants.contains(inflightCompacted)) {
          return Pair.of(instant, inflightCompacted);
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
    // Completed instants will have true as the value where as instants removed due to rollback will have false as value.
    private final List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants;
    // Completed instants will have true as the value where as instants removed due to rollback will have false as value.
    private final List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceCommitInstants;
    private final boolean canSyncIncrementally;

    public static final TimelineDiffResult UNSAFE_SYNC_RESULT =
        new TimelineDiffResult(null, null, null, null, false);

    public TimelineDiffResult(List<HoodieInstant> newlySeenInstants, List<HoodieInstant> finishedCompactionInstants,
                              List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants,
                              List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceCommitInstants, boolean canSyncIncrementally) {
      this.newlySeenInstants = newlySeenInstants;
      this.finishedCompactionInstants = finishedCompactionInstants;
      this.finishedOrRemovedLogCompactionInstants = finishedOrRemovedLogCompactionInstants;
      this.finishedOrRemovedReplaceCommitInstants = finishedOrRemovedReplaceCommitInstants;
      this.canSyncIncrementally = canSyncIncrementally;
    }

    public List<HoodieInstant> getNewlySeenInstants() {
      return newlySeenInstants;
    }

    public List<HoodieInstant> getFinishedCompactionInstants() {
      return finishedCompactionInstants;
    }

    public List<Pair<HoodieInstant, Boolean>> getFinishedOrRemovedLogCompactionInstants() {
      return finishedOrRemovedLogCompactionInstants;
    }

    public List<Pair<HoodieInstant, Boolean>> getFinishedOrRemovedReplaceCommitInstants() {
      return finishedOrRemovedReplaceCommitInstants;
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
          + ", finishedOrRemovedReplaceCommitInstants=" + finishedOrRemovedReplaceCommitInstants
          + ", canSyncIncrementally=" + canSyncIncrementally
          + '}';
    }
  }
}
