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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A helper class used to diff timeline.
 */
public class TimelineDiffHelper {

  private static final Logger LOG = LoggerFactory.getLogger(TimelineDiffHelper.class);

  /**
   * It performs validation to see if the incremental sync is possible. Once it determines it is safe to do,
   * It tracks both the instants that are newly added and also instants that have transitioned from pending
   * state to completed or removed.
   * @param oldCompletedAndRewriteTimeline old completed and rewrite timeline
   * @param newCompletedAndRewriteTimeline new completed and rewrite  timeline
   * @return TimelineDiffResult object which is useful for incrementally syncing the timeline.
   */
  public static TimelineDiffResult getNewInstantsForIncrementalSync(HoodieTimeline oldCompletedAndRewriteTimeline,
                                                                    HoodieTimeline newCompletedAndRewriteTimeline) {

    Option<HoodieInstant> lastSeenInstant = oldCompletedAndRewriteTimeline.lastInstant();
    Option<HoodieInstant> firstInstantInNewTimeline = newCompletedAndRewriteTimeline.firstInstant();

    if (lastSeenInstant.isPresent() && firstInstantInNewTimeline.isPresent()) {
      if (HoodieTimeline.compareTimestamps(lastSeenInstant.get().getTimestamp(),
          HoodieTimeline.LESSER_THAN, firstInstantInNewTimeline.get().getTimestamp())) {
        // The last seen instant is no longer in the timeline. Do not incrementally Sync.
        return TimelineDiffResult.UNSAFE_SYNC_RESULT;
      }
      Set<HoodieInstant> newTimelineInstants = newCompletedAndRewriteTimeline.getInstantsAsStream().collect(Collectors.toSet());

      // Check If any pending compaction is lost. If so, do not allow incremental timeline sync
      List<Pair<HoodieInstant, HoodieInstant>> compactionInstants =
          getStateTransitionsForCompaction(oldCompletedAndRewriteTimeline, newTimelineInstants);
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

      // Collect new instants that are not present in oldCompletedAndRewriteTimeline
      // but are present in new active timeline.
      Set<HoodieInstant> oldTimelineInstants = oldCompletedAndRewriteTimeline.getInstantsAsStream().collect(Collectors.toSet());
      List<HoodieInstant> newCompletedAndRewriteInstants = new ArrayList<>();
      newCompletedAndRewriteTimeline.getInstantsAsStream()
          .filter(instant -> !oldTimelineInstants.contains(instant))
          .forEach(newCompletedAndRewriteInstants::add);

      // Check for log compaction commits completed or removed.
      List<Pair<HoodieInstant, HoodieInstant>> logCompactionInstants =
          getStateTransitionsForLogCompaction(oldCompletedAndRewriteTimeline, newTimelineInstants);
      List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants = logCompactionInstants.stream()
          .filter(instantPair -> !instantPair.getKey().isCompleted()
              && (instantPair.getValue() == null || instantPair.getValue().isCompleted()))
          .map(instantPair -> Pair.of(instantPair.getKey(), instantPair.getValue() != null))
          .collect(Collectors.toList());

      // Check if replacecommits are completed or removed.
      List<Pair<HoodieInstant, HoodieInstant>> replaceCommitInstants =
          getStateTransitionsForReplaceCommit(oldCompletedAndRewriteTimeline, newTimelineInstants);
      List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceCommitInstants = replaceCommitInstants.stream()
          .filter(instantPair -> !instantPair.getKey().isCompleted()
              && (instantPair.getValue() == null || instantPair.getValue().isCompleted()))
          .map(instantPair -> Pair.of(instantPair.getKey(), instantPair.getValue() != null))
          .collect(Collectors.toList());

      // New instants will contains instants that are newly completed commits or newly created pending rewrite commits
      // (i.e. compaction, log-compaction, clustering)
      // Finished or removed rewrite commits are handled independently.
      return new TimelineDiffResult(newCompletedAndRewriteInstants, finishedCompactionInstants, finishedOrRemovedLogCompactionInstants,
          finishedOrRemovedReplaceCommitInstants, true);
    } else {
      // One or more timelines is empty
      LOG.warn("One or more timelines is empty");
      return TimelineDiffResult.UNSAFE_SYNC_RESULT;
    }
  }

  /**
   * Getting pending compaction transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getStateTransitionsForCompaction(
      HoodieTimeline oldCompletedAndRewriteTimelineInstants, Set<HoodieInstant> newCompletedAndRewriteTimelineInstants) {
    return getStateTransitionsForAnAction(HoodieTimeline::filterPendingCompactionTimeline, oldCompletedAndRewriteTimelineInstants,
        newCompletedAndRewriteTimelineInstants, HoodieTimeline.COMMIT_ACTION, HoodieTimeline.COMPACTION_ACTION);
  }

  /**
   * Getting pending log compaction transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getStateTransitionsForLogCompaction(
      HoodieTimeline oldCompletedAndRewriteTimelineInstants, Set<HoodieInstant> newCompletedAndRewriteTimelineInstants) {
    return getStateTransitionsForAnAction(HoodieTimeline::filterPendingLogCompactionTimeline, oldCompletedAndRewriteTimelineInstants,
        newCompletedAndRewriteTimelineInstants, HoodieTimeline.DELTA_COMMIT_ACTION, HoodieTimeline.LOG_COMPACTION_ACTION);
  }

  /**
   * Get pending replacecommit transitions.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getStateTransitionsForReplaceCommit(
      HoodieTimeline oldCompletedAndRewriteTimelineInstants, Set<HoodieInstant> newCompletedAndRewriteTimelineInstants) {
    return getStateTransitionsForAnAction(HoodieTimeline::filterPendingReplaceTimeline, oldCompletedAndRewriteTimelineInstants,
        newCompletedAndRewriteTimelineInstants, HoodieTimeline.REPLACE_COMMIT_ACTION, HoodieTimeline.REPLACE_COMMIT_ACTION);
  }

  /**
   * Util method used by compaction, logcompaction and replacecommit to find the state transitions happened between two timelines.
   */
  private static List<Pair<HoodieInstant, HoodieInstant>> getStateTransitionsForAnAction(
      Function<HoodieTimeline, HoodieTimeline> pendingTimelineActionFunction, HoodieTimeline oldCompletedAndRewriteTimeline,
      Set<HoodieInstant> newCompletedAndRewriteTimelineInstants, String completedAction, String inflightAction) {
    return pendingTimelineActionFunction.apply(oldCompletedAndRewriteTimeline).getInstantsAsStream().map(instant -> {
      if (newCompletedAndRewriteTimelineInstants.contains(instant)) {
        return Pair.of(instant, instant);
      } else {
        HoodieInstant completedInstant =
            new HoodieInstant(State.COMPLETED, completedAction, instant.getTimestamp());
        if (newCompletedAndRewriteTimelineInstants.contains(completedInstant)) {
          return Pair.of(instant, completedInstant);
        }
        HoodieInstant inflightInstant =
            new HoodieInstant(State.INFLIGHT, inflightAction, instant.getTimestamp());
        if (newCompletedAndRewriteTimelineInstants.contains(inflightInstant)) {
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

    private final List<HoodieInstant> newlySeenCompletedAndRewriteInstants;
    private final List<HoodieInstant> finishedCompactionInstants;
    // Completed instants will have true as the value where as instants removed due to rollback will have false as value.
    private final List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants;
    // Completed instants will have true as the value where as instants removed due to rollback will have false as value.
    private final List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceInstants;
    private final boolean canSyncIncrementally;

    public static final TimelineDiffResult UNSAFE_SYNC_RESULT =
        new TimelineDiffResult(null, null, null, null, false);

    public TimelineDiffResult(List<HoodieInstant> newlySeenCompletedAndRewriteInstants, List<HoodieInstant> finishedCompactionInstants,
                              List<Pair<HoodieInstant, Boolean>> finishedOrRemovedLogCompactionInstants,
                              List<Pair<HoodieInstant, Boolean>> finishedOrRemovedReplaceInstants, boolean canSyncIncrementally) {
      this.newlySeenCompletedAndRewriteInstants = newlySeenCompletedAndRewriteInstants;
      this.finishedCompactionInstants = finishedCompactionInstants;
      this.finishedOrRemovedLogCompactionInstants = finishedOrRemovedLogCompactionInstants;
      this.finishedOrRemovedReplaceInstants = finishedOrRemovedReplaceInstants;
      this.canSyncIncrementally = canSyncIncrementally;
    }

    public List<HoodieInstant> getNewlySeenCompletedAndRewriteInstants() {
      return newlySeenCompletedAndRewriteInstants;
    }

    public List<HoodieInstant> getFinishedCompactionInstants() {
      return finishedCompactionInstants;
    }

    public List<Pair<HoodieInstant, Boolean>> getFinishedOrRemovedLogCompactionInstants() {
      return finishedOrRemovedLogCompactionInstants;
    }

    public List<Pair<HoodieInstant, Boolean>> getFinishedOrRemovedReplaceInstants() {
      return finishedOrRemovedReplaceInstants;
    }

    public boolean canSyncIncrementally() {
      return canSyncIncrementally;
    }

    @Override
    public String toString() {
      return "TimelineDiffResult{"
          + "newlySeenCompletedAndRewriteInstants=" + newlySeenCompletedAndRewriteInstants
          + ", finishedCompactionInstants=" + finishedCompactionInstants
          + ", finishedOrRemovedLogCompactionInstants=" + finishedOrRemovedLogCompactionInstants
          + ", finishedOrRemovedReplaceInstants=" + finishedOrRemovedReplaceInstants
          + ", canSyncIncrementally=" + canSyncIncrementally
          + '}';
    }
  }
}
