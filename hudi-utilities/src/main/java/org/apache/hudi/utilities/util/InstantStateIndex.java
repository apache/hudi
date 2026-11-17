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

package org.apache.hudi.utilities.util;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * What the active timeline said at {@code T_prev}, in a form small enough to broadcast.
 *
 * <p><b>Built over the FULL active timeline, every action.</b> {@code getCommitsTimeline()} admits
 * only commit, deltacommit, replacecommit and clustering ({@code BaseTimelineV2:90}), so a rollback,
 * clean, compaction or log-compaction instant looked up through it reports <em>absent</em> while
 * being plainly present. An index that inherited that blindness would send live instants down the
 * archive path and, worse, let an absence be read as proof.
 */
public class InstantStateIndex implements Serializable {

  private static final long serialVersionUID = 1L;

  /**
   * Actions that name an instant time without owning it — the instant they describe is some other
   * action's. A savepoint is named with the instant it savepoints ({@code SAVEPOINT_ACTION}), so it
   * routinely shares a time with a write action; recording it would present an ordinary savepoint as
   * a state regression on the commit it savepoints. Every other action on the active timeline —
   * commits, replace/cluster, compaction, log-compaction, clean and rollback alike — owns its own
   * instant time and must be visible to the lookup (see class javadoc: {@code getCommitsTimeline()}
   * is not enough).
   */
  private static final Set<String> ACTIONS_SHARING_ANOTHER_INSTANTS_TIME = Collections.unmodifiableSet(
      new HashSet<>(java.util.Arrays.asList(HoodieTimeline.SAVEPOINT_ACTION)));

  private final Map<String, InstantState> statesByTime;
  private final List<String> orderedInstants;
  private final String oldestActiveCommit;

  private InstantStateIndex(Map<String, InstantState> statesByTime, List<String> orderedInstants,
                            String oldestActiveCommit) {
    this.statesByTime = statesByTime;
    this.orderedInstants = orderedInstants;
    this.oldestActiveCommit = oldestActiveCommit;
  }

  public static InstantStateIndex from(HoodieTimeline activeTimeline) {
    Map<String, InstantState> states = new HashMap<>();
    Set<String> ordered = new TreeSet<>();
    String oldestCommit = null;
    for (HoodieInstant instant : activeTimeline.getInstants()) {
      String time = instant.requestedTime();
      if (time == null) {
        continue;
      }
      ordered.add(time);
      if (ACTIONS_SHARING_ANOTHER_INSTANTS_TIME.contains(instant.getAction())) {
        states.putIfAbsent(time, null);
        continue;
      }
      InstantState state = stateOf(instant);
      InstantState seen = states.get(time);
      // Most advanced wins: revertCompleteToInflight creates .requested and .inflight before
      // deleting .completed (ActiveTimelineV2:607-641), so a mid-revert read sees several at once.
      if (seen == null || state.ordinal() > seen.ordinal()) {
        states.put(time, state);
      }
      if (instant.isCompleted() && isDataCommit(instant.getAction())) {
        oldestCommit = oldestCommit == null
            ? time : InstantComparison.minInstant(oldestCommit, time);
      }
    }
    return new InstantStateIndex(states, new ArrayList<>(ordered), oldestCommit);
  }

  private static boolean isDataCommit(String action) {
    return HoodieTimeline.COMMIT_ACTION.equals(action)
        || HoodieTimeline.DELTA_COMMIT_ACTION.equals(action)
        || HoodieTimeline.REPLACE_COMMIT_ACTION.equals(action)
        || HoodieTimeline.CLUSTERING_ACTION.equals(action);
  }

  private static InstantState stateOf(HoodieInstant instant) {
    if (instant.isCompleted()) {
      return InstantState.COMPLETED;
    }
    return instant.isInflight() ? InstantState.INFLIGHT : InstantState.REQUESTED;
  }

  /**
   * The state at {@code T_prev}, or {@link Resolution#unresolved()} when the active timeline does
   * not hold the instant at all — only the archive can tell a completed-and-archived instant from
   * one that never existed, and this class never guesses.
   */
  public Resolution resolveAtPrev(String instantTime) {
    if (instantTime == null) {
      return Resolution.unresolved();
    }
    InstantState state = statesByTime.get(instantTime);
    return state == null ? Resolution.unresolved() : Resolution.of(state);
  }

  public boolean isOnActiveTimeline(String instantTime) {
    return instantTime != null && statesByTime.get(instantTime) != null;
  }

  /** Every instant time on the active timeline, ascending. */
  public List<String> getOrderedInstants() {
    return Collections.unmodifiableList(orderedInstants);
  }

  /** The earliest completed data commit; null when the timeline holds none. */
  public String getOldestActiveCommit() {
    return oldestActiveCommit;
  }

  /** A state, or the fact that it could not be established. The two are not the same answer. */
  public static final class Resolution implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Resolution UNRESOLVED = new Resolution(null);

    private final InstantState state;

    private Resolution(InstantState state) {
      this.state = state;
    }

    public static Resolution of(InstantState state) {
      return new Resolution(state);
    }

    public static Resolution unresolved() {
      return UNRESOLVED;
    }

    public boolean isResolved() {
      return state != null;
    }

    /** Null when unresolved. A caller must ask {@link #isResolved()} first. */
    public InstantState getState() {
      return state;
    }
  }
}
