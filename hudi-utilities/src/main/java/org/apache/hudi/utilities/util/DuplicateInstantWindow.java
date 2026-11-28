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

import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Whether a duplicate base-file group's writing instant could have been transitioning at some point
 * between two reads of the active timeline that bracket a validation run.
 *
 * <p>A single point-in-time read of the timeline cannot separate two populations that look
 * identical to it: an instant genuinely in flight right now, and an instant with no file anywhere on
 * the timeline that never existed and never will. Both answer "not completed" to a one-shot check.
 * This class instead closes over a <b>window</b>, {@code [T_prevD, T_aftD]}, and lets an instant
 * through only if it could plausibly have been transitioning somewhere inside it:
 *
 * <ul>
 *   <li>it was requested or inflight at {@code T_prevD} (an ordinary write racing the validation
 *       run, already under way when the window opened); or
 *   <li>it is absent from {@code T_prevD} in every state and present, in any state, at
 *       {@code T_aftD} (it can only have started after {@code T_prevD} was read, so its whole life so
 *       far is inside the window).
 * </ul>
 *
 * <p>An instant present at neither read could not have been transitioning at any point in the
 * window — this is exactly the case a single-point gate could not distinguish from a real in-flight
 * write, and letting it through here is what makes a duplicate under a wholly fictional instant (two
 * base files sharing one file-slice identity with nothing on the timeline naming it, at either read)
 * a reported finding rather than a silently dropped one.
 *
 * <p>Built on top of {@link InstantStateIndex} rather than iterating a timeline directly, {@link
 * Serializable}, and free of Spark or storage I/O, so an instance is safe to capture in a task
 * closure or broadcast.
 */
public final class DuplicateInstantWindow implements Serializable {

  private static final long serialVersionUID = 1L;

  /** Which of the two populations let an instant through the window. */
  public enum Reason {
    /** Requested or inflight at {@code T_prevD}: an ordinary write already under way. */
    PREV_INFLIGHT,
    /** Absent from {@code T_prevD} in every state, present in some state at {@code T_aftD}. */
    AFT_EXCLUSIVE
  }

  private final Set<String> prevInflight;
  private final Set<String> aftExclusive;

  private DuplicateInstantWindow(Set<String> prevInflight, Set<String> aftExclusive) {
    this.prevInflight = prevInflight;
    this.aftExclusive = aftExclusive;
  }

  /**
   * Builds the window from the two active-timeline reads that bracket one dimension's run.
   *
   * @param tPrevD the active timeline (every action, every state) read before phase 1 starts
   * @param tAftD  the active timeline read again, through a separate meta client, only once phase 1
   *               has been forced to completion — the same ordering precondition
   *               {@code FsMdtDiffDimension#run} already enforces for its own barrier read, and for
   *               the same reason: a listing that raced ahead of {@code T_aftD} would produce a
   *               suspect this window cannot explain
   */
  public static DuplicateInstantWindow between(HoodieTimeline tPrevD, HoodieTimeline tAftD) {
    InstantStateIndex prevIndex = InstantStateIndex.from(tPrevD);
    InstantStateIndex aftIndex = InstantStateIndex.from(tAftD);

    Set<String> prevInflight = new HashSet<>();
    for (String instantTime : prevIndex.getOrderedInstants()) {
      InstantStateIndex.Resolution resolution = prevIndex.resolveAtPrev(instantTime);
      if (resolution.isResolved() && resolution.getState() != InstantState.COMPLETED) {
        prevInflight.add(instantTime);
      }
    }

    Set<String> prevAll = new HashSet<>(prevIndex.getOrderedInstants());
    Set<String> aftExclusive = new HashSet<>();
    for (String instantTime : aftIndex.getOrderedInstants()) {
      if (!prevAll.contains(instantTime)) {
        aftExclusive.add(instantTime);
      }
    }
    return new DuplicateInstantWindow(prevInflight, aftExclusive);
  }

  /**
   * Why {@code instantTime} could have been transitioning somewhere in the window, or empty when it
   * could not have been.
   *
   * <p>Empty is exactly the case this class exists to let through as a genuine finding: an instant
   * with no file anywhere on the timeline, at either read (the worked example this mechanism was
   * built from is a duplicate slice under an instant with no requested, inflight, completed or
   * archived file at all, at either read).
   */
  public Option<Reason> sanctionReasonFor(String instantTime) {
    if (instantTime != null && prevInflight.contains(instantTime)) {
      return Option.of(Reason.PREV_INFLIGHT);
    }
    if (instantTime != null && aftExclusive.contains(instantTime)) {
      return Option.of(Reason.AFT_EXCLUSIVE);
    }
    return Option.empty();
  }

  /** Whether {@code instantTime} could have been transitioning somewhere in the window. */
  public boolean sanctions(String instantTime) {
    return sanctionReasonFor(instantTime).isPresent();
  }
}
