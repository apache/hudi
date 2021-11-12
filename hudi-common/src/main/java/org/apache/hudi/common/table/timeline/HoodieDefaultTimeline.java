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
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.reverse;

/**
 * HoodieDefaultTimeline is a default implementation of the HoodieTimeline. It provides methods to inspect a
 * List[HoodieInstant]. Function to get the details of the instant is passed in as a lambda.
 *
 * @see HoodieTimeline
 */
public class HoodieDefaultTimeline implements HoodieTimeline {

  private static final long serialVersionUID = 1L;

  private static final String HASHING_ALGORITHM = "SHA-256";

  protected transient Function<HoodieInstant, Option<byte[]>> details;
  private List<HoodieInstant> instants;
  private String timelineHash;

  public HoodieDefaultTimeline(Stream<HoodieInstant> instants, Function<HoodieInstant, Option<byte[]>> details) {
    this.details = details;
    setInstants(instants.collect(Collectors.toList()));
  }

  public void setInstants(List<HoodieInstant> instants) {
    this.instants = instants;
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance(HASHING_ALGORITHM);
      this.instants.forEach(i -> md
          .update(StringUtils.joinUsingDelim("_", i.getTimestamp(), i.getAction(), i.getState().name()).getBytes()));
    } catch (NoSuchAlgorithmException nse) {
      throw new HoodieException(nse);
    }
    this.timelineHash = StringUtils.toHexString(md.digest());
  }

  /**
   * For serializing and de-serializing.
   *
   * @deprecated
   */
  public HoodieDefaultTimeline() {}

  @Override
  public HoodieTimeline filterInflights() {
    return new HoodieDefaultTimeline(instants.stream().filter(HoodieInstant::isInflight), details);
  }

  @Override
  public HoodieTimeline filterInflightsAndRequested() {
    return new HoodieDefaultTimeline(
        instants.stream().filter(i -> i.getState().equals(State.REQUESTED) || i.getState().equals(State.INFLIGHT)),
        details);
  }

  @Override
  public HoodieTimeline filterPendingExcludingCompaction() {
    return new HoodieDefaultTimeline(instants.stream().filter(instant -> (!instant.isCompleted())
            && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION))), details);
  }

  @Override
  public HoodieTimeline filterCompletedInstants() {
    return new HoodieDefaultTimeline(instants.stream().filter(HoodieInstant::isCompleted), details);
  }

  @Override
  public HoodieTimeline filterCompletedAndCompactionInstants() {
    return new HoodieDefaultTimeline(instants.stream().filter(s -> s.isCompleted()
            || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)), details);
  }

  @Override
  public HoodieDefaultTimeline getWriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return new HoodieDefaultTimeline(instants.stream().filter(s -> validActions.contains(s.getAction())), details);
  }

  @Override
  public HoodieTimeline getCompletedReplaceTimeline() {
    return new HoodieDefaultTimeline(
        instants.stream().filter(s -> s.getAction().equals(REPLACE_COMMIT_ACTION)).filter(s -> s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline filterPendingReplaceTimeline() {
    return new HoodieDefaultTimeline(instants.stream().filter(
        s -> s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline filterPendingCompactionTimeline() {
    return new HoodieDefaultTimeline(
        instants.stream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsInRange(String startTs, String endTs) {
    return new HoodieDefaultTimeline(
        instants.stream().filter(s -> HoodieTimeline.isInRange(s.getTimestamp(), startTs, endTs)), details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsAfter(String instantTime, int numCommits) {
    return new HoodieDefaultTimeline(instants.stream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN, instantTime)).limit(numCommits),
        details);
  }

  @Override
  public HoodieTimeline findInstantsAfter(String instantTime) {
    return new HoodieDefaultTimeline(instants.stream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN, instantTime)), details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsAfterOrEquals(String commitTime, int numCommits) {
    return new HoodieDefaultTimeline(instants.stream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, commitTime))
        .limit(numCommits), details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsBefore(String instantTime) {
    return new HoodieDefaultTimeline(instants.stream()
            .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN, instantTime)),
            details);
  }

  @Override
  public HoodieDefaultTimeline findInstantsBeforeOrEquals(String instantTime) {
    return new HoodieDefaultTimeline(instants.stream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, instantTime)),
        details);
  }

  @Override
  public HoodieTimeline filter(Predicate<HoodieInstant> filter) {
    return new HoodieDefaultTimeline(instants.stream().filter(filter), details);
  }

  /**
   * Get all instants (commits, delta commits) that produce new data, in the active timeline.
   */
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, compaction, clean, savepoint, rollback) that result in actions,
   * in the active timeline.
   */
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION,
            CLEAN_ACTION, COMPACTION_ACTION, SAVEPOINT_ACTION, ROLLBACK_ACTION, REPLACE_COMMIT_ACTION));
  }

  /**
   * Get only pure commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCommitTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  /**
   * Get only the delta commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getDeltaCommitTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(DELTA_COMMIT_ACTION),
            (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get a timeline of a specific set of actions. useful to create a merged timeline of multiple actions.
   *
   * @param actions actions allowed in the timeline
   */
  public HoodieTimeline getTimelineOfActions(Set<String> actions) {
    return new HoodieDefaultTimeline(getInstants().filter(s -> actions.contains(s.getAction())),
            (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the cleaner action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCleanerTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(CLEAN_ACTION),
            (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the rollback action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRollbackTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(ROLLBACK_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the rollback and restore action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRollbackAndRestoreTimeline() {
    return  getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION, RESTORE_ACTION));
  }

  /**
   * Get only the save point action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getSavePointTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(SAVEPOINT_ACTION),
            (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  /**
   * Get only the restore action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRestoreTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(RESTORE_ACTION),
            (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  protected Stream<HoodieInstant> filterInstantsByAction(String action) {
    return getInstants().filter(s -> s.getAction().equals(action));
  }

  @Override
  public boolean empty() {
    return !instants.stream().findFirst().isPresent();
  }

  @Override
  public int countInstants() {
    return instants.size();
  }

  @Override
  public Option<HoodieInstant> firstInstant() {
    return Option.fromJavaOptional(instants.stream().findFirst());
  }

  @Override
  public Option<HoodieInstant> nthInstant(int n) {
    if (empty() || n >= countInstants()) {
      return Option.empty();
    }
    return Option.of(instants.get(n));
  }

  @Override
  public Option<HoodieInstant> lastInstant() {
    return empty() ? Option.empty() : nthInstant(countInstants() - 1);
  }

  @Override
  public Option<HoodieInstant> nthFromLastInstant(int n) {
    if (countInstants() < n + 1) {
      return Option.empty();
    }
    return nthInstant(countInstants() - 1 - n);
  }

  @Override
  public boolean containsInstant(HoodieInstant instant) {
    return instants.stream().anyMatch(s -> s.equals(instant));
  }

  @Override
  public boolean containsInstant(String ts) {
    return instants.stream().anyMatch(s -> s.getTimestamp().equals(ts));
  }

  @Override
  public boolean containsOrBeforeTimelineStarts(String instant) {
    return instants.stream().anyMatch(s -> s.getTimestamp().equals(instant)) || isBeforeTimelineStarts(instant);
  }

  @Override
  public String getTimelineHash() {
    return timelineHash;
  }

  @Override
  public Stream<HoodieInstant> getInstants() {
    return instants.stream();
  }

  @Override
  public Stream<HoodieInstant> getReverseOrderedInstants() {
    List<HoodieInstant> instants = getInstants().collect(Collectors.toList());
    reverse(instants);
    return instants.stream();
  }

  @Override
  public boolean isBeforeTimelineStarts(String instant) {
    Option<HoodieInstant> firstCommit = firstInstant();
    return firstCommit.isPresent()
        && HoodieTimeline.compareTimestamps(instant, LESSER_THAN, firstCommit.get().getTimestamp());
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return details.apply(instant);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ": " + instants.stream().map(Object::toString).collect(Collectors.joining(","));
  }
}
