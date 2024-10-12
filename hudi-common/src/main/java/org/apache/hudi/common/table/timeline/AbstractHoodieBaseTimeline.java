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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.GREATER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparatorUtils.compareTimestamps;
import static org.apache.hudi.common.util.StringUtils.getUTF8Bytes;

/**
 * HoodieDefaultTimeline is a default implementation of the HoodieTimeline. It provides methods to inspect a
 * List[HoodieInstant]. Function to get the details of the instant is passed in as a lambda.
 *
 * @see HoodieTimeline
 */
public abstract class AbstractHoodieBaseTimeline implements HoodieTimeline {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractHoodieBaseTimeline.class);

  private static final long serialVersionUID = 1L;

  private static final String HASHING_ALGORITHM = "SHA-256";

  protected transient Function<HoodieInstant, Option<byte[]>> details;
  private List<HoodieInstant> instants;
  // for efficient #contains queries.
  private transient volatile Set<String> instantTimeSet;
  // for efficient #isPendingClusteringInstant queries
  protected transient volatile Set<String> pendingClusteringInstants;
  // for efficient #isBeforeTimelineStarts check.
  private transient volatile Option<HoodieInstant> firstNonSavepointCommit;
  private String timelineHash;

  protected TimelineFactory factory;
  protected InstantComparator instantComparator;
  protected InstantFactory instantFactory;

  public AbstractHoodieBaseTimeline(TimelineLayout layout) {
    this.factory = layout.getTimelineFactory();
    this.instantComparator = layout.getInstantComparator();
    this.instantFactory = layout.getInstantFactory();
  }

  public AbstractHoodieBaseTimeline(Stream<HoodieInstant> instants, Function<HoodieInstant, Option<byte[]>> details,
                                    TimelineFactory factory, InstantComparator instantComparator, InstantFactory instantFactory) {
    this.details = details;
    this.factory = factory;
    this.instantComparator = instantComparator;
    this.instantFactory = instantFactory;
    setInstants(instants.collect(Collectors.toList()));
  }

  @Override
  public void setInstants(List<HoodieInstant> instants) {
    this.instants = instants;
    this.timelineHash = computeTimelineHash(this.instants);
    clearState();
  }

  protected void appendInstants(List<HoodieInstant> newInstants) {
    if (newInstants.isEmpty()) {
      // the new instants is empty, nothing to do.
      return;
    }
    if (this.instants.isEmpty()) {
      // the existing instants is empty, set up the new ones directly.
      setInstants(newInstants);
      return;
    }
    this.instants = mergeInstants(newInstants, this.instants);
    this.timelineHash = computeTimelineHash(this.instants);
    clearState();
  }

  /**
   * For serializing and de-serializing.
   *
   * @deprecated
   */
  public AbstractHoodieBaseTimeline() {
  }

  @Override
  public HoodieTimeline filterInflights() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(HoodieInstant::isInflight), details);
  }

  @Override
  public HoodieTimeline filterInflightsAndRequested() {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(i -> i.getState().equals(State.REQUESTED) || i.getState().equals(State.INFLIGHT)),
        details);
  }

  @Override
  public HoodieTimeline filterPendingExcludingCompaction() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
        && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION))), details);
  }

  @Override
  public HoodieTimeline filterPendingExcludingLogCompaction() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
        && (!instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))), details);
  }

  @Override
  public HoodieTimeline filterPendingExcludingCompactionAndLogCompaction() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
        && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
        || !instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))), details);
  }

  @Override
  public HoodieTimeline filterCompletedInstants() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(HoodieInstant::isCompleted), details);
  }

  @Override
  public HoodieTimeline filterCompletedAndCompactionInstants() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted()
        || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)), details);
  }

  @Override
  public HoodieTimeline filterCompletedOrMajorOrMinorCompactionInstants() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted()
        || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) || s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)), details);
  }

  @Override
  public HoodieTimeline filterCompletedInstantsOrRewriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted() || validActions.contains(s.getAction())), details);
  }

  @Override
  public HoodieTimeline getWriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION);
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> validActions.contains(s.getAction())), details);
  }

  @Override
  public HoodieTimeline getContiguousCompletedWriteTimeline() {
    Option<HoodieInstant> earliestPending = getWriteTimeline().filterInflightsAndRequested().firstInstant();
    if (earliestPending.isPresent()) {
      return getWriteTimeline().filterCompletedInstants()
          .filter(instant -> compareTimestamps(instant.getRequestTime(), LESSER_THAN, earliestPending.get().getRequestTime()));
    }
    return getWriteTimeline().filterCompletedInstants();
  }

  @Override
  public HoodieTimeline getCompletedReplaceTimeline() {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(REPLACE_COMMIT_ACTION)).filter(HoodieInstant::isCompleted), details);
  }

  @Override
  public HoodieTimeline filterPendingReplaceTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public abstract HoodieTimeline filterPendingClusteringTimeline();

  @Override
  public abstract HoodieTimeline filterPendingReplaceOrClusteringTimeline();

  @Override
  public abstract HoodieTimeline filterPendingReplaceClusteringAndCompactionTimeline();

  @Override
  public HoodieTimeline filterPendingRollbackTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.ROLLBACK_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline filterRequestedRollbackTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.ROLLBACK_ACTION) && s.isRequested()), details);
  }

  @Override
  public HoodieTimeline filterPendingCompactionTimeline() {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline filterPendingLogCompactionTimeline() {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION) && !s.isCompleted()), details);
  }

  /**
   * Compaction and logcompaction operation on MOR table is called major and minor compaction respectively.
   */
  @Override
  public HoodieTimeline filterPendingMajorOrMinorCompactionTimeline() {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
            || s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)
            && !s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline findInstantsInRange(String startTs, String endTs) {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> InstantComparatorUtils.isInRange(s.getRequestTime(), startTs, endTs)), details);
  }

  @Override
  public HoodieTimeline findInstantsInClosedRange(String startTs, String endTs) {
    return factory.createDefaultTimeline(
        instants.stream().filter(instant -> InstantComparatorUtils.isInClosedRange(instant.getRequestTime(), startTs, endTs)), details);
  }

  @Override
  public HoodieTimeline findInstantsInRangeByCompletionTime(String startTs, String endTs) {
    return factory.createDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getCompletionTime() != null && InstantComparatorUtils.isInRange(s.getCompletionTime(), startTs, endTs)),
        details);
  }

  @Override
  public HoodieTimeline findInstantsModifiedAfterByCompletionTime(String instantTime) {
    return factory.createDefaultTimeline(instants.stream()
        // either pending or completionTime greater than instantTime
        .filter(s -> (s.getCompletionTime() == null && compareTimestamps(s.getRequestTime(), GREATER_THAN, instantTime))
            || (s.getCompletionTime() != null && compareTimestamps(s.getCompletionTime(), GREATER_THAN, instantTime) && !s.getRequestTime().equals(instantTime))),
        details);
  }

  @Override
  public HoodieTimeline findInstantsAfter(String instantTime, int numCommits) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), GREATER_THAN, instantTime)).limit(numCommits),
        details);
  }

  @Override
  public HoodieTimeline findInstantsAfter(String instantTime) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), GREATER_THAN, instantTime)), details);
  }

  @Override
  public HoodieTimeline findInstantsAfterOrEquals(String commitTime, int numCommits) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), GREATER_THAN_OR_EQUALS, commitTime))
        .limit(numCommits), details);
  }

  @Override
  public HoodieTimeline findInstantsAfterOrEquals(String commitTime) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), GREATER_THAN_OR_EQUALS, commitTime)), details);
  }

  @Override
  public HoodieTimeline findInstantsBefore(String instantTime) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), LESSER_THAN, instantTime)),
        details);
  }

  @Override
  public Option<HoodieInstant> findInstantBefore(String instantTime) {
    return Option.fromJavaOptional(instants.stream()
        .filter(instant -> compareTimestamps(instant.getRequestTime(), LESSER_THAN, instantTime))
        .max(Comparator.comparing(HoodieInstant::getRequestTime)));
  }

  @Override
  public HoodieTimeline findInstantsBeforeOrEquals(String instantTime) {
    return factory.createDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getRequestTime(), LESSER_THAN_OR_EQUALS, instantTime)),
        details);
  }

  @Override
  public HoodieTimeline filter(Predicate<HoodieInstant> filter) {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(filter), details);
  }

  @Override
  public HoodieTimeline filterPendingIndexTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> s.getAction().equals(INDEXING_ACTION) && !s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline filterCompletedIndexTimeline() {
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> s.getAction().equals(INDEXING_ACTION) && s.isCompleted()), details);
  }

  @Override
  public HoodieTimeline getCommitsAndCompactionTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION, COMPACTION_ACTION));
  }

  @Override
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION,
        CLEAN_ACTION, COMPACTION_ACTION, SAVEPOINT_ACTION, ROLLBACK_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION,
        INDEXING_ACTION, LOG_COMPACTION_ACTION));
  }

  @Override
  public HoodieTimeline getCommitAndReplaceTimeline() {
    //TODO: Make sure this change does not break existing functionality.
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION));
  }

  @Override
  public HoodieTimeline getCommitTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION));
  }

  @Override
  public HoodieTimeline getDeltaCommitTimeline() {
    return factory.createDefaultTimeline(filterInstantsByAction(DELTA_COMMIT_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  @Override
  public HoodieTimeline getTimelineOfActions(Set<String> actions) {
    System.out.println("Factory =" + factory);
    return factory.createDefaultTimeline(getInstantsAsStream().filter(s -> actions.contains(s.getAction())),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  @Override
  public HoodieTimeline getCleanerTimeline() {
    return factory.createDefaultTimeline(filterInstantsByAction(CLEAN_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  @Override
  public HoodieTimeline getRollbackTimeline() {
    return factory.createDefaultTimeline(filterInstantsByAction(ROLLBACK_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  @Override
  public HoodieTimeline getRollbackAndRestoreTimeline() {
    return  getTimelineOfActions(CollectionUtils.createSet(ROLLBACK_ACTION, RESTORE_ACTION));
  }

  @Override
  public HoodieTimeline getSavePointTimeline() {
    return factory.createDefaultTimeline(filterInstantsByAction(SAVEPOINT_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  @Override
  public HoodieTimeline getRestoreTimeline() {
    return factory.createDefaultTimeline(filterInstantsByAction(RESTORE_ACTION),
        (Function<HoodieInstant, Option<byte[]>> & Serializable) this::getInstantDetails);
  }

  protected Stream<HoodieInstant> filterInstantsByAction(String action) {
    return getInstantsAsStream().filter(s -> s.getAction().equals(action));
  }

  @Override
  public boolean empty() {
    return instants.isEmpty();
  }

  @Override
  public int countInstants() {
    return instants.size();
  }

  @Override
  public Option<HoodieInstant> firstInstant() {
    return Option.fromJavaOptional(getInstantsAsStream().findFirst());
  }

  @Override
  public Option<HoodieInstant> firstInstant(String action, State state) {
    return Option.fromJavaOptional(getInstantsAsStream()
        .filter(s -> action.equals(s.getAction()) && state.equals(s.getState())).findFirst());
  }

  @Override
  public Option<HoodieInstant> nthInstant(int n) {
    if (empty() || n >= countInstants()) {
      return Option.empty();
    }
    return Option.of(getInstants().get(n));
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
    return getInstantsAsStream().anyMatch(s -> s.equals(instant));
  }

  @Override
  public boolean containsInstant(String ts) {
    // Check for 0.10.0+ timestamps which have msec granularity
    if (getOrCreateInstantSet().contains(ts)) {
      return true;
    }

    // Check for older timestamp which have sec granularity and an extension of DEFAULT_MILLIS_EXT may have been added via Timeline operations
    if (ts.length() == HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH && ts.endsWith(HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT)) {
      final String actualOlderFormatTs = ts.substring(0, ts.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length());
      return containsInstant(actualOlderFormatTs);
    }

    return false;
  }

  @Override
  public boolean containsOrBeforeTimelineStarts(String instant) {
    return containsInstant(instant) || isBeforeTimelineStarts(instant);
  }

  @Override
  public String getTimelineHash() {
    return timelineHash;
  }

  @Override
  public Stream<HoodieInstant> getInstantsAsStream() {
    return instants.stream();
  }

  @Override
  public List<HoodieInstant> getInstants() {
    return new ArrayList<>(instants);
  }

  @Override
  public Stream<HoodieInstant> getReverseOrderedInstants() {
    return getInstantsAsStream().sorted(instantComparator.getRequestTimePrimaryOrderingComparator().reversed());
  }

  @Override
  public Stream<HoodieInstant> getInstantsOrderedByCompletionTime() {
    return getInstantsAsStream().filter(s -> s.getCompletionTime() != null)
        .sorted(instantComparator.getCompletionTimePrimaryOrderingComparator());
  }

  @Override
  public boolean isBeforeTimelineStarts(String instant) {
    Option<HoodieInstant> firstNonSavepointCommit = getFirstNonSavepointCommit();
    return firstNonSavepointCommit.isPresent()
        && compareTimestamps(instant, LESSER_THAN, firstNonSavepointCommit.get().getRequestTime());
  }

  @Override
  public Option<HoodieInstant> getFirstNonSavepointCommit() {
    if (this.firstNonSavepointCommit == null) {
      synchronized (this) {
        if (this.firstNonSavepointCommit == null) {
          this.firstNonSavepointCommit = findFirstNonSavepointCommit(this.instants);
        }
      }
    }
    return this.firstNonSavepointCommit;
  }

  @Override
  public abstract Option<HoodieInstant> getLastClusteringInstant();

  @Override
  public abstract Option<HoodieInstant> getFirstPendingClusterInstant();

  @Override
  public abstract Option<HoodieInstant> getLastPendingClusterInstant();

  @Override
  public abstract boolean isPendingClusteringInstant(String instantTime);

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return details.apply(instant);
  }

  @Override
  public boolean isEmpty(HoodieInstant instant) {
    return getInstantDetails(instant).get().length == 0;
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ": " + getInstantsAsStream().map(Object::toString).collect(Collectors.joining(","));
  }

  private Set<String> getOrCreateInstantSet() {
    if (this.instantTimeSet == null) {
      synchronized (this) {
        if (this.instantTimeSet == null) {
          this.instantTimeSet = this.instants.stream().map(HoodieInstant::getRequestTime).collect(Collectors.toSet());
        }
      }
    }
    return this.instantTimeSet;
  }

  /**
   * Returns the first non savepoint commit on the timeline.
   */
  private static Option<HoodieInstant> findFirstNonSavepointCommit(List<HoodieInstant> instants) {
    Set<String> savepointTimestamps = instants.stream()
        .filter(entry -> entry.getAction().equals(HoodieTimeline.SAVEPOINT_ACTION))
        .map(HoodieInstant::getRequestTime)
        .collect(Collectors.toSet());
    if (!savepointTimestamps.isEmpty()) {
      // There are chances that there could be holes in the timeline due to archival and savepoint interplay.
      // So, the first non-savepoint commit is considered as beginning of the active timeline.
      return Option.fromJavaOptional(instants.stream()
          .filter(entry -> !savepointTimestamps.contains(entry.getRequestTime()))
          .findFirst());
    }
    return Option.fromJavaOptional(instants.stream().findFirst());
  }

  private void clearState() {
    instantTimeSet = null;
    firstNonSavepointCommit = null;
  }

  @Override
  public HoodieTimeline mergeTimeline(HoodieTimeline timeline) {
    Stream<HoodieInstant> instantStream = Stream.concat(getInstantsAsStream(), timeline.getInstantsAsStream()).sorted();
    Function<HoodieInstant, Option<byte[]>> details = instant -> {
      if (getInstantsAsStream().anyMatch(i -> i.equals(instant))) {
        return this.getInstantDetails(instant);
      } else {
        return timeline.getInstantDetails(instant);
      }
    };
    return factory.createDefaultTimeline(instantStream, details);
  }

  /**
   * Computes the timeline hash and returns.
   */
  private String computeTimelineHash(List<HoodieInstant> instants) {
    final MessageDigest md;
    try {
      md = MessageDigest.getInstance(HASHING_ALGORITHM);
      instants.forEach(i -> md
          .update(getUTF8Bytes(StringUtils.joinUsingDelim("_", i.getRequestTime(), i.getAction(), i.getState().name()))));
    } catch (NoSuchAlgorithmException nse) {
      throw new HoodieException(nse);
    }
    return StringUtils.toHexString(md.digest());
  }

  /**
   * Merges the given instant list into one and keep the sequence.
   */
  private static List<HoodieInstant> mergeInstants(List<HoodieInstant> instants1, List<HoodieInstant> instants2) {
    ValidationUtils.checkArgument(!instants1.isEmpty() && !instants2.isEmpty(), "The instants to merge can not be empty");
    // some optimizations are based on the assumption all the instant lists are already sorted.
    // skip when one list contains all the instants of the other one.
    final List<HoodieInstant> merged;
    if (InstantComparatorUtils.compareTimestamps(instants1.get(instants1.size() - 1).getRequestTime(), LESSER_THAN_OR_EQUALS, instants2.get(0).getRequestTime())) {
      merged = new ArrayList<>(instants1);
      merged.addAll(instants2);
    } else if (InstantComparatorUtils.compareTimestamps(instants2.get(instants2.size() - 1).getRequestTime(), LESSER_THAN_OR_EQUALS, instants1.get(0).getRequestTime())) {
      merged = new ArrayList<>(instants2);
      merged.addAll(instants1);
    } else {
      merged = new ArrayList<>(instants1);
      merged.addAll(instants2);
      // sort the instants explicitly
      Collections.sort(merged);
    }
    return merged;
  }
}
