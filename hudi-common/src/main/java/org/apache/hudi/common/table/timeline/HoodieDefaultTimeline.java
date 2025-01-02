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
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.compareTimestamps;

/**
 * HoodieDefaultTimeline is a default implementation of the HoodieTimeline. It provides methods to inspect a
 * List[HoodieInstant]. Function to get the details of the instant is passed in as a lambda.
 *
 * @see HoodieTimeline
 */
public class HoodieDefaultTimeline implements HoodieTimeline {
  private static final Map<Class<?>, DatumReader<?>> DATUM_READERS = new ConcurrentHashMap<>();

  private static final long serialVersionUID = 1L;

  private static final String HASHING_ALGORITHM = "SHA-256";

  protected transient HoodieInstantReader instantReader;
  private List<HoodieInstant> instants;
  // for efficient #contains queries.
  private transient volatile Set<String> instantTimeSet;
  // for efficient #isPendingClusterInstant queries
  private transient volatile Set<String> pendingReplaceClusteringInstants;
  // for efficient #isBeforeTimelineStarts check.
  private transient volatile Option<HoodieInstant> firstNonSavepointCommit;
  private String timelineHash;

  public HoodieDefaultTimeline(Stream<HoodieInstant> instants,
                               HoodieInstantReader instantReader) {
    this.instantReader = instantReader;
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
  public HoodieDefaultTimeline() {
  }

  @Override
  public HoodieTimeline filterInflights() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(HoodieInstant::isInflight), instantReader);
  }

  @Override
  public HoodieTimeline filterInflightsAndRequested() {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(i -> i.getState().equals(State.REQUESTED) || i.getState().equals(State.INFLIGHT)),
        instantReader);
  }

  @Override
  public HoodieTimeline filterPendingExcludingCompaction() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
            && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION))), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingExcludingLogCompaction() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
        && (!instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))), instantReader);
  }

  //TODO: Use a better naming convention for this.
  @Override
  public HoodieTimeline filterPendingExcludingMajorAndMinorCompaction() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(instant -> (!instant.isCompleted())
        && (!instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
        || !instant.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION))), instantReader);
  }

  @Override
  public HoodieTimeline filterCompletedInstants() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(HoodieInstant::isCompleted), instantReader);
  }

  @Override
  public HoodieTimeline filterCompletedAndCompactionInstants() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted()
            || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)), instantReader);
  }

  @Override
  public HoodieTimeline filterCompletedOrMajorOrMinorCompactionInstants() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted()
        || s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) || s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)), instantReader);
  }

  @Override
  public HoodieDefaultTimeline filterCompletedInstantsOrRewriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> s.isCompleted() || validActions.contains(s.getAction())), instantReader);
  }

  @Override
  public HoodieDefaultTimeline getWriteTimeline() {
    Set<String> validActions = CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION);
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> validActions.contains(s.getAction())), instantReader);
  }

  @Override
  public HoodieTimeline getContiguousCompletedWriteTimeline() {
    Option<HoodieInstant> earliestPending = getWriteTimeline().filterInflightsAndRequested().firstInstant();
    if (earliestPending.isPresent()) {
      return getWriteTimeline().filterCompletedInstants()
          .filter(instant -> compareTimestamps(instant.getTimestamp(), LESSER_THAN, earliestPending.get().getTimestamp()));
    }
    return getWriteTimeline().filterCompletedInstants();
  }

  @Override
  public HoodieTimeline getCompletedReplaceTimeline() {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(REPLACE_COMMIT_ACTION)).filter(HoodieInstant::isCompleted), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingReplaceTimeline() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION) && !s.isCompleted()), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingRollbackTimeline() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.ROLLBACK_ACTION) && !s.isCompleted()), instantReader);
  }

  @Override
  public HoodieTimeline filterRequestedRollbackTimeline() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(
        s -> s.getAction().equals(HoodieTimeline.ROLLBACK_ACTION) && s.isRequested()), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingCompactionTimeline() {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION) && !s.isCompleted()), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingLogCompactionTimeline() {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION) && !s.isCompleted()), instantReader);
  }

  /**
   * Compaction and logcompaction operation on MOR table is called major and minor compaction respectively.
   */
  @Override
  public HoodieTimeline filterPendingMajorOrMinorCompactionTimeline() {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> s.getAction().equals(HoodieTimeline.COMPACTION_ACTION)
            || s.getAction().equals(HoodieTimeline.LOG_COMPACTION_ACTION)
            && !s.isCompleted()), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsInRange(String startTs, String endTs) {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> HoodieTimeline.isInRange(s.getTimestamp(), startTs, endTs)), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsInClosedRange(String startTs, String endTs) {
    return new HoodieDefaultTimeline(
        instants.stream().filter(instant -> HoodieTimeline.isInClosedRange(instant.getTimestamp(), startTs, endTs)), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsInRangeByStateTransitionTime(String startTs, String endTs) {
    return new HoodieDefaultTimeline(
        getInstantsAsStream().filter(s -> HoodieTimeline.isInRange(s.getStateTransitionTime(), startTs, endTs)),
        instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsModifiedAfterByStateTransitionTime(String instantTime) {
    return new HoodieDefaultTimeline(instants.stream()
        .filter(s -> HoodieTimeline.compareTimestamps(s.getStateTransitionTime(),
            GREATER_THAN, instantTime) && !s.getTimestamp().equals(instantTime)), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsAfter(String instantTime, int numCommits) {
    return new HoodieDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getTimestamp(), GREATER_THAN, instantTime)).limit(numCommits),
        instantReader);
  }

  @Override
  public HoodieTimeline findInstantsAfter(String instantTime) {
    return new HoodieDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getTimestamp(), GREATER_THAN, instantTime)), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsAfterOrEquals(String commitTime, int numCommits) {
    return new HoodieDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getTimestamp(), GREATER_THAN_OR_EQUALS, commitTime))
        .limit(numCommits), instantReader);
  }

  @Override
  public HoodieDefaultTimeline findInstantsBefore(String instantTime) {
    return new HoodieDefaultTimeline(getInstantsAsStream()
            .filter(s -> compareTimestamps(s.getTimestamp(), LESSER_THAN, instantTime)),
        instantReader);
  }

  @Override
  public Option<HoodieInstant> findInstantBefore(String instantTime) {
    return Option.fromJavaOptional(instants.stream()
        .filter(instant -> compareTimestamps(instant.getTimestamp(), LESSER_THAN, instantTime))
        .max(Comparator.comparing(HoodieInstant::getTimestamp)));
  }

  @Override
  public HoodieDefaultTimeline findInstantsBeforeOrEquals(String instantTime) {
    return new HoodieDefaultTimeline(getInstantsAsStream()
        .filter(s -> compareTimestamps(s.getTimestamp(), LESSER_THAN_OR_EQUALS, instantTime)),
        instantReader);
  }

  @Override
  public HoodieTimeline filter(Predicate<HoodieInstant> filter) {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(filter), instantReader);
  }

  @Override
  public HoodieTimeline filterPendingIndexTimeline() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> s.getAction().equals(INDEXING_ACTION) && !s.isCompleted()), instantReader);
  }

  @Override
  public HoodieTimeline filterCompletedIndexTimeline() {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> s.getAction().equals(INDEXING_ACTION) && s.isCompleted()), instantReader);
  }

  /**
   * Get all instants (commits, delta commits) that produce new data, in the active timeline.
   */
  public HoodieTimeline getCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, replace, compaction) that produce new data or merge file, in the active timeline.
   */
  public HoodieTimeline getCommitsAndCompactionTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION, REPLACE_COMMIT_ACTION, COMPACTION_ACTION));
  }

  /**
   * Get all instants (commits, delta commits, compaction, clean, savepoint, rollback, replace commits, index) that result in actions,
   * in the active timeline.
   */
  public HoodieTimeline getAllCommitsTimeline() {
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, DELTA_COMMIT_ACTION,
        CLEAN_ACTION, COMPACTION_ACTION, SAVEPOINT_ACTION, ROLLBACK_ACTION, REPLACE_COMMIT_ACTION, INDEXING_ACTION,
        LOG_COMPACTION_ACTION));
  }

  /**
   * Get only pure commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCommitTimeline() {
    //TODO: Make sure this change does not break existing functionality.
    return getTimelineOfActions(CollectionUtils.createSet(COMMIT_ACTION, REPLACE_COMMIT_ACTION));
  }

  /**
   * Get only the delta commits (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getDeltaCommitTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(DELTA_COMMIT_ACTION), instantReader);
  }

  /**
   * Get a timeline of a specific set of actions. useful to create a merged timeline of multiple actions.
   *
   * @param actions actions allowed in the timeline
   */
  public HoodieTimeline getTimelineOfActions(Set<String> actions) {
    return new HoodieDefaultTimeline(getInstantsAsStream().filter(s -> actions.contains(s.getAction())), instantReader);
  }

  /**
   * Get only the cleaner action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getCleanerTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(CLEAN_ACTION), instantReader);
  }

  /**
   * Get only the rollback action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRollbackTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(ROLLBACK_ACTION), instantReader);
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
    return new HoodieDefaultTimeline(filterInstantsByAction(SAVEPOINT_ACTION), instantReader);
  }

  /**
   * Get only the restore action (inflight and completed) in the active timeline.
   */
  public HoodieTimeline getRestoreTimeline() {
    return new HoodieDefaultTimeline(filterInstantsByAction(RESTORE_ACTION), instantReader);
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
    return getInstantsAsStream().sorted(HoodieInstant.COMPARATOR.reversed());
  }

  @Override
  public Stream<HoodieInstant> getInstantsOrderedByStateTransitionTime() {
    return getInstantsAsStream().sorted(HoodieInstant.STATE_TRANSITION_COMPARATOR);
  }

  @Override
  public boolean isBeforeTimelineStarts(String instant) {
    Option<HoodieInstant> firstNonSavepointCommit = getFirstNonSavepointCommit();
    return firstNonSavepointCommit.isPresent()
        && compareTimestamps(instant, LESSER_THAN, firstNonSavepointCommit.get().getTimestamp());
  }

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
  public Option<HoodieInstant> getLastClusteringInstant() {
    return Option.fromJavaOptional(getCommitsTimeline().filter(s -> s.getAction().equalsIgnoreCase(HoodieTimeline.REPLACE_COMMIT_ACTION))
        .getReverseOrderedInstants()
        .filter(i -> ClusteringUtils.isClusteringInstant(this, i))
        .findFirst());
  }

  @Override
  public Option<HoodieInstant> getFirstPendingClusterInstant() {
    return getLastOrFirstPendingClusterInstant(false);
  }

  @Override
  public Option<HoodieInstant> getLastPendingClusterCommit() {
    return getLastOrFirstPendingClusterInstant(true);
  }

  private Option<HoodieInstant> getLastOrFirstPendingClusterInstant(boolean isLast) {
    HoodieTimeline replaceTimeline = filterPendingReplaceTimeline();
    Stream<HoodieInstant> replaceStream;
    if (isLast) {
      replaceStream = replaceTimeline.getReverseOrderedInstants();
    } else {
      replaceStream = replaceTimeline.getInstantsAsStream();
    }
    return  Option.fromJavaOptional(replaceStream
        .filter(i -> ClusteringUtils.isClusteringInstant(this, i)).findFirst());
  }

  @Override
  public boolean isPendingClusterInstant(String instantTime) {
    return getOrCreatePendingClusteringInstantSet().contains(instantTime);
  }

  @Override
  public Option<byte[]> getInstantDetails(HoodieInstant instant) {
    return instantReader.getInstantDetails(instant);
  }

  @Override
  public InputStream getInstantContentStream(HoodieInstant instant) {
    return instantReader.getContentStream(instant);
  }

  @Override
  public <T> T deserializeInstantContent(HoodieInstant instant, Class<T> clazz) throws IOException {
    if (SpecificRecord.class.isAssignableFrom(clazz)) {
      DatumReader<T> reader = (DatumReader<T>) DATUM_READERS.computeIfAbsent(clazz, SpecificDatumReader::new);
      try (InputStream inputStream = getInstantContentStream(instant);
           DataFileStream<T> fileReader = new DataFileStream<>(inputStream, reader)) {
        ValidationUtils.checkArgument(fileReader.hasNext(), "Could not deserialize metadata of type " + clazz);
        return fileReader.next();
      }
    } else {
      try (InputStream inputStream = getInstantContentStream(instant)) {
        return JsonUtils.getObjectMapper().readValue(inputStream, clazz);
      } catch (MismatchedInputException ex) {
        if (ex.getMessage().startsWith("No content to map")) {
          return ReflectionUtils.loadClass(clazz.getName());
        }
        throw ex;
      }
    }
  }

  @Override
  public boolean isEmpty(HoodieInstant instant) {
    return instantReader.isEmpty(instant);
  }

  @Override
  public String toString() {
    return this.getClass().getName() + ": " + getInstantsAsStream().map(Object::toString).collect(Collectors.joining(","));
  }

  /**
   * Merge this timeline with the given timeline.
   */
  public HoodieDefaultTimeline mergeTimeline(HoodieDefaultTimeline timeline) {
    Stream<HoodieInstant> instantStream = Stream.concat(getInstantsAsStream(), timeline.getInstantsAsStream()).sorted();
    return new HoodieDefaultTimeline(instantStream, new MergedReader(this, timeline));
  }

  private Set<String> getOrCreateInstantSet() {
    if (this.instantTimeSet == null) {
      synchronized (this) {
        if (this.instantTimeSet == null) {
          this.instantTimeSet = this.instants.stream().map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
        }
      }
    }
    return this.instantTimeSet;
  }

  private Set<String> getOrCreatePendingClusteringInstantSet() {
    if (this.pendingReplaceClusteringInstants == null) {
      synchronized (this) {
        if (this.pendingReplaceClusteringInstants == null) {
          List<HoodieInstant> pendingReplaceInstants = getCommitsTimeline().filterPendingReplaceTimeline().getInstants();
          // Validate that there are no instants with same timestamp
          pendingReplaceInstants.stream().collect(Collectors.groupingBy(HoodieInstant::getTimestamp)).forEach((timestamp, instants) -> {
            if (instants.size() > 1) {
              throw new IllegalStateException("Multiple instants with same timestamp: " + timestamp + " instants: " + instants);
            }
          });
          // Filter replace commits down to those that are due to clustering
          this.pendingReplaceClusteringInstants = pendingReplaceInstants.stream()
              .filter(instant -> ClusteringUtils.isClusteringInstant(this, instant))
              .map(HoodieInstant::getTimestamp).collect(Collectors.toSet());
        }
      }
    }
    return this.pendingReplaceClusteringInstants;
  }

  /**
   * Returns the first non savepoint commit on the timeline.
   */
  private static Option<HoodieInstant> findFirstNonSavepointCommit(List<HoodieInstant> instants) {
    Set<String> savepointTimestamps = instants.stream()
        .filter(entry -> entry.getAction().equals(HoodieTimeline.SAVEPOINT_ACTION))
        .map(HoodieInstant::getTimestamp)
        .collect(Collectors.toSet());
    if (!savepointTimestamps.isEmpty()) {
      // There are chances that there could be holes in the timeline due to archival and savepoint interplay.
      // So, the first non-savepoint commit is considered as beginning of the active timeline.
      return Option.fromJavaOptional(instants.stream()
          .filter(entry -> !savepointTimestamps.contains(entry.getTimestamp()))
          .findFirst());
    }
    return Option.fromJavaOptional(instants.stream().findFirst());
  }

  public HoodieInstantReader getInstantReader() {
    return instantReader;
  }

  private static class MergedReader implements HoodieInstantReader, Serializable {
    private final HoodieTimeline timeline1;
    private final HoodieTimeline timeline2;

    public MergedReader(HoodieTimeline timeline1, HoodieTimeline timeline2) {
      this.timeline1 = timeline1;
      this.timeline2 = timeline2;
    }

    @Override
    public InputStream getContentStream(HoodieInstant instant) {
      if (timeline1.getInstantsAsStream().anyMatch(i -> i.equals(instant))) {
        return timeline1.getInstantContentStream(instant);
      } else {
        return timeline2.getInstantContentStream(instant);
      }
    }

    @Override
    public boolean isEmpty(HoodieInstant instant) {
      if (timeline1.getInstantsAsStream().anyMatch(i -> i.equals(instant))) {
        return timeline1.isEmpty(instant);
      } else {
        return timeline2.isEmpty(instant);
      }
    }
  }
}
