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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.Serializable;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * HoodieTimeline is a view of meta-data instants in the hoodie table. Instants are specific points in time
 * represented as HoodieInstant.
 * <p>
 * Timelines are immutable once created and operations create new instance of timelines which filter on the instants and
 * this can be chained.
 *
 * @see HoodieTableMetaClient
 * @see HoodieDefaultTimeline
 * @see HoodieInstant
 * @since 0.3.0
 */
public interface HoodieTimeline extends Serializable {

  String COMMIT_ACTION = "commit";
  String DELTA_COMMIT_ACTION = "deltacommit";
  String CLEAN_ACTION = "clean";
  String ROLLBACK_ACTION = "rollback";
  String SAVEPOINT_ACTION = "savepoint";
  String REPLACE_COMMIT_ACTION = "replacecommit";
  String INFLIGHT_EXTENSION = ".inflight";
  // With Async Compaction, compaction instant can be in 3 states :
  // (compaction-requested), (compaction-inflight), (completed)
  String COMPACTION_ACTION = "compaction";
  String REQUESTED_EXTENSION = ".requested";
  String RESTORE_ACTION = "restore";

  String[] VALID_ACTIONS_IN_TIMELINE = {COMMIT_ACTION, DELTA_COMMIT_ACTION,
      CLEAN_ACTION, SAVEPOINT_ACTION, RESTORE_ACTION, ROLLBACK_ACTION,
      COMPACTION_ACTION, REPLACE_COMMIT_ACTION};

  String COMMIT_EXTENSION = "." + COMMIT_ACTION;
  String DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION;
  String CLEAN_EXTENSION = "." + CLEAN_ACTION;
  String ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION;
  String SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION;
  // this is to preserve backwards compatibility on commit in-flight filenames
  String INFLIGHT_COMMIT_EXTENSION = INFLIGHT_EXTENSION;
  String REQUESTED_COMMIT_EXTENSION = "." + COMMIT_ACTION + REQUESTED_EXTENSION;
  String REQUESTED_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + INFLIGHT_EXTENSION;
  String INFLIGHT_CLEAN_EXTENSION = "." + CLEAN_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_CLEAN_EXTENSION = "." + CLEAN_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_COMPACTION_SUFFIX = StringUtils.join(COMPACTION_ACTION, REQUESTED_EXTENSION);
  String REQUESTED_COMPACTION_EXTENSION = StringUtils.join(".", REQUESTED_COMPACTION_SUFFIX);
  String INFLIGHT_COMPACTION_EXTENSION = StringUtils.join(".", COMPACTION_ACTION, INFLIGHT_EXTENSION);
  String INFLIGHT_RESTORE_EXTENSION = "." + RESTORE_ACTION + INFLIGHT_EXTENSION;
  String RESTORE_EXTENSION = "." + RESTORE_ACTION;
  String INFLIGHT_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + REQUESTED_EXTENSION;
  String REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION;

  String INVALID_INSTANT_TS = "0";

  // Instant corresponding to pristine state of the table after its creation
  String INIT_INSTANT_TS = "00000000000000";
  // Instant corresponding to METADATA bootstrapping of table/partitions
  String METADATA_BOOTSTRAP_INSTANT_TS = "00000000000001";
  // Instant corresponding to full bootstrapping of table/partitions
  String FULL_BOOTSTRAP_INSTANT_TS = "00000000000002";

  /**
   * Filter this timeline to just include the in-flights.
   *
   * @return New instance of HoodieTimeline with just in-flights
   */
  HoodieTimeline filterInflights();

  /**
   * Filter this timeline to include requested and in-flights.
   *
   * @return New instance of HoodieTimeline with just in-flights and requested instants
   */
  HoodieTimeline filterInflightsAndRequested();

  /**
   * Filter this timeline to just include the in-flights excluding compaction instants.
   *
   * @return New instance of HoodieTimeline with just in-flights excluding compaction instants
   */
  HoodieTimeline filterPendingExcludingCompaction();

  /**
   * Filter this timeline to just include the completed instants.
   *
   * @return New instance of HoodieTimeline with just completed instants
   */
  HoodieTimeline filterCompletedInstants();

  /**
   * Filter this timeline to just include the completed + compaction (inflight + requested) instants A RT filesystem
   * view is constructed with this timeline so that file-slice after pending compaction-requested instant-time is also
   * considered valid. A RT file-system view for reading must then merge the file-slices before and after pending
   * compaction instant so that all delta-commits are read.
   * 
   * @return New instance of HoodieTimeline with just completed instants
   */
  HoodieTimeline filterCompletedAndCompactionInstants();

  /**
   * Timeline to just include commits (commit/deltacommit), compaction and replace actions.
   * 
   * @return
   */
  HoodieTimeline getWriteTimeline();

  /**
   * Timeline to just include replace instants that have valid (commit/deltacommit) actions.
   *
   * @return
   */
  HoodieTimeline getCompletedReplaceTimeline();

  /**
   * Filter this timeline to just include requested and inflight compaction instants.
   * 
   * @return
   */
  HoodieTimeline filterPendingCompactionTimeline();

  /**
   * Filter this timeline to just include requested and inflight replacecommit instants.
   */
  HoodieTimeline filterPendingReplaceTimeline();

  /**
   * Filter this timeline to include pending rollbacks.
   */
  HoodieTimeline filterPendingRollbackTimeline();

  /**
   * Create a new Timeline with all the instants after startTs.
   */
  HoodieTimeline findInstantsAfterOrEquals(String commitTime, int numCommits);

  /**
   * Create a new Timeline with instants after startTs and before or on endTs.
   */
  HoodieTimeline findInstantsInRange(String startTs, String endTs);

  /**
   * Create a new Timeline with all the instants after startTs.
   */
  HoodieTimeline findInstantsAfter(String instantTime, int numCommits);

  /**
   * Create a new Timeline with all the instants after startTs.
   */
  HoodieTimeline findInstantsAfter(String instantTime);

  /**
   * Create a new Timeline with all instants before specified time.
   */
  HoodieTimeline findInstantsBefore(String instantTime);

  /**
   * Create new timeline with all instants before or equals specified time.
   */
  HoodieTimeline findInstantsBeforeOrEquals(String instantTime);
  
  /**
   * Custom Filter of Instants.
   */
  HoodieTimeline filter(Predicate<HoodieInstant> filter);

  /**
   * If the timeline has any instants.
   *
   * @return true if timeline is empty
   */
  boolean empty();

  /**
   * @return total number of completed instants
   */
  int countInstants();

  /**
   * @return first completed instant if available
   */
  Option<HoodieInstant> firstInstant();

  /**
   * @param action Instant action String.
   * @param state  Instant State.
   * @return first instant of a specific action and state if available
   */
  Option<HoodieInstant> firstInstant(String action, State state);

  /**
   * @return nth completed instant from the first completed instant
   */
  Option<HoodieInstant> nthInstant(int n);

  /**
   * @return last completed instant if available
   */
  Option<HoodieInstant> lastInstant();


  /**
   * Get hash of timeline.
   * 
   * @return
   */
  String getTimelineHash();

  /**
   * @return nth completed instant going back from the last completed instant
   */
  Option<HoodieInstant> nthFromLastInstant(int n);

  /**
   * @return true if the passed instant is present as a completed instant on the timeline
   */
  boolean containsInstant(HoodieInstant instant);

  /**
   * @return true if the passed instant is present as a completed instant on the timeline
   */
  boolean containsInstant(String ts);

  /**
   * @return true if the passed instant is present as a completed instant on the timeline or if the instant is before
   *         the first completed instant in the timeline
   */
  boolean containsOrBeforeTimelineStarts(String ts);

  /**
   * @return Get the stream of completed instants
   */
  Stream<HoodieInstant> getInstants();

  /**
   * @return Get the stream of completed instants in reverse order TODO Change code references to getInstants() that
   *         reverse the instants later on to use this method instead.
   */
  Stream<HoodieInstant> getReverseOrderedInstants();

  /**
   * @return true if the passed in instant is before the first completed instant in the timeline
   */
  boolean isBeforeTimelineStarts(String ts);

  /**
   * Read the completed instant details.
   */
  Option<byte[]> getInstantDetails(HoodieInstant instant);

  /**
   * Helper methods to compare instants.
   **/
  BiPredicate<String, String> EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) == 0;
  BiPredicate<String, String> GREATER_THAN_OR_EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) >= 0;
  BiPredicate<String, String> GREATER_THAN = (commit1, commit2) -> commit1.compareTo(commit2) > 0;
  BiPredicate<String, String> LESSER_THAN_OR_EQUALS = (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
  BiPredicate<String, String> LESSER_THAN = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

  static boolean compareTimestamps(String commit1, BiPredicate<String, String> predicateToApply, String commit2) {
    return predicateToApply.test(commit1, commit2);
  }

  /**
   * Return true if specified timestamp is in range (startTs, endTs].
   */
  static boolean isInRange(String timestamp, String startTs, String endTs) {
    return HoodieTimeline.compareTimestamps(timestamp, GREATER_THAN, startTs)
            && HoodieTimeline.compareTimestamps(timestamp, LESSER_THAN_OR_EQUALS, endTs);
  }

  static HoodieInstant getCompletedInstant(final HoodieInstant instant) {
    return new HoodieInstant(State.COMPLETED, instant.getAction(), instant.getTimestamp());
  }

  static HoodieInstant getRequestedInstant(final HoodieInstant instant) {
    return new HoodieInstant(State.REQUESTED, instant.getAction(), instant.getTimestamp());
  }

  static HoodieInstant getCleanRequestedInstant(final String timestamp) {
    return new HoodieInstant(State.REQUESTED, CLEAN_ACTION, timestamp);
  }

  static HoodieInstant getCleanInflightInstant(final String timestamp) {
    return new HoodieInstant(State.INFLIGHT, CLEAN_ACTION, timestamp);
  }

  static HoodieInstant getCompactionRequestedInstant(final String timestamp) {
    return new HoodieInstant(State.REQUESTED, COMPACTION_ACTION, timestamp);
  }

  static HoodieInstant getCompactionInflightInstant(final String timestamp) {
    return new HoodieInstant(State.INFLIGHT, COMPACTION_ACTION, timestamp);
  }

  static HoodieInstant getReplaceCommitRequestedInstant(final String timestamp) {
    return new HoodieInstant(State.REQUESTED, REPLACE_COMMIT_ACTION, timestamp);
  }

  static HoodieInstant getReplaceCommitInflightInstant(final String timestamp) {
    return new HoodieInstant(State.INFLIGHT, REPLACE_COMMIT_ACTION, timestamp);
  }

  /**
   * Returns the inflight instant corresponding to the instant being passed. Takes care of changes in action names
   * between inflight and completed instants (compaction <=> commit).
   * @param instant Hoodie Instant
   * @param tableType Hoodie Table Type
   * @return Inflight Hoodie Instant
   */
  static HoodieInstant getInflightInstant(final HoodieInstant instant, final HoodieTableType tableType) {
    if ((tableType == HoodieTableType.MERGE_ON_READ) && instant.getAction().equals(COMMIT_ACTION)) {
      return new HoodieInstant(true, COMPACTION_ACTION, instant.getTimestamp());
    }
    return new HoodieInstant(true, instant.getAction(), instant.getTimestamp());
  }

  static String makeCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  static String makeInflightCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
  }

  static String makeRequestedCommitFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_COMMIT_EXTENSION);
  }

  static String makeCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.CLEAN_EXTENSION);
  }

  static String makeRequestedCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_CLEAN_EXTENSION);
  }

  static String makeInflightCleanerFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_CLEAN_EXTENSION);
  }

  static String makeRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.ROLLBACK_EXTENSION);
  }

  static String makeRequestedRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_ROLLBACK_EXTENSION);
  }

  static String makeInflightRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  static String makeInflightSavePointFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION);
  }

  static String makeSavePointFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.SAVEPOINT_EXTENSION);
  }

  static String makeInflightDeltaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  static String makeRequestedDeltaFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_DELTA_COMMIT_EXTENSION);
  }

  static String makeInflightCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  static String makeRequestedCompactionFileName(String instantTime) {
    return StringUtils.join(instantTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION);
  }

  static String makeRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.RESTORE_EXTENSION);
  }

  static String makeInflightRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_RESTORE_EXTENSION);
  }

  static String makeReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REPLACE_COMMIT_EXTENSION);
  }

  static String makeInflightReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_REPLACE_COMMIT_EXTENSION);
  }

  static String makeRequestedReplaceFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.REQUESTED_REPLACE_COMMIT_EXTENSION);
  }

  static String makeDeltaFileName(String instantTime) {
    return instantTime + HoodieTimeline.DELTA_COMMIT_EXTENSION;
  }

  static String getCommitFromCommitFile(String commitFileName) {
    return commitFileName.split("\\.")[0];
  }

  static String makeFileNameAsComplete(String fileName) {
    return fileName.replace(HoodieTimeline.INFLIGHT_EXTENSION, "");
  }

  static String makeFileNameAsInflight(String fileName) {
    return StringUtils.join(fileName, HoodieTimeline.INFLIGHT_EXTENSION);
  }
}
