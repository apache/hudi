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

package org.apache.hudi.common.table;

import org.apache.hudi.common.table.timeline.HoodieDefaultTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.Serializable;
import java.util.function.BiPredicate;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * HoodieTimeline is a view of meta-data instants in the hoodie dataset. Instants are specific points in time
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
  String INFLIGHT_EXTENSION = ".inflight";
  // With Async Compaction, compaction instant can be in 3 states :
  // (compaction-requested), (compaction-inflight), (completed)
  String COMPACTION_ACTION = "compaction";
  String REQUESTED_EXTENSION = ".requested";
  String RESTORE_ACTION = "restore";

  String COMMIT_EXTENSION = "." + COMMIT_ACTION;
  String DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION;
  String CLEAN_EXTENSION = "." + CLEAN_ACTION;
  String ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION;
  String SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION;
  // this is to preserve backwards compatibility on commit in-flight filenames
  String INFLIGHT_COMMIT_EXTENSION = INFLIGHT_EXTENSION;
  String INFLIGHT_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + INFLIGHT_EXTENSION;
  String INFLIGHT_CLEAN_EXTENSION = "." + CLEAN_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_CLEAN_EXTENSION = "." + CLEAN_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_ROLLBACK_EXTENSION = "." + ROLLBACK_ACTION + INFLIGHT_EXTENSION;
  String INFLIGHT_SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_COMPACTION_SUFFIX = StringUtils.join(COMPACTION_ACTION, REQUESTED_EXTENSION);
  String REQUESTED_COMPACTION_EXTENSION = StringUtils.join(".", REQUESTED_COMPACTION_SUFFIX);
  String INFLIGHT_COMPACTION_EXTENSION = StringUtils.join(".", COMPACTION_ACTION, INFLIGHT_EXTENSION);
  String INFLIGHT_RESTORE_EXTENSION = "." + RESTORE_ACTION + INFLIGHT_EXTENSION;
  String RESTORE_EXTENSION = "." + RESTORE_ACTION;

  String INVALID_INSTANT_TS = "0";

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
   * @return New instance of HoodieTimeline with just in-flights excluding compaction inflights
   */
  HoodieTimeline filterInflightsExcludingCompaction();

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
   * Timeline to just include commits (commit/deltacommit) and compaction actions.
   * 
   * @return
   */
  HoodieTimeline getCommitsAndCompactionTimeline();

  /**
   * Filter this timeline to just include requested and inflight compaction instants.
   * 
   * @return
   */
  HoodieTimeline filterPendingCompactionTimeline();

  /**
   * Create a new Timeline with instants after startTs and before or on endTs.
   */
  HoodieTimeline findInstantsInRange(String startTs, String endTs);

  /**
   * Create a new Timeline with all the instants after startTs.
   */
  HoodieTimeline findInstantsAfter(String commitTime, int numCommits);

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
  BiPredicate<String, String> EQUAL = (commit1, commit2) -> commit1.compareTo(commit2) == 0;
  BiPredicate<String, String> GREATER_OR_EQUAL = (commit1, commit2) -> commit1.compareTo(commit2) >= 0;
  BiPredicate<String, String> GREATER = (commit1, commit2) -> commit1.compareTo(commit2) > 0;
  BiPredicate<String, String> LESSER_OR_EQUAL = (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
  BiPredicate<String, String> LESSER = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

  static boolean compareTimestamps(String commit1, String commit2, BiPredicate<String, String> predicateToApply) {
    return predicateToApply.test(commit1, commit2);
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

  static HoodieInstant getInflightInstant(final HoodieInstant instant) {
    return new HoodieInstant(true, instant.getAction(), instant.getTimestamp());
  }

  static String makeCommitFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.COMMIT_EXTENSION);
  }

  static String makeInflightCommitFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.INFLIGHT_COMMIT_EXTENSION);
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

  static String makeInflightRollbackFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_ROLLBACK_EXTENSION);
  }

  static String makeInflightSavePointFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION);
  }

  static String makeSavePointFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.SAVEPOINT_EXTENSION);
  }

  static String makeInflightDeltaFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION);
  }

  static String makeInflightCompactionFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION);
  }

  static String makeRequestedCompactionFileName(String commitTime) {
    return StringUtils.join(commitTime, HoodieTimeline.REQUESTED_COMPACTION_EXTENSION);
  }

  static String makeRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.RESTORE_EXTENSION);
  }

  static String makeInflightRestoreFileName(String instant) {
    return StringUtils.join(instant, HoodieTimeline.INFLIGHT_RESTORE_EXTENSION);
  }

  static String makeDeltaFileName(String commitTime) {
    return commitTime + HoodieTimeline.DELTA_COMMIT_EXTENSION;
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
