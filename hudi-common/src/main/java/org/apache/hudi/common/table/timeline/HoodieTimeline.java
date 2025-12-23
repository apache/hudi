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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieIndexPlan;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.storage.HoodieInstantWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
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
 * @see HoodieInstant
 * @since 0.3.0
 */
public interface HoodieTimeline extends HoodieInstantReader, Serializable {

  String COMMIT_ACTION = "commit";
  String DELTA_COMMIT_ACTION = "deltacommit";
  String CLEAN_ACTION = "clean";
  String ROLLBACK_ACTION = "rollback";
  String SAVEPOINT_ACTION = "savepoint";
  String REPLACE_COMMIT_ACTION = "replacecommit";
  String CLUSTERING_ACTION = "clustering";
  String INFLIGHT_EXTENSION = ".inflight";
  // With Async Compaction, compaction instant can be in 3 states :
  // (compaction-requested), (compaction-inflight), (completed)
  String COMPACTION_ACTION = "compaction";
  String LOG_COMPACTION_ACTION = "logcompaction";
  String REQUESTED_EXTENSION = ".requested";
  String COMPLETED_EXTENSION = ".completed";
  String RESTORE_ACTION = "restore";
  String INDEXING_ACTION = "indexing";
  // only for schema save
  String SCHEMA_COMMIT_ACTION = "schemacommit";
  String[] VALID_ACTIONS_IN_TIMELINE = {COMMIT_ACTION, DELTA_COMMIT_ACTION,
      CLEAN_ACTION, SAVEPOINT_ACTION, RESTORE_ACTION, ROLLBACK_ACTION,
      COMPACTION_ACTION, LOG_COMPACTION_ACTION, REPLACE_COMMIT_ACTION, CLUSTERING_ACTION, INDEXING_ACTION};

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
  String COMPLETED_COMPACTION_SUFFIX = StringUtils.join(COMPACTION_ACTION, COMPLETED_EXTENSION);
  String INFLIGHT_COMPACTION_EXTENSION = StringUtils.join(".", COMPACTION_ACTION, INFLIGHT_EXTENSION);
  String REQUESTED_RESTORE_EXTENSION = "." + RESTORE_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_RESTORE_EXTENSION = "." + RESTORE_ACTION + INFLIGHT_EXTENSION;
  String RESTORE_EXTENSION = "." + RESTORE_ACTION;
  String INFLIGHT_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION + REQUESTED_EXTENSION;
  String REPLACE_COMMIT_EXTENSION = "." + REPLACE_COMMIT_ACTION;
  String INFLIGHT_CLUSTERING_COMMIT_EXTENSION = "." + CLUSTERING_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_CLUSTERING_COMMIT_EXTENSION = "." + CLUSTERING_ACTION + REQUESTED_EXTENSION;
  String INFLIGHT_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION + REQUESTED_EXTENSION;
  String INDEX_COMMIT_EXTENSION = "." + INDEXING_ACTION;
  String SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION;
  String INFLIGHT_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + INFLIGHT_EXTENSION;
  String REQUESTED_SAVE_SCHEMA_ACTION_EXTENSION = "." + SCHEMA_COMMIT_ACTION + REQUESTED_EXTENSION;
  // Log compaction action
  String REQUESTED_LOG_COMPACTION_SUFFIX = StringUtils.join(LOG_COMPACTION_ACTION, REQUESTED_EXTENSION);
  String REQUESTED_LOG_COMPACTION_EXTENSION = StringUtils.join(".", REQUESTED_LOG_COMPACTION_SUFFIX);
  String INFLIGHT_LOG_COMPACTION_EXTENSION = StringUtils.join(".", LOG_COMPACTION_ACTION, INFLIGHT_EXTENSION);

  String INVALID_INSTANT_TS = "0";

  // Instant corresponding to pristine state of the table after its creation
  String INIT_INSTANT_TS = "00000000000000";
  // Instant corresponding to METADATA bootstrapping of table/partitions
  String METADATA_BOOTSTRAP_INSTANT_TS = "00000000000001";
  // Instant corresponding to full bootstrapping of table/partitions
  String FULL_BOOTSTRAP_INSTANT_TS = "00000000000002";

  /**
   * Read and deserialize the content of an instant into the specified class type.
   *
   * @param instant The instant to read content from
   * @param clazz   The target class to deserialize into
   * @return Deserialized instant content
   * @throws IOException when reading instant content fails
   */
  default <T> T readInstantContent(HoodieInstant instant, Class<T> clazz) throws IOException {
    TimelineLayout layout = TimelineLayout.fromVersion(getTimelineLayoutVersion());
    return layout.getCommitMetadataSerDe().deserialize(
        instant, getInstantContentStream(instant), () -> isEmpty(instant), clazz);
  }

  /**
   * Read and deserialize the content of an instant into the specified class type
   * assuming that the instant file must exist and not empty. If the file does not
   * exist or is empty, IOException is thrown.
   *
   * @param instant The instant to read content from
   * @param clazz   The target class to deserialize into
   * @return Deserialized instant content
   * @throws IOException when reading instant content fails
   */
  default <T> T readNonEmptyInstantContent(HoodieInstant instant, Class<T> clazz) throws IOException {
    TimelineLayout layout = TimelineLayout.fromVersion(getTimelineLayoutVersion());
    return layout.getCommitMetadataSerDe().deserialize(
        instant, getInstantContentStream(instant), () -> false, clazz);
  }

  /**
   * Read and deserialize commit metadata from an instant
   *
   * @param instant the instant to read
   * @return deserialized commit metadata in POJO
   */
  default org.apache.hudi.common.model.HoodieCommitMetadata readCommitMetadata(HoodieInstant instant)
      throws IOException {
    return readInstantContent(instant, org.apache.hudi.common.model.HoodieCommitMetadata.class);
  }

  /**
   * Read and deserialize replacecommit metadata from an instant
   *
   * @param instant the instant to read
   * @return deserialized replacecommit metadata in POJO
   */
  default org.apache.hudi.common.model.HoodieReplaceCommitMetadata readReplaceCommitMetadata(
      HoodieInstant instant) throws IOException {
    return readInstantContent(instant, org.apache.hudi.common.model.HoodieReplaceCommitMetadata.class);
  }

  /**
   * Read and deserialize commit metadata in Avro format from an instant.
   *
   * @param instant The instant containing the commit metadata
   * @return Deserialized HoodieCommitMetadata
   * @throws IOException when reading instant content fails
   */
  default org.apache.hudi.avro.model.HoodieCommitMetadata readCommitMetadataToAvro(HoodieInstant instant)
      throws IOException {
    return readInstantContent(instant, org.apache.hudi.avro.model.HoodieCommitMetadata.class);
  }

  /**
   * Read and deserialize replace commit metadata in Avro format from an instant.
   *
   * @param instant The instant containing the replace commit metadata
   * @return Deserialized HoodieReplaceCommitMetadata
   * @throws IOException when reading instant content fails
   */
  default org.apache.hudi.avro.model.HoodieReplaceCommitMetadata readReplaceCommitMetadataToAvro(HoodieInstant instant)
      throws IOException {
    return readInstantContent(instant, org.apache.hudi.avro.model.HoodieReplaceCommitMetadata.class);
  }

  /**
   * Read and deserialize cleaner plan from an instant.
   *
   * @param instant The instant containing the cleaner plan
   * @return Deserialized HoodieCleanerPlan
   * @throws IOException when reading instant content fails
   */
  default HoodieCleanerPlan readCleanerPlan(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieCleanerPlan.class);
  }

  /**
   * Read and deserialize compaction plan from an instant.
   *
   * @param instant The instant containing the compaction plan
   * @return Deserialized HoodieCompactionPlan
   * @throws IOException when reading instant content fails
   */
  default HoodieCompactionPlan readCompactionPlan(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieCompactionPlan.class);
  }

  /**
   * Read and deserialize clean metadata from an instant.
   *
   * @param instant The instant containing the clean metadata
   * @return Deserialized HoodieCleanMetadata
   * @throws IOException when reading instant content fails
   */
  default HoodieCleanMetadata readCleanMetadata(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieCleanMetadata.class);
  }

  /**
   * Read and deserialize rollback plan from an instant.
   *
   * @param instant The instant containing the rollback plan
   * @return Deserialized HoodieRollbackPlan
   * @throws IOException when reading instant content fails
   */
  default HoodieRollbackPlan readRollbackPlan(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieRollbackPlan.class);
  }

  /**
   * Read and deserialize rollback metadata from an instant.
   *
   * @param instant The instant containing the rollback metadata
   * @return Deserialized HoodieRollbackMetadata
   * @throws IOException when reading instant content fails
   */
  default HoodieRollbackMetadata readRollbackMetadata(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieRollbackMetadata.class);
  }

  /**
   * Read and deserialize restore plan from an instant.
   *
   * @param instant The instant containing the restore plan
   * @return Deserialized HoodieRestorePlan
   * @throws IOException when reading instant content fails
   */
  default HoodieRestorePlan readRestorePlan(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieRestorePlan.class);
  }

  /**
   * Read and deserialize restore metadata from an instant.
   *
   * @param instant The instant containing the restore metadata
   * @return Deserialized HoodieRestoreMetadata
   * @throws IOException when reading instant content fails
   */
  default HoodieRestoreMetadata readRestoreMetadata(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieRestoreMetadata.class);
  }

  /**
   * Read and deserialize savepoint metadata from an instant.
   *
   * @param instant The instant containing the savepoint metadata
   * @return Deserialized HoodieSavepointMetadata
   * @throws IOException when reading instant content fails
   */
  default HoodieSavepointMetadata readSavepointMetadata(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieSavepointMetadata.class);
  }

  /**
   * Read and deserialize requested replace metadata from an instant.
   *
   * @param instant The instant containing the requested replace metadata
   * @return Deserialized HoodieRequestedReplaceMetadata
   * @throws IOException when reading instant content fails
   */
  default HoodieRequestedReplaceMetadata readRequestedReplaceMetadata(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieRequestedReplaceMetadata.class);
  }

  /**
   * Read and deserialize index plan from an instant.
   *
   * @param instant The instant containing the index plan
   * @return Deserialized HoodieIndexPlan
   * @throws IOException when reading instant content fails
   */
  default HoodieIndexPlan readIndexPlan(HoodieInstant instant) throws IOException {
    return readNonEmptyInstantContent(instant, HoodieIndexPlan.class);
  }

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
   * Filter this timeline to just include the in-flights excluding logcompaction instants.
   *
   * @return New instance of HoodieTimeline with just in-flights excluding compaction instants
   */
  HoodieTimeline filterPendingExcludingLogCompaction();

  /**
   * Filter this timeline to just include the in-flights excluding compaction and log compaction instants.
   *
   * @return New instance of HoodieTimeline with just in-flights excluding compaction and log compaction instants
   */
  HoodieTimeline filterPendingExcludingCompactionAndLogCompaction();

  /**
   * Filter this timeline to just include the completed instants.
   *
   * @return New instance of HoodieTimeline with just completed instants
   */
  HoodieTimeline filterCompletedInstants();

  default <T> Option<HoodieInstantWriter> getInstantWriter(Option<T> metadata) {
    if (metadata.isEmpty()) {
      return Option.empty();
    }
    TimelineLayout layout = TimelineLayout.fromVersion(getTimelineLayoutVersion());
    return layout.getCommitMetadataSerDe().getInstantWriter(metadata.get());
  }

  // TODO: Check if logcompaction also needs to be included in this API.

  /**
   * Filter this timeline to just include the completed + compaction (inflight + requested) instants A RT filesystem
   * view is constructed with this timeline so that file-slice after pending compaction-requested instant-time is also
   * considered valid. A RT file-system view for reading must then merge the file-slices before and after pending
   * compaction instant so that all delta-commits are read.
   *
   * @return New instance of HoodieTimeline with just completed instants
   */
  HoodieTimeline filterCompletedAndCompactionInstants();

  HoodieTimeline filterCompletedOrMajorOrMinorCompactionInstants();

  /**
   * Timeline to just include completed commits or all rewrites like compaction, logcompaction and replace actions.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline filterCompletedInstantsOrRewriteTimeline();

  /**
   * Timeline to just include commits (commit/deltacommit), compaction and replace actions.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline getWriteTimeline();

  /**
   * Timeline to just include commits (commit/deltacommit), compaction and replace actions that are completed and contiguous.
   * For example, if timeline is [C0.completed, C1.completed, C2.completed, C3.inflight, C4.completed].
   * Then, a timeline of [C0.completed, C1.completed, C2.completed] will be returned.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline getContiguousCompletedWriteTimeline();

  /**
   * Timeline to just include replace instants that have valid (commit/deltacommit) actions.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline getCompletedReplaceTimeline();

  /**
   * Filter this timeline to just include requested and inflight compaction instants.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline filterPendingCompactionTimeline();

  /**
   * Filter this timeline to just include requested and inflight log compaction instants.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline filterPendingLogCompactionTimeline();

  /**
   * Filter this timeline to just include requested and inflight from both major and minor compaction instants.
   *
   * @return {@link HoodieTimeline}
   */
  HoodieTimeline filterPendingMajorOrMinorCompactionTimeline();

  /**
   * Filter this timeline to just include requested and inflight replacecommit instants.
   */
  HoodieTimeline filterPendingReplaceTimeline();

  /**
   * Filter this timeline to just include requested and inflight cluster commit instants.
   */
  HoodieTimeline filterPendingClusteringTimeline();

  /**
   * Filter this timeline to just include requested and inflight cluster or replace commit instants.
   */
  HoodieTimeline filterPendingReplaceOrClusteringTimeline();

  /**
   * Filter this timeline to just include requested and inflight cluster, replace commit or compaction instants.
   */
  HoodieTimeline filterPendingReplaceClusteringAndCompactionTimeline();

  /**
   * Filter this timeline to include pending rollbacks.
   */
  HoodieTimeline filterPendingRollbackTimeline();

  /**
   * Filter this timeline for requested rollbacks.
   */
  HoodieTimeline filterRequestedRollbackTimeline();

  /**
   * Create a new Timeline with numCommits after startTs.
   */
  HoodieTimeline findInstantsAfterOrEquals(String commitTime, int numCommits);

  /**
   * Create a new Timeline with all the instants after startTs.
   */
  HoodieTimeline findInstantsAfterOrEquals(String commitTime);

  /**
   * Create a new Timeline with instants after startTs and before or on endTs.
   */
  HoodieTimeline findInstantsInRange(String startTs, String endTs);

  /**
   * Create a new Timeline with instants after or equals startTs and before or on endTs.
   */
  HoodieTimeline findInstantsInClosedRange(String startTs, String endTs);

  /**
   * `
   * Create a new Timeline with instants after startTs and before or on endTs
   * by state transition timestamp of actions.
   */
  HoodieTimeline findInstantsInRangeByCompletionTime(String startTs, String endTs);

  /**
   * Create new timeline with all instants that were modified after specified time.
   */
  HoodieTimeline findInstantsModifiedAfterByCompletionTime(String instantTime);

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
   * Finds the instant before specified time.
   */
  Option<HoodieInstant> findInstantBefore(String instantTime);

  /**
   * Create new timeline with all instants before or equals specified time.
   */
  HoodieTimeline findInstantsBeforeOrEquals(String instantTime);

  /**
   * Custom Filter of Instants.
   */
  HoodieTimeline filter(Predicate<HoodieInstant> filter);

  /**
   * Filter this timeline to just include requested and inflight index instants.
   */
  HoodieTimeline filterPendingIndexTimeline();

  /**
   * Filter this timeline to just include completed index instants.
   */
  HoodieTimeline filterCompletedIndexTimeline();

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
   * @return {@link String} timeline hash.
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
   * the first completed instant in the timeline
   */
  boolean containsOrBeforeTimelineStarts(String ts);

  /**
   * @return Get the stream of completed instants
   */
  Stream<HoodieInstant> getInstantsAsStream();

  /**
   * @return Get tht list of instants
   */
  List<HoodieInstant> getInstants();

  /**
   * First instant that matches the action and state
   * @param action
   * @param state
   * @return {@link Option<HoodieInstant>}
   */
  Option<HoodieInstant> firstInstant(String action, HoodieInstant.State state);

  /**
   * @return Get the stream of completed instants in reverse order TODO Change code references to getInstants() that
   * reverse the instants later on to use this method instead.
   */
  Stream<HoodieInstant> getReverseOrderedInstants();

  /**
   * Get the stream of instants in reverse order by completion time. Any incomplete instants are returned as the first elements of the stream.
   * @return a stream of sorted instants
   */
  Stream<HoodieInstant> getReverseOrderedInstantsByCompletionTime();

  /**
   * @return the latest completion time of the instants
   */
  Option<String> getLatestCompletionTime();

  /**
   * Get the stream of completed instants in order by completion timestamp of actions.
   */
  Stream<HoodieInstant> getInstantsOrderedByCompletionTime();

  /**
   * @return true if the passed in instant is before the first completed instant in the timeline
   */
  boolean isBeforeTimelineStarts(String ts);

  /**
   * @return true if the passed-in completion time is smaller than the minimum completion time in the timeline
   */
  boolean isBeforeTimelineStartsByCompletionTime(String completionTime);

  /**
   * First non-savepoint commit in the active data timeline. Examples:
   * 1. An active data timeline C1, C2, C3, C4, C5 returns C1.
   * 2. If archival is allowed beyond savepoint and let's say C1, C2, C4 have been archived
   * while C3, C5 have been savepointed, then for the data timeline
   * C3, C3_Savepoint, C5, C5_Savepoint, C6, C7 returns C6.
   */
  Option<HoodieInstant> getFirstNonSavepointCommit();

  /**
   * get the most recent cluster commit if present
   */
  Option<HoodieInstant> getLastClusteringInstant();

  /**
   * get the most recent pending cluster commit if present
   */
  Option<HoodieInstant> getLastPendingClusterInstant();

  /**
   * get the least recent pending cluster commit if present
   */
  Option<HoodieInstant> getFirstPendingClusterInstant();

  /**
   * return true if instant is a pending clustering commit, otherwise false
   */
  boolean isPendingClusteringInstant(String instantTime);

  /**
   * Read the instant content to an input stream.
   * @param instant the instant to fetch
   * @return stream option with content for instant. If the instant file is empty, return empty option.
   */
  InputStream getInstantContentStream(HoodieInstant instant);

  boolean isEmpty(HoodieInstant instant);

  /**
   * Get all instants (commits, delta commits) that produce new data, in the active timeline.
   */
  HoodieTimeline getCommitsTimeline();

  /**
   * Get all instants (commits, delta commits, replace, compaction) that produce new data or merge file, in the active timeline.
   */
  HoodieTimeline getCommitsAndCompactionTimeline();

  /**
   * Get all instants (commits, delta commits, compaction, clean, savepoint, rollback, replace commits, index) that result in actions,
   * in the active timeline.
   */
  HoodieTimeline getAllCommitsTimeline();

  /**
   * Get only pure commit and replace commits (inflight and completed) in the active timeline.
   */
  HoodieTimeline getCommitAndReplaceTimeline();

  /**
   * Get only pure commits (inflight and completed) in the active timeline.
   */
  HoodieTimeline getCommitTimeline();

  /**
   * Get only the delta commits (inflight and completed) in the active timeline.
   */
  HoodieTimeline getDeltaCommitTimeline();

  /**
   * Get a timeline of a specific set of actions. useful to create a merged timeline of multiple actions.
   *
   * @param actions actions allowed in the timeline
   */
  HoodieTimeline getTimelineOfActions(Set<String> actions);

  /**
   * Get only the cleaner action (inflight and completed) in the active timeline.
   */
  HoodieTimeline getCleanerTimeline();

  /**
   * Get only the rollback action (inflight and completed) in the active timeline.
   */
  HoodieTimeline getRollbackTimeline();

  /**
   * Get only the rollback and restore action (inflight and completed) in the active timeline.
   */
  HoodieTimeline getRollbackAndRestoreTimeline();

  /**
   * Get only the save point action (inflight and completed) in the active timeline.
   */
  HoodieTimeline getSavePointTimeline();

  /**
   * Get only the restore action (inflight and completed) in the active timeline.
   */
  HoodieTimeline getRestoreTimeline();

  /**
   * Merge this timeline with the given timeline.
   */
  HoodieTimeline mergeTimeline(HoodieTimeline timeline);

  /**
   * Set Instants directly.
   */
  void setInstants(List<HoodieInstant> instants);

  /**
   * Get layout version of this timeline
   * @return {@link TimelineLayoutVersion}
   */
  TimelineLayoutVersion getTimelineLayoutVersion();

  /**
   * Get instant reader
   * */
  HoodieInstantReader getInstantReader();
}
