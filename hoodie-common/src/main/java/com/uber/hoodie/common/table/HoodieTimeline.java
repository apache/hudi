/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table;

import com.uber.hoodie.common.table.timeline.HoodieDefaultTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;

import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * HoodieTimeline is a view of meta-data instants in the hoodie dataset.
 * Instants are specific points in time represented as HoodieInstant.
 * <p>
 * Timelines are immutable once created and operations create new instance of
 * timelines which filter on the instants and this can be chained.
 *
 * @see com.uber.hoodie.common.table.HoodieTableMetaClient
 * @see HoodieDefaultTimeline
 * @see HoodieInstant
 * @since 0.3.0
 */
public interface HoodieTimeline extends Serializable {
    String COMMIT_ACTION = "commit";
    String DELTA_COMMIT_ACTION = "deltacommit";
    String CLEAN_ACTION = "clean";
    String SAVEPOINT_ACTION = "savepoint";
    String COMPACTION_ACTION = "compaction";
    String INFLIGHT_EXTENSION = ".inflight";

    String COMMIT_EXTENSION = "." + COMMIT_ACTION;
    String DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION;
    String CLEAN_EXTENSION = "." + CLEAN_ACTION;
    String SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION;
    String COMPACTION_EXTENSION = "." + COMPACTION_ACTION;
    //this is to preserve backwards compatibility on commit in-flight filenames
    String INFLIGHT_COMMIT_EXTENSION = INFLIGHT_EXTENSION;
    String INFLIGHT_DELTA_COMMIT_EXTENSION = "." + DELTA_COMMIT_ACTION + INFLIGHT_EXTENSION;
    String INFLIGHT_CLEAN_EXTENSION = "." + CLEAN_ACTION + INFLIGHT_EXTENSION;
    String INFLIGHT_SAVEPOINT_EXTENSION = "." + SAVEPOINT_ACTION + INFLIGHT_EXTENSION;
    String INFLIGHT_COMPACTION_EXTENSION = "." + COMPACTION_ACTION + INFLIGHT_EXTENSION;

    /**
     * Filter this timeline to just include the in-flights
     *
     * @return New instance of HoodieTimeline with just in-flights
     */
    HoodieTimeline filterInflights();

    /**
     * Filter this timeline to just include the completed instants
     *
     * @return New instance of HoodieTimeline with just completed instants
     */
    HoodieTimeline filterCompletedInstants();


    /**
     * Create a new Timeline with instants after startTs and before or on endTs
     *
     * @param startTs
     * @param endTs
     */
    HoodieTimeline findInstantsInRange(String startTs, String endTs);

    /**
     * Create a new Timeline with all the instants after startTs
     *
     * @param commitTime
     * @param numCommits
     */
    HoodieTimeline findInstantsAfter(String commitTime, int numCommits);

    /**
     * If the timeline has any instants
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
    Optional<HoodieInstant> firstInstant();

    /**
     * @param n
     * @return nth completed instant from the first completed instant
     */
    Optional<HoodieInstant> nthInstant(int n);

    /**
     * @return last completed instant if available
     */
    Optional<HoodieInstant> lastInstant();

    /**
     * @param n
     * @return nth completed instant going back from the last completed instant
     */
    Optional<HoodieInstant> nthFromLastInstant(int n);

    /**
     * @return true if the passed instant is present as a completed instant on the timeline
     */
    boolean containsInstant(HoodieInstant instant);

    /**
     * @return true if the passed instant is present as a completed instant on the timeline or
     * if the instant is before the first completed instant in the timeline
     */
    boolean containsOrBeforeTimelineStarts(String ts);

    /**
     * @return Get the stream of completed instants
     */
    Stream<HoodieInstant> getInstants();

    /**
     * @return true if the passed in instant is before the first completed instant in the timeline
     */
    boolean isBeforeTimelineStarts(String ts);

    /**
     * Read the completed instant details
     *
     * @param instant
     * @return
     */
    Optional<byte[]> getInstantDetails(HoodieInstant instant);

    /**
     * Helper methods to compare instants
     **/
    BiPredicate<String, String> GREATER_OR_EQUAL =
        (commit1, commit2) -> commit1.compareTo(commit2) >= 0;
    BiPredicate<String, String> GREATER = (commit1, commit2) -> commit1.compareTo(commit2) > 0;
    BiPredicate<String, String> LESSER_OR_EQUAL =
        (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
    BiPredicate<String, String> LESSER = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

    default boolean compareTimestamps(String commit1, String commit2,
        BiPredicate<String, String> predicateToApply) {
        return predicateToApply.test(commit1, commit2);
    }

    static HoodieInstant getCompletedInstant(final HoodieInstant instant) {
        return new HoodieInstant(false, instant.getAction(), instant.getTimestamp());
    }


    static HoodieInstant getInflightInstant(final HoodieInstant instant) {
        return new HoodieInstant(true, instant.getAction(), instant.getTimestamp());
    }

    static String makeCommitFileName(String commitTime) {
        return commitTime + HoodieTimeline.COMMIT_EXTENSION;
    }

    static String makeInflightCommitFileName(String commitTime) {
        return commitTime + HoodieTimeline.INFLIGHT_COMMIT_EXTENSION;
    }

    static String makeCleanerFileName(String instant) {
        return instant + HoodieTimeline.CLEAN_EXTENSION;
    }

    static String makeInflightCleanerFileName(String instant) {
        return instant + HoodieTimeline.INFLIGHT_CLEAN_EXTENSION;
    }

    static String makeInflightSavePointFileName(String commitTime) {
        return commitTime + HoodieTimeline.INFLIGHT_SAVEPOINT_EXTENSION;
    }

    static String makeSavePointFileName(String commitTime) {
        return commitTime + HoodieTimeline.SAVEPOINT_EXTENSION;
    }

    static String makeInflightCompactionFileName(String commitTime) {
        return commitTime + HoodieTimeline.INFLIGHT_COMPACTION_EXTENSION;
    }

    static String makeCompactionFileName(String commitTime) {
        return commitTime + HoodieTimeline.COMPACTION_EXTENSION;
    }

    static String makeInflightDeltaFileName(String commitTime) {
        return commitTime + HoodieTimeline.INFLIGHT_DELTA_COMMIT_EXTENSION;
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
        return fileName + HoodieTimeline.INFLIGHT_EXTENSION;
    }


}
