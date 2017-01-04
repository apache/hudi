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

import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Stream;

/**
 * HoodieTimeline allows representation of meta-data events as a timeline.
 * Instants are specific points in time represented as strings.
 * in this format YYYYMMDDHHmmSS. e.g. 20170101193218
 * Any operation on the timeline starts with the inflight instant and then when complete marks
 * the completed instant and removes the inflight instant.
 * Completed instants are plainly referred to as just instants
 * <p>
 * Timelines as immutable once created. Any operation to change the timeline (like create/delete instants)
 * will not be reflected unless explicitly reloaded using the reload()
 *
 * @see com.uber.hoodie.common.table.HoodieTableMetaClient
 * @see HoodieDefaultTimeline
 * @since 0.3.0
 */
public interface HoodieTimeline extends Serializable {
    /**
     * Find all the completed instants after startTs and before or on endTs
     *
     * @param startTs
     * @param endTs
     * @return Stream of instants
     */
    Stream<String> findInstantsInRange(String startTs, String endTs);

    /**
     * Find all the completed instants after startTs
     *
     * @param commitTime
     * @param numCommits
     * @return Stream of instants
     */
    Stream<String> findInstantsAfter(String commitTime, int numCommits);

    /**
     * If the timeline has any completed instants
     *
     * @return true if timeline is not empty
     */
    boolean hasInstants();

    /**
     * If the timeline has any in-complete instants
     *
     * @return true if timeline has any in-complete instants
     */
    boolean hasInflightInstants();

    /**
     * @return total number of completed instants
     */
    int getTotalInstants();

    /**
     * @return first completed instant if available
     */
    Optional<String> firstInstant();

    /**
     * @param n
     * @return nth completed instant from the first completed instant
     */
    Optional<String> nthInstant(int n);

    /**
     * @return last completed instant if available
     */
    Optional<String> lastInstant();

    /**
     * @param n
     * @return nth completed instant going back from the last completed instant
     */
    Optional<String> nthFromLastInstant(int n);

    /**
     * @return true if the passed instant is present as a completed instant on the timeline
     */
    boolean containsInstant(String instant);

    /**
     * @return true if the passed instant is present as a completed instant on the timeline or
     * if the instant is before the first completed instant in the timeline
     */
    boolean containsOrBeforeTimelineStarts(String instant);

    /**
     * @return Get the stream of completed instants
     */
    Stream<String> getInstants();

    /**
     * @return Get the stream of in-flight instants
     */
    Stream<String> getInflightInstants();

    /**
     * @return true if the passed in instant is before the first completed instant in the timeline
     */
    boolean isInstantBeforeTimelineStarts(String instant);

    /**
     * Register the passed in instant as a in-flight
     *
     * @param instant
     */
    void saveInstantAsInflight(String instant);

    /**
     * Register the passed in instant as a completed instant.
     * It needs to have a corresponding in-flight instant, otherwise it will fail.
     * Pass a optional byte[] to save with the instant.
     *
     * @param instant
     * @param data
     */
    void saveInstantAsComplete(String instant, Optional<byte[]> data);

    /**
     * Un-Register a completed instant as in-flight. This is usually atomic way to
     * revert the effects of a operation on hoodie datasets
     *
     * @param instant
     */
    void revertInstantToInflight(String instant);

    /**
     * Remove the in-flight instant from the timeline
     *
     * @param instant
     */
    void removeInflightFromTimeline(String instant);

    /**
     * Reload the timeline. Timelines are immutable once created.
     *
     * @return
     * @throws IOException
     */
    HoodieTimeline reload() throws IOException;

    /**
     * Read the completed instant details
     *
     * @param instant
     * @return
     */
    Optional<byte[]> readInstantDetails(String instant);

    /**
     * Helper methods to compare instants
     **/
    BiPredicate<String, String> GREATER_OR_EQUAL =
        (commit1, commit2) -> commit1.compareTo(commit2) >= 0;
    BiPredicate<String, String> GREATER = (commit1, commit2) -> commit1.compareTo(commit2) > 0;
    BiPredicate<String, String> LESSER_OR_EQUAL =
        (commit1, commit2) -> commit1.compareTo(commit2) <= 0;
    BiPredicate<String, String> LESSER = (commit1, commit2) -> commit1.compareTo(commit2) < 0;

    default boolean compareInstants(String commit1, String commit2,
        BiPredicate<String, String> predicateToApply) {
        return predicateToApply.test(commit1, commit2);
    }
}
