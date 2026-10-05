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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * A frozen view of which commit instants are committed, taken from a commits timeline.
 *
 * <p>Answers the question log readers of tables before version 8 ask per log block: is the block's instant
 * committed, i.e. not inflight, and either completed or older than the start of the active timeline. It holds
 * only instant times, so it can be built once where the timeline is already loaded and shipped to readers.
 */
public final class CommittedInstants implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Set<String> completedInstants;
  private final Set<String> inflightInstants;
  // Requested time of the first non-savepoint completed instant; instants before it are treated as committed.
  private final String timelineStartInstant;

  private CommittedInstants(Set<String> completedInstants, Set<String> inflightInstants, String timelineStartInstant) {
    this.completedInstants = completedInstants;
    this.inflightInstants = inflightInstants;
    this.timelineStartInstant = timelineStartInstant;
  }

  /**
   * Creates the view from a commits timeline, with the same semantics as
   * {@code !commitsTimeline.filterInflights().containsInstant(t)
   * && commitsTimeline.filterCompletedInstants().containsOrBeforeTimelineStarts(t)}.
   */
  public static CommittedInstants fromCommitsTimeline(HoodieTimeline commitsTimeline) {
    HoodieTimeline completedTimeline = commitsTimeline.filterCompletedInstants();
    Option<HoodieInstant> timelineStart = completedTimeline.getFirstNonSavepointCommit();
    return new CommittedInstants(
        requestedTimes(completedTimeline),
        requestedTimes(commitsTimeline.filterInflights()),
        timelineStart.map(HoodieInstant::requestedTime).orElse(null));
  }

  /**
   * Creates the view from instant times, e.g. when it is transported as plain strings.
   */
  public static CommittedInstants of(List<String> completedInstants, List<String> inflightInstants, Option<String> timelineStartInstant) {
    return new CommittedInstants(new HashSet<>(completedInstants), new HashSet<>(inflightInstants), timelineStartInstant.orElse(null));
  }

  public boolean isCommitted(String instantTime) {
    return !contains(inflightInstants, instantTime)
        && (contains(completedInstants, instantTime)
        || (timelineStartInstant != null && compareTimestamps(instantTime, LESSER_THAN, timelineStartInstant)));
  }

  public Set<String> getCompletedInstants() {
    return Collections.unmodifiableSet(completedInstants);
  }

  public Set<String> getInflightInstants() {
    return Collections.unmodifiableSet(inflightInstants);
  }

  public Option<String> getTimelineStartInstant() {
    return Option.ofNullable(timelineStartInstant);
  }

  private static Set<String> requestedTimes(HoodieTimeline timeline) {
    return timeline.getInstantsAsStream().map(HoodieInstant::requestedTime).collect(Collectors.toCollection(HashSet::new));
  }

  /**
   * Same lookup as {@link HoodieTimeline#containsInstant(String)}, including the fallback for second-granularity
   * instant times that were extended with the default millisecond suffix.
   */
  private static boolean contains(Set<String> instants, String instantTime) {
    if (instants.contains(instantTime)) {
      return true;
    }
    if (instantTime.length() == HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH
        && instantTime.endsWith(HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT)) {
      return contains(instants, instantTime.substring(0, instantTime.length() - HoodieInstantTimeGenerator.DEFAULT_MILLIS_EXT.length()));
    }
    return false;
  }
}
