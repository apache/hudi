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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.table.timeline.CompletionTimeQueryView;
import org.apache.hudi.common.table.timeline.HoodieArchivedTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;
import static org.apache.hudi.common.table.timeline.InstantComparison.compareTimestamps;

/**
 * Analyzer for incremental queries on the timeline, to filter instants based on specified ranges
 * using optional start and end completion time (see the details below).
 *
 * <p>The analyzer is supplied the following information:
 * <ul>
 *   <li>The archived instants;</li>
 *   <li>The active instants;</li>
 *   <li>The instant filtering predicate, e.g the instant range with a "startCompletionTime" and "endCompletionTime"</li>
 *   <li>Whether the query starts from the "earliest" available instant;</li>
 *   <li>Whether the query ends to the "latest" available instant;</li>
 *   <li>The max completion time used for fs view file slice version filtering.</li>
 * </ul>
 *
 * <p><h2>Criteria for different query ranges:</h2>
 *
 * <table>
 *   <tr>
 *     <th>Query Range</th>
 *     <th>File selection criteria</th>
 *     <th>Instant filtering predicate applied to selected files</th>
 *   </tr>
 *   <tr>
 *     <td>[earliest, +INF]</td>
 *     <td>The latest snapshot files from table metadata</td>
 *     <td>_</td>
 *   </tr>
 *   <tr>
 *     <td>[earliest, endCompletionTime]</td>
 *     <td>The latest snapshot files from table metadata</td>
 *     <td>'_hoodie_commit_time' in setA, setA contains the instant times for actions completed before or on 'endCompletionTime'</td>
 *   </tr>
 *   <tr>
 *     <td>[-INF, +INF]</td>
 *     <td>The latest completed instant metadata</td>
 *     <td>'_hoodie_commit_time' = i_n, i_n is the latest completed instant</td>
 *   </tr>
 *   <tr>
 *     <td>[-INF, endCompletionTime]</td>
 *     <td>I) find the last completed instant i_n before or on 'endCompletionTime';
 *         II) read the latest snapshot from table metadata if i_n is archived or the commit metadata if it is still active</td>
 *     <td>'_hoodie_commit_time' = i_n</td>
 *   </tr>
 *   <tr>
 *     <td>[startCompletionTime, +INF]</td>
 *     <td>i).find the instant set setA, setA is a collection of all the instants completed after or on 'startCompletionTime';
 *     ii). read the latest snapshot from table metadata if setA has archived instants or the commit metadata if all the instants are still active</td>
 *     <td>'_hoodie_commit_time' in setA</td>
 *   </tr>
 *   <tr>
 *     <td>[earliest, endCompletionTime]</td>
 *     <td>i).find the instant set setA, setA is a collection of all the instants completed in the given time range;
 *     ii). read the latest snapshot from table metadata if setA has archived instants or the commit metadata if all the instants are still active</td>
 *     <td>'_hoodie_commit_time' in setA</td>
 *   </tr>
 * </table>
 *
 * <p> A {@code RangeType} is required for analyzing the query so that the query range boundary inclusiveness have clear semantics.
 *
 * <p>IMPORTANT: the reader may optionally choose to fall back to reading the latest snapshot if there are files decoding the commit metadata are already cleaned.
 */
@Slf4j
public class IncrementalQueryAnalyzer {

  public static final String START_COMMIT_EARLIEST = "earliest";

  private final HoodieTableMetaClient metaClient;
  @Getter
  private final Option<String> startCompletionTime;
  private final Option<String> endCompletionTime;
  private final InstantRange.RangeType rangeType;
  private final boolean skipCompaction;
  private final boolean skipClustering;
  private final boolean skipInsertOverwrite;
  private final boolean readCdcFromChangelog;
  private final int limit;

  private IncrementalQueryAnalyzer(
      HoodieTableMetaClient metaClient,
      String startCompletionTime,
      String endCompletionTime,
      InstantRange.RangeType rangeType,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean readCdcFromChangelog,
      int limit) {
    this.metaClient = metaClient;
    this.startCompletionTime = Option.ofNullable(startCompletionTime);
    this.endCompletionTime = Option.ofNullable(endCompletionTime);
    this.rangeType = rangeType;
    this.skipCompaction = skipCompaction;
    this.skipClustering = skipClustering;
    this.skipInsertOverwrite = skipInsertOverwrite;
    this.readCdcFromChangelog = readCdcFromChangelog;
    this.limit = limit;
  }

  /**
   * Returns a builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Analyzes the incremental query context with given completion time range.
   *
   * @return An incremental query context including the instant time range info.
   */
  public QueryContext analyze() {
    try (CompletionTimeQueryView completionTimeQueryView = metaClient.getTableFormat().getTimelineFactory().createCompletionTimeQueryView(this.metaClient)) {
      if (completionTimeQueryView.isEmptyTable()) {
        // no dataset committed in the table
        return QueryContext.EMPTY;
      }
      HoodieTimeline filteredTimeline = getFilteredTimeline(this.metaClient);
      if (TimelineLayoutVersion.LAYOUT_VERSION_1.equals(metaClient.getTimelineLayoutVersion())) {
        return analyzeForV1Timeline(filteredTimeline, completionTimeQueryView);
      }
      return analyzeForV2Timeline(filteredTimeline, completionTimeQueryView);
    } catch (Exception ex) {
      log.error("Got exception when generating incremental query info", ex);
      throw new HoodieException(ex);
    }
  }

  /**
   * Analyzes a layout V1 timeline by applying the completion-time range directly to requested time,
   * since V1 does not persist a separate completion time. This path filters active instants first
   * and loads the archived timeline at most once, only when the range may overlap it.
   */
  private QueryContext analyzeForV1Timeline(
      HoodieTimeline filteredTimeline,
      CompletionTimeQueryView completionTimeQueryView) {
    final boolean startFromEarliest = START_COMMIT_EARLIEST.equalsIgnoreCase(startCompletionTime.orElse(null));
    final Option<String> effectiveStart = startFromEarliest ? Option.empty() : startCompletionTime;
    final HoodieTimeline completedTimeline = filteredTimeline.filterCompletedInstants();

    if (effectiveStart.isEmpty() && endCompletionTime.isEmpty()) {
      // (_, _) reads the latest completed instant while [earliest, +INF] is a snapshot read.
      List<HoodieInstant> activeInstants = completedTimeline.lastInstant()
          .map(Collections::singletonList)
          .orElse(Collections.emptyList());
      return createQueryContext(Collections.emptyList(), activeInstants, filteredTimeline, null);
    }

    List<HoodieInstant> archivedInstants = Collections.emptyList();
    List<HoodieInstant> activeInstants;
    HoodieTimeline archivedReadTimeline = null;

    if (startCompletionTime.isEmpty() && endCompletionTime.isPresent()) {
      // (_, end] returns the last eligible instant at or before end. An old savepointed commit can
      // remain active before the archive boundary, so only use the active-only shortcut when the
      // selected active instant is on or after that boundary.
      activeInstants = getLastInstantAtOrBefore(completedTimeline, endCompletionTime.get());
      if (activeInstants.isEmpty()
          || completionTimeQueryView.isArchived(activeInstants.get(0).requestedTime())) {
        archivedReadTimeline = getArchivedReadTimeline(metaClient, "");
        archivedInstants = getLastInstantAtOrBefore(archivedReadTimeline, endCompletionTime.get());
        if (!archivedInstants.isEmpty()
            && (activeInstants.isEmpty()
                || compareTimestamps(
                    activeInstants.get(0).requestedTime(),
                    LESSER_THAN_OR_EQUALS,
                    archivedInstants.get(0).requestedTime()))) {
          activeInstants = Collections.emptyList();
        } else {
          archivedInstants = Collections.emptyList();
        }
      }
    } else {
      InstantRange instantRange = InstantRange.builder()
          .rangeType(rangeType)
          .startInstant(effectiveStart.orElse(null))
          .endInstant(endCompletionTime.orElse(null))
          .nullableBoundary(true)
          .build();
      activeInstants = getInstantsInRange(completedTimeline, instantRange);

      boolean needsArchivedInstants = startFromEarliest
          || (effectiveStart.isPresent() && completionTimeQueryView.isArchived(effectiveStart.get()));
      if (needsArchivedInstants) {
        archivedReadTimeline = getArchivedReadTimeline(metaClient, effectiveStart.orElse(""));
        archivedInstants = getInstantsInRange(archivedReadTimeline, instantRange);
      }
    }

    return createQueryContext(
        archivedInstants, activeInstants, filteredTimeline, archivedReadTimeline);
  }

  /**
   * Analyzes a layout V2 timeline through {@link CompletionTimeQueryView}, since completion time can
   * differ from requested time. The query view resolves requested-time candidates before they are
   * materialized from the active and archived timelines.
   */
  private QueryContext analyzeForV2Timeline(
      HoodieTimeline filteredTimeline,
      CompletionTimeQueryView completionTimeQueryView) {
    List<String> instantTimeList = completionTimeQueryView.getInstantTimes(
        filteredTimeline, startCompletionTime, endCompletionTime, rangeType);
    if (instantTimeList.isEmpty()) {
      // no instants completed within the given time range, returns early.
      return QueryContext.EMPTY;
    }

    Pair<List<String>, List<String>> splitInstantTime = splitInstantByActiveness(
        instantTimeList, completionTimeQueryView);
    Set<String> instantTimeSet = new HashSet<>(instantTimeList);
    List<String> archivedInstantTimes = splitInstantTime.getLeft();
    List<String> activeInstantTimes = splitInstantTime.getRight();
    List<HoodieInstant> archivedInstants = new ArrayList<>();
    List<HoodieInstant> activeInstants = new ArrayList<>();
    HoodieTimeline archivedReadTimeline = null;
    if (!activeInstantTimes.isEmpty()) {
      activeInstants = filteredTimeline.getInstantsAsStream()
          .filter(instant -> instantTimeSet.contains(instant.requestedTime()))
          .collect(Collectors.toList());
    }
    if (!archivedInstantTimes.isEmpty()) {
      archivedReadTimeline = getArchivedReadTimeline(metaClient, archivedInstantTimes.get(0));
      archivedInstants = archivedReadTimeline.getInstantsAsStream()
          .filter(instant -> instantTimeSet.contains(instant.requestedTime()))
          .collect(Collectors.toList());
    }
    return createQueryContext(
        archivedInstants, activeInstants, filteredTimeline, archivedReadTimeline);
  }

  private QueryContext createQueryContext(
      List<HoodieInstant> archivedInstants,
      List<HoodieInstant> activeInstants,
      HoodieTimeline filteredTimeline,
      @Nullable HoodieTimeline archivedReadTimeline) {
    if (limit > 0 && limit < activeInstants.size()) {
      // streaming read speed limit, limits the maximum number of active commits allowed per run
      activeInstants = activeInstants.subList(0, limit);
    }
    List<String> instants = Stream.concat(archivedInstants.stream(), activeInstants.stream())
        .map(HoodieInstant::requestedTime)
        .distinct()
        .sorted()
        .collect(Collectors.toList());
    if (instants.isEmpty()) {
      // no instants completed within the given time range, returns early.
      return QueryContext.EMPTY;
    }
    if (startCompletionTime.isEmpty() && endCompletionTime.isPresent()) {
      instants = Collections.singletonList(instants.get(instants.size() - 1));
    }
    String lastInstant = instants.get(instants.size() - 1);
    // null => if starting from earliest. If no startCompletionTime is specified, start from the
    // latest instant like usual streaming read semantics. Otherwise use the earliest selected instant.
    String startInstant = START_COMMIT_EARLIEST.equalsIgnoreCase(startCompletionTime.orElse(null)) ? null :
        startCompletionTime.isEmpty() ? lastInstant : instants.get(0);
    String endInstant = endCompletionTime.isEmpty() ? null : lastInstant;
    return QueryContext.create(
        startInstant, endInstant, instants, archivedInstants, activeInstants,
        filteredTimeline, archivedReadTimeline);
  }

  private static List<HoodieInstant> getInstantsInRange(
      HoodieTimeline timeline, InstantRange instantRange) {
    return timeline.getInstantsAsStream()
        .filter(instant -> instantRange.isInRange(instant.requestedTime()))
        .sorted(Comparator.comparing(HoodieInstant::requestedTime))
        .collect(Collectors.toList());
  }

  private static List<HoodieInstant> getLastInstantAtOrBefore(
      HoodieTimeline timeline, String endInstant) {
    return timeline.getInstantsAsStream()
        .filter(instant -> compareTimestamps(
            instant.requestedTime(), LESSER_THAN_OR_EQUALS, endInstant))
        .max(Comparator.comparing(HoodieInstant::requestedTime))
        .map(Collections::singletonList)
        .orElse(Collections.emptyList());
  }

  /**
   * Splits the given instant time list into a pair of archived instant list and active instant list.
   */
  private static Pair<List<String>, List<String>> splitInstantByActiveness(List<String> instantTimeList, CompletionTimeQueryView completionTimeQueryView) {
    int firstActiveIdx = IntStream.range(0, instantTimeList.size()).filter(i -> !completionTimeQueryView.isArchived(instantTimeList.get(i))).findFirst().orElse(-1);
    if (firstActiveIdx == -1) {
      return Pair.of(instantTimeList, Collections.emptyList());
    } else if (firstActiveIdx == 0) {
      return Pair.of(Collections.emptyList(), instantTimeList);
    } else {
      return Pair.of(instantTimeList.subList(0, firstActiveIdx), instantTimeList.subList(firstActiveIdx, instantTimeList.size()));
    }
  }

  private HoodieTimeline getFilteredTimeline(HoodieTableMetaClient metaClient) {
    HoodieTimeline timeline = metaClient.getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants();
    return filterInstantsAsPerUserConfigs(metaClient, timeline, this.skipCompaction, this.skipClustering, this.skipInsertOverwrite, this.readCdcFromChangelog);
  }

  private HoodieTimeline getArchivedReadTimeline(HoodieTableMetaClient metaClient, String startInstant) {
    HoodieArchivedTimeline archivedTimeline = metaClient.getArchivedTimeline(startInstant, false);
    HoodieTimeline archivedCompleteTimeline = archivedTimeline.getCommitsTimeline().filterCompletedInstants();
    return filterInstantsAsPerUserConfigs(metaClient, archivedCompleteTimeline, this.skipCompaction, this.skipClustering, this.skipInsertOverwrite, this.readCdcFromChangelog);
  }

  @VisibleForTesting
  public static HoodieTimeline filterInstantsAsPerUserConfigs(
      HoodieTableMetaClient metaClient,
      HoodieTimeline timeline,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite) {
    return filterInstantsAsPerUserConfigs(metaClient, timeline, skipCompaction, skipClustering, skipInsertOverwrite, false);
  }

  /**
   * Filters out the unnecessary instants as per user specified configs.
   *
   * @param timeline The timeline.
   *
   * @return the filtered timeline
   */
  private static HoodieTimeline filterInstantsAsPerUserConfigs(
      HoodieTableMetaClient metaClient,
      HoodieTimeline timeline,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean readCdcFromChangelog) {
    final HoodieTimeline oriTimeline = timeline;
    if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ && skipCompaction) {
      // the compaction commit uses 'commit' as action which is tricky
      timeline = timeline.filter(instant -> !instant.getAction().equals(HoodieTimeline.COMMIT_ACTION));
    }
    if (metaClient.getTableType() == HoodieTableType.MERGE_ON_READ && metaClient.getTableConfig().isCDCEnabled() && readCdcFromChangelog) {
      // only compaction yields changelog file
      timeline = timeline.filter(instant -> !instant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));
    }
    if (skipClustering) {
      timeline = timeline.filter(instant -> !ClusteringUtils.isCompletedClusteringInstant(instant, oriTimeline));
    }
    if (skipInsertOverwrite) {
      timeline = timeline.filter(instant -> !ClusteringUtils.isInsertOverwriteInstant(instant, oriTimeline));
    }
    return timeline;
  }

  // -------------------------------------------------------------------------
  //  Inner Class
  // -------------------------------------------------------------------------
  /**
   * Builder for {@link IncrementalQueryAnalyzer}.
   */
  @NoArgsConstructor
  public static class Builder {
    /**
     * Start completion time.
     */
    private String startCompletionTime;
    /**
     * End completion time.
     */
    private String endCompletionTime;
    private InstantRange.RangeType rangeType;
    private HoodieTableMetaClient metaClient;
    private boolean skipCompaction = false;
    private boolean skipClustering = false;
    private boolean skipInsertOverwrite = false;
    private boolean readCdcFromChangelog = false;
    /**
     * Maximum number of instants to read per run.
     */
    private int limit = -1;

    public Builder startCompletionTime(String startCompletionTime) {
      this.startCompletionTime = startCompletionTime;
      return this;
    }

    public Builder endCompletionTime(String endCompletionTime) {
      this.endCompletionTime = endCompletionTime;
      return this;
    }

    public Builder rangeType(InstantRange.RangeType rangeType) {
      this.rangeType = rangeType;
      return this;
    }

    public Builder metaClient(HoodieTableMetaClient metaClient) {
      this.metaClient = metaClient;
      return this;
    }

    public Builder skipCompaction(boolean skipCompaction) {
      this.skipCompaction = skipCompaction;
      return this;
    }

    public Builder skipClustering(boolean skipClustering) {
      this.skipClustering = skipClustering;
      return this;
    }

    public Builder skipInsertOverwrite(boolean skipInsertOverwrite) {
      this.skipInsertOverwrite = skipInsertOverwrite;
      return this;
    }

    public Builder readCdcFromChangelog(boolean readCdcFromChangelog) {
      this.readCdcFromChangelog = readCdcFromChangelog;
      return this;
    }

    public Builder limit(int limit) {
      this.limit = limit;
      return this;
    }

    public IncrementalQueryAnalyzer build() {
      return new IncrementalQueryAnalyzer(Objects.requireNonNull(this.metaClient), this.startCompletionTime, this.endCompletionTime,
          Objects.requireNonNull(this.rangeType), this.skipCompaction, this.skipClustering, this.skipInsertOverwrite, this.readCdcFromChangelog, this.limit);
    }
  }

  /**
   * Represents the analyzed query context.
   */
  @AllArgsConstructor(access = AccessLevel.PRIVATE)
  @Getter
  public static class QueryContext {

    public static final QueryContext EMPTY =
        new QueryContext(Option.empty(), Option.empty(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null);

    /**
     * An empty option indicates consumption from the earliest instant.
     */
    private final Option<String> startInstant;
    /**
     * An empty option indicates consumption to the latest instant.
     */
    private final Option<String> endInstant;
    @Getter(AccessLevel.NONE)
    private final List<String> instants;
    private final List<HoodieInstant> archivedInstants;
    private final List<HoodieInstant> activeInstants;
    /**
     * The active timeline to read filtered by given configurations.
     */
    private final HoodieTimeline activeTimeline;
    /**
     * The archived timeline to read filtered by given configurations.
     */
    @Nullable
    @Getter(AccessLevel.NONE)
    private final HoodieTimeline archivedTimeline;

    public static QueryContext create(
        @Nullable String startInstant,
        @Nullable String endInstant,
        List<String> instants,
        List<HoodieInstant> archivedInstants,
        List<HoodieInstant> activeInstants,
        HoodieTimeline activeTimeline,
        @Nullable HoodieTimeline archivedTimeline) {
      return new QueryContext(Option.ofNullable(startInstant), Option.ofNullable(endInstant), instants, archivedInstants, activeInstants, activeTimeline, archivedTimeline);
    }

    public boolean isEmpty() {
      return this.instants.isEmpty();
    }

    public List<String> getInstantTimeList() {
      return this.instants;
    }

    /**
     * Returns the latest instant time which should be included physically in reading.
     */
    public String getLastInstant() {
      ValidationUtils.checkState(!this.instants.isEmpty());
      return this.instants.get(this.instants.size() - 1);
    }

    public List<HoodieInstant> getInstants() {
      return Stream.concat(archivedInstants.stream(), activeInstants.stream()).collect(Collectors.toList());
    }

    public boolean isConsumingFromEarliest() {
      return startInstant.isEmpty();
    }

    public boolean isConsumingToLatest() {
      return endInstant.isEmpty();
    }

    public String getMaxCompletionTime() {
      if (this.activeInstants.size() > 0) {
        return this.activeInstants.stream().map(HoodieInstant::getCompletionTime).filter(Objects::nonNull).max(String::compareTo).get();
      } else {
        // all the query instants are archived, use the latest active instant completion time as
        // the file slice version upper threshold, because very probably these files already got cleaned,
        // use the max completion time of the archived instants could yield empty file slices.
        return this.activeTimeline.getInstantsAsStream().map(HoodieInstant::getCompletionTime).filter(Objects::nonNull).max(String::compareTo).get();
      }
    }

    public Option<InstantRange> getInstantRange() {
      if (isConsumingFromEarliest()) {
        if (isConsumingToLatest()) {
          // A null instant range indicates no filtering.
          // short-cut for snapshot read
          return Option.empty();
        }
        return Option.of(InstantRange.builder()
            .startInstant(startInstant.orElse(null))
            .endInstant(endInstant.orElse(null))
            .rangeType(InstantRange.RangeType.CLOSED_CLOSED)
            .nullableBoundary(true)
            .build());
      } else {
        return Option.of(InstantRange.builder()
                .rangeType(InstantRange.RangeType.EXACT_MATCH)
                .explicitInstants(new HashSet<>(instants))
                .build());
      }
    }

    public @Nullable HoodieTimeline getArchivedTimeline() {
      return archivedTimeline;
    }
  }
}
