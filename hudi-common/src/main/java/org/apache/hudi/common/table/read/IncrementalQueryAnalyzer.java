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
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Analyzer for incremental queries on the timeline, to filter instants based on specified ranges.
 *
 * <p>The analyzer is supplied the following information:
 * <ul>
 *   <li>The archived instants;</li>
 *   <li>The active instants;</li>
 *   <li>The instant filtering predicate, e.g the instant range with a "startTime" and "endTime"</li>
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
 *     <td>[earliest, endTime]</td>
 *     <td>The latest snapshot files from table metadata</td>
 *     <td>'_hoodie_commit_time' in setA, setA contains the begin instant times for actions completed before or on 'endTime'</td>
 *   </tr>
 *   <tr>
 *     <td>[-INF, +INF]</td>
 *     <td>The latest completed instant metadata</td>
 *     <td>'_hoodie_commit_time' = i_n, i_n is the latest completed instant</td>
 *   </tr>
 *   <tr>
 *     <td>[-INF, endTime]</td>
 *     <td>I) find the last completed instant i_n before or on 'endTime;
 *         II) read the latest snapshot from table metadata if i_n is archived or the commit metadata if it is still active</td>
 *     <td>'_hoodie_commit_time' = i_n</td>
 *   </tr>
 *   <tr>
 *     <td>[startTime, +INF]</td>
 *     <td>i).find the instant set setA, setA is a collection of all the instants completed after or on 'startTime';
 *     ii). read the latest snapshot from table metadata if setA has archived instants or the commit metadata if all the instants are still active</td>
 *     <td>'_hoodie_commit_time' in setA</td>
 *   </tr>
 *   <tr>
 *     <td>[earliest, endTime]</td>
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
public class IncrementalQueryAnalyzer {
  public static final String START_COMMIT_EARLIEST = "earliest";

  private final HoodieTableMetaClient metaClient;
  private final Option<String> startTime;
  private final Option<String> endTime;
  private final InstantRange.RangeType rangeType;
  private final boolean skipCompaction;
  private final boolean skipClustering;
  private final boolean skipInsertOverwrite;
  private final boolean readCdcFromChangelog;
  private final int limit;

  private IncrementalQueryAnalyzer(
      HoodieTableMetaClient metaClient,
      String startTime,
      String endTime,
      InstantRange.RangeType rangeType,
      boolean skipCompaction,
      boolean skipClustering,
      boolean skipInsertOverwrite,
      boolean readCdcFromChangelog,
      int limit) {
    this.metaClient = metaClient;
    this.startTime = Option.ofNullable(startTime);
    this.endTime = Option.ofNullable(endTime);
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
    try (CompletionTimeQueryView completionTimeQueryView = new CompletionTimeQueryView(this.metaClient)) {
      if (completionTimeQueryView.isEmptyTable()) {
        // no dataset committed in the table
        return QueryContext.EMPTY;
      }
      HoodieTimeline filteredTimeline = getFilteredTimeline(this.metaClient);
      List<String> instantTimeList = completionTimeQueryView.getStartTimes(filteredTimeline, startTime, endTime, rangeType);
      if (instantTimeList.isEmpty()) {
        // no instants completed within the give time range, returns early.
        return QueryContext.EMPTY;
      }
      // get hoodie instants
      Pair<List<String>, List<String>> splitInstantTime = splitInstantByActiveness(instantTimeList, completionTimeQueryView);
      Set<String> instantTimeSet = new HashSet<>(instantTimeList);
      List<String> archivedInstantTime = splitInstantTime.getLeft();
      List<String> activeInstantTime = splitInstantTime.getRight();
      List<HoodieInstant> archivedInstants = new ArrayList<>();
      List<HoodieInstant> activeInstants = new ArrayList<>();
      HoodieTimeline archivedReadTimeline = null;
      if (!activeInstantTime.isEmpty()) {
        activeInstants = filteredTimeline.getInstantsAsStream().filter(instant -> instantTimeSet.contains(instant.getRequestTime())).collect(Collectors.toList());
        if (limit > 0 && limit < activeInstants.size()) {
          // streaming read speed limit, limits the maximum number of commits allowed to read for each run
          activeInstants = activeInstants.subList(0, limit);
        }
      }
      if (!archivedInstantTime.isEmpty()) {
        archivedReadTimeline = getArchivedReadTimeline(metaClient, archivedInstantTime.get(0));
        archivedInstants = archivedReadTimeline.getInstantsAsStream().filter(instant -> instantTimeSet.contains(instant.getRequestTime())).collect(Collectors.toList());
      }
      List<String> instants = Stream.concat(archivedInstants.stream(), activeInstants.stream()).map(HoodieInstant::getRequestTime).collect(Collectors.toList());
      if (instants.isEmpty()) {
        // no instants completed within the give time range, returns early.
        return QueryContext.EMPTY;
      }
      if (startTime.isEmpty() && endTime.isPresent()) {
        instants = Collections.singletonList(instants.get(instants.size() - 1));
      }
      String lastInstant = instants.get(instants.size() - 1);
      // null => if starting from earliest, if no start time is specified, start from the latest instant like usual streaming read semantics.
      // if startTime is neither, then use the earliest instant as the start instant.
      String startInstant = START_COMMIT_EARLIEST.equalsIgnoreCase(startTime.orElse(null)) ? null : startTime.isEmpty() ? lastInstant : instants.get(0);
      String endInstant = endTime.isEmpty() ? null : lastInstant;
      return QueryContext.create(startInstant, endInstant, instants, archivedInstants, activeInstants, filteredTimeline, archivedReadTimeline);
    }
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
  public static class Builder {
    /**
     * Start completion time.
     */
    private String startTime;
    /**
     * End completion time.
     */
    private String endTime;
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

    public Builder() {
    }

    public Builder startTime(String startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder endTime(String endTime) {
      this.endTime = endTime;
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
      return new IncrementalQueryAnalyzer(Objects.requireNonNull(this.metaClient), this.startTime, this.endTime,
          Objects.requireNonNull(this.rangeType), this.skipCompaction, this.skipClustering, this.skipInsertOverwrite, this.readCdcFromChangelog, this.limit);
    }
  }

  /**
   * Represents the analyzed query context.
   */
  public static class QueryContext {
    public static final QueryContext EMPTY = new QueryContext(null, null, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), null, null);

    /**
     * An empty option indicates consumption from the earliest instant.
     */
    private final Option<String> startInstant;
    /**
     * An empty option indicates consumption to the latest instant.
     */
    private final Option<String> endInstant;
    private final List<HoodieInstant> archivedInstants;
    private final List<HoodieInstant> activeInstants;
    /**
     * The active timeline to read filtered by given configurations.
     */
    private final HoodieTimeline activeTimeline;
    /**
     * The archived timeline to read filtered by given configurations.
     */
    private final HoodieTimeline archivedTimeline;
    private final List<String> instants;

    private QueryContext(
        @Nullable String startInstant,
        @Nullable String endInstant,
        List<String> instants,
        List<HoodieInstant> archivedInstants,
        List<HoodieInstant> activeInstants,
        HoodieTimeline activeTimeline,
        @Nullable HoodieTimeline archivedTimeline) {
      this.startInstant = Option.ofNullable(startInstant);
      this.endInstant = Option.ofNullable(endInstant);
      this.archivedInstants = archivedInstants;
      this.activeInstants = activeInstants;
      this.activeTimeline = activeTimeline;
      this.archivedTimeline = archivedTimeline;
      this.instants = instants;
    }

    public static QueryContext create(
        @Nullable String startInstant,
        @Nullable String endInstant,
        List<String> instants,
        List<HoodieInstant> archivedInstants,
        List<HoodieInstant> activeInstants,
        HoodieTimeline activeTimeline,
        @Nullable HoodieTimeline archivedTimeline) {
      return new QueryContext(startInstant, endInstant, instants, archivedInstants, activeInstants, activeTimeline, archivedTimeline);
    }

    public boolean isEmpty() {
      return this.instants.isEmpty();
    }

    public Option<String> getStartInstant() {
      return startInstant;
    }

    public Option<String> getEndInstant() {
      return endInstant;
    }

    /**
     * Returns the latest instant time which should be included physically in reading.
     */
    public String getLastInstant() {
      return this.instants.get(this.instants.size() - 1);
    }

    public List<HoodieInstant> getArchivedInstants() {
      return archivedInstants;
    }

    public List<HoodieInstant> getActiveInstants() {
      return activeInstants;
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

    public HoodieTimeline getActiveTimeline() {
      return this.activeTimeline;
    }

    public @Nullable HoodieTimeline getArchivedTimeline() {
      return archivedTimeline;
    }
  }
}
