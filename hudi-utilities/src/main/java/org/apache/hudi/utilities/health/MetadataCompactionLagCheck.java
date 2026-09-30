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

package org.apache.hudi.utilities.health;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * Checks whether compaction on the metadata table is keeping pace with the delta commits landing
 * on it.
 *
 * <p>The metadata table is a Merge-on-Read table of its own under {@code .hoodie/metadata}, and
 * every data-table write adds a delta commit to it. Its compaction is not a separately scheduled
 * service: the data-table writer runs it inline, after its own commit, when the configured number
 * of delta commits has accumulated. When that compaction stops running, every metadata-table read
 * has to merge an ever-growing chain of log files, and the data table's own archival -- which
 * cannot move past the last metadata-table compaction -- stalls with it.
 *
 * <p>The most common reason it stops is not a failure but the rule the writer applies to pick the
 * compaction instant. To keep a compacted base file consistent with the data table, the writer
 * pins the compaction instant just below the earliest data-table instant that is still pending
 * but whose delta commit has already been applied to the metadata table. Once a compaction at
 * that pinned instant exists, every later attempt lands on the same instant and is skipped, until
 * the pending data-table instant completes or is rolled back. A long-running or abandoned
 * data-table instant therefore freezes metadata-table compaction, and it is the first thing to
 * look at.
 *
 * <p>The same slack factor as the data-table compaction check is applied before calling the table
 * unhealthy: compaction lands one or two writes after the trigger point, and the aim is to catch a
 * compaction that has stopped, not scheduling jitter.
 */
public class MetadataCompactionLagCheck implements HealthCheck {

  static final String NAME = "mdt-compaction";

  /**
   * How far past the configured trigger point delta commits may accumulate on the metadata table
   * before it is called unhealthy. Compaction is attempted after the write that crosses the
   * trigger, and one more write can land while a pending data-table instant briefly pins it.
   */
  static final double TRIGGER_SLACK_FACTOR = 2.0;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether compaction on the metadata table is keeping pace with its delta commits";
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) throws Exception {
    Option<String> missing = context.firstMissingConfig(
        HoodieMetadataConfig.ENABLE.key(), HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key());
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    boolean metadataEnabled = Boolean.parseBoolean(context.getProps().getString(
        HoodieMetadataConfig.ENABLE.key(), String.valueOf(HoodieMetadataConfig.ENABLE.defaultValue())));
    if (!metadataEnabled) {
      return HealthCheckResult.skipped(NAME, "Metadata table is disabled for this writer ("
          + HoodieMetadataConfig.ENABLE.key() + "=false), so there is no metadata table to compact");
    }

    HoodieTableMetaClient dataMetaClient = context.getMetaClient();
    Option<String> noMetadataTable = MetadataTableSyncCheck.MetadataTablePresence.reasonAbsent(dataMetaClient);
    if (noMetadataTable.isPresent()) {
      return HealthCheckResult.skipped(NAME, noMetadataTable.get());
    }

    int maxDeltaCommits = Integer.parseInt(context.getProps().getString(
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(),
        String.valueOf(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.defaultValue())));
    int threshold = (int) Math.ceil(TRIGGER_SLACK_FACTOR * maxDeltaCommits);

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieMetadataConfig.ENABLE.key(), true)
        .config(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(), maxDeltaCommits)
        .config("effective.unhealthy.threshold.delta.commits", threshold);

    HoodieActiveTimeline metadataTimeline = metadataTableMetaClient(dataMetaClient).getActiveTimeline();
    Option<Pair<HoodieTimeline, HoodieInstant>> sinceLastCompaction =
        CompactionUtils.getCompletedDeltaCommitsSinceLatestCompaction(metadataTimeline);
    if (!sinceLastCompaction.isPresent()) {
      return result.status(HealthStatus.HEALTHY)
          .summary("No delta commits have landed on the metadata table yet; nothing for compaction to fall behind on")
          .build();
    }

    // When no compaction has completed, the instant returned is the first delta commit, not a compaction.
    Option<HoodieInstant> lastCompaction = metadataTimeline.getCommitTimeline().filterCompletedInstants().lastInstant();
    HoodieTimeline deltaCommitsSince = sinceLastCompaction.get().getLeft();
    String lagStart = lagStartTime(lastCompaction, deltaCommitsSince);
    Option<HoodieInstant> latestDeltaCommit = deltaCommitsSince.lastInstant();
    int deltaCommitsSinceCompaction = deltaCommitsSince.countInstants();
    Option<Long> lagMinutes = latestDeltaCommit.flatMap(latest -> minutesBetween(lagStart, latest.requestedTime()));
    int pendingMetadataCompactions = metadataTimeline.filterPendingCompactionTimeline().countInstants();
    Option<HoodieInstant> earliestPendingDataInstant = dataMetaClient.getActiveTimeline().filterInflightsAndRequested().firstInstant();

    result.config("observed.mdt.delta.commits.since.compaction", deltaCommitsSinceCompaction)
        .config("observed.last.mdt.compaction.instant", lastCompaction.map(HoodieInstant::requestedTime).orElse("none"))
        .config("observed.latest.mdt.delta.commit.instant", latestDeltaCommit.map(HoodieInstant::requestedTime).orElse("none"))
        .config("observed.lag.duration.minutes", lagMinutes.map(String::valueOf).orElse("unknown"))
        .config("observed.pending.mdt.compactions", pendingMetadataCompactions)
        .config("observed.data.table.earliest.pending.instant", earliestPendingDataInstant.map(HoodieInstant::requestedTime).orElse("none"));

    String since = lastCompaction.isPresent()
        ? "compaction at " + lastCompaction.get().requestedTime()
        : "the metadata table's first delta commit at " + lagStart;
    String duration = lagMinutes.map(m -> String.format(" over roughly %d minute(s)", m)).orElse("");

    if (deltaCommitsSinceCompaction <= threshold) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format("%d metadata-table delta commit(s) since %s, within the threshold of %d",
              deltaCommitsSinceCompaction, since, threshold))
          .build();
    }

    result.status(HealthStatus.UNHEALTHY)
        .summary(String.format("%d metadata-table delta commit(s) have accumulated since %s%s, past the threshold of %d",
            deltaCommitsSinceCompaction, since, duration, threshold))
        .finding(String.format(
            "The writer is configured to compact the metadata table every %d delta commit(s); %d have accumulated%s. "
                + "Every metadata-table read merges that many log files, and data-table archival cannot advance past "
                + "the last metadata-table compaction.",
            maxDeltaCommits, deltaCommitsSinceCompaction, duration));

    if (earliestPendingDataInstant.isPresent()) {
      result.finding(String.format(
          "Data-table instant %s (%s) is pending. Metadata-table compaction runs inline with the data-table writer "
              + "and pins its instant just below the earliest pending data-table instant already applied to the "
              + "metadata table; once a compaction at that pinned instant exists, further attempts are skipped "
              + "until the pending instant completes or is rolled back. Check that instant first: if it belongs to "
              + "a writer that is gone, rolling it back unblocks compaction ('timeline show incomplete' in hudi-cli "
              + "lists it).",
          earliestPendingDataInstant.get().requestedTime(), earliestPendingDataInstant.get().getAction()));
    } else {
      result.finding("No data-table instant is pending, so nothing is pinning the compaction instant. Confirm the "
          + "writer runs metadata-table table services after its commits, and look in the writer logs for "
          + "'Error in scheduling and executing compaction in metadata table'.");
    }

    if (pendingMetadataCompactions > 0) {
      result.finding(String.format(
          "%d compaction(s) are scheduled on the metadata table but not complete ('metadata timeline show incomplete' "
              + "in hudi-cli lists them). The writer re-runs pending metadata-table compactions before its next "
              + "write; repeated failures there point at the writer logs.",
          pendingMetadataCompactions));
    }

    return result.finding("Do not delete or rebuild the metadata table for this condition; it recovers on its own "
        + "once compaction runs.").build();
  }

  private HoodieTableMetaClient metadataTableMetaClient(HoodieTableMetaClient dataMetaClient) {
    StoragePath metadataTablePath = HoodieTableMetadata.getMetadataTableBasePath(dataMetaClient.getBasePath());
    return HoodieTableMetaClient.builder()
        .setConf(dataMetaClient.getStorageConf().newInstance())
        .setBasePath(metadataTablePath)
        .setLoadActiveTimelineOnLoad(true)
        .build();
  }

  /**
   * Where the lag is measured from: the last compaction, or, when there has never been one, the
   * first delta commit whose time is a real timestamp. The metadata table's bootstrap delta commit
   * carries a zero placeholder instant that would otherwise make every never-compacted table look
   * infinitely behind.
   */
  private static String lagStartTime(Option<HoodieInstant> lastCompaction, HoodieTimeline deltaCommitsSince) {
    if (lastCompaction.isPresent()) {
      return lastCompaction.get().requestedTime();
    }
    return deltaCommitsSince.getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .filter(time -> TimelineUtils.parseDateFromInstantTimeSafely(time).isPresent())
        .findFirst()
        .orElse(HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP);
  }

  private static Option<Long> minutesBetween(String fromInstant, String toInstant) {
    Option<Date> from = TimelineUtils.parseDateFromInstantTimeSafely(fromInstant);
    Option<Date> to = TimelineUtils.parseDateFromInstantTimeSafely(toInstant);
    if (!from.isPresent() || !to.isPresent()) {
      return Option.empty();
    }
    return Option.of(TimeUnit.MILLISECONDS.toMinutes(to.get().getTime() - from.get().getTime()));
  }
}
