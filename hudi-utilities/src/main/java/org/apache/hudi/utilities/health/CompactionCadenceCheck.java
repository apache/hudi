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

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.table.action.compact.CompactionTriggerStrategy;

import java.sql.Date;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

/**
 * Checks whether compaction is keeping pace with ingestion on a Merge-on-Read table.
 *
 * <p>The question is not "has compaction run recently" -- a quiet table legitimately has no recent
 * compaction. It is whether delta commits have accumulated well past the point where the configured
 * trigger should have fired. So the check computes how many delta commits the writer's trigger
 * strategy says should elapse before compaction is scheduled, then compares that against how many
 * have actually accumulated since the last completed compaction.
 *
 * <p>A slack factor is applied before calling the table unhealthy. Compaction scheduling and
 * compaction execution are separate: a plan can be scheduled promptly while the execution strategy
 * defers individual file slices, and async compaction can trail ingestion by a cycle or two without
 * anything being wrong. Alerting at exactly the trigger threshold would fire on every healthy MOR
 * table. The factor below is deliberately generous -- this check is meant to catch compaction that
 * has stopped or fallen badly behind, not to police scheduling jitter.
 */
public class CompactionCadenceCheck implements HealthCheck {

  static final String NAME = "compaction";

  /**
   * How far past the configured trigger point delta commits may accumulate before the table is
   * called unhealthy. Absorbs the gap between scheduling and execution, and async table services
   * running on their own cadence.
   */
  static final double TRIGGER_SLACK_FACTOR = 2.0;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether compaction is keeping pace with delta commits on a Merge-on-Read table";
  }

  @Override
  public boolean appliesTo(HealthCheckContext context) {
    return context.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ;
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) {
    if (!appliesTo(context)) {
      return HealthCheckResult.skipped(NAME, "Table is Copy-on-Write, which has no compaction");
    }

    Option<String> missing = context.firstMissingConfig(
        HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(),
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key());
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    CompactionTriggerStrategy strategy = CompactionTriggerStrategy.valueOf(
        context.getProps().getString(
            HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(),
            HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.defaultValue()));
    int maxDeltaCommits = Integer.parseInt(context.getProps().getString(
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(),
        HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.defaultValue()));
    int maxDeltaSeconds = Integer.parseInt(context.getProps().getString(
        HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS.key(),
        HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS.defaultValue()));

    HoodieActiveTimeline activeTimeline = context.getMetaClient().getActiveTimeline();
    int expectedBeforeTrigger = expectedDeltaCommitsBeforeTrigger(
        strategy, maxDeltaCommits, maxDeltaSeconds, activeTimeline);
    int threshold = (int) Math.ceil(TRIGGER_SLACK_FACTOR * expectedBeforeTrigger);

    Option<Pair<HoodieTimeline, HoodieInstant>> sinceLastCompaction =
        CompactionUtils.getCompletedDeltaCommitsSinceLatestCompaction(activeTimeline);

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieCompactionConfig.INLINE_COMPACT_TRIGGER_STRATEGY.key(), strategy)
        .config(HoodieCompactionConfig.INLINE_COMPACT_NUM_DELTA_COMMITS.key(), maxDeltaCommits)
        .config(HoodieCompactionConfig.INLINE_COMPACT_TIME_DELTA_SECONDS.key(), maxDeltaSeconds)
        .config("effective.unhealthy.threshold.delta.commits", threshold);

    if (!sinceLastCompaction.isPresent()) {
      return result.status(HealthStatus.HEALTHY)
          .summary("No delta commits have landed yet; nothing for compaction to fall behind on")
          .build();
    }

    int pendingDeltaCommits = sinceLastCompaction.get().getLeft().countInstants();
    int pendingCompactions = CompactionUtils.getPendingCompactionInstantTimes(context.getMetaClient()).size();
    HoodieInstant lastCompaction = sinceLastCompaction.get().getRight();

    result.config("observed.delta.commits.since.last.compaction", pendingDeltaCommits)
        .config("observed.pending.compactions", pendingCompactions)
        .config("observed.last.compaction.instant", lastCompaction.requestedTime());

    if (pendingDeltaCommits <= threshold) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format(
              "%d delta commit(s) since compaction at %s, within the threshold of %d",
              pendingDeltaCommits, lastCompaction.requestedTime(), threshold))
          .build();
    }

    result.status(HealthStatus.UNHEALTHY)
        .summary(String.format(
            "%d delta commit(s) have accumulated since compaction at %s, past the threshold of %d",
            pendingDeltaCommits, lastCompaction.requestedTime(), threshold))
        .finding(String.format(
            "Trigger strategy %s expects compaction roughly every %d delta commit(s); %d have accumulated.",
            strategy, expectedBeforeTrigger, pendingDeltaCommits));

    if (pendingCompactions > 0) {
      result.finding(String.format(
          "%d compaction(s) are already scheduled but not complete -- compaction is being scheduled "
              + "and not executed. Check that an async compactor is running and not failing.",
          pendingCompactions));
    } else {
      result.finding("No compaction is scheduled. Check that compaction is enabled for this table "
          + "and that the scheduling job is running.");
    }

    return result.build();
  }

  /**
   * Number of delta commits the configured trigger strategy allows before compaction should be
   * scheduled. For the time-based strategies this is expressed in delta commits too, by counting
   * how many have landed inside the configured time window, so that the caller has a single unit
   * to compare against.
   */
  private int expectedDeltaCommitsBeforeTrigger(CompactionTriggerStrategy strategy,
                                                int maxDeltaCommits,
                                                int maxDeltaSeconds,
                                                HoodieActiveTimeline activeTimeline) {
    HoodieTimeline completedDeltaCommits = activeTimeline.getDeltaCommitTimeline().filterCompletedInstants();
    String windowStart = TimelineUtils.formatDate(
        Date.from(Instant.now().minus(maxDeltaSeconds, ChronoUnit.SECONDS)));
    int deltaCommitsInTimeWindow = completedDeltaCommits.findInstantsAfter(windowStart).countInstants();

    switch (strategy) {
      case NUM_COMMITS:
        return maxDeltaCommits;

      case NUM_COMMITS_AFTER_LAST_REQUEST:
        // The budget restarts from the last compaction *request*, so delta commits that landed
        // between the last completed compaction and that request are already spoken for.
        Option<HoodieInstant> lastCompleted = activeTimeline.getCommitTimeline().filterCompletedInstants().lastInstant();
        Option<HoodieInstant> lastRequested = activeTimeline.filterPendingCompactionTimeline().lastInstant();
        if (lastCompleted.isPresent() && lastRequested.isPresent()) {
          int between = completedDeltaCommits
              .findInstantsInRange(lastCompleted.get().requestedTime(), lastRequested.get().requestedTime())
              .countInstants();
          return between + maxDeltaCommits;
        }
        return maxDeltaCommits;

      case TIME_ELAPSED:
        return deltaCommitsInTimeWindow;

      case NUM_OR_TIME:
        // Either condition fires it, so the earlier one governs.
        return Math.min(deltaCommitsInTimeWindow, maxDeltaCommits);

      case NUM_AND_TIME:
        // Both must hold, so the later one governs.
        return Math.max(deltaCommitsInTimeWindow, maxDeltaCommits);

      default:
        throw new IllegalArgumentException("Unknown compaction trigger strategy: " + strategy);
    }
  }
}
