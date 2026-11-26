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

import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;

import java.text.ParseException;
import java.time.Instant;

import static org.apache.hudi.common.table.timeline.TimelineUtils.parseDateFromInstantTime;

/**
 * Checks whether archival is keeping the active timeline bounded.
 *
 * <p>Archival moves old instants out of the active timeline into the archived timeline. When it
 * stops, the active timeline grows without limit, and because it is read and parsed on essentially
 * every table operation, everything touching the table gets progressively slower. Unlike a failed
 * write this produces no error -- the table simply degrades.
 *
 * <p>The configured {@code hoodie.keep.max.commits} is not on its own the bound the archiver
 * enforces. {@code ArchivalUtils.getMinAndMaxInstantsToKeep} raises both bounds whenever the
 * cleaner is retaining more history than archival would: archival must not move an instant off the
 * active timeline while the cleaner still considers its files live, or incremental readers can be
 * left reading files whose commits are gone. So a table with {@code clean.commits.retained=100} and
 * {@code keep.max.commits=30} legitimately holds around a hundred instants, and calling that
 * unhealthy is a false alarm. This check reproduces that calculation from the cleaner
 * configuration, and reports the effective bound it derived.
 *
 * <p>Two independent signals are reported. The first compares the active timeline against that
 * effective retention, with slack: archival runs periodically rather than on every commit, so a
 * timeline slightly over the maximum is normal. The second is an absolute ceiling, because past a
 * few thousand instants timeline loading is a problem regardless of what the configuration permits
 * -- a table configured to retain a very large number of commits is misconfigured rather than
 * healthy.
 *
 * <p>Attribution is deliberately limited. Archival also holds back for the earliest pending write
 * instant, for compaction retention on MOR tables, and for the metadata table's own compaction, and
 * this check cannot see which of those is binding. Where a pending instant is visible on the
 * timeline it is named, because that is the one an operator can act on; otherwise the finding says
 * what to look at rather than guessing. A blocked archival is most often a savepoint, which
 * {@link SavepointArchivalBlockCheck} diagnoses specifically; the two are meant to be read
 * together.
 */
public class ArchivalCadenceCheck implements HealthCheck {

  static final String NAME = "archival";

  /**
   * How far past the configured retention the active timeline may grow before being called
   * unhealthy. Archival is periodic, so the timeline routinely sits somewhat above the maximum
   * between runs.
   */
  static final double RETENTION_SLACK_FACTOR = 1.1;

  /**
   * Active timeline size past which the table is called unhealthy regardless of configuration.
   * Beyond roughly this many instants, loading and parsing the timeline is itself a performance
   * problem on every operation against the table.
   */
  static final int MAX_REASONABLE_ACTIVE_INSTANTS = 5000;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether archival is keeping the active timeline bounded";
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) {
    Option<String> missing = context.firstMissingConfig(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key());
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    int maxCommitsToKeep = Integer.parseInt(context.getProps().getString(
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(),
        HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.defaultValue()));
    int minCommitsToKeep = Integer.parseInt(context.getProps().getString(
        HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(),
        HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.defaultValue()));

    HoodieActiveTimeline activeTimeline = context.getMetaClient().getActiveTimeline();
    // The archiver reasons over completed commits; getWriteTimeline() also counts requested and
    // inflight compaction and clustering, which archival has not had the chance to consider.
    HoodieTimeline completedCommitsTimeline =
        activeTimeline.getCommitsTimeline().filterCompletedInstants();
    int completedCommits = completedCommitsTimeline.countInstants();
    int completedInstants = activeTimeline.filterCompletedInstants().countInstants();

    int effectiveMaxCommitsToKeep =
        effectiveMaxCommitsToKeep(context, completedCommitsTimeline, minCommitsToKeep, maxCommitsToKeep);
    int retentionThreshold = (int) Math.ceil(RETENTION_SLACK_FACTOR * effectiveMaxCommitsToKeep);

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), maxCommitsToKeep)
        .config(HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(), minCommitsToKeep)
        .config("effective.max.commits.to.keep", effectiveMaxCommitsToKeep)
        .config("effective.unhealthy.threshold.completed.commits", retentionThreshold)
        .config("effective.absolute.ceiling.completed.instants", MAX_REASONABLE_ACTIVE_INSTANTS)
        .config("observed.completed.commits", completedCommits)
        .config("observed.completed.instants", completedInstants);

    boolean overAbsoluteCeiling = completedInstants >= MAX_REASONABLE_ACTIVE_INSTANTS;
    boolean overConfiguredRetention = completedCommits >= retentionThreshold;

    if (!overAbsoluteCeiling && !overConfiguredRetention) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format("Active timeline holds %d completed commit(s), within the "
              + "effective threshold of %d", completedCommits, retentionThreshold))
          .build();
    }

    if (overAbsoluteCeiling) {
      result.finding(String.format(
          "The active timeline holds %d completed instants. Past roughly %d, loading and parsing "
              + "the timeline slows down every operation against the table, whatever the configured "
              + "retention allows.",
          completedInstants, MAX_REASONABLE_ACTIVE_INSTANTS));
    }
    if (overConfiguredRetention) {
      if (effectiveMaxCommitsToKeep > maxCommitsToKeep) {
        result.finding(String.format(
            "The active timeline holds %d completed commits. The configured maximum is %d, but the "
                + "cleaner's retention raises the bound the archiver enforces to %d, and the "
                + "timeline is past even that.",
            completedCommits, maxCommitsToKeep, effectiveMaxCommitsToKeep));
      } else {
        result.finding(String.format(
            "The active timeline holds %d completed commits against a configured maximum of %d. "
                + "Archival is not keeping up with ingestion.",
            completedCommits, maxCommitsToKeep));
      }
    }

    // Archival never moves past the earliest pending write instant, so when one is present it is
    // the concrete thing to look at before anything else.
    Option<HoodieInstant> earliestPending =
        activeTimeline.getWriteTimeline().filterInflightsAndRequested().firstInstant();
    if (earliestPending.isPresent()) {
      result.finding(String.format(
          "Archival cannot move past the earliest pending write instant, and %s (%s) is still %s. "
              + "Complete or roll it back and archival will resume.",
          earliestPending.get().requestedTime(), earliestPending.get().getAction(),
          earliestPending.get().getState()));
    }

    return result.status(HealthStatus.UNHEALTHY)
        .summary(String.format("Active timeline is not being archived down: %d completed commit(s), "
            + "%d completed instant(s)", completedCommits, completedInstants))
        .finding("Check the '" + SavepointArchivalBlockCheck.NAME + "' check first -- a savepoint is "
            + "the most common reason archival stalls. Archival also holds back for compaction "
            + "retention on MOR tables and for the metadata table's own compaction; if no savepoint "
            + "and no pending instant explains this, confirm the archival service is running and "
            + "look for repeated archival failures in the writer logs.")
        .build();
  }

  /**
   * The maximum the archiver will actually enforce, which is not always the configured one.
   *
   * <p>Mirrors {@code ArchivalUtils.getMinAndMaxInstantsToKeep}: when the cleaner retains more
   * history than archival would, archival's bounds are raised to cover it, preserving the
   * configured gap between min and max. Reproduced here rather than called because that method
   * needs a {@code HoodieTable}, and this tool reads only the timeline.
   *
   * <p>Falls back to the configured maximum when the cleaner's earliest commit to retain cannot be
   * determined, which keeps this check no worse than it was before.
   */
  private int effectiveMaxCommitsToKeep(HealthCheckContext context,
                                        HoodieTimeline completedCommitsTimeline,
                                        int minCommitsToKeep,
                                        int maxCommitsToKeep) {
    Option<HoodieInstant> earliestCommitToRetain =
        earliestCommitToRetain(context, completedCommitsTimeline);
    if (!earliestCommitToRetain.isPresent()) {
      return maxCommitsToKeep;
    }
    int minBasedOnCleaning = completedCommitsTimeline
        .findInstantsAfter(earliestCommitToRetain.get().requestedTime()).countInstants() + 2;
    if (minCommitsToKeep >= minBasedOnCleaning) {
      return maxCommitsToKeep;
    }
    return minBasedOnCleaning + maxCommitsToKeep - minCommitsToKeep;
  }

  private Option<HoodieInstant> earliestCommitToRetain(HealthCheckContext context,
                                                       HoodieTimeline completedCommitsTimeline) {
    HoodieCleaningPolicy cleanerPolicy;
    try {
      cleanerPolicy = HoodieCleaningPolicy.valueOf(
          context.getProps().getString(HoodieCleanConfig.CLEANER_POLICY.key(),
              HoodieCleanConfig.CLEANER_POLICY.defaultValue()));
    } catch (IllegalArgumentException e) {
      return Option.empty();
    }
    int commitsRetained = Integer.parseInt(
        context.getProps().getString(HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key(),
            HoodieCleanConfig.CLEANER_COMMITS_RETAINED.defaultValue()));
    int hoursRetained = Integer.parseInt(
        context.getProps().getString(HoodieCleanConfig.CLEANER_HOURS_RETAINED.key(),
            HoodieCleanConfig.CLEANER_HOURS_RETAINED.defaultValue()));
    // Only KEEP_LATEST_BY_HOURS needs the latest commit as a wall-clock instant. Parsing it
    // unconditionally would let an instant time this tool cannot parse disable the whole
    // calculation, silently falling back to the configured bound.
    Instant latestInstant = Instant.now();
    if (HoodieCleaningPolicy.KEEP_LATEST_BY_HOURS == cleanerPolicy) {
      Option<HoodieInstant> latestCommit = completedCommitsTimeline.lastInstant();
      if (latestCommit.isPresent()) {
        try {
          latestInstant = parseDateFromInstantTime(latestCommit.get().requestedTime()).toInstant();
        } catch (ParseException e) {
          // Without a parsable latest commit the hours-based window cannot be placed; fall back to
          // the configured bound rather than inventing one.
          return Option.empty();
        }
      }
    }
    try {
      return CleanerUtils.getEarliestCommitToRetain(
          context.getMetaClient().getActiveTimeline().getCommitsTimeline(),
          cleanerPolicy,
          commitsRetained,
          latestInstant,
          hoursRetained,
          context.getMetaClient().getTableConfig().getTimelineTimezone(),
          Option.empty(),
          Long.MAX_VALUE);
    } catch (IllegalArgumentException e) {
      // A cleaner policy we do not recognise.
      return Option.empty();
    }
  }
}
