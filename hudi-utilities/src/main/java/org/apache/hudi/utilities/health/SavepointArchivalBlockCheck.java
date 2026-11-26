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

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Checks for savepoints that are holding back archival.
 *
 * <p>Archival stops at the earliest savepoint and will not advance past it. A savepoint taken for a
 * one-off restore and then forgotten therefore pins the active timeline indefinitely. The timeline
 * grows without bound, every timeline load gets slower, and because archival is blocked the
 * cleaner's retention interacts with it in ways that keep data files alive too. This is a quiet
 * failure: the table keeps accepting writes and nothing errors.
 *
 * <p>Two signals are reported. A savepoint sitting further back than the configured archival
 * retention is actively blocking archival now. A savepoint older than {@link #STALE_SAVEPOINT_DAYS}
 * is flagged separately as likely forgotten, since savepoints are normally short-lived.
 *
 * <p>The remedy this check points at is always to release the savepoint once the operation it was
 * taken for is done. It deliberately does not suggest loosening archival to step over savepoints.
 */
public class SavepointArchivalBlockCheck implements HealthCheck {

  static final String NAME = "savepoint";

  /**
   * Age past which a savepoint is called out as probably forgotten. Savepoints are normally taken
   * around a specific operation and released soon after; one lingering for weeks is usually an
   * oversight rather than an intent.
   */
  static final int STALE_SAVEPOINT_DAYS = 7;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether a savepoint is blocking archival or has been left behind";
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
    HoodieTimeline writeTimeline = context.getMetaClient().getActiveTimeline().getWriteTimeline();
    List<HoodieInstant> savepoints = context.getMetaClient().getActiveTimeline()
        .getSavePointTimeline().filterCompletedInstants().getInstants();

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), maxCommitsToKeep)
        .config("observed.savepoint.count", savepoints.size());

    if (savepoints.isEmpty()) {
      return result.status(HealthStatus.HEALTHY).summary("No savepoints on the table").build();
    }

    List<String> savepointTimes = savepoints.stream()
        .map(HoodieInstant::requestedTime).collect(Collectors.toList());
    result.config("observed.savepoints", String.join(",", savepointTimes));

    List<String> blocking = savepointsBeyondRetention(writeTimeline, savepointTimes, maxCommitsToKeep);
    List<String> stale = savepointTimes.stream()
        .filter(SavepointArchivalBlockCheck::isOlderThanStaleThreshold)
        .collect(Collectors.toList());

    if (!stale.isEmpty()) {
      result.finding(String.format(
          "Savepoint(s) %s are more than %d days old. If the operation they were taken for is "
              + "finished, delete them so archival can advance.",
          String.join(", ", stale), STALE_SAVEPOINT_DAYS));
    }

    if (blocking.isEmpty()) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format("%d savepoint(s) present, none older than the archival retention of %d commits",
              savepoints.size(), maxCommitsToKeep))
          .build();
    }

    return result.status(HealthStatus.UNHEALTHY)
        .summary(String.format(
            "%d savepoint(s) older than the archival retention are blocking archival: %s",
            blocking.size(), String.join(", ", blocking)))
        .finding(String.format(
            "Archival stops at the earliest savepoint. The active timeline cannot shrink past %s "
                + "until that savepoint is released.", blocking.get(0)))
        .finding("Delete the savepoint once the operation it was taken for is complete, using the "
            + "'savepoint delete' command in hudi-cli or SavepointsCommand.")
        .build();
  }

  /**
   * Savepoints that fall outside the window archival is allowed to retain -- that is, old enough
   * that archival would have removed them by now were they not savepointed.
   */
  private static List<String> savepointsBeyondRetention(HoodieTimeline writeTimeline,
                                                        List<String> savepointTimes,
                                                        int maxCommitsToKeep) {
    List<HoodieInstant> instants = writeTimeline.getInstants();
    if (instants.size() <= maxCommitsToKeep) {
      return Collections.emptyList();
    }
    return instants.subList(0, instants.size() - maxCommitsToKeep).stream()
        .map(HoodieInstant::requestedTime)
        .filter(savepointTimes::contains)
        .collect(Collectors.toList());
  }

  private static boolean isOlderThanStaleThreshold(String instantTime) {
    return TimelineUtils.parseDateFromInstantTimeSafely(instantTime)
        .map(date -> date.toInstant()
            .isBefore(Instant.now().minus(STALE_SAVEPOINT_DAYS, ChronoUnit.DAYS)))
        .orElse(false);
  }
}
