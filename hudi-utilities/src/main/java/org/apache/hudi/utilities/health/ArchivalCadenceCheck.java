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

import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;

/**
 * Checks whether archival is keeping the active timeline bounded.
 *
 * <p>Archival moves old instants out of the active timeline into the archived timeline. When it
 * stops, the active timeline grows without limit, and because it is read and parsed on essentially
 * every table operation, everything touching the table gets progressively slower. Unlike a failed
 * write this produces no error -- the table simply degrades.
 *
 * <p>Two independent signals are reported. The first compares the active timeline against the
 * configured retention, with slack: archival runs periodically rather than on every commit, so a
 * timeline slightly over the configured maximum is normal. The second is an absolute ceiling,
 * because past a few thousand instants timeline loading is a problem regardless of what the
 * configuration permits -- a table configured to retain a very large number of commits is
 * misconfigured rather than healthy.
 *
 * <p>This check deliberately does not attribute the cause. A blocked archival is most often a
 * savepoint, which {@link SavepointArchivalBlockCheck} diagnoses specifically; the two are meant to
 * be read together.
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

    HoodieActiveTimeline activeTimeline = context.getMetaClient().getActiveTimeline();
    int completedInstants = activeTimeline.filterCompletedInstants().countInstants();
    int writeInstants = activeTimeline.getWriteTimeline().countInstants();
    int retentionThreshold = (int) Math.ceil(RETENTION_SLACK_FACTOR * maxCommitsToKeep);

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieArchivalConfig.MAX_COMMITS_TO_KEEP.key(), maxCommitsToKeep)
        .config("effective.unhealthy.threshold.write.instants", retentionThreshold)
        .config("effective.absolute.ceiling.completed.instants", MAX_REASONABLE_ACTIVE_INSTANTS)
        .config("observed.write.instants", writeInstants)
        .config("observed.completed.instants", completedInstants);

    boolean overAbsoluteCeiling = completedInstants >= MAX_REASONABLE_ACTIVE_INSTANTS;
    boolean overConfiguredRetention = writeInstants >= retentionThreshold;

    if (!overAbsoluteCeiling && !overConfiguredRetention) {
      return result.status(HealthStatus.HEALTHY)
          .summary(String.format("Active timeline holds %d write instant(s), within the threshold of %d",
              writeInstants, retentionThreshold))
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
      result.finding(String.format(
          "The active timeline holds %d write instants against a configured maximum of %d. Archival "
              + "is not keeping up with ingestion.",
          writeInstants, maxCommitsToKeep));
    }

    return result.status(HealthStatus.UNHEALTHY)
        .summary(String.format("Active timeline is not being archived down: %d write instant(s), "
            + "%d completed instant(s)", writeInstants, completedInstants))
        .finding("Check the '" + SavepointArchivalBlockCheck.NAME + "' check first -- a savepoint is "
            + "the most common reason archival stalls. Otherwise confirm the archival service is "
            + "running and look for repeated archival failures in the writer logs.")
        .build();
  }
}
