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

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCleanConfig;

/**
 * Checks whether the cleaner is keeping pace with ingestion.
 *
 * <p>Cleaning is what releases storage: every commit leaves behind older file versions that stay on
 * disk until a clean removes them. When the cleaner stops, the table keeps working and nothing
 * errors -- storage simply grows, and with it the cost of every listing. Like the other cadence
 * checks this one asks not "has a clean run recently" but whether write commits have accumulated
 * well past the point where the configured trigger should have scheduled one.
 *
 * <p>Two allowances are made before calling the table unhealthy. The configured trigger is
 * multiplied by a slack factor, since async cleaning trails ingestion by design. And a floor is
 * applied on top, because the trigger defaults to one commit: without a floor, a cleaner that is
 * two commits behind -- routine when cleaning is asynchronous or briefly loses a lock to a
 * concurrent writer -- would be reported as a failure.
 */
public class CleanerCadenceCheck implements HealthCheck {

  static final String NAME = "cleaner";

  /** Multiplier on the configured trigger, absorbing async cleaning running on its own cadence. */
  static final double TRIGGER_SLACK_FACTOR = 2.0;

  /**
   * Minimum number of commits since the last clean before the table can be called unhealthy,
   * regardless of how low the trigger is set. Keeps a trigger of one commit from turning ordinary
   * async lag into an alert.
   */
  static final int MIN_UNHEALTHY_COMMITS = 5;

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getDescription() {
    return "Whether the cleaner is keeping pace with write commits";
  }

  @Override
  public HealthCheckResult check(HealthCheckContext context) throws Exception {
    Option<String> missing = context.firstMissingConfig(HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key());
    if (missing.isPresent()) {
      return context.skipForMissingConfig(NAME, missing.get());
    }

    int triggerCommits = Integer.parseInt(context.getProps().getString(
        HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key(),
        HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.defaultValue()));
    int threshold = Math.max((int) Math.ceil(TRIGGER_SLACK_FACTOR * triggerCommits), MIN_UNHEALTHY_COMMITS);

    HoodieActiveTimeline activeTimeline = context.getMetaClient().getActiveTimeline();
    HoodieTimeline completedWrites = activeTimeline.getCommitsTimeline().filterCompletedInstants();
    Option<HoodieInstant> lastClean = activeTimeline.getCleanerTimeline().filterCompletedInstants().lastInstant();

    HealthCheckResult.Builder result = HealthCheckResult.builder(NAME)
        .config(HoodieCleanConfig.CLEAN_TRIGGER_MAX_COMMITS.key(), triggerCommits)
        .config("effective.unhealthy.threshold.commits", threshold)
        .config("observed.completed.write.commits", completedWrites.countInstants());
    echoIfSupplied(context, result, HoodieCleanConfig.CLEANER_POLICY.key());
    echoIfSupplied(context, result, HoodieCleanConfig.CLEANER_COMMITS_RETAINED.key());

    int commitsSinceClean;
    if (lastClean.isPresent()) {
      commitsSinceClean = completedWrites.findInstantsAfter(lastClean.get().requestedTime()).countInstants();
      result.config("observed.last.clean.instant", lastClean.get().requestedTime());
      describeLastClean(context, lastClean.get(), result);
    } else {
      commitsSinceClean = completedWrites.countInstants();
      result.config("observed.last.clean.instant", "none");
    }
    result.config("observed.commits.since.last.clean", commitsSinceClean);

    if (commitsSinceClean <= threshold) {
      return result.status(HealthStatus.HEALTHY)
          .summary(lastClean.isPresent()
              ? String.format("%d commit(s) since clean at %s, within the threshold of %d",
                  commitsSinceClean, lastClean.get().requestedTime(), threshold)
              : String.format("%d commit(s) and no clean yet, within the threshold of %d",
                  commitsSinceClean, threshold))
          .build();
    }

    result.status(HealthStatus.UNHEALTHY)
        .summary(lastClean.isPresent()
            ? String.format("%d commit(s) have accumulated since clean at %s, past the threshold of %d",
                commitsSinceClean, lastClean.get().requestedTime(), threshold)
            : String.format("%d commit(s) have accumulated and the table has never been cleaned, past the threshold of %d",
                commitsSinceClean, threshold))
        .finding(String.format(
            "The cleaner is configured to run every %d commit(s); %d have landed without one completing.",
            triggerCommits, commitsSinceClean));

    if (activeTimeline.getCleanerTimeline().filterInflightsAndRequested().countInstants() > 0) {
      result.finding("A clean is scheduled or in flight but has not completed. Look for a failing or "
          + "stuck clean in the writer logs; a clean that repeatedly fails to acquire the table lock "
          + "under concurrent writers is a common cause.");
    } else {
      result.finding("No clean is scheduled. Check that cleaning is enabled for this table ("
          + HoodieCleanConfig.AUTO_CLEAN.key() + ") and that the job responsible for it is running.");
    }
    return result.build();
  }

  /** Surfaces what the last clean retained, so the operator can see the retention watermark moving. */
  private static void describeLastClean(HealthCheckContext context, HoodieInstant lastClean,
                                        HealthCheckResult.Builder result) {
    try {
      HoodieCleanMetadata metadata = CleanerUtils.getCleanerMetadata(context.getMetaClient(), lastClean);
      result.config("observed.last.clean.earliest.commit.retained", metadata.getEarliestCommitToRetain());
      result.config("observed.last.clean.files.deleted", metadata.getTotalFilesDeleted());
    } catch (Exception e) {
      // The metadata is informational only; an unreadable clean must not fail the verdict.
      result.config("observed.last.clean.metadata", "unreadable: " + e.getMessage());
    }
  }

  private static void echoIfSupplied(HealthCheckContext context, HealthCheckResult.Builder result, String key) {
    if (context.getProps().containsKey(key)) {
      result.config(key, context.getProps().getString(key));
    }
  }
}
