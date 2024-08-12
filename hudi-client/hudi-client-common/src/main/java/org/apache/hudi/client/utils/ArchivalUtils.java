/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.hudi.client.utils;

import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.time.Instant;

import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.NOT_PARSABLE_TIMESTAMPS;
import static org.apache.hudi.common.table.timeline.HoodieActiveTimeline.parseDateFromInstantTime;
import static org.apache.hudi.config.HoodieArchivalConfig.MAX_COMMITS_TO_KEEP;
import static org.apache.hudi.config.HoodieArchivalConfig.MIN_COMMITS_TO_KEEP;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_COMMITS_RETAINED;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_FILE_VERSIONS_RETAINED;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_HOURS_RETAINED;
import static org.apache.hudi.config.HoodieCleanConfig.CLEANER_POLICY;

/**
 * Provides utilities for timeline archival service.
 */
public class ArchivalUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ArchivalUtils.class);

  /**
   *  getMinAndMaxInstantsToKeep is used by archival service to find the
   *  min instants and max instants to keep in the active timeline
   * @param table table implementation extending org.apache.hudi.table.HoodieTable
   * @param metaClient meta client
   * @return Pair containing min instants and max instants to keep.
   */
  public static Pair<Integer,Integer> getMinAndMaxInstantsToKeep(HoodieTable<?, ?, ?, ?> table, HoodieTableMetaClient metaClient) {
    HoodieWriteConfig config = table.getConfig();
    HoodieTimeline completedCommitsTimeline = table.getCompletedCommitsTimeline();
    Option<HoodieInstant> latestCommit = completedCommitsTimeline.lastInstant();
    HoodieCleaningPolicy cleanerPolicy = config.getCleanerPolicy();
    int cleanerCommitsRetained = config.getCleanerCommitsRetained();
    int cleanerHoursRetained = config.getCleanerHoursRetained();
    int maxInstantsToKeep;
    int minInstantsToKeep;
    Option<HoodieInstant> earliestCommitToRetain = getEarliestCommitToRetain(metaClient, latestCommit, cleanerPolicy, cleanerCommitsRetained, cleanerHoursRetained);
    int configuredMinInstantsToKeep = config.getMinCommitsToKeep();
    int configuredMaxInstantsToKeep = config.getMaxCommitsToKeep();
    if (earliestCommitToRetain.isPresent()) {
      int minInstantsToKeepBasedOnCleaning =
          completedCommitsTimeline.findInstantsAfter(earliestCommitToRetain.get().getTimestamp())
              .countInstants() + 2;
      if (configuredMinInstantsToKeep < minInstantsToKeepBasedOnCleaning) {
        maxInstantsToKeep = minInstantsToKeepBasedOnCleaning
            + configuredMaxInstantsToKeep - configuredMinInstantsToKeep;
        minInstantsToKeep = minInstantsToKeepBasedOnCleaning;
        LOG.warn("The configured archival configs {}={} is more aggressive than the cleaning "
                + "configs as the earliest commit to retain is {}. Adjusted the archival configs "
                + "to be {}={} and {}={}",
            MIN_COMMITS_TO_KEEP.key(), configuredMinInstantsToKeep, earliestCommitToRetain.get(),
            MIN_COMMITS_TO_KEEP.key(), minInstantsToKeep,
            MAX_COMMITS_TO_KEEP.key(), maxInstantsToKeep);
        switch (cleanerPolicy) {
          case KEEP_LATEST_COMMITS:
            LOG.warn("Cleaning configs: {}=KEEP_LATEST_COMMITS {}={}", CLEANER_POLICY.key(),
                CLEANER_COMMITS_RETAINED.key(), cleanerCommitsRetained);
            break;
          case KEEP_LATEST_BY_HOURS:
            LOG.warn("Cleaning configs: {}=KEEP_LATEST_BY_HOURS {}={}", CLEANER_POLICY.key(),
                CLEANER_HOURS_RETAINED.key(), cleanerHoursRetained);
            break;
          case KEEP_LATEST_FILE_VERSIONS:
            LOG.warn("Cleaning configs: {}=CLEANER_FILE_VERSIONS_RETAINED {}={}", CLEANER_POLICY.key(),
                CLEANER_FILE_VERSIONS_RETAINED.key(), config.getCleanerFileVersionsRetained());
            break;
          default:
            break;
        }
      } else {
        maxInstantsToKeep = configuredMaxInstantsToKeep;
        minInstantsToKeep = configuredMinInstantsToKeep;
      }
    } else {
      maxInstantsToKeep = configuredMaxInstantsToKeep;
      minInstantsToKeep = configuredMinInstantsToKeep;
    }
    return Pair.of(minInstantsToKeep, maxInstantsToKeep);
  }

  private static Option<HoodieInstant> getEarliestCommitToRetain(
      HoodieTableMetaClient metaClient,
      Option<HoodieInstant> latestCommit,
      HoodieCleaningPolicy cleanerPolicy,
      int cleanerCommitsRetained, int cleanerHoursRetained) {
    Option<HoodieInstant> earliestCommitToRetain = Option.empty();
    try {
      earliestCommitToRetain = CleanerUtils.getEarliestCommitToRetain(
          metaClient.getActiveTimeline().getCommitsTimeline(),
          cleanerPolicy,
          cleanerCommitsRetained,
          latestCommit.isPresent()
              ? parseDateFromInstantTime(latestCommit.get().getTimestamp()).toInstant()
              : Instant.now(),
          cleanerHoursRetained,
          metaClient.getTableConfig().getTimelineTimezone());
    } catch (ParseException e) {
      if (NOT_PARSABLE_TIMESTAMPS.stream().noneMatch(ts -> latestCommit.get().getTimestamp().startsWith(ts))) {
        LOG.warn("Error parsing instant time: " + latestCommit.get().getTimestamp());
      }
    }
    return earliestCommitToRetain;
  }
}
