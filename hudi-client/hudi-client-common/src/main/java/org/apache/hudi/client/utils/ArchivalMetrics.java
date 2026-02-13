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

package org.apache.hudi.client.utils;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Constants and utilities for archival metrics.
 */
public final class ArchivalMetrics {
  public static final String ARCHIVAL_OOM_FAILURE = "archivalOutOfMemory";
  public static final String ARCHIVAL_NUM_ALL_COMMITS = "archivalNumAllCommits";
  public static final String ARCHIVAL_NUM_WRITE_COMMITS = "archivalNumWriteCommits";
  public static final String ARCHIVAL_FAILURE = "archivalFailure";
  public static final String ARCHIVAL_STATUS = "archivalStatus";

  private static final Set<String> WRITE_COMMIT_ACTIONS = CollectionUtils.createSet(
      HoodieTimeline.COMMIT_ACTION,
      HoodieTimeline.DELTA_COMMIT_ACTION,
      HoodieTimeline.REPLACE_COMMIT_ACTION);

  private ArchivalMetrics() {
    // Private constructor to prevent instantiation
  }

  /**
   * Adds archival commit metrics to the given metrics map based on the completed instants.
   *
   * @param completedInstants stream of completed instants to archive
   * @param metrics map to populate with archival metrics
   */
  public static void addArchivalCommitMetrics(Stream<HoodieInstant> completedInstants, Map<String, Long> metrics) {
    // Collect to list since we need to iterate twice
    List<HoodieInstant> instantsList = completedInstants.collect(Collectors.toList());
    metrics.put(ARCHIVAL_NUM_ALL_COMMITS, (long) instantsList.size());
    long writeCommitCount = instantsList.stream()
        .filter(instant -> WRITE_COMMIT_ACTIONS.contains(instant.getAction()))
        .count();
    metrics.put(ARCHIVAL_NUM_WRITE_COMMITS, writeCommitCount);
  }
}
