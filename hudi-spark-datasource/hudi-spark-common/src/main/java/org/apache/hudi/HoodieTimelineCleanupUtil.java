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

package org.apache.hudi;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Date;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HoodieTimelineCleanupUtil {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieTimelineCleanupUtil.class);

  public static List<HoodieInstant> inflightWriteCommitsOlderThan(HoodieTableMetaClient metaClient, long ageMinutes, boolean includeIngestionCommits) {
    long goBackMs = Duration.ofMinutes(ageMinutes).toMillis();
    String oldestAllowedTimestamp = HoodieInstantTimeGenerator.formatDate(new Date(System.currentTimeMillis() - goBackMs));

    Stream<HoodieInstant> inflightInstants = metaClient
        .reloadActiveTimeline()
        .getWriteTimeline()
        .filterInflightsAndRequested()
        .findInstantsBefore(oldestAllowedTimestamp)
        .getInstants().stream();

    if (!includeIngestionCommits) {
      Predicate<HoodieInstant> ingestionCommitsFilter =
          (x) -> x.getAction().equals(HoodieTimeline.COMMIT_ACTION) || x.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION);
      inflightInstants = inflightInstants.filter(ingestionCommitsFilter.negate());
    }
    List<HoodieInstant> inflightInstantsList = inflightInstants.collect(Collectors.toList());
    LOG.info("Inflight commits older than {} minutes: {}", ageMinutes, inflightInstantsList);
    return inflightInstantsList;
  }
}
