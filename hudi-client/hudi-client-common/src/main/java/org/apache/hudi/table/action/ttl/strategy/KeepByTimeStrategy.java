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

package org.apache.hudi.table.action.ttl.strategy;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.table.HoodieTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.fixInstantTimeCompatibility;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.instantTimePlusMillis;

/**
 * KeepByTimeStrategy will return expired partitions by their lastCommitTime.
 */
public class KeepByTimeStrategy extends PartitionTTLStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(KeepByTimeStrategy.class);

  protected final long ttlInMilis;

  public KeepByTimeStrategy(HoodieTable hoodieTable, String instantTime) {
    super(hoodieTable, instantTime);
    this.ttlInMilis = writeConfig.getPartitionTTLStrategyDaysRetain() * 1000 * 3600 * 24;
  }

  @Override
  public List<String> getExpiredPartitionPaths() {
    Option<HoodieInstant> lastCompletedInstant = hoodieTable.getActiveTimeline().filterCompletedInstants().lastInstant();
    if (!lastCompletedInstant.isPresent() || ttlInMilis <= 0
        || !hoodieTable.getMetaClient().getTableConfig().getPartitionFields().isPresent()) {
      return Collections.emptyList();
    }
    List<String> expiredPartitions = getExpiredPartitionsForTimeStrategy(getPartitionPathsForTTL());
    int limit = writeConfig.getPartitionTTLMaxPartitionsToDelete();
    LOG.info("Total expired partitions count {}, limit {}", expiredPartitions.size(), limit);
    return expiredPartitions.stream()
        .limit(limit) // Avoid a single replace commit too large
        .collect(Collectors.toList());
  }

  protected List<String> getExpiredPartitionsForTimeStrategy(List<String> partitionsForTTLManagement) {
    HoodieTimer timer = HoodieTimer.start();
    Map<String, Option<String>> lastCommitTimeForPartitions = getLastCommitTimeForPartitions(partitionsForTTLManagement);
    LOG.info("Collect last commit time for partitions cost {} ms", timer.endTimer());
    return lastCommitTimeForPartitions.entrySet()
        .stream()
        .filter(entry -> entry.getValue().isPresent())
        .filter(entry -> isPartitionExpired(entry.getValue().get()))
        .map(Map.Entry::getKey)
        .collect(Collectors.toList());
  }

  /**
   * @param partitionPaths Partitions to collect stats.
   */
  private Map<String, Option<String>> getLastCommitTimeForPartitions(List<String> partitionPaths) {
    int statsParallelism = Math.min(partitionPaths.size(), 200);
    return hoodieTable.getContext().map(partitionPaths, partitionPath -> {
      Option<String> partitionLastModifiedTime = hoodieTable.getHoodieView()
          .getLatestFileSlicesBeforeOrOn(partitionPath, instantTime, true)
          .map(FileSlice::getBaseInstantTime)
          .max(Comparator.naturalOrder())
          .map(Option::ofNullable)
          .orElse(Option.empty());
      return Pair.of(partitionPath, partitionLastModifiedTime);
    }, statsParallelism).stream().collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  /**
   * Determines if a partition's reference time has exceeded its time-to-live (TTL).
   * <p>
   * This method checks if the current time has passed the TTL threshold based on a
   * reference time, which could be the creation time or the last commit time of the partition.
   *
   * @param referenceTime last commit time or creation time for partition
   */
  protected boolean isPartitionExpired(String referenceTime) {
    String expiredTime = instantTimePlusMillis(referenceTime, ttlInMilis);
    return fixInstantTimeCompatibility(instantTime).compareTo(expiredTime) > 0;
  }

}
