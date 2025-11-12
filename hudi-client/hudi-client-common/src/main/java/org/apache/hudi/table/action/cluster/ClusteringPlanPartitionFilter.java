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

package org.apache.hudi.table.action.cluster;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;

import org.joda.time.DateTime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Partition filter utilities. Currently, we support three mode:
 *  NONE: skip filter
 *  RECENT DAYS: output recent partition given skip num and days lookback config
 *  SELECTED_PARTITIONS: output partition falls in the [start, end] condition
 *  DAY_ROLLING: Clustering all partitions once a day to avoid clustering data of all partitions each time.
 *  sort partitions asc, choose which partition index % 24 = now_hour.
 *  tips: If hoodie.clustering.inline=true, try to reach the limit of hoodie.clustering.inline.max.commits every hour.
 *        If hoodie.clustering.async.enabled=true, try to reach the limit of hoodie.clustering.async.max.commits every hour.
 *
 */
public class ClusteringPlanPartitionFilter {

  public static Pair<List<String>, List<String>> filter(List<String> partitions, HoodieWriteConfig config) {
    ClusteringPlanPartitionFilterMode mode = config.getClusteringPlanPartitionFilterMode();
    switch (mode) {
      case NONE:
        return Pair.of(partitions, Collections.emptyList());
      case RECENT_DAYS:
        return recentDaysFilter(partitions, config);
      case SELECTED_PARTITIONS:
        return selectedPartitionsFilter(partitions, config);
      case DAY_ROLLING:
        return dayRollingFilter(partitions, config);
      default:
        throw new HoodieClusteringException("Unknown partition filter, filter mode: " + mode);
    }
  }

  private static Pair<List<String>, List<String>> dayRollingFilter(List<String> partitions, HoodieWriteConfig config) {
    int hour = DateTime.now().getHourOfDay();
    int len = partitions.size();
    List<String> selectPt = new ArrayList<>();
    partitions.sort(String::compareTo);
    for (int i = 0; i < len; i++) {
      if (i % 24 == hour) {
        selectPt.add(partitions.get(i));
      }
    }
    return Pair.of(selectPt, Collections.emptyList());
  }

  private static Pair<List<String>, List<String>> recentDaysFilter(List<String> partitions, HoodieWriteConfig config) {
    int targetPartitionsForClustering = config.getTargetPartitionsForClustering();
    int skipPartitionsFromLatestForClustering = config.getSkipPartitionsFromLatestForClustering();
    int skipOffset = Math.max(skipPartitionsFromLatestForClustering, 0);
    List<String> sortedPartitions = partitions.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
    List<String> missingPartitions = sortedPartitions.subList(0, skipOffset);
    return Pair.of(sortedPartitions.stream()
        .skip(skipOffset)
        .limit(targetPartitionsForClustering > 0 ? targetPartitionsForClustering : partitions.size())
        .collect(Collectors.toList()), missingPartitions);
  }

  private static Pair<List<String>, List<String>> selectedPartitionsFilter(List<String> partitions, HoodieWriteConfig config) {
    Stream<String> filteredPartitions = partitions.stream();

    String beginPartition = config.getBeginPartitionForClustering();
    if (beginPartition != null) {
      filteredPartitions = filteredPartitions.filter(path -> path.compareTo(beginPartition) >= 0);
    }

    String endPartition = config.getEndPartitionForClustering();
    if (endPartition != null) {
      filteredPartitions = filteredPartitions.filter(path -> path.compareTo(endPartition) <= 0);
    }

    return Pair.of(filteredPartitions.collect(Collectors.toList()), Collections.emptyList());
  }
}
