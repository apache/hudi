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

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieClusteringException;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Partition filter utilities. Currently, we support three mode:
 *  NONE: skip filter
 *  RECENT DAYS: output recent partition given skip num and days lookback config
 *  SELECTED_PARTITIONS: output partition falls in the [start, end] condition
 */
public class ClusteringPlanPartitionFilter {

  public static List<String> filter(List<String> partitions, HoodieWriteConfig config) {
    ClusteringPlanPartitionFilterMode mode = config.getClusteringPlanPartitionFilterMode();
    switch (mode) {
      case NONE:
        return partitions;
      case RECENT_DAYS:
        return recentDaysFilter(partitions, config);
      case SELECTED_PARTITIONS:
        return selectedPartitionsFilter(partitions, config);
      default:
        throw new HoodieClusteringException("Unknown partition filter, filter mode: " + mode);
    }
  }

  private static List<String> recentDaysFilter(List<String> partitions, HoodieWriteConfig config) {
    int targetPartitionsForClustering = config.getTargetPartitionsForClustering();
    int skipPartitionsFromLatestForClustering = config.getSkipPartitionsFromLatestForClustering();
    return partitions.stream()
        .sorted(Comparator.reverseOrder())
        .skip(Math.max(skipPartitionsFromLatestForClustering, 0))
        .limit(targetPartitionsForClustering > 0 ? targetPartitionsForClustering : partitions.size())
        .collect(Collectors.toList());
  }

  private static List<String> selectedPartitionsFilter(List<String> partitions, HoodieWriteConfig config) {
    String beginPartition = config.getBeginPartitionForClustering();
    String endPartition = config.getEndPartitionForClustering();
    List<String> filteredPartitions = partitions.stream()
        .filter(path -> path.compareTo(beginPartition) >= 0 && path.compareTo(endPartition) <= 0)
        .collect(Collectors.toList());
    return filteredPartitions;
  }
}
