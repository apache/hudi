/*
 * Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.table.WorkloadProfile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;
import scala.Tuple3;

/**
 * HoodiePartitionHelper is responsible for splitting Hoodie upsert stage into smaller spark
 * stages.
 */
public class HoodiePartitionHelper {

  private static Logger logger = LogManager.getLogger(HoodiePartitionHelper.class);

  /**
   * Split an upsert workload
   *
   * @param profile WorkloadProfile for the input record
   * @param isUpsert indicates upsert workflow or insert only workflow
   * @param maxPartitions max spark partitions allowed for single datafile write stage
   * @return List of conditions in Tuple3 of (lowBound, highBound, count)
   */
  public static List<Tuple3<String, String, Long>> splitWorkload(WorkloadProfile profile,
      boolean isUpsert,
      int maxPartitions) {

    // We can't split input records if we don't know the workload or if this is a plain insert
    if (profile == null || !isUpsert) {
      return Collections.emptyList();
    }

    Set<String> sortedPartitions = new TreeSet<>();
    List<Tuple2<String, Long>> splitPoints = new ArrayList<>();
    sortedPartitions.addAll(profile.getPartitionPaths());
    long totalPartitions = 0;
    for (String partition : sortedPartitions) {
      int updateCount = profile.getWorkloadStat(partition)
          .getUpdateLocationToCount().size();
      // this ensures that totalPartitions will not exceed maxPartitions
      if (totalPartitions == 0 || (totalPartitions + updateCount) <= maxPartitions) {
        totalPartitions += updateCount;
      } else {
        splitPoints.add(new Tuple2<>(partition, totalPartitions));
        totalPartitions = updateCount;
      }
    }

    if (splitPoints.isEmpty()) {
      return Collections.emptyList();
    }

    // Figure filter conditions
    List<Tuple3<String, String, Long>> conditions = new ArrayList<>();
    conditions.add(new Tuple3<>(null, splitPoints.get(0)._1(), splitPoints.get(0)._2()));
    for (int i = 0; i < splitPoints.size(); ++i) {
      if (i == splitPoints.size() - 1) {
        conditions.add(new Tuple3<>(splitPoints.get(i)._1(), null, totalPartitions));
      } else {
        conditions.add(new Tuple3<>(splitPoints.get(i)._1(), splitPoints.get(i + 1)._1(),
            splitPoints.get(i + 1)._2()));
      }
    }

    for (Tuple3<String, String, Long> condition : conditions) {
      logger.info("Workload filter condition: " + condition._1() + "," + condition._2() +
          " with partitions - " + condition._3());
    }

    return conditions;
  }

  /**
   * Filter input records by the given condition
   *
   * @param hoodieRecord HoodieRecord to filter
   * @param condition the condition used to filter a given HoodieRecord
   * @return a Boolean indicates the filter result
   */
  public static Boolean filterHoodieRecord(HoodieRecord hoodieRecord,
      Tuple3<String, String, Long> condition) {

    boolean result;
    final String partitionPath = hoodieRecord.getKey().getPartitionPath();

    if (condition._1() != null && condition._2() != null) {
      // Case: condition._1() <= partitionPath < condition._2()
      result = (condition._1().compareTo(partitionPath) <= 0) &&
          (partitionPath.compareTo(condition._2()) < 0);
    } else if (condition._1() == null) {
      // Case: partitionPath < condition._2()
      result = (partitionPath.compareTo(condition._2()) < 0);
    } else {
      // Case: condition._1() <= partitionPath
      result = (condition._1().compareTo(partitionPath) <= 0);
    }
    return Boolean.valueOf(result);
  }
}
