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

package org.apache.hudi.table.action.compact.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This strategy orders compactions in reverse order of creation of Hive Partitions. It helps to compact data in latest
 * partitions first and then older capped at the Total_IO allowed.
 */
public class DayBasedCompactionStrategy extends CompactionStrategy {

  // For now, use SimpleDateFormat as default partition format
  protected static final String DATE_PARTITION_FORMAT = "yyyy/MM/dd";
  // Sorts compaction in LastInFirstCompacted order

  // NOTE: {@code SimpleDataFormat} is NOT thread-safe
  // TODO replace w/ DateTimeFormatter
  private static final ThreadLocal<SimpleDateFormat> DATE_FORMAT =
      ThreadLocal.withInitial(() -> new SimpleDateFormat(DATE_PARTITION_FORMAT, Locale.ENGLISH));

  protected static Comparator<String> comparator = (String leftPartition, String rightPartition) -> {
    try {
      leftPartition = getPartitionPathWithoutPartitionKeys(leftPartition);
      rightPartition = getPartitionPathWithoutPartitionKeys(rightPartition);
      Date left = DATE_FORMAT.get().parse(leftPartition);
      Date right = DATE_FORMAT.get().parse(rightPartition);
      return left.after(right) ? -1 : right.after(left) ? 1 : 0;
    } catch (ParseException e) {
      throw new HoodieException("Invalid Partition Date Format", e);
    }
  };

  public Comparator<String> getComparator() {
    return comparator;
  }

  @Override
  public List<HoodieCompactionOperation> orderAndFilter(HoodieWriteConfig writeConfig,
      List<HoodieCompactionOperation> operations, List<HoodieCompactionPlan> pendingCompactionPlans) {
    // Iterate through the operations and accept operations as long as we are within the configured target partitions
    // limit
    return operations.stream()
        .collect(Collectors.groupingBy(HoodieCompactionOperation::getPartitionPath)).entrySet().stream()
        .sorted(Map.Entry.comparingByKey(comparator)).limit(writeConfig.getTargetPartitionsPerDayBasedCompaction())
        .flatMap(e -> e.getValue().stream()).collect(Collectors.toList());
  }

  @Override
  public List<String> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    return allPartitionPaths.stream().map(partition -> partition.replace("/", "-"))
        .sorted(Comparator.reverseOrder()).map(partitionPath -> partitionPath.replace("-", "/"))
        .collect(Collectors.toList()).subList(0, writeConfig.getTargetPartitionsPerDayBasedCompaction());
  }

  /**
   * If is Hive style partition path, convert it to regular partition path. e.g. year=2019/month=11/day=24 => 2019/11/24
   */
  protected static String getPartitionPathWithoutPartitionKeys(String partitionPath) {
    if (partitionPath.contains("=")) {
      return partitionPath.replaceFirst(".*?=", "").replaceAll("/.*?=", "/");
    }
    return partitionPath;
  }
}
