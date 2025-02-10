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

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

/**
 * This strategy orders compactions in reverse order of creation of Hive Partitions. It helps to compact data in latest
 * partitions first and then older capped at the Total_IO allowed.
 */
public class DayBasedCompactionStrategy extends BoundedIOCompactionStrategy {

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

  /**
   * For DayBasedCompactionStrategy, we will not record rolled partitions as missing partitions.
   */
  @Override
  public Pair<List<String>, List<String>> filterPartitionPaths(HoodieWriteConfig writeConfig, List<String> allPartitionPaths) {
    List<String> res = allPartitionPaths.stream().sorted(comparator)
        .collect(Collectors.toList()).subList(0, Math.min(allPartitionPaths.size(),
            writeConfig.getTargetPartitionsPerDayBasedCompaction()));
    return Pair.of(res, Collections.emptyList());
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
