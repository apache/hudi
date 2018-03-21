/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.io.strategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.Maps;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.io.compact.CompactionOperation;
import com.uber.hoodie.io.compact.strategy.BoundedIOCompactionStrategy;
import com.uber.hoodie.io.compact.strategy.DayBasedCompactionStrategy;
import com.uber.hoodie.io.compact.strategy.LogFileSizeBasedCompactionStrategy;
import com.uber.hoodie.io.compact.strategy.UnBoundedCompactionStrategy;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.Test;

public class TestHoodieCompactionStrategy {

  private static final long MB = 1024 * 1024L;
  private String[] partitionPaths = {"2017/01/01", "2017/01/02", "2017/01/03"};

  @Test
  public void testUnBounded() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));
    UnBoundedCompactionStrategy strategy = new UnBoundedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).build()).build();
    List<CompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<CompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations);
    assertEquals("UnBounded should not re-order or filter", operations, returned);
  }

  @Test
  public void testBoundedIOSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));
    BoundedIOCompactionStrategy strategy = new BoundedIOCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy)
            .withTargetIOPerCompactionInMB(400).build()).build();
    List<CompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<CompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations);

    assertTrue("BoundedIOCompaction should have resulted in fewer compactions",
        returned.size() < operations.size());
    assertEquals("BoundedIOCompaction should have resulted in 2 compactions being chosen", 2,
        returned.size());
    // Total size of all the log files
    Long returnedSize = returned.stream()
        .map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB)).map(s -> (Long) s)
        .reduce((size1, size2) -> size1 + size2).orElse(0L);
    assertEquals("Should chose the first 2 compactions which should result in a total IO of 690 MB",
        610, (long) returnedSize);
  }

  @Test
  public void testLogFileSizeCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));
    LogFileSizeBasedCompactionStrategy strategy = new LogFileSizeBasedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy)
            .withTargetIOPerCompactionInMB(400).build()).build();
    List<CompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<CompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations);

    assertTrue("LogFileSizeBasedCompactionStrategy should have resulted in fewer compactions",
        returned.size() < operations.size());
    assertEquals("LogFileSizeBasedCompactionStrategy should have resulted in 1 compaction", 1,
        returned.size());
    // Total size of all the log files
    Long returnedSize = returned.stream()
        .map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB)).map(s -> (Long) s)
        .reduce((size1, size2) -> size1 + size2).orElse(0L);
    assertEquals("Should chose the first 2 compactions which should result in a total IO of 690 MB",
        1204, (long) returnedSize);
  }

  @Test
  public void testPartitionAwareCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));
    DayBasedCompactionStrategy strategy = new DayBasedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy)
            .withTargetIOPerCompactionInMB(400).build()).build();
    List<CompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<CompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations);

    assertTrue("DayBasedCompactionStrategy should have resulted in fewer compactions",
        returned.size() < operations.size());

    int comparision = strategy.getComparator()
        .compare(returned.get(returned.size() - 1), returned.get(0));
    // Either the partition paths are sorted in descending order or they are equal
    assertTrue("DayBasedCompactionStrategy should sort partitions in descending order",
        comparision >= 0);
  }

  private List<CompactionOperation> createCompactionOperations(HoodieWriteConfig config,
      Map<Long, List<Long>> sizesMap) {
    List<CompactionOperation> operations = Lists.newArrayList(sizesMap.size());
    sizesMap.forEach((k, v) -> {
      operations.add(new CompactionOperation(TestHoodieDataFile.newDataFile(k),
          partitionPaths[new Random().nextInt(partitionPaths.length - 1)],
          v.stream().map(TestHoodieLogFile::newLogFile).collect(Collectors.toList()), config));
    });
    return operations;
  }
}
