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

package org.apache.hudi.io.strategy;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.compact.strategy.BoundedIOCompactionStrategy;
import org.apache.hudi.io.compact.strategy.BoundedPartitionAwareCompactionStrategy;
import org.apache.hudi.io.compact.strategy.DayBasedCompactionStrategy;
import org.apache.hudi.io.compact.strategy.LogFileSizeBasedCompactionStrategy;
import org.apache.hudi.io.compact.strategy.UnBoundedCompactionStrategy;
import org.apache.hudi.io.compact.strategy.UnBoundedPartitionAwareCompactionStrategy;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());
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
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(
        HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).withTargetIOPerCompactionInMB(400).build())
        .build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue("BoundedIOCompaction should have resulted in fewer compactions", returned.size() < operations.size());
    assertEquals("BoundedIOCompaction should have resulted in 2 compactions being chosen", 2, returned.size());
    // Total size of all the log files
    Long returnedSize = returned.stream().map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB))
        .map(s -> s.longValue()).reduce((size1, size2) -> size1 + size2).orElse(0L);
    assertEquals("Should chose the first 2 compactions which should result in a total IO of 690 MB", 610,
        (long) returnedSize);
  }

  @Test
  public void testLogFileSizeCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));
    LogFileSizeBasedCompactionStrategy strategy = new LogFileSizeBasedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(
        HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).withTargetIOPerCompactionInMB(400).build())
        .build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue("LogFileSizeBasedCompactionStrategy should have resulted in fewer compactions",
        returned.size() < operations.size());
    assertEquals("LogFileSizeBasedCompactionStrategy should have resulted in 1 compaction", 1, returned.size());
    // Total size of all the log files
    Long returnedSize = returned.stream().map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB))
        .map(s -> s.longValue()).reduce((size1, size2) -> size1 + size2).orElse(0L);
    assertEquals("Should chose the first 2 compactions which should result in a total IO of 690 MB", 1204,
        (long) returnedSize);
  }

  @Test
  public void testDayBasedCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));

    Map<Long, String> keyToPartitionMap = new ImmutableMap.Builder().put(120 * MB, partitionPaths[2])
        .put(110 * MB, partitionPaths[2]).put(100 * MB, partitionPaths[1]).put(90 * MB, partitionPaths[0]).build();

    DayBasedCompactionStrategy strategy = new DayBasedCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(1).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue("DayBasedCompactionStrategy should have resulted in fewer compactions",
        returned.size() < operations.size());
    Assert.assertEquals("DayBasedCompactionStrategy should have resulted in fewer compactions", returned.size(), 2);

    int comparision = strategy.getComparator().compare(returned.get(returned.size() - 1).getPartitionPath(),
        returned.get(0).getPartitionPath());
    // Either the partition paths are sorted in descending order or they are equal
    assertTrue("DayBasedCompactionStrategy should sort partitions in descending order", comparision >= 0);
  }

  @Test
  public void testBoundedPartitionAwareCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(70 * MB, Lists.newArrayList(MB));
    sizesMap.put(80 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));

    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
    Date today = new Date();
    String currentDay = format.format(today);

    String currentDayMinus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-1));
    String currentDayMinus2 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-2));
    String currentDayMinus3 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-3));
    String currentDayPlus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(1));
    String currentDayPlus5 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(5));

    Map<Long, String> keyToPartitionMap = new ImmutableMap.Builder().put(120 * MB, currentDay)
        .put(110 * MB, currentDayMinus1).put(100 * MB, currentDayMinus2).put(80 * MB, currentDayMinus3)
        .put(90 * MB, currentDayPlus1).put(70 * MB, currentDayPlus5).build();

    BoundedPartitionAwareCompactionStrategy strategy = new BoundedPartitionAwareCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(2).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue("BoundedPartitionAwareCompactionStrategy should have resulted in fewer compactions",
        returned.size() < operations.size());
    Assert.assertEquals("BoundedPartitionAwareCompactionStrategy should have resulted in fewer compactions",
        returned.size(), 5);

    int comparision = strategy.getComparator().compare(returned.get(returned.size() - 1).getPartitionPath(),
        returned.get(0).getPartitionPath());
    // Either the partition paths are sorted in descending order or they are equal
    assertTrue("BoundedPartitionAwareCompactionStrategy should sort partitions in descending order", comparision >= 0);
  }

  @Test
  public void testUnboundedPartitionAwareCompactionSimple() {
    Map<Long, List<Long>> sizesMap = Maps.newHashMap();
    sizesMap.put(120 * MB, Lists.newArrayList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, Lists.newArrayList());
    sizesMap.put(100 * MB, Lists.newArrayList(MB));
    sizesMap.put(80 * MB, Lists.newArrayList(MB));
    sizesMap.put(70 * MB, Lists.newArrayList(MB));
    sizesMap.put(90 * MB, Lists.newArrayList(1024 * MB));

    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
    Date today = new Date();
    String currentDay = format.format(today);

    String currentDayMinus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-1));
    String currentDayMinus2 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-2));
    String currentDayMinus3 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-3));
    String currentDayPlus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(1));
    String currentDayPlus5 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(5));

    Map<Long, String> keyToPartitionMap = new ImmutableMap.Builder().put(120 * MB, currentDay)
        .put(110 * MB, currentDayMinus1).put(100 * MB, currentDayMinus2).put(80 * MB, currentDayMinus3)
        .put(90 * MB, currentDayPlus1).put(70 * MB, currentDayPlus5).build();

    UnBoundedPartitionAwareCompactionStrategy strategy = new UnBoundedPartitionAwareCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(2).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(
        "UnBoundedPartitionAwareCompactionStrategy should not include last "
            + writeConfig.getTargetPartitionsPerDayBasedCompaction() + " partitions or later partitions from today",
        returned.size() < operations.size());
    Assert.assertEquals("BoundedPartitionAwareCompactionStrategy should have resulted in 1 compaction", returned.size(),
        1);
  }

  private List<HoodieCompactionOperation> createCompactionOperations(HoodieWriteConfig config,
      Map<Long, List<Long>> sizesMap) {
    Map<Long, String> keyToPartitionMap = sizesMap.entrySet().stream()
        .map(e -> Pair.of(e.getKey(), partitionPaths[new Random().nextInt(partitionPaths.length - 1)]))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return createCompactionOperations(config, sizesMap, keyToPartitionMap);
  }

  private List<HoodieCompactionOperation> createCompactionOperations(HoodieWriteConfig config,
      Map<Long, List<Long>> sizesMap, Map<Long, String> keyToPartitionMap) {
    List<HoodieCompactionOperation> operations = new ArrayList<>(sizesMap.size());

    sizesMap.forEach((k, v) -> {
      HoodieDataFile df = TestHoodieDataFile.newDataFile(k);
      String partitionPath = keyToPartitionMap.get(k);
      List<HoodieLogFile> logFiles = v.stream().map(TestHoodieLogFile::newLogFile).collect(Collectors.toList());
      operations.add(new HoodieCompactionOperation(df.getCommitTime(),
          logFiles.stream().map(s -> s.getPath().toString()).collect(Collectors.toList()), df.getPath(), df.getFileId(),
          partitionPath,
          config.getCompactionStrategy().captureMetrics(config, Option.of(df), partitionPath, logFiles)));
    });
    return operations;
  }
}
