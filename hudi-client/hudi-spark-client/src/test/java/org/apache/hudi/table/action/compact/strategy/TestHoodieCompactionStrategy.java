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
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import org.junit.jupiter.api.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieCompactionStrategy {

  private static final long MB = 1024 * 1024L;
  private static final Random RANDOM = new Random();
  private String[] partitionPaths = {"2017/01/01", "2017/01/02", "2017/01/03"};

  @Test
  public void testUnBounded() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));
    UnBoundedCompactionStrategy strategy = new UnBoundedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp")
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());
    assertEquals(operations, returned, "UnBounded should not re-order or filter");
  }

  @Test
  public void testBoundedIOSimple() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));
    BoundedIOCompactionStrategy strategy = new BoundedIOCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).withTargetIOPerCompactionInMB(400).build())
        .build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(), "BoundedIOCompaction should have resulted in fewer compactions");
    assertEquals(2, returned.size(), "BoundedIOCompaction should have resulted in 2 compactions being chosen");
    // Total size of all the log files
    Long returnedSize = returned.stream().map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB))
        .map(Double::longValue).reduce(Long::sum).orElse(0L);
    assertEquals(610, (long) returnedSize,
        "Should chose the first 2 compactions which should result in a total IO of 690 MB");
  }

  @Test
  public void testLogFileSizeCompactionSimple() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));
    LogFileSizeBasedCompactionStrategy strategy = new LogFileSizeBasedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).withTargetIOPerCompactionInMB(1205)
                .withLogFileSizeThresholdBasedCompaction(100 * 1024 * 1024).build())
        .build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(),
        "LogFileSizeBasedCompactionStrategy should have resulted in fewer compactions");
    assertEquals(2, returned.size(), "LogFileSizeBasedCompactionStrategy should have resulted in 2 compaction");
    // Total size of all the log files
    Long returnedSize = returned.stream().map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB))
        .map(Double::longValue).reduce(Long::sum).orElse(0L);
    assertEquals(1594, (long) returnedSize,
        "Should chose the first 2 compactions which should result in a total IO of 1594 MB");
  }

  @Test
  public void testDayBasedCompactionSimple() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));

    Map<Long, String> keyToPartitionMap = Collections.unmodifiableMap(new HashMap<Long, String>() {
      {
        put(120 * MB, partitionPaths[2]);
        put(110 * MB, partitionPaths[2]);
        put(100 * MB, partitionPaths[1]);
        put(90 * MB, partitionPaths[0]);
      }
    });

    DayBasedCompactionStrategy strategy = new DayBasedCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(1).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(),
        "DayBasedCompactionStrategy should have resulted in fewer compactions");
    assertEquals(2, returned.size(), "DayBasedCompactionStrategy should have resulted in fewer compactions");

    int comparison = strategy.getComparator().compare(returned.get(returned.size() - 1).getPartitionPath(),
        returned.get(0).getPartitionPath());
    // Either the partition paths are sorted in descending order or they are equal
    assertTrue(comparison >= 0, "DayBasedCompactionStrategy should sort partitions in descending order");
  }

  @Test
  public void testBoundedPartitionAwareCompactionSimple() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(70 * MB, Collections.singletonList(MB));
    sizesMap.put(80 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));

    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
    Date today = new Date();
    String currentDay = format.format(today);

    String currentDayMinus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-1));
    String currentDayMinus2 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-2));
    String currentDayMinus3 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-3));
    String currentDayPlus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(1));
    String currentDayPlus5 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(5));

    Map<Long, String> keyToPartitionMap = Collections.unmodifiableMap(new HashMap<Long, String>() {
      {
        put(120 * MB, currentDay);
        put(110 * MB, currentDayMinus1);
        put(100 * MB, currentDayMinus2);
        put(80 * MB, currentDayMinus3);
        put(90 * MB, currentDayPlus1);
        put(70 * MB, currentDayPlus5);
      }
    });

    BoundedPartitionAwareCompactionStrategy strategy = new BoundedPartitionAwareCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(2).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(),
        "BoundedPartitionAwareCompactionStrategy should have resulted in fewer compactions");
    assertEquals(5, returned.size(),
        "BoundedPartitionAwareCompactionStrategy should have resulted in fewer compactions");

    int comparison = strategy.getComparator().compare(returned.get(returned.size() - 1).getPartitionPath(),
        returned.get(0).getPartitionPath());
    // Either the partition paths are sorted in descending order or they are equal
    assertTrue(comparison >= 0, "BoundedPartitionAwareCompactionStrategy should sort partitions in descending order");
  }

  @Test
  public void testUnboundedPartitionAwareCompactionSimple() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(MB));
    sizesMap.put(80 * MB, Collections.singletonList(MB));
    sizesMap.put(70 * MB, Collections.singletonList(MB));
    sizesMap.put(90 * MB, Collections.singletonList(1024 * MB));

    SimpleDateFormat format = new SimpleDateFormat("yyyy/MM/dd");
    Date today = new Date();
    String currentDay = format.format(today);

    String currentDayMinus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-1));
    String currentDayMinus2 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-2));
    String currentDayMinus3 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(-3));
    String currentDayPlus1 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(1));
    String currentDayPlus5 = format.format(BoundedPartitionAwareCompactionStrategy.getDateAtOffsetFromToday(5));

    Map<Long, String> keyToPartitionMap = Collections.unmodifiableMap(new HashMap<Long, String>() {
      {
        put(120 * MB, currentDay);
        put(110 * MB, currentDayMinus1);
        put(100 * MB, currentDayMinus2);
        put(80 * MB, currentDayMinus3);
        put(90 * MB, currentDayPlus1);
        put(70 * MB, currentDayPlus5);
      }
    });

    UnBoundedPartitionAwareCompactionStrategy strategy = new UnBoundedPartitionAwareCompactionStrategy();
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withCompactionStrategy(strategy).withTargetPartitionsPerDayBasedCompaction(2).build()).build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap, keyToPartitionMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(),
        "UnBoundedPartitionAwareCompactionStrategy should not include last "
            + writeConfig.getTargetPartitionsPerDayBasedCompaction() + " partitions or later partitions from today");
    assertEquals(1, returned.size(),
        "BoundedPartitionAwareCompactionStrategy should have resulted in 1 compaction");
  }

  @Test
  public void testLogFileLengthBasedCompactionStrategy() {
    Map<Long, List<Long>> sizesMap = new HashMap<>();
    sizesMap.put(120 * MB, Arrays.asList(60 * MB, 10 * MB, 80 * MB));
    sizesMap.put(110 * MB, new ArrayList<>());
    sizesMap.put(100 * MB, Collections.singletonList(2048 * MB));
    sizesMap.put(90 * MB, Arrays.asList(512 * MB, 512 * MB));
    LogFileLengthBasedCompactionStrategy strategy = new LogFileLengthBasedCompactionStrategy();
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder().withPath("/tmp").withCompactionConfig(
            HoodieCompactionConfig.newBuilder().withCompactionStrategy(strategy).withTargetIOPerCompactionInMB(1024)
                .withLogFileLengthThresholdBasedCompaction(2).build())
        .build();
    List<HoodieCompactionOperation> operations = createCompactionOperations(writeConfig, sizesMap);
    List<HoodieCompactionOperation> returned = strategy.orderAndFilter(writeConfig, operations, new ArrayList<>());

    assertTrue(returned.size() < operations.size(),
        "LogFileLengthBasedCompactionStrategy should have resulted in fewer compactions");
    assertEquals(2, returned.size(), "LogFileLengthBasedCompactionStrategy should have resulted in 2 compaction");

    // Delte log File length
    Integer allFileLength = returned.stream().map(s -> s.getDeltaFilePaths().size())
        .reduce(Integer::sum).orElse(0);

    assertEquals(5, allFileLength);
    assertEquals(3, returned.get(0).getDeltaFilePaths().size());
    assertEquals(2, returned.get(1).getDeltaFilePaths().size());
    // Total size of all the log files
    Long returnedSize = returned.stream().map(s -> s.getMetrics().get(BoundedIOCompactionStrategy.TOTAL_IO_MB))
        .map(Double::longValue).reduce(Long::sum).orElse(0L);
    // TOTAL_IO_MB: ( 120 + 90 ) * 2 + 521 + 521 + 60 + 10 + 80
    assertEquals(1594, (long) returnedSize,
        "Should chose the first 2 compactions which should result in a total IO of 1594 MB");


  }

  private List<HoodieCompactionOperation> createCompactionOperations(HoodieWriteConfig config,
                                                                     Map<Long, List<Long>> sizesMap) {
    Map<Long, String> keyToPartitionMap = sizesMap.keySet().stream()
        .map(e -> Pair.of(e, partitionPaths[RANDOM.nextInt(partitionPaths.length - 1)]))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
    return createCompactionOperations(config, sizesMap, keyToPartitionMap);
  }

  private List<HoodieCompactionOperation> createCompactionOperations(HoodieWriteConfig config,
                                                                     Map<Long, List<Long>> sizesMap, Map<Long, String> keyToPartitionMap) {
    List<HoodieCompactionOperation> operations = new ArrayList<>(sizesMap.size());

    sizesMap.forEach((k, v) -> {
      HoodieBaseFile df = TestHoodieBaseFile.newDataFile(k);
      String partitionPath = keyToPartitionMap.get(k);
      List<HoodieLogFile> logFiles = v.stream().map(TestHoodieLogFile::newLogFile).collect(Collectors.toList());
      FileSlice slice = new FileSlice(new HoodieFileGroupId(partitionPath, df.getFileId()), df.getCommitTime());
      slice.setBaseFile(df);
      logFiles.stream().forEach(f -> slice.addLogFile(f));
      operations.add(new HoodieCompactionOperation(df.getCommitTime(),
          logFiles.stream().map(s -> s.getPath().toString()).collect(Collectors.toList()), df.getPath(), df.getFileId(),
          partitionPath,
          config.getCompactionStrategy().captureMetrics(config, slice),
          df.getBootstrapBaseFile().map(BaseFile::getPath).orElse(null))
      );
    });
    return operations;
  }

  public static class TestHoodieBaseFile extends HoodieBaseFile {

    private final long size;

    public TestHoodieBaseFile(long size) {
      super("/tmp/XYXYXYXYXYYX_11_20180918020003" + HoodieTableConfig.BASE_FILE_FORMAT.defaultValue().getFileExtension());
      this.size = size;
    }

    public static HoodieBaseFile newDataFile(long size) {
      return new TestHoodieBaseFile(size);
    }

    @Override
    public String getPath() {
      return "/tmp/test";
    }

    @Override
    public String getFileId() {
      return UUID.randomUUID().toString();
    }

    @Override
    public String getCommitTime() {
      return "100";
    }

    @Override
    public long getFileSize() {
      return size;
    }
  }

  public static class TestHoodieLogFile extends HoodieLogFile {

    private static int version = 0;
    private final long size;

    public TestHoodieLogFile(long size) {
      super("/tmp/.ce481ee7-9e53-4a2e-999-f9e295fa79c0_20180919184844.log." + version++);
      this.size = size;
    }

    public static HoodieLogFile newLogFile(long size) {
      return new TestHoodieLogFile(size);
    }

    @Override
    public long getFileSize() {
      return size;
    }
  }
}
