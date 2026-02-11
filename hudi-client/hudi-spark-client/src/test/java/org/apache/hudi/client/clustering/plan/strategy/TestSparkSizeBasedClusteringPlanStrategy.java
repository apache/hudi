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

package org.apache.hudi.client.clustering.plan.strategy;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieSliceInfo;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkCopyOnWriteTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestSparkSizeBasedClusteringPlanStrategy {

  @Mock
  HoodieSparkCopyOnWriteTable table;
  @Mock
  HoodieSparkEngineContext context;

  @Test
  public void testBuildClusteringGroup() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(2000)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(500)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategy = new SparkSizeBasedClusteringPlanStrategy(table, context, config);

    ArrayList<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSlice(200));
    fileSlices.add(createFileSlice(200));
    fileSlices.add(createFileSlice(300));
    fileSlices.add(createFileSlice(300));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));
    fileSlices.add(createFileSlice(400));

    Stream<HoodieClusteringGroup> clusteringGroupStream = (Stream<HoodieClusteringGroup>) planStrategy.buildClusteringGroupsForPartition("p0", fileSlices).getLeft();
    List<HoodieClusteringGroup> clusteringGroups = clusteringGroupStream.collect(Collectors.toList());

    // FileSlices will be divided into two clusteringGroups
    Assertions.assertEquals(2, clusteringGroups.size());

    // First group: 400, 400, 400, 400, 300, and they will be merged into 2 files
    Assertions.assertEquals(5, clusteringGroups.get(0).getSlices().size());
    Assertions.assertEquals(2, clusteringGroups.get(0).getNumOutputFileGroups());

    // Second group: 300, 200, 200, and they will be merged into 1 file
    Assertions.assertEquals(3, clusteringGroups.get(1).getSlices().size());
    Assertions.assertEquals(1, clusteringGroups.get(1).getNumOutputFileGroups());
  }

  @Test
  public void testEarlierInstantsFirstEnabled() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(3000)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(50)
            .withEarlierInstantsFirst(true)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategy = new SparkSizeBasedClusteringPlanStrategy(table, context, config);

    ArrayList<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSliceWithCommitTime(400, "003"));
    fileSlices.add(createFileSliceWithCommitTime(500, "001"));
    fileSlices.add(createFileSliceWithCommitTime(200, "001"));
    fileSlices.add(createFileSliceWithCommitTime(100, "002"));
    fileSlices.add(createFileSliceWithCommitTime(300, "002"));

    Stream<HoodieClusteringGroup> clusteringGroupStream =
        (Stream<HoodieClusteringGroup>) planStrategy.buildClusteringGroupsForPartition("p0", fileSlices).getLeft();
    List<HoodieClusteringGroup> clusteringGroups = clusteringGroupStream.collect(Collectors.toList());

    Assertions.assertTrue(clusteringGroups.size() > 0);

    List<HoodieSliceInfo> allSlices = new ArrayList<>();
    for (HoodieClusteringGroup group : clusteringGroups) {
      allSlices.addAll(group.getSlices());
    }

    Assertions.assertEquals(5, allSlices.size());

    List<String> actualOrder = new ArrayList<>();
    for (HoodieSliceInfo slice : allSlices) {
      actualOrder.add(slice.getFileId());
    }

    Assertions.assertEquals(5, actualOrder.size());
  }

  @Test
  public void testEarlierInstantsFirstDisabled() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(2000)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(500)
            .withEarlierInstantsFirst(false)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategy = new SparkSizeBasedClusteringPlanStrategy(table, context, config);

    ArrayList<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSliceWithCommitTime(400, "003"));
    fileSlices.add(createFileSliceWithCommitTime(200, "001"));
    fileSlices.add(createFileSliceWithCommitTime(300, "002"));
    fileSlices.add(createFileSliceWithCommitTime(500, "001"));

    Stream<HoodieClusteringGroup> clusteringGroupStream =
        (Stream<HoodieClusteringGroup>) planStrategy.buildClusteringGroupsForPartition("p0", fileSlices).getLeft();
    List<HoodieClusteringGroup> clusteringGroups = clusteringGroupStream.collect(Collectors.toList());

    Assertions.assertTrue(clusteringGroups.size() > 0);

    HoodieClusteringGroup firstGroup = clusteringGroups.get(0);
    Assertions.assertTrue(firstGroup.getSlices().size() > 0);
  }

  @Test
  public void testCommitTimeOrderingWithSameSizes() {
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(300)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(1000)
            .withEarlierInstantsFirst(true)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategy = new SparkSizeBasedClusteringPlanStrategy(table, context, config);

    ArrayList<FileSlice> fileSlices = new ArrayList<>();
    fileSlices.add(createFileSliceWithCommitTime(300, "003"));
    fileSlices.add(createFileSliceWithCommitTime(300, "001"));
    fileSlices.add(createFileSliceWithCommitTime(300, "002"));

    Stream<HoodieClusteringGroup> clusteringGroupStream =
        (Stream<HoodieClusteringGroup>) planStrategy.buildClusteringGroupsForPartition("p0", fileSlices).getLeft();
    List<HoodieClusteringGroup> clusteringGroups = clusteringGroupStream.collect(Collectors.toList());

    Assertions.assertTrue(clusteringGroups.get(0).getSlices().get(0).getDataFilePath().contains("001"));
    Assertions.assertTrue(clusteringGroups.get(1).getSlices().get(0).getDataFilePath().contains("002"));
    Assertions.assertTrue(clusteringGroups.get(2).getSlices().get(0).getDataFilePath().contains("003"));
  }

  @Test
  public void testSortingBehaviorComparisonWithAndWithoutEarlierInstantsFirst() {
    HoodieWriteConfig configEnabled = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(200)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(1000)
            .withEarlierInstantsFirst(true)
            .build())
        .build();

    HoodieWriteConfig configDisabled = HoodieWriteConfig.newBuilder()
        .withPath("")
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringPlanStrategyClass(SparkSizeBasedClusteringPlanStrategy.class.getName())
            .withClusteringMaxBytesInGroup(200)
            .withClusteringTargetFileMaxBytes(1000)
            .withClusteringPlanSmallFileLimit(1000)
            .withEarlierInstantsFirst(false)
            .build())
        .build();

    SparkSizeBasedClusteringPlanStrategy planStrategyEnabled = new SparkSizeBasedClusteringPlanStrategy(table, context, configEnabled);
    SparkSizeBasedClusteringPlanStrategy planStrategyDisabled = new SparkSizeBasedClusteringPlanStrategy(table, context, configDisabled);

    ArrayList<FileSlice> fileSlicesEnabled = new ArrayList<>();
    ArrayList<FileSlice> fileSlicesDisabled = new ArrayList<>();

    String[] commitTimes = {"001", "002", "003"};
    long[] fileSizes = {100, 200, 150};

    for (int i = 0; i < commitTimes.length; i++) {
      fileSlicesEnabled.add(createFileSliceWithCommitTime(fileSizes[i], commitTimes[i]));
      fileSlicesDisabled.add(createFileSliceWithCommitTime(fileSizes[i], commitTimes[i]));
    }

    Stream<HoodieClusteringGroup> streamEnabled =
        (Stream<HoodieClusteringGroup>) planStrategyEnabled.buildClusteringGroupsForPartition("p0", fileSlicesEnabled).getLeft();
    List<HoodieClusteringGroup> groupsEnabled = streamEnabled.collect(Collectors.toList());

    Stream<HoodieClusteringGroup> streamDisabled =
        (Stream<HoodieClusteringGroup>) planStrategyDisabled.buildClusteringGroupsForPartition("p0", fileSlicesDisabled).getLeft();
    List<HoodieClusteringGroup> groupsDisabled = streamDisabled.collect(Collectors.toList());

    Assertions.assertTrue(groupsEnabled.size() > 0);
    Assertions.assertTrue(groupsDisabled.size() > 0);

    int totalFilesEnabled = groupsEnabled.stream().mapToInt(g -> g.getSlices().size()).sum();
    int totalFilesDisabled = groupsDisabled.stream().mapToInt(g -> g.getSlices().size()).sum();

    Assertions.assertEquals(totalFilesEnabled, totalFilesDisabled);
    Assertions.assertEquals(3, totalFilesEnabled);

    Assertions.assertTrue(groupsEnabled.get(0).getSlices().get(0).getDataFilePath().contains("001"));
    Assertions.assertTrue(groupsEnabled.get(1).getSlices().get(0).getDataFilePath().contains("002"));
    Assertions.assertTrue(groupsEnabled.get(2).getSlices().get(0).getDataFilePath().contains("003"));

    Assertions.assertTrue(groupsDisabled.get(0).getSlices().get(0).getDataFilePath().contains("002"));
    Assertions.assertTrue(groupsDisabled.get(1).getSlices().get(0).getDataFilePath().contains("003"));
    Assertions.assertTrue(groupsDisabled.get(2).getSlices().get(0).getDataFilePath().contains("001"));
  }

  private FileSlice createFileSlice(long baseFileSize) {
    return createFileSliceWithCommitTime(baseFileSize, "001");
  }

  private FileSlice createFileSliceWithCommitTime(long baseFileSize, String commitTime) {
    String fileId = FSUtils.createNewFileId(FSUtils.createNewFileIdPfx(), 0);
    FileSlice fs = new FileSlice("p0", commitTime, fileId);
    String basePath = "/test/path/" + fileId + "_" + commitTime + ".parquet";
    HoodieBaseFile f = new HoodieBaseFile(basePath);
    f.setFileLen(baseFileSize);
    fs.setBaseFile(f);
    return fs;
  }
}
