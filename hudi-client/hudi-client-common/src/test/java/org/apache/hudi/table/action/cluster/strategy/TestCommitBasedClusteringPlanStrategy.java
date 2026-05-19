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

package org.apache.hudi.table.action.cluster.strategy;

import static org.apache.hudi.table.action.cluster.strategy.CommitBasedClusteringPlanStrategy.CLUSTERING_COMMIT_CHECKPOINT_KEY;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.hudi.avro.model.HoodieClusteringGroup;
import org.apache.hudi.avro.model.HoodieSliceInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v1.InstantComparatorV1;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

/**
 * Tests for {@link CommitBasedClusteringPlanStrategy} focusing on
 * generateClusteringPlan functionality.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class TestCommitBasedClusteringPlanStrategy {

  @Mock
  private HoodieTable<Object, Object, Object, Object> hoodieTable;

  @Mock
  private HoodieEngineContext engineContext;

  private HoodieWriteConfig writeConfig;

  @Mock
  private HoodieTableMetaClient metaClient;

  @Mock
  private HoodieTimeline timeline;

  @Mock
  private HoodieActiveTimeline activeTimeline;

  @Mock
  private HoodieStorage storage;

  private CommitBasedClusteringPlanStrategy<Object, Object, Object, Object> strategy;

  private static final long MAX_BYTES_PER_GROUP = 1000000L;

  @BeforeEach
  void setUp() {
    this.initializeClusteringPlanStrategy(true);
  }

  @Test
  void testGenerateClusteringPlanWithNoCommits() {
    // Setup: No commits in timeline
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(timeline.getInstants()).thenReturn(new ArrayList<>());

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    assertFalse(result.isPresent());

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertNull(extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithoutCheckpoint() throws IOException {
    this.initializeClusteringPlanStrategy(false);
    // Setup: Single commit with file slices
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);

    // Create a single commit instant
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20231201120000", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant);
    when(timeline.getInstants()).thenReturn(instants);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    String fileId = "a9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String filepath = "path/to/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setPath(filepath);
    writeStat.setFileId(fileId);
    writeStat.setPartitionPath("partition1");
    commitMetadata.addWriteStat("partition1", writeStat);
    try {
      when(activeTimeline.readCommitMetadata(commitInstant)).thenReturn(commitMetadata);
    } catch (Exception e) {
      // Handle exception for testing
    }

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    assertFalse(result.isPresent());
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testGenerateClusteringPlanWithSingleCommitSinglePartition(boolean largerCommit) throws IOException {
    // Setup: Single commit with file slices
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);
    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId3 = "c9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String file1path = "path/to/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file2path = "path/to/b9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file3path = "path/to/c9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    List<String> fileIds = new ArrayList<>();
    fileIds.add(fileId1);
    fileIds.add(fileId2);
    fileIds.add(fileId3);
    List<String> filePaths = new ArrayList<>();
    filePaths.add(file1path);
    filePaths.add(file2path);
    filePaths.add(file3path);
    for (String filePath : filePaths) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), filePath);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(filePath));
      if (largerCommit) {
        // for large commit, set large file size that two files construct a group
        when(pathInfo.getLength()).thenReturn(MAX_BYTES_PER_GROUP / 2);
      } else {
        // Otherwise, all files in a commit is smaller than max bytes per group
        when(pathInfo.getLength()).thenReturn(100L);
      }
    }

    // Create a single commit instant
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "20231201120000", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant);
    when(timeline.getInstants()).thenReturn(instants);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (String filePath : filePaths) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPath(filePath);
      writeStat.setFileId(fileIds.get(filePaths.indexOf(filePath)));
      writeStat.setPartitionPath("partition1");
      commitMetadata.addWriteStat("partition1", writeStat);
    }
    try {
      when(activeTimeline.readCommitMetadata(commitInstant)).thenReturn(commitMetadata);
    } catch (Exception e) {
      // Handle exception for testing
    }

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    // Validate that the result is present and contains the expected clustering plan
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    List<String> allSlicePaths = plan.getInputGroups().stream()
        .flatMap(g -> g.getSlices().stream())
        .map(HoodieSliceInfo::getDataFilePath)
        .collect(Collectors.toList());
    if (!largerCommit) {
      assertEquals(1, plan.getInputGroups().size());
      HoodieClusteringGroup inputGroup = plan.getInputGroups().get(0);
      assertEquals(3, inputGroup.getSlices().size());
      assertThat(allSlicePaths, containsInAnyOrder(file1path, file2path, file3path));
    } else {
      assertEquals(2, plan.getInputGroups().size());
      HoodieClusteringGroup inputGroup1 = plan.getInputGroups().get(0);
      assertEquals(2, inputGroup1.getSlices().size());
      HoodieClusteringGroup inputGroup2 = plan.getInputGroups().get(1);
      assertEquals(1, inputGroup2.getSlices().size());
      assertThat(allSlicePaths, containsInAnyOrder(file1path, file2path, file3path));
    }

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(commitInstant.requestedTime(), extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithSingleCommitMultiPartitions() throws IOException {
    // Setup: Single commit with file slices
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);
    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String file1path = "partition1/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file2path = "partition2/b9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    List<String> fileIds = new ArrayList<>();
    fileIds.add(fileId1);
    fileIds.add(fileId2);
    List<String> filePaths = new ArrayList<>();
    filePaths.add(file1path);
    filePaths.add(file2path);
    for (String filePath : filePaths) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), filePath);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(filePath));
      when(pathInfo.getLength()).thenReturn(100L);
    }
    List<String> partitions = new ArrayList<>();
    partitions.add("partition1");
    partitions.add("partition2");

    // Create a commit instant
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
        "20231201120000", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant);
    when(timeline.getInstants()).thenReturn(instants);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (int i = 0; i < filePaths.size(); i++) {
      String filePath = filePaths.get(i);
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPath(filePath);
      writeStat.setFileId(fileIds.get(i));
      writeStat.setPartitionPath(partitions.get(i));
      commitMetadata.addWriteStat(partitions.get(i), writeStat);
    }
    try {
      when(activeTimeline.readCommitMetadata(commitInstant)).thenReturn(commitMetadata);
    } catch (Exception e) {
      // Handle exception for testing
    }

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    // Validate that the result is present and contains the expected clustering plan
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    assertEquals(2, plan.getInputGroups().size());
    HoodieClusteringGroup inputGroup1 = plan.getInputGroups().get(0);
    assertEquals(1, inputGroup1.getSlices().size());
    HoodieClusteringGroup inputGroup2 = plan.getInputGroups().get(1);
    assertEquals(1, inputGroup2.getSlices().size());
    List<String> clusteringFiles = new ArrayList<>();
    clusteringFiles.add(inputGroup1.getSlices().get(0).getDataFilePath());
    clusteringFiles.add(inputGroup2.getSlices().get(0).getDataFilePath());
    assertEquals(2, clusteringFiles.size());
    assertThat(clusteringFiles, containsInAnyOrder(filePaths.toArray(new String[0])));

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(commitInstant.requestedTime(), extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithMultiCommits() throws IOException {
    // Setup: Multiple commits with file slices
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);
    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId3 = "c9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId4 = "d9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String file1path = "p1/to/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file2path = "p1/to/b9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file3path = "p1/to/c9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    String file4path = "p2/to/d9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20250809161811178.parquet";
    List<String> fileIds = new ArrayList<>();
    fileIds.add(fileId1);
    fileIds.add(fileId2);
    fileIds.add(fileId3);
    fileIds.add(fileId4);
    List<String> filePaths = new ArrayList<>();
    filePaths.add(file1path);
    filePaths.add(file2path);
    filePaths.add(file3path);
    filePaths.add(file4path);
    for (String filePath : filePaths) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), filePath);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(filePath));
      when(pathInfo.getLength()).thenReturn(100L);
    }
    List<String> partitions = new ArrayList<>();
    partitions.add("p1");
    partitions.add("p1");
    partitions.add("p1");
    partitions.add("p2");

    // Create two commit instants
    HoodieInstant commitInstant1 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
        "20231201120000", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant commitInstant2 = new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION,
        "20231201120001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant1);
    instants.add(commitInstant2);
    when(timeline.getInstants()).thenReturn(instants);

    for (int i = 0; i < instants.size(); i++) {
      HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
      for (int j = 0; j < 2; j++) {
        int k = i * 2 + j;
        String filePath = filePaths.get(k);
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPath(filePath);
        writeStat.setFileId(fileIds.get(k));
        writeStat.setPartitionPath(partitions.get(k));
        commitMetadata.addWriteStat(partitions.get(k), writeStat);
      }
      try {
        when(activeTimeline.readCommitMetadata(instants.get(i))).thenReturn(commitMetadata);
      } catch (Exception e) {
        // Handle exception for testing
      }
    }

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    // Validate that the result is present and contains the expected clustering plan
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    assertEquals(2, plan.getInputGroups().size());
    HoodieClusteringGroup inputGroup1 = plan.getInputGroups().get(0);
    HoodieClusteringGroup inputGroup2 = plan.getInputGroups().get(1);
    assertNotEquals(inputGroup1.getSlices().size(), inputGroup2.getSlices().size());
    assertTrue(inputGroup1.getSlices().size() == 3 || inputGroup1.getSlices().size() == 1);
    assertTrue(inputGroup2.getSlices().size() == 3 || inputGroup2.getSlices().size() == 1);
    List<HoodieClusteringGroup> inputGroups = new ArrayList<>();
    inputGroups.add(inputGroup1);
    inputGroups.add(inputGroup2);
    for (HoodieClusteringGroup inputGroup : inputGroups) {
      List<String> slicePaths = inputGroup.getSlices().stream()
          .map(HoodieSliceInfo::getDataFilePath)
          .collect(Collectors.toList());
      if (inputGroup.getSlices().size() == 3) {
        assertThat(slicePaths, containsInAnyOrder(file1path, file2path, file3path));
      } else {
        assertEquals(1, inputGroup.getSlices().size());
        assertEquals(file4path, slicePaths.get(0));
      }
    }

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(commitInstant2.requestedTime(), extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithReplaceCommit() throws IOException {
    // Setup: A regular commit writes 3 files, then a replace commit replaces one of them.
    // The clustering plan should NOT include file groups that were replaced.
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);

    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String fileId3 = "c9d3e0e8-89c2-4987-a692-5a61e99d4812_0";
    String file1path = "partition1/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20231201120000.parquet";
    String file2path = "partition1/b9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20231201120000.parquet";
    String file3path = "partition1/c9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_20231201120000.parquet";

    for (String filePath : new String[]{file1path, file2path, file3path}) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), filePath);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(filePath));
      when(pathInfo.getLength()).thenReturn(100L);
    }

    // Commit 1: regular commit writes 3 files
    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, "20231201120000", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    // Commit 2: replace commit that replaces fileId1
    HoodieInstant replaceInstant = new HoodieInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.REPLACE_COMMIT_ACTION, "20231201120001", InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant);
    instants.add(replaceInstant);
    when(timeline.getInstants()).thenReturn(instants);

    // Regular commit metadata with 3 files
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    for (String[] info : new String[][]{{file1path, fileId1}, {file2path, fileId2}, {file3path, fileId3}}) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPath(info[0]);
      writeStat.setFileId(info[1]);
      writeStat.setPartitionPath("partition1");
      commitMetadata.addWriteStat("partition1", writeStat);
    }
    when(activeTimeline.readCommitMetadata(commitInstant)).thenReturn(commitMetadata);

    // Replace commit metadata that replaces fileId1
    HoodieReplaceCommitMetadata replaceCommitMetadata = new HoodieReplaceCommitMetadata();
    replaceCommitMetadata.addReplaceFileId("partition1", fileId1);
    when(activeTimeline.readReplaceCommitMetadata(replaceInstant)).thenReturn(replaceCommitMetadata);

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    assertEquals(1, plan.getInputGroups().size());
    HoodieClusteringGroup inputGroup = plan.getInputGroups().get(0);
    // Only fileId2 and fileId3 should be in the plan, fileId1 was replaced
    assertEquals(2, inputGroup.getSlices().size());
    List<String> clusteringFileIds = new ArrayList<>();
    for (HoodieSliceInfo slice : inputGroup.getSlices()) {
      clusteringFileIds.add(slice.getFileId());
    }
    assertFalse(clusteringFileIds.contains(fileId1), "Replaced file group should not be in clustering plan");
    assertTrue(clusteringFileIds.contains(fileId2));
    assertTrue(clusteringFileIds.contains(fileId3));

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(replaceInstant.requestedTime(), extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithDeltaCommit() throws IOException {
    // Setup: Delta commit (MOR table) with base files and log files
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);

    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812";
    String commitTime = "20231201120000";

    // Base files
    String file1BasePath = "partition1/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_" + commitTime + ".parquet";
    // Log file for fileId1
    String file1LogPath = "partition1/.a9d3e0e8-89c2-4987-a692-5a61e99d4812_" + commitTime + ".log.1_1-0-1";
    // fileId2 has only a log file (update-only delta commit)
    String file2LogPath = "partition1/.b9d3e0e8-89c2-4987-a692-5a61e99d4812_" + commitTime + ".log.1_1-0-1";

    // Mock storage for all file paths
    List<String[]> fileInfos = new ArrayList<>();
    fileInfos.add(new String[]{file1BasePath, "false"});
    fileInfos.add(new String[]{file1LogPath, "true"});
    fileInfos.add(new String[]{file2LogPath, "true"});
    for (String[] fileInfo : fileInfos) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), fileInfo[0]);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(fileInfo[0]));
      when(pathInfo.getLength()).thenReturn(100L);
    }

    // Create a delta commit instant
    HoodieInstant deltaCommitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, commitTime, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(deltaCommitInstant);
    when(timeline.getInstants()).thenReturn(instants);

    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    // Base file write stat for fileId1
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPath(file1BasePath);
    writeStat1.setFileId(fileId1);
    writeStat1.setPartitionPath("partition1");
    commitMetadata.addWriteStat("partition1", writeStat1);
    // Log file write stat for fileId1
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setPath(file1LogPath);
    writeStat2.setFileId(fileId1);
    writeStat2.setPartitionPath("partition1");
    commitMetadata.addWriteStat("partition1", writeStat2);
    // Log file write stat for fileId2 (log-only)
    HoodieWriteStat writeStat3 = new HoodieWriteStat();
    writeStat3.setPath(file2LogPath);
    writeStat3.setFileId(fileId2);
    writeStat3.setPartitionPath("partition1");
    commitMetadata.addWriteStat("partition1", writeStat3);

    when(activeTimeline.readCommitMetadata(deltaCommitInstant)).thenReturn(commitMetadata);

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    // Should have 2 groups: fileId2 (log-only) gets parquetMaxFileSize in parent class
    // which exceeds MAX_BYTES_PER_GROUP, so it forms its own group
    assertEquals(2, plan.getInputGroups().size());
    int totalSlices = plan.getInputGroups().stream()
        .mapToInt(g -> g.getSlices().size())
        .sum();
    assertEquals(2, totalSlices);

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(commitTime, extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  @Test
  void testGenerateClusteringPlanWithMixedCommitAndDeltaCommit() throws IOException {
    // Setup: Mix of COMMIT and DELTA_COMMIT (MOR table scenario)
    when(hoodieTable.getMetaClient()).thenReturn(metaClient);
    when(metaClient.getCommitsTimeline()).thenReturn(timeline);
    when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
    when(timeline.findInstantsAfter(anyString())).thenReturn(timeline);
    when(timeline.filterCompletedInstants()).thenReturn(timeline);
    when(metaClient.getStorage()).thenReturn(storage);

    String fileId1 = "a9d3e0e8-89c2-4987-a692-5a61e99d4812";
    String fileId2 = "b9d3e0e8-89c2-4987-a692-5a61e99d4812";
    String commitTime1 = "20231201120000";
    String commitTime2 = "20231201120001";

    // Commit 1: regular commit with base file
    String file1BasePath = "partition1/a9d3e0e8-89c2-4987-a692-5a61e99d4812-0_1064-11-65065_" + commitTime1 + ".parquet";
    // Commit 2: delta commit with log file
    String file2LogPath = "partition1/.b9d3e0e8-89c2-4987-a692-5a61e99d4812_" + commitTime2 + ".log.1_1-0-1";

    for (String filePath : new String[]{file1BasePath, file2LogPath}) {
      StoragePath path = new StoragePath(this.writeConfig.getBasePath(), filePath);
      StoragePathInfo pathInfo = mock(StoragePathInfo.class);
      when(storage.getPathInfo(path)).thenReturn(pathInfo);
      when(pathInfo.getPath()).thenReturn(new StoragePath(filePath));
      when(pathInfo.getLength()).thenReturn(100L);
    }

    HoodieInstant commitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION, commitTime1, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant deltaCommitInstant = new HoodieInstant(HoodieInstant.State.COMPLETED,
        HoodieTimeline.DELTA_COMMIT_ACTION, commitTime2, InstantComparatorV1.REQUESTED_TIME_BASED_COMPARATOR);
    List<HoodieInstant> instants = new ArrayList<>();
    instants.add(commitInstant);
    instants.add(deltaCommitInstant);
    when(timeline.getInstants()).thenReturn(instants);

    // Commit metadata for regular commit
    HoodieCommitMetadata commitMetadata1 = new HoodieCommitMetadata();
    HoodieWriteStat writeStat1 = new HoodieWriteStat();
    writeStat1.setPath(file1BasePath);
    writeStat1.setFileId(fileId1);
    writeStat1.setPartitionPath("partition1");
    commitMetadata1.addWriteStat("partition1", writeStat1);
    when(activeTimeline.readCommitMetadata(commitInstant)).thenReturn(commitMetadata1);

    // Commit metadata for delta commit
    HoodieCommitMetadata commitMetadata2 = new HoodieCommitMetadata();
    HoodieWriteStat writeStat2 = new HoodieWriteStat();
    writeStat2.setPath(file2LogPath);
    writeStat2.setFileId(fileId2);
    writeStat2.setPartitionPath("partition1");
    commitMetadata2.addWriteStat("partition1", writeStat2);
    when(activeTimeline.readCommitMetadata(deltaCommitInstant)).thenReturn(commitMetadata2);

    Option<HoodieClusteringPlan> result = strategy.generateClusteringPlan();

    assertNotNull(result);
    assertTrue(result.isPresent());
    HoodieClusteringPlan plan = result.get();
    assertNotNull(plan);
    // fileId2 (log-only) gets parquetMaxFileSize in parent class which exceeds
    // MAX_BYTES_PER_GROUP, so it forms its own group separate from fileId1
    assertEquals(2, plan.getInputGroups().size());
    int totalSlices = plan.getInputGroups().stream()
        .mapToInt(g -> g.getSlices().size())
        .sum();
    assertEquals(2, totalSlices);

    Map<String, String> extraMetadata = strategy.getExtraMetadata();
    assertEquals(commitTime2, extraMetadata.get(CLUSTERING_COMMIT_CHECKPOINT_KEY));
  }

  private void initializeClusteringPlanStrategy(boolean withCheckpoint) {
    // Create a real HoodieWriteConfig instead of mocking it
    HoodieClusteringConfig.Builder clusteringConfigBuilder = HoodieClusteringConfig.newBuilder()
        .withClusteringMaxBytesInGroup(MAX_BYTES_PER_GROUP)
        .withClusteringMaxNumGroups(10)
        .withClusteringExecutionStrategyClass(
            "org.apache.hudi.client.clustering.run.strategy.SparkSingleFileSortExecutionStrategy");
    if (withCheckpoint) {
      clusteringConfigBuilder.withClusteringPlanEarliestCommitToCluster("20001201120000");
    }

    HoodieClusteringConfig clusteringConfig = clusteringConfigBuilder.build();

    this.writeConfig = HoodieWriteConfig.newBuilder()
        .withPath("/tmp/test")
        .withClusteringConfig(clusteringConfig)
        .build();
    this.strategy = new CommitBasedClusteringPlanStrategy<>(hoodieTable, engineContext, writeConfig);
  }
}
