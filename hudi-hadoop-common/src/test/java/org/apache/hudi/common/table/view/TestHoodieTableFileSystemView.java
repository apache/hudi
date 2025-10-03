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

package org.apache.hudi.common.table.view;

import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieClusteringStrategy;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex.IndexWriter;
import org.apache.hudi.common.bootstrap.index.hfile.HFileBootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.ClusteringUtils;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE_PREFIX;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie table file system view {@link HoodieTableFileSystemView}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestHoodieTableFileSystemView extends HoodieCommonTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieTableFileSystemView.class);
  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with bootstrap enable={0}";
  private static final String TEST_NAME_WITH_PARAMS_2 = "[{index}] Test with bootstrap enable={0}, preTableVersion8={1}";

  private static final String TEST_WRITE_TOKEN = "1-0-1";
  private static final String BOOTSTRAP_SOURCE_PATH = "/usr/warehouse/hive/data/tables/src1/";

  protected SyncableFileSystemView fsView;
  protected BaseFileOnlyView roView;
  protected SliceView rtView;

  public static Stream<Arguments> configParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  public static Stream<Arguments> configParams2x2() {
    return Arrays.stream(new Boolean[][] {{true, false}, {false, false}, {true, true}, {false, true}}).map(Arguments::of);
  }

  @BeforeEach
  public void setup() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType(), BOOTSTRAP_SOURCE_PATH, false);
    basePath = metaClient.getBasePath().toString();
    refreshFsView();
  }

  @AfterEach
  public void tearDown() throws Exception {
    closeFsView();
    cleanMetaClient();
  }

  protected void refreshFsView() throws IOException {
    refreshFsView(false);
  }

  protected void refreshFsView(boolean preTableVersion8) throws IOException {
    super.refreshFsView();
    closeFsView();
    if (preTableVersion8) {
      metaClient.getTableConfig().setTableVersion(HoodieTableVersion.SIX);
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), metaClient.getTableConfig().getProps());
      metaClient.reloadTableConfig();
    }
    fsView = getFileSystemView(metaClient.getActiveTimeline().filterCompletedAndCompactionInstants());
    roView = fsView;
    rtView = fsView;
  }

  private void closeFsView() {
    if (null != fsView) {
      fsView.close();
      fsView = null;
    }
  }

  /**
   * Test case for view generation on a file group where the only file-slice does not have data-file. This is the case
   * where upserts directly go to log-files
   */
  @ParameterizedTest
  @MethodSource("configParams")
  public void testViewForFileSlicesWithNoBaseFile(boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0, "2016/05/01", preTableVersion8);
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testViewForFileSlicesWithNoBaseFileNonPartitioned(boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0, "", preTableVersion8);
  }

  @Test
  public void testCloseHoodieTableFileSystemView() throws Exception {
    String instantTime1 = "1";
    String instantTime2 = "2";
    String clusteringInstantTime3 = "3";
    String clusteringInstantTime4 = "4";

    // prepare metadata
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add("fake_file_id");
    partitionToReplaceFileIds.put("fake_partition_path", replacedFileIds);

    // prepare Instants
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime2);
    HoodieInstant clusteringInstant3 =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringInstantTime3);
    HoodieInstant clusteringInstant4 =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, clusteringInstantTime4);
    HoodieCommitMetadata commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds,
            Option.empty(), WriteOperationType.CLUSTER, "", HoodieTimeline.REPLACE_COMMIT_ACTION);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, instant2, new HoodieCommitMetadata());
    saveAsCompleteCluster(commitTimeline, clusteringInstant3, (HoodieReplaceCommitMetadata) commitMetadata);
    saveAsCompleteCluster(
        commitTimeline,
        clusteringInstant4,
        (HoodieReplaceCommitMetadata) commitMetadata);

    refreshFsView();

    // Now create a scenario where archiving deleted replace commits (requested,inflight and replacecommit)
    StoragePath completeInstantPath = HoodieTestUtils.getCompleteInstantPath(
        metaClient.getStorage(), metaClient.getTimelinePath(),
        clusteringInstantTime3,
        HoodieTimeline.REPLACE_COMMIT_ACTION);

    boolean deleteReplaceCommit = metaClient.getStorage().deleteDirectory(completeInstantPath);
    boolean deleteClusterCommitRequested = new File(
        this.basePath + "/.hoodie/timeline/" + clusteringInstantTime3 + ".clustering.requested").delete();
    boolean deleteClusterCommitInflight = new File(
        this.basePath + "/.hoodie/timeline/" + clusteringInstantTime3 + ".clustering.inflight").delete();

    // confirm deleted
    assertTrue(deleteReplaceCommit && deleteClusterCommitInflight && deleteClusterCommitRequested);
    assertDoesNotThrow(() -> fsView.close());
    if ((fsView.getClass().isAssignableFrom(AbstractTableFileSystemView.class))) {
      // completionTimeQueryView will be set to null after close.
      Assertions.assertThrows(NullPointerException.class, () -> ((AbstractTableFileSystemView) fsView).getCompletionTime(""));
    }
  }

  protected void testViewForFileSlicesWithNoBaseFile(int expNumTotalFileSlices,
                                                     int expNumTotalDataFiles,
                                                     String partitionPath,
                                                     boolean preTableVersion8) throws Exception {
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime2 = "2";
    String deltaInstantTime3 = "3";

    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, deltaInstantTime2, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? deltaInstantTime2 : deltaInstantTime3, 1, TEST_WRITE_TOKEN);

    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    Paths.get(basePath, partitionPath, fileName2).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);
    HoodieInstant deltaInstant3 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime3);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant2, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant3, new HoodieCommitMetadata());

    refreshFsView();

    List<HoodieBaseFile> dataFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertTrue(dataFiles.isEmpty(), "No data file expected");
    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(deltaInstantTime2, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");

    // Check Merged File Slices API
    fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime3).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(deltaInstantTime2, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");

    // Verify latest merged file slice API for a given file id
    Option<FileSlice> fileSliceOpt = rtView.getLatestMergedFileSliceBeforeOrOn(partitionPath, deltaInstantTime3, fileId);
    assertTrue(fileSliceOpt.isPresent());
    fileSlice = fileSliceOpt.get();
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(deltaInstantTime2, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");

    // Check UnCompacted File Slices API
    fileSliceList = rtView.getLatestUnCompactedFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(deltaInstantTime2, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");

    assertEquals(expNumTotalFileSlices, rtView.getAllFileSlices(partitionPath).count(),
        "Total number of file-slices in view matches expected");
    assertEquals(expNumTotalDataFiles, roView.getAllBaseFiles(partitionPath).count(),
        "Total number of data-files in view matches expected");
    assertEquals(1, fsView.getAllFileGroups(partitionPath).count(),
        "Total number of file-groups in view matches expected");
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS_2)
  @MethodSource("configParams2x2")
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction(boolean testBootstrap, boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false, 2, 1, true, testBootstrap, preTableVersion8);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS_2)
  @MethodSource("configParams2x2")
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction(boolean testBootstrap, boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false, 2, 2, true, testBootstrap, true);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS_2)
  @MethodSource("configParams2x2")
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction(boolean testBootstrap, boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true, 2, 1, true, testBootstrap, preTableVersion8);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS_2)
  @MethodSource("configParams2x2")
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction(boolean testBootstrap, boolean preTableVersion8) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true, 2, 2, true, testBootstrap, preTableVersion8);
  }

  @Test
  public void testViewForFileSlicesWithPartitionMetadataFile() throws Exception {
    String partitionPath = "2023/09/13";
    new File(basePath + "/" + partitionPath).mkdirs();
    new File(basePath + "/" + partitionPath + "/"
        + HOODIE_PARTITION_METAFILE_PREFIX + ".parquet").mkdirs();

    // create 2 fileId in partition
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();

    List<FileSlice> fileSlices =
        fsView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(2, fileSlices.size());
    FileSlice fileSlice = fileSlices.get(0);
    assertEquals(commitTime1, fileSlice.getBaseInstantTime());
    assertEquals(2, fsView.getAllFileGroups(partitionPath).count());
  }

  @Test
  public void testViewForGetAllFileGroupsStateless() throws Exception {
    String partitionPath1 = "2023/11/22";
    new File(basePath + "/" + partitionPath1).mkdirs();
    new File(basePath + "/" + partitionPath1 + "/"
        + HOODIE_PARTITION_METAFILE_PREFIX + ".parquet").mkdirs();
    String partitionPath2 = "2023/11/23";
    new File(basePath + "/" + partitionPath2).mkdirs();
    new File(basePath + "/" + partitionPath2 + "/"
        + HOODIE_PARTITION_METAFILE_PREFIX + ".parquet").mkdirs();

    // create 2 fileId in partition1
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());

    // create 2 fileId in partition2
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();
    String commitTime2 = "2";
    String fileName3 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    String fileName4 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId4, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();

    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, new HoodieCommitMetadata());

    fsView.sync();
    // invokes the stateless API first then the normal API, assert the result equality with different file group objects
    List<HoodieFileGroup> actual1 = fsView.getAllFileGroupsStateless(partitionPath1)
        .sorted(Comparator.comparing(HoodieFileGroup::getFileGroupId)).collect(Collectors.toList());
    List<HoodieFileGroup> expected1 = fsView.getAllFileGroups(partitionPath1)
        .sorted(Comparator.comparing(HoodieFileGroup::getFileGroupId)).collect(Collectors.toList());
    for (int i = 0; i < expected1.size(); i++) {
      assertThat("The stateless API should return the same result", actual1.get(i).toString(),
          is(expected1.get(i).toString()));
      assertNotSame(actual1.get(i), expected1.get(i), "The stateless API does not cache");
    }

    List<HoodieFileGroup> expected2 = fsView.getAllFileGroupsStateless(partitionPath2)
        .sorted(Comparator.comparing(HoodieFileGroup::getFileGroupId)).collect(Collectors.toList());
    List<HoodieFileGroup> actual2 = fsView.getAllFileGroups(partitionPath2)
        .sorted(Comparator.comparing(HoodieFileGroup::getFileGroupId)).collect(Collectors.toList());
    for (int i = 0; i < expected2.size(); i++) {
      assertThat("The stateless API should return the same result", actual2.get(i).toString(),
          is(expected2.get(i).toString()));
      assertNotSame(actual2.get(i), expected2.get(i), "The stateless API does not cache");
    }
  }

  @Test
  public void testViewForGetLatestFileSlicesStateless() throws Exception {
    String partitionPath1 = "2023/11/22";
    new File(basePath + "/" + partitionPath1).mkdirs();
    new File(basePath + "/" + partitionPath1 + "/"
        + HOODIE_PARTITION_METAFILE_PREFIX + ".parquet").mkdirs();
    String partitionPath2 = "2023/11/23";
    new File(basePath + "/" + partitionPath2).mkdirs();
    new File(basePath + "/" + partitionPath2 + "/"
        + HOODIE_PARTITION_METAFILE_PREFIX + ".parquet").mkdirs();

    // create 2 fileId in partition1
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());

    // create 2 fileId in partition2
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();
    String commitTime2 = "2";
    String fileName3 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    String fileName4 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId4, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();

    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, new HoodieCommitMetadata());

    fsView.sync();

    // invokes the stateless API first then the normal API, assert the result equality with different file slice objects
    List<FileSlice> actual1 = fsView.getLatestFileSlicesStateless(partitionPath1)
        .sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    List<FileSlice> expected1 = fsView.getLatestFileSlices(partitionPath1)
        .sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    for (int i = 0; i < expected1.size(); i++) {
      assertThat("The stateless API should return the same result", actual1.get(i),
          is(expected1.get(i)));
      assertNotSame(actual1.get(i), expected1.get(i), "The stateless API does not cache");
    }

    List<FileSlice> expected2 = fsView.getLatestFileSlicesStateless(partitionPath2)
        .sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    List<FileSlice> actual2 = fsView.getLatestFileSlices(partitionPath2)
        .sorted(Comparator.comparing(FileSlice::getFileId)).collect(Collectors.toList());
    for (int i = 0; i < expected2.size(); i++) {
      assertThat("The stateless API should return the same result", actual2.get(i),
          is(expected2.get(i)));
      assertNotSame(actual2.get(i), expected2.get(i), "The stateless API does not cache");
    }
  }

  @ParameterizedTest
  @MethodSource("configParams")
  protected void testInvalidLogFiles(boolean preTableVersion8) throws Exception {
    String partitionPath = "2016/05/01";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, deltaInstantTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? deltaInstantTime1 : deltaInstantTime2, 1, TEST_WRITE_TOKEN);
    // create a dummy log file mimicing cloud stores marker files
    String fileName3 = "_GCS_SYNCABLE_TEMPFILE_" + fileName1;
    String fileName4 = "_DUMMY_" + fileName1.substring(1);

    // this file should not be deduced as a log file.

    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    Paths.get(basePath, partitionPath, fileName2).toFile().createNewFile();
    Paths.get(basePath, partitionPath, fileName3).toFile().createNewFile();
    Paths.get(basePath, partitionPath, fileName4).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant2, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant3, new HoodieCommitMetadata());

    refreshFsView();

    List<HoodieBaseFile> dataFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertTrue(dataFiles.isEmpty(), "No data file expected");
    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(deltaInstantTime1, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");
  }

  /**
   * The demo to test:
   *
   * <pre>
   * fg_t10 -> very first commit with start time, end time as [t10, t20].
   * l1 -> log file version 1 start time, end time as [t21, t40].
   * l2 -> concurrent log file version 2 [t30, t50].
   * fg_t60 -> base file due to compaction [t60, t80].
   * l3 -> concurrent log file version 3 [t35, t90].
   * </pre>
   *
   * <p>In this case, file_slice_barriers list is [t60, t10]. For a query at `t100`, `build_file_slices` should build the
   * following file slices corresponding to each barrier time:
   *
   * <pre>
   * [
   *   {t60, fg_t60.parquet, {l3}},
   *   {t10, fg_t10.parquet, {l1, l2}}
   * ]
   * </pre>
   *
   * <p>This assumes that file slicing is done based on completion time.
   */
  @Test
  void testFileSlicingWithMultipleDeltaWriters() throws Exception {
    String partitionPath = "2016/05/01";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "10";      // base
    String deltaInstantTime1 = "21"; // 21 -> 40
    String deltaInstantTime2 = "30"; // 30 -> 50
    String deltaInstantTime3 = "35"; // 35 -> 90

    String baseFile1 = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    String deltaFile1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, deltaInstantTime1, 0,
            TEST_WRITE_TOKEN);
    String deltaFile2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, deltaInstantTime2, 0,
            TEST_WRITE_TOKEN);
    String deltaFile3 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, deltaInstantTime3, 0,
            TEST_WRITE_TOKEN);

    Paths.get(basePath, partitionPath, baseFile1).toFile().createNewFile();
    Paths.get(basePath, partitionPath, deltaFile1).toFile().createNewFile();
    Paths.get(basePath, partitionPath, deltaFile2).toFile().createNewFile();
    Paths.get(basePath, partitionPath, deltaFile3).toFile().createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);
    HoodieInstant deltaInstant3 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime3);

    // delta instant 3 starts but finishes in the last
    metaClient.getActiveTimeline().createNewInstant(deltaInstant3);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant2, new HoodieCommitMetadata());

    refreshFsView();
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(1));
    FileSlice fileSlice = fileSlices.get(0);
    assertThat("Base file is missing", fileSlice.getBaseFile().isPresent());
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(instantTime1));
    assertThat("File-Id must be set correctly", fileSlice.getFileId(), is(fileId));
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(deltaFile2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(deltaFile1, logFiles.get(1).getFileName(), "Log File Order check");

    // schedules a compaction
    String compactionInstantTime1 = HoodieInstantTimeGenerator.getCurrentInstantTimeStr(); // 60 -> 80
    String compactionFile1 = FSUtils.makeBaseFileName(compactionInstantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());
    // Create a Data-file but this should be skipped by view
    Paths.get(basePath, partitionPath, compactionFile1).toFile().createNewFile();
    HoodieInstant compactionInstant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstantTime1);
    HoodieInstant requested = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstant.requestedTime());
    commitTimeline.saveToCompactionRequested(requested, compactionPlan);
    commitTimeline.transitionCompactionRequestedToInflight(requested);

    // check the view immediately
    refreshFsView();
    fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(1));
    fileSlice = fileSlices.get(0);
    assertFalse(fileSlice.getBaseFile().isPresent(), "No base file for pending compaction");
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(compactionInstantTime1));
    assertThat("File-Id must be set correctly", fileSlice.getFileId(), is(fileId));
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(0, logFiles.size(), "Correct number of log-files shows up in file-slice");

    // now finished the compaction
    saveAsComplete(commitTimeline, compactionInstant, new HoodieCommitMetadata());

    refreshFsView();
    fileSlices = rtView.getAllFileSlices(partitionPath).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(2));
    fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent(), "Base file missing");
    assertThat("Base file set correctly", fileSlice.getBaseFile().get().getCommitTime(), is(compactionInstantTime1));
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(compactionInstantTime1));
    assertThat("File-Id must be set correctly", fileSlice.getFileId(), is(fileId));
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(1, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(deltaFile3, logFiles.get(0).getFileName(), "Log File Order check");

    // now finished the long pending delta instant 3
    commitTimeline.saveAsComplete(deltaInstant3, Option.empty());

    refreshFsView();
    fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(1));
    fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent(), "Base file missing");
    assertThat("Base file set correctly", fileSlice.getBaseFile().get().getCommitTime(), is(compactionInstantTime1));
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(compactionInstantTime1));
    assertThat("File-Id must be set correctly", fileSlice.getFileId(), is(fileId));
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(1, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(deltaFile3, logFiles.get(0).getFileName(), "Log File Order check");

    // validate before or on
    fileSlices = rtView.getLatestFileSlicesBeforeOrOn(partitionPath, "15", true).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(1));
    fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent(), "Base file missing");
    assertThat("Base file set correctly", fileSlice.getBaseFile().get().getCommitTime(), is(instantTime1));
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(instantTime1));
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(deltaFile2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(deltaFile1, logFiles.get(1).getFileName(), "Log File Order check");

    // validate range query
    fileSlices = rtView.getLatestFileSliceInRange(Arrays.asList(instantTime1, compactionInstantTime1)).collect(Collectors.toList());
    assertThat("Multiple file slices", fileSlices.size(), is(1));
    fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent(), "Base file missing");
    assertThat("Base file set correctly", fileSlice.getBaseFile().get().getCommitTime(), is(compactionInstantTime1));
    assertThat("Base Instant for file-group set correctly", fileSlice.getBaseInstantTime(), is(compactionInstantTime1));
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(1, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(deltaFile3, logFiles.get(0).getFileName(), "Log File Order check");
  }

  @Test
  void testLoadPartitions_unPartitioned() throws Exception {
    String partitionPath = "";
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);

    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();

    // Assert that no base files are returned without the partitions being loaded
    assertEquals(0, fsView.getLatestFileSliceInRange(Collections.singletonList("1")).count());
    // Assert that load does not fail for un-partitioned tables
    fsView.loadPartitions(Collections.singletonList(partitionPath));
    // Assert that base files are returned after the empty-string partition is loaded
    assertEquals(1, fsView.getLatestFileSliceInRange(Collections.singletonList("1")).count());
  }

  @Test
  void testLoadPartitions_partitioned() throws Exception {
    String partitionPath1 = "2016/05/01";
    String partitionPath2 = "2016/05/02";
    Paths.get(basePath, partitionPath1).toFile().mkdirs();
    Paths.get(basePath, partitionPath2).toFile().mkdirs();
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String instantTime1 = "1";
    String fileName1 =
        FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);

    Paths.get(basePath, partitionPath1, fileName1).toFile().createNewFile();
    Paths.get(basePath, partitionPath2, fileName2).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();

    // Assert that no base files are returned without the partitions being loaded
    assertEquals(0, fsView.getLatestFileSliceInRange(Collections.singletonList("1")).count());
    // Only load a single partition path
    fsView.loadPartitions(Collections.singletonList(partitionPath1));
    // Assert that base file is returned for partitionPath1 only
    assertEquals(1, fsView.getLatestFileSliceInRange(Collections.singletonList("1")).count());
  }

  /**
   * Returns all file-slices including uncommitted ones.
   *
   * @param partitionPath
   * @return
   */
  private Stream<FileSlice> getAllRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath).flatMap(HoodieFileGroup::getAllFileSlicesIncludingInflight);
  }

  /**
   * Returns latest raw file-slices including uncommitted ones.
   *
   * @param partitionPath
   * @return
   */
  public Stream<FileSlice> getLatestRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath).map(HoodieFileGroup::getLatestFileSlicesIncludingInflight)
        .filter(Option::isPresent).map(Option::get);
  }

  private void checkExternalFile(HoodieFileStatus srcFileStatus,
                                 Option<BaseFile> bootstrapBaseFile, boolean testBootstrap) {
    if (testBootstrap) {
      assertTrue(bootstrapBaseFile.isPresent());
      assertEquals(HadoopFSUtils.toPath(srcFileStatus.getPath()),
          new Path(bootstrapBaseFile.get().getPath()));
      assertEquals(srcFileStatus.getPath(),
          HadoopFSUtils.fromPath(new Path(bootstrapBaseFile.get().getPath())));
      assertEquals(srcFileStatus.getModificationTime(),
          new Long(bootstrapBaseFile.get().getPathInfo().getModificationTime()));
      assertEquals(srcFileStatus.getBlockSize(), new Long(bootstrapBaseFile.get().getPathInfo().getBlockSize()));
      assertEquals(srcFileStatus.getLength(),
          new Long(bootstrapBaseFile.get().getPathInfo().getLength()));
      assertEquals(srcFileStatus.getIsDir() != null && srcFileStatus.getIsDir(),
          bootstrapBaseFile.get().getPathInfo().isDirectory());
    } else {
      assertFalse(bootstrapBaseFile.isPresent());
    }
  }

  @ParameterizedTest
  @MethodSource("configParams")
  public void testGetLatestFileSlicesIncludingInflight(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8);
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String dataFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + dataFileName).createNewFile();
    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? instantTime1 : deltaInstantTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? instantTime1 : deltaInstantTime2, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);

    // just commit instant1 and deltaInstant2, keep deltaInstant3 inflight
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant2, new HoodieCommitMetadata());
    refreshFsView(preTableVersion8);

    // getLatestFileSlices should return just 1 log file due to deltaInstant2
    rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList()).forEach(fs -> {
      assertEquals(fs.getBaseInstantTime(), instantTime1);
      assertTrue(fs.getBaseFile().isPresent());
      assertEquals(fs.getBaseFile().get().getCommitTime(), instantTime1);
      assertEquals(fs.getBaseFile().get().getFileId(), fileId);
      assertEquals(fs.getBaseFile().get().getPath(), "file:" + basePath + "/" + partitionPath + "/" + dataFileName);
      if (preTableVersion8) {
        // filtering is done using the log header.
        assertEquals(2, fs.getLogFiles().count());
      } else {
        // filtering is done using the time in the log file name.
        assertEquals(1, fs.getLogFiles().count());
      }
    });

    // getLatestFileSlicesIncludingInflight should return both the log files
    rtView.getLatestFileSlicesIncludingInflight(partitionPath).collect(Collectors.toList()).forEach(fs -> {
      assertEquals(fs.getBaseInstantTime(), instantTime1);
      assertTrue(fs.getBaseFile().isPresent());
      assertEquals(fs.getBaseFile().get().getCommitTime(), instantTime1);
      assertEquals(fs.getBaseFile().get().getFileId(), fileId);
      assertEquals(fs.getBaseFile().get().getPath(), "file:" + basePath + "/" + partitionPath + "/" + dataFileName);
      assertEquals(fs.getLogFiles().count(), 2);
    });
  }

  /**
   * Helper method to test Views in the presence of concurrent compaction.
   *
   * @param skipCreatingDataFile      if set, first File Slice will not have data-file set. This would simulate inserts going
   *                                  directly to log files
   * @param isCompactionInFlight      if set, compaction was inflight (running) when view was tested first time, otherwise
   *                                  compaction was in requested state
   * @param expTotalFileSlices        Total number of file-slices across file-groups in the partition path
   * @param expTotalDataFiles         Total number of data-files across file-groups in the partition path
   * @param includeInvalidAndInflight Whether view includes inflight and invalid file-groups.
   * @param testBootstrap             enable Bootstrap and test
   * @throws Exception -
   */
  protected void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingDataFile,
                                                          boolean isCompactionInFlight,
                                                          int expTotalFileSlices,
                                                          int expTotalDataFiles,
                                                          boolean includeInvalidAndInflight,
                                                          boolean testBootstrap,
                                                          boolean preTableVersion8) throws Exception {

    initMetaClient(preTableVersion8);
    if (testBootstrap) {
      metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType(), BOOTSTRAP_SOURCE_PATH, testBootstrap, null, "datestr",
          preTableVersion8 ? Option.of(HoodieTableVersion.SIX) : Option.of(HoodieTableVersion.current()));
    }
    metaClient.getTableConfig().setTableVersion(preTableVersion8 ? HoodieTableVersion.SIX : HoodieTableVersion.current());

    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();
    String srcName = "part_0000" + metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    HoodieFileStatus srcFileStatus = HoodieFileStatus.newBuilder()
        .setPath(
            HoodiePath.newBuilder().setUri(BOOTSTRAP_SOURCE_PATH + partitionPath + "/" + srcName).build())
        .setLength(256 * 1024 * 1024L)
        .setAccessTime(new Date().getTime())
        .setModificationTime(new Date().getTime() + 99999)
        .setBlockReplication(2)
        .setOwner("hudi")
        .setGroup("hudi")
        .setBlockSize(128 * 1024 * 1024L)
        .setPermission(HoodieFSPermission.newBuilder().setUserAction(FsAction.ALL.name())
                .setGroupAction(FsAction.READ.name()).setOtherAction(FsAction.NONE.name()).setStickyBit(true)
                .build())
        .build();

    // if skipCreatingDataFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = testBootstrap && !skipCreatingDataFile ? HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS : "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String dataFileName = null;
    if (!skipCreatingDataFile) {
      dataFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
      new File(basePath + "/" + partitionPath + "/" + dataFileName).createNewFile();
    }
    String logTime1 = preTableVersion8 ? (skipCreatingDataFile ? deltaInstantTime1 : instantTime1) : deltaInstantTime1;
    String logTime2 = preTableVersion8 ? (skipCreatingDataFile ? deltaInstantTime1 : instantTime1) : deltaInstantTime2;
    String fileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, logTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, logTime2, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    if (testBootstrap && !skipCreatingDataFile) {
      try (IndexWriter writer = new HFileBootstrapIndex(metaClient).createWriter(BOOTSTRAP_SOURCE_PATH)) {
        writer.begin();
        BootstrapFileMapping mapping = new BootstrapFileMapping(BOOTSTRAP_SOURCE_PATH, partitionPath,
            partitionPath, srcFileStatus, fileId);
        List<BootstrapFileMapping> b = new ArrayList<>();
        b.add(mapping);
        writer.appendNextPartition(partitionPath, b);
        writer.finish();
      }
    }
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant2, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant3, new HoodieCommitMetadata());

    refreshFsView(preTableVersion8);

    List<FileSlice> fileSlices =
        rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSlices.size());
    FileSlice fileSlice = fileSlices.get(0);
    assertEquals(!skipCreatingDataFile ? instantTime1 : deltaInstantTime1,
        fileSlice.getBaseInstantTime());
    if (!skipCreatingDataFile) {
      assertTrue(fileSlice.getBaseFile().isPresent());
      checkExternalFile(srcFileStatus, fileSlice.getBaseFile().get().getBootstrapBaseFile(),
          testBootstrap);
    }
    String compactionRequestedTime = "4";
    String compactDataFileName = FSUtils.makeBaseFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(),
            Option.empty());
    HoodieInstant compactionInstant;
    if (isCompactionInFlight) {
      // Create a Data-file but this should be skipped by view
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      compactionInstant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION,
          compactionRequestedTime);
      HoodieInstant requested =
          INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstant.requestedTime());
      commitTimeline.saveToCompactionRequested(requested, compactionPlan);
      commitTimeline.transitionCompactionRequestedToInflight(requested);
    } else {
      compactionInstant = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      commitTimeline.saveToCompactionRequested(compactionInstant, compactionPlan);
    }

    // View immediately after scheduling compaction
    refreshFsView(preTableVersion8);

    List<FileSlice> slices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, slices.size(), "Expected latest file-slices");
    assertEquals(compactionRequestedTime, slices.get(0).getBaseInstantTime(),
        "Base-Instant must be compaction Instant");
    assertFalse(slices.get(0).getBaseFile().isPresent(), "Latest File Slice must not have data-file");
    assertEquals(0, slices.get(0).getLogFiles().count(), "Latest File Slice must not have any log-files");

    // delta-commits after compaction-requested
    String deltaInstantTime4 = "5";
    String deltaInstantTime5 = "6";
    List<String> allInstantTimes = Arrays.asList(instantTime1, deltaInstantTime1, deltaInstantTime2,
        compactionRequestedTime, deltaInstantTime4, deltaInstantTime5);

    String logTime4 = preTableVersion8 ? compactionRequestedTime : deltaInstantTime4;
    String logTime5 = preTableVersion8 ? compactionRequestedTime : deltaInstantTime5;
    String fileName3 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, logTime4, preTableVersion8 ? 2 : 0, TEST_WRITE_TOKEN);
    String fileName4 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, logTime5, preTableVersion8 ? 3 : 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant deltaInstant4 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);
    HoodieInstant deltaInstant5 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    saveAsComplete(commitTimeline, deltaInstant4, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant5, new HoodieCommitMetadata());

    refreshFsView(preTableVersion8);

    List<HoodieBaseFile> dataFiles = roView.getAllBaseFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertTrue(dataFiles.isEmpty(), "No data file expected");
    } else {
      assertEquals(1, dataFiles.size(), "One data-file is expected as there is only one file-group");
      assertEquals(dataFileName, dataFiles.get(0).getFileName(), "Expect only valid data-file");
    }

    // Merge API Tests
    List<FileSlice> fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size(), "Expect file-slice to be merged");
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    if (!skipCreatingDataFile) {
      assertEquals(dataFileName, fileSlice.getBaseFile().get().getFileName(), "Data file must be present");
      checkExternalFile(srcFileStatus, fileSlice.getBaseFile().get().getBootstrapBaseFile(), testBootstrap);
    } else {
      assertFalse(fileSlice.getBaseFile().isPresent(), "No data-file expected as it was not created");
    }
    assertEquals(!skipCreatingDataFile ? instantTime1 : deltaInstantTime1, fileSlice.getBaseInstantTime(),
        "Base Instant of penultimate file-slice must be base instant");
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(4, logFiles.size(), "Log files must include those after compaction request");
    assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");
    assertEquals(fileName2, logFiles.get(2).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(3).getFileName(), "Log File Order check");

    fileSliceList =
        rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5, true).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size(), "Expect only one file-id");
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    assertFalse(fileSlice.getBaseFile().isPresent(), "No data-file expected in latest file-slice");
    assertEquals(compactionRequestedTime, fileSlice.getBaseInstantTime(),
        "Compaction requested instant must be base instant");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Log files must include only those after compaction request");
    assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");

    // Data Files API tests
    dataFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals(0, dataFiles.size(), "Expect no data file to be returned");
    } else {
      assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
      dataFiles.forEach(df -> assertEquals(df.getCommitTime(), instantTime1, "Expect data-file for instant 1 be returned"));
      checkExternalFile(srcFileStatus, dataFiles.get(0).getBootstrapBaseFile(), testBootstrap);
    }

    dataFiles = roView.getLatestBaseFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals(0, dataFiles.size(), "Expect no data file to be returned");
    } else {
      assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
      dataFiles.forEach(df -> assertEquals(df.getCommitTime(), instantTime1, "Expect data-file for instant 1 be returned"));
      checkExternalFile(srcFileStatus, dataFiles.get(0).getBootstrapBaseFile(), testBootstrap);
    }

    dataFiles = roView.getLatestBaseFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals(0, dataFiles.size(), "Expect no data file to be returned");
    } else {
      assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
      dataFiles.forEach(df -> assertEquals(df.getCommitTime(), instantTime1, "Expect data-file for instant 1 be returned"));
      checkExternalFile(srcFileStatus, dataFiles.get(0).getBootstrapBaseFile(), testBootstrap);
    }

    dataFiles = roView.getLatestBaseFilesInRange(allInstantTimes).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals(0, dataFiles.size(), "Expect no data file to be returned");
    } else {
      assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
      dataFiles.forEach(df -> assertEquals(df.getCommitTime(), instantTime1, "Expect data-file for instant 1 be returned"));
      checkExternalFile(srcFileStatus, dataFiles.get(0).getBootstrapBaseFile(), testBootstrap);
    }

    // Inflight/Orphan File-groups needs to be in the view

    // There is a data-file with this inflight file-id
    final String inflightFileId1 = UUID.randomUUID().toString();
    // There is a log-file with this inflight file-id
    final String inflightFileId2 = UUID.randomUUID().toString();
    // There is an orphan data file with this file-id
    final String orphanFileId1 = UUID.randomUUID().toString();
    // There is an orphan log data file with this file-id
    final String orphanFileId2 = UUID.randomUUID().toString();
    final String invalidInstantId = "INVALIDTIME";
    String inflightDeltaInstantTime = "7";
    String orphanDataFileName = FSUtils.makeBaseFileName(invalidInstantId, TEST_WRITE_TOKEN, orphanFileId1, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + orphanDataFileName).createNewFile();
    String orphanLogFileName =
        FSUtils.makeLogFileName(orphanFileId2, HoodieLogFile.DELTA_EXTENSION, invalidInstantId, 0,
            TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + orphanLogFileName).createNewFile();
    String inflightDataFileName = FSUtils.makeBaseFileName(inflightDeltaInstantTime, TEST_WRITE_TOKEN, inflightFileId1, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + inflightDataFileName).createNewFile();
    String inflightLogFileName =
        FSUtils.makeLogFileName(inflightFileId2, HoodieLogFile.DELTA_EXTENSION,
            inflightDeltaInstantTime, 0, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + inflightLogFileName).createNewFile();
    // Mark instant as inflight
    commitTimeline.createNewInstant(
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION,
            inflightDeltaInstantTime));
    commitTimeline.transitionRequestedToInflight(
        INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION,
            inflightDeltaInstantTime), Option.empty());
    refreshFsView(preTableVersion8);

    List<FileSlice> allRawFileSlices =
        getAllRawFileSlices(partitionPath).collect(Collectors.toList());
    dataFiles = allRawFileSlices.stream().flatMap(slice -> {
      if (slice.getBaseFile().isPresent()) {
        return Stream.of(slice.getBaseFile().get());
      }
      return Stream.empty();
    }).collect(Collectors.toList());

    if (includeInvalidAndInflight) {
      assertEquals(2 + (isCompactionInFlight ? 1 : 0) + (skipCreatingDataFile ? 0 : 1),
          dataFiles.size(),
          "Inflight/Orphan data-file is also expected");
      Set<String> fileNames =
          dataFiles.stream().map(HoodieBaseFile::getFileName).collect(Collectors.toSet());
      assertTrue(fileNames.contains(orphanDataFileName), "Expect orphan data-file to be present");
      assertTrue(fileNames.contains(inflightDataFileName),
          "Expect inflight data-file to be present");
      if (!skipCreatingDataFile) {
        assertTrue(fileNames.contains(dataFileName), "Expect old committed data-file");
      }

      if (isCompactionInFlight) {
        assertTrue(fileNames.contains(compactDataFileName),
            "Expect inflight compacted data file to be present");
      }

      fileSliceList = getLatestRawFileSlices(partitionPath).collect(Collectors.toList());
      assertEquals(includeInvalidAndInflight ? 5 : 1, fileSliceList.size(),
          "Expect both inflight and orphan file-slice to be included");
      Map<String, FileSlice> fileSliceMap =
          fileSliceList.stream().collect(Collectors.toMap(FileSlice::getFileId, r -> r));
      FileSlice orphanFileSliceWithDataFile = fileSliceMap.get(orphanFileId1);
      FileSlice orphanFileSliceWithLogFile = fileSliceMap.get(orphanFileId2);
      FileSlice inflightFileSliceWithDataFile = fileSliceMap.get(inflightFileId1);
      FileSlice inflightFileSliceWithLogFile = fileSliceMap.get(inflightFileId2);

      assertEquals(invalidInstantId, orphanFileSliceWithDataFile.getBaseInstantTime(),
          "Orphan File Slice with data-file check base-commit");
      assertEquals(orphanDataFileName, orphanFileSliceWithDataFile.getBaseFile().get().getFileName(),
          "Orphan File Slice with data-file check data-file");
      assertEquals(0, orphanFileSliceWithDataFile.getLogFiles().count(),
          "Orphan File Slice with data-file check data-file");
      assertEquals(inflightDeltaInstantTime, inflightFileSliceWithDataFile.getBaseInstantTime(),
          "Inflight File Slice with data-file check base-commit");
      assertEquals(inflightDataFileName, inflightFileSliceWithDataFile.getBaseFile().get().getFileName(),
          "Inflight File Slice with data-file check data-file");
      assertEquals(0, inflightFileSliceWithDataFile.getLogFiles().count(),
          "Inflight File Slice with data-file check data-file");
      assertEquals(invalidInstantId, orphanFileSliceWithLogFile.getBaseInstantTime(),
          "Orphan File Slice with log-file check base-commit");
      assertFalse(orphanFileSliceWithLogFile.getBaseFile().isPresent(),
          "Orphan File Slice with log-file check data-file");
      logFiles = orphanFileSliceWithLogFile.getLogFiles().collect(Collectors.toList());
      assertEquals(1, logFiles.size(), "Orphan File Slice with log-file check data-file");
      assertEquals(orphanLogFileName, logFiles.get(0).getFileName(),
          "Orphan File Slice with log-file check data-file");
      assertEquals(inflightDeltaInstantTime, inflightFileSliceWithLogFile.getBaseInstantTime(),
          "Inflight File Slice with log-file check base-commit");
      assertFalse(inflightFileSliceWithLogFile.getBaseFile().isPresent(),
          "Inflight File Slice with log-file check data-file");
      logFiles = inflightFileSliceWithLogFile.getLogFiles().collect(Collectors.toList());
      assertEquals(1, logFiles.size(), "Inflight File Slice with log-file check data-file");
      assertEquals(inflightLogFileName, logFiles.get(0).getFileName(),
          "Inflight File Slice with log-file check data-file");
    }

    compactionInstant = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
    // Now simulate Compaction completing - Check the view
    if (!isCompactionInFlight) {
      // For inflight compaction, we already create a data-file to test concurrent inflight case.
      // If we skipped creating data file corresponding to compaction commit, create it now
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      commitTimeline.createNewInstant(compactionInstant);
    }

    commitTimeline.saveAsComplete(compactionInstant, Option.empty());
    refreshFsView(preTableVersion8);
    // populate the cache
    roView.getAllBaseFiles(partitionPath);

    fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    LOG.info("FILESLICE LIST=" + fileSliceList);
    dataFiles = fileSliceList.stream().map(FileSlice::getBaseFile).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "Expect only one data-files in latest view as there is only one file-group");
    assertEquals(compactDataFileName, dataFiles.get(0).getFileName(), "Data Filename must match");
    assertEquals(1, fileSliceList.size(), "Only one latest file-slice in the partition");
    assertFalse(dataFiles.get(0).getBootstrapBaseFile().isPresent(), "No external data file must be present");
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "Check file-Id is set correctly");
    assertEquals(compactDataFileName, fileSlice.getBaseFile().get().getFileName(),
        "Check data-filename is set correctly");
    assertEquals(compactionRequestedTime, fileSlice.getBaseInstantTime(),
        "Ensure base-instant is now compaction request instant");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Only log-files after compaction request shows up");
    assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");

    // Data Files API tests
    dataFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
    assertFalse(dataFiles.get(0).getBootstrapBaseFile().isPresent(), "No external data file must be present");

    dataFiles.forEach(df -> {
      assertEquals(df.getCommitTime(), compactionRequestedTime, "Expect data-file created by compaction be returned");
      assertFalse(df.getBootstrapBaseFile().isPresent(), "No external data file must be present");
    });
    dataFiles = roView.getLatestBaseFiles(partitionPath).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
    dataFiles.forEach(df -> {
      assertEquals(df.getCommitTime(), compactionRequestedTime, "Expect data-file created by compaction be returned");
      assertFalse(df.getBootstrapBaseFile().isPresent(), "No external data file must be present");
    });
    dataFiles = roView.getLatestBaseFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
    dataFiles.forEach(df -> {
      assertEquals(df.getCommitTime(), compactionRequestedTime, "Expect data-file created by compaction be returned");
      assertFalse(df.getBootstrapBaseFile().isPresent(), "No external data file must be present");
    });
    dataFiles = roView.getLatestBaseFilesInRange(allInstantTimes).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "Expect only one data-file to be sent");
    dataFiles.forEach(df -> {
      assertEquals(df.getCommitTime(), compactionRequestedTime, "Expect data-file created by compaction be returned");
      assertFalse(df.getBootstrapBaseFile().isPresent(), "No external data file must be present");
    });

    assertEquals(expTotalFileSlices, rtView.getAllFileSlices(partitionPath).count(),
        "Total number of file-slices in partitions matches expected");
    assertEquals(expTotalDataFiles, roView.getAllBaseFiles(partitionPath).count(),
        "Total number of data-files in partitions matches expected");
    // file-groups includes inflight/invalid file-ids
    assertEquals(5, fsView.getAllFileGroups(partitionPath).count(),
        "Total number of file-groups in partitions matches expected");
  }

  @Test
  public void testGetLatestDataFilesForFileId() throws IOException {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath).anyMatch(dfile -> dfile.getFileId().equals(fileId)),
        "No commit, should not find any data file");

    // Only one commit, but is not safe
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView();
    assertFalse(roView.getLatestBaseFiles(partitionPath).anyMatch(dfile -> dfile.getFileId().equals(fileId)),
        "No commit, should not find any data file");

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();
    assertEquals(fileName1, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    refreshFsView();
    assertEquals(fileName1, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Make it safe
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, new HoodieCommitMetadata());
    refreshFsView();
    assertEquals(fileName2, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStreamLatestVersionInPartition(boolean preTableVersion8) throws IOException {
    testStreamLatestVersionInPartition(false, preTableVersion8);
  }

  public void testStreamLatestVersionInPartition(boolean isLatestFileSliceOnly, boolean preTableVersion8) throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String cleanTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String commitTime5 = "5";

    String fileId1 = "fg-A";
    String fileId2 = "fg-B";
    String fileId3 = "fg-C";
    String fileId4 = "fg-D";

    // file group 1
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime4, 0, TEST_WRITE_TOKEN))
        .createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime5, 1, TEST_WRITE_TOKEN))
        .createNewFile();

    // file group 2
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime4 : commitTime5, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // file group 3
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime4 : commitTime5, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // Create commit/clean files
    String relativeTimelinePath = preTableVersion8 ? "/.hoodie/" : "/.hoodie/timeline/";
    new File(basePath + relativeTimelinePath + cleanTime1 + ".clean").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime2 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime3 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime4 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime5 + ".commit").createNewFile();

    assertStreamLatestVersionInPartition(isLatestFileSliceOnly, fullPartitionPath, commitTime2,
        commitTime3, commitTime4, commitTime5, fileId1, fileId2, fileId3, fileId4, preTableVersion8);

    // Note: the separate archiving of clean and rollback actions is removed since 1.0.0,
    // now all the instants archive continuously.

    // Now create a scenario where archiving deleted commits (2,3, and 4) but retained cleaner clean1. Now clean1 is
    // the lowest commit time. Scenario for HUDI-162 - Here clean is the earliest action in active timeline
    new File(basePath + relativeTimelinePath + commitTime2 + ".commit").delete();
    new File(basePath + relativeTimelinePath + commitTime3 + ".commit").delete();
    new File(basePath + relativeTimelinePath + commitTime4 + ".commit").delete();

    assertStreamLatestVersionInPartition(isLatestFileSliceOnly, fullPartitionPath, commitTime2, commitTime3, commitTime4,
        commitTime5, fileId1, fileId2, fileId3, fileId4, preTableVersion8);
  }

  private void assertStreamLatestVersionInPartition(boolean isLatestFileSliceOnly, String fullPartitionPath,
                                                    String commitTime2, String commitTime3, String commitTime4, String commitTime5,
                                                    String fileId1, String fileId2, String fileId3, String fileId4,
                                                    boolean preTableVersion8) throws IOException {

    // Now we list the entire partition
    List<StoragePathInfo> partitionFileList =
        metaClient.getStorage().listDirectEntries(new StoragePath(fullPartitionPath));
    assertEquals(11, partitionFileList.size());
    refreshFsView(preTableVersion8);

    // Check files as of latest commit.
    List<FileSlice> allSlices = rtView.getAllFileSlices("2016/05/01").collect(Collectors.toList());
    assertEquals(isLatestFileSliceOnly ? 4 : 8, allSlices.size());
    Map<String, Long> fileSliceMap =
        allSlices.stream()
            .collect(Collectors.groupingBy(FileSlice::getFileId, Collectors.counting()));
    assertEquals(isLatestFileSliceOnly ? 1 : 2, fileSliceMap.get(fileId1).longValue());
    assertEquals(isLatestFileSliceOnly ? 1 : 3, fileSliceMap.get(fileId2).longValue());
    assertEquals(isLatestFileSliceOnly ? 1 : 2, fileSliceMap.get(fileId3).longValue());
    assertEquals(1, fileSliceMap.get(fileId4).longValue());

    List<HoodieBaseFile> dataFileList =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime5).collect(Collectors.toList());
    assertEquals(3, dataFileList.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)));

    filenames = new HashSet<>();
    List<HoodieLogFile> logFilesList =
        rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime5, true)
            .flatMap(FileSlice::getLogFiles).collect(Collectors.toList());
    assertEquals(4, logFilesList.size());
    for (HoodieLogFile logFile : logFilesList) {
      filenames.add(logFile.getFileName());
    }
    String l = FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime5, 0,
        TEST_WRITE_TOKEN);
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime4, 0,
            TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime5, 1,
            TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime4 : commitTime5, 0,
            TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime4 : commitTime5, 0,
            TEST_WRITE_TOKEN)));

    // Reset the max commit time
    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime4).collect(Collectors.toList());
    filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    if (!isLatestFileSliceOnly) {
      assertEquals(3, dataFiles.size());
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)));
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)));
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)));
    } else {
      assertEquals(1, dataFiles.size());
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)));
    }

    List<String> logFilesNames = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime4, true)
        .flatMap(FileSlice::getLogFiles).map(HoodieLogFile::getFileName).collect(Collectors.toList());
    assertEquals(preTableVersion8 ? 4 : 3, logFilesNames.size());
    assertTrue(logFilesNames.contains(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime4 : commitTime5, 0, TEST_WRITE_TOKEN)));
  }

  @Test
  public void testStreamEveryVersionInPartition() throws IOException {
    testStreamEveryVersionInPartition(false);
  }

  protected void testStreamEveryVersionInPartition(boolean isLatestFileSliceOnly) throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION)).createNewFile();

    new File(basePath + "/.hoodie/timeline/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    List<StoragePathInfo> partitionFileList =
        metaClient.getStorage().listDirectEntries(new StoragePath(fullPartitionPath));
    assertEquals(7, partitionFileList.size());

    refreshFsView();
    List<HoodieFileGroup> fileGroups =
        fsView.getAllFileGroups("2016/05/01").collect(Collectors.toList());
    assertEquals(3, fileGroups.size());

    for (HoodieFileGroup fileGroup : fileGroups) {
      String fileId = fileGroup.getFileGroupId().getFileId();
      Set<String> filenames = new HashSet<>();
      fileGroup.getAllBaseFiles().forEach(dataFile -> {
        assertEquals(fileId, dataFile.getFileId(), "All same fileId should be grouped");
        filenames.add(dataFile.getFileName());
      });
      Set<String> expFileNames = new HashSet<>();
      if (fileId.equals(fileId1)) {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION));
        assertEquals(expFileNames, filenames);
      } else if (fileId.equals(fileId2)) {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION));
          expFileNames.add(FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION));
        assertEquals(expFileNames, filenames);
      } else {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION));
        assertEquals(expFileNames, filenames);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStreamLatestVersionInRange(boolean preTableVersion8) throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileIdA = "fg-A";
    String fileIdB = "fg-B";
    String fileIdC = "fg-C";

    // file group A
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileIdA, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime1 : commitTime2, 0, TEST_WRITE_TOKEN))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)).createNewFile();

    // file group B
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileIdB, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime4, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // file group C
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)).createNewFile();

    String relativeTimelinePath = preTableVersion8 ? "/.hoodie/" : "/.hoodie/timeline/";
    new File(basePath + relativeTimelinePath + commitTime1 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime2 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime3 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    List<StoragePathInfo> partitionFileList =
        metaClient.getStorage().listDirectEntries(new StoragePath(fullPartitionPath));
    assertEquals(9, partitionFileList.size());

    refreshFsView(preTableVersion8);
    // Populate view for partition
    roView.getAllBaseFiles("2016/05/01/");

    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesInRange(Arrays.asList(commitTime2, commitTime3))
            .collect(Collectors.toList());
    assertEquals(3, dataFiles.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFiles) {
      filenames.add(status.getFileName());
    }

    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)));

    List<FileSlice> slices =
        rtView.getLatestFileSliceInRange(Arrays.asList(commitTime3, commitTime4))
            .collect(Collectors.toList());
    assertEquals(3, slices.size());
    for (FileSlice slice : slices) {
      if (slice.getFileId().equals(fileIdA)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      } else if (slice.getFileId().equals(fileIdB)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 1);
      } else if (slice.getFileId().equals(fileIdC)) {
        assertEquals(slice.getBaseInstantTime(), commitTime4);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStreamLatestVersionsBefore(boolean preTableVersion8) throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01/";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileIdA = "fg-A";
    String fileIdB = "fg-B";
    String fileIdC = "fg-C";

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)).createNewFile();

    String relativeTimelinePath = preTableVersion8 ? "/.hoodie/" : "/.hoodie/timeline/";
    new File(basePath + relativeTimelinePath + commitTime1 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime2 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime3 + ".commit").createNewFile();
    new File(basePath + relativeTimelinePath + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    List<StoragePathInfo> partitionFileList =
        metaClient.getStorage().listDirectEntries(new StoragePath(fullPartitionPath));
    assertEquals(7, partitionFileList.size());

    refreshFsView(preTableVersion8);
    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime2)
            .collect(Collectors.toList());
    assertEquals(2, dataFiles.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testStreamLatestVersions(boolean preTableVersion8) throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileIdA = "fg-A";
    String fileIdB = "fg-B";
    String fileIdC = "fg-C";

    // file group A
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileIdA, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime1 : commitTime2, 0, TEST_WRITE_TOKEN))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileIdA, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime3 : commitTime4, 0,
        TEST_WRITE_TOKEN))
        .createNewFile();

    // file group B
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileIdB, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? commitTime2 : commitTime3, 0, TEST_WRITE_TOKEN))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION))
        .createNewFile();

    // file group C
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION))
        .createNewFile();

    new File(basePath + "/.hoodie/timeline/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/timeline/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    List<StoragePathInfo> partitionFileList =
        metaClient.getStorage().listDirectEntries(new StoragePath(fullPartitionPath));
    assertEquals(10, partitionFileList.size());

    refreshFsView();
    fsView.getAllBaseFiles(partitionPath);
    List<HoodieFileGroup> fileGroups =
        fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    assertEquals(3, fileGroups.size());
    for (HoodieFileGroup fileGroup : fileGroups) {
      List<FileSlice> slices = fileGroup.getAllFileSlices().collect(Collectors.toList());
      String fileId = fileGroup.getFileGroupId().getFileId();
      if (fileId.equals(fileIdA)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime3, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime1, slices.get(1).getBaseInstantTime());
      } else if (fileId.equals(fileIdB)) {
        assertEquals(3, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime2, slices.get(1).getBaseInstantTime());
        assertEquals(commitTime1, slices.get(2).getBaseInstantTime());
      } else if (fileId.equals(fileIdC)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime3, slices.get(1).getBaseInstantTime());
      }
    }

    List<HoodieBaseFile> statuses1 = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertEquals(3, statuses1.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : statuses1) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileIdA, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdB, BASE_FILE_EXTENSION)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileIdC, BASE_FILE_EXTENSION)));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testPendingCompactionWithDuplicateFileIdsAcrossPartitions(boolean preTableVersion8) throws Exception {
    initMetaClient(preTableVersion8);
    // Put some files in the partition
    String partitionPath1 = "2016/05/01";
    String partitionPath2 = "2016/05/02";
    String partitionPath3 = "2016/05/03";

    String fullPartitionPath1 = basePath + "/" + partitionPath1 + "/";
    new File(fullPartitionPath1).mkdirs();
    String fullPartitionPath2 = basePath + "/" + partitionPath2 + "/";
    new File(fullPartitionPath2).mkdirs();
    String fullPartitionPath3 = basePath + "/" + partitionPath3 + "/";
    new File(fullPartitionPath3).mkdirs();
    String instantTime1 = "1";
    String deltaInstantTime3 = "3";
    String deltaInstantTime4 = "4";
    String fileId = "fg-X";

    String baseFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    String logFileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? instantTime1 : deltaInstantTime3, 0,
        TEST_WRITE_TOKEN);
    new File(fullPartitionPath1 + baseFileName).createNewFile();
    new File(fullPartitionPath1 + logFileName1).createNewFile();
    new File(fullPartitionPath2 + FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath2 + logFileName1).createNewFile();
    new File(fullPartitionPath3 + FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION)).createNewFile();
    new File(fullPartitionPath3 + logFileName1).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant3 =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime3);
    HoodieInstant deltaInstant4 =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);

    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant3, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant4, new HoodieCommitMetadata());

    // Now we list all partitions
    List<StoragePath> list = new ArrayList<>();
    list.add(new StoragePath(fullPartitionPath1));
    list.add(new StoragePath(fullPartitionPath2));
    list.add(new StoragePath(fullPartitionPath3));
    List<StoragePathInfo> fileList = metaClient.getStorage().listDirectEntries(list);
    assertEquals(6, fileList.size());
    refreshFsView();
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3)
        .forEach(p -> fsView.getAllFileGroups(p).count());

    List<HoodieFileGroup> groups = Stream.of(partitionPath1, partitionPath2, partitionPath3)
        .flatMap(p -> fsView.getAllFileGroups(p)).collect(Collectors.toList());
    assertEquals(3, groups.size(), "Expected number of file-groups");
    assertEquals(3,
        groups.stream().map(HoodieFileGroup::getPartitionPath).collect(Collectors.toSet()).size(),
        "Partitions must be different for file-groups");
    Set<String> fileIds =
        groups.stream().map(HoodieFileGroup::getFileGroupId).map(HoodieFileGroupId::getFileId)
            .collect(Collectors.toSet());
    assertEquals(1, fileIds.size(), "File Id must be same");
    assertTrue(fileIds.contains(fileId), "Expected FileId");

    // Setup Pending compaction for all of these fileIds.
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    List<FileSlice> fileSlices =
        rtView.getLatestFileSlices(partitionPath1).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath1, fileSlices.get(0)));
    fileSlices = rtView.getLatestFileSlices(partitionPath2).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath2, fileSlices.get(0)));
    fileSlices = rtView.getLatestFileSlices(partitionPath3).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath3, fileSlices.get(0)));

    String compactionRequestedTime = "2";
    String compactDataFileName = FSUtils.makeBaseFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(),
            Option.empty());

    // Create a Data-file for some of the partitions but this should be skipped by view
    new File(basePath + "/" + partitionPath1 + "/" + compactDataFileName).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + compactDataFileName).createNewFile();

    HoodieInstant compactionInstant =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION,
            compactionRequestedTime);
    HoodieInstant requested = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstant.requestedTime());
    metaClient.getActiveTimeline().saveToCompactionRequested(requested, compactionPlan);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(requested);

    // Fake delta-ingestion after compaction-requested
    String deltaInstantTime5 = "5";
    String deltaInstantTime7 = "7";
    String fileName3 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? compactionRequestedTime : deltaInstantTime5, 0, TEST_WRITE_TOKEN);
    String fileName4 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, preTableVersion8 ? compactionRequestedTime : deltaInstantTime7, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName4).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();
    new File(basePath + "/" + partitionPath3 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath3 + "/" + fileName4).createNewFile();

    HoodieInstant deltaInstant5 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    HoodieInstant deltaInstant7 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime7);
    saveAsComplete(commitTimeline, deltaInstant5, new HoodieCommitMetadata());
    saveAsComplete(commitTimeline, deltaInstant7, new HoodieCommitMetadata());
    refreshFsView(preTableVersion8);

    // Test Data Files
    List<HoodieBaseFile> dataFiles = roView.getAllBaseFiles(partitionPath1).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "One data-file is expected as there is only one file-group");
    assertEquals(instantTime1, dataFiles.get(0).getCommitTime(), "Expect only valid commit");
    dataFiles = roView.getAllBaseFiles(partitionPath2).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "One data-file is expected as there is only one file-group");
    assertEquals(instantTime1, dataFiles.get(0).getCommitTime(), "Expect only valid commit");

    // Merge API Tests
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3).forEach(partitionPath -> {
      List<FileSlice> fileSliceList =
          rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime7).collect(Collectors.toList());
      assertEquals(1, fileSliceList.size(), "Expect file-slice to be merged");
      FileSlice fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName(), "Data file must be present");
      assertEquals(instantTime1, fileSlice.getBaseInstantTime(),
          "Base Instant of penultimate file-slice must be base instant");
      List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals(3, logFiles.size(), "Log files must include those after compaction request");
      assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
      assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");
      assertEquals(logFileName1, logFiles.get(2).getFileName(), "Log File Order check");

      fileSliceList =
          rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime7, true).collect(Collectors.toList());
      assertEquals(1, fileSliceList.size(), "Expect only one file-id");
      fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertFalse(fileSlice.getBaseFile().isPresent(), "No data-file expected in latest file-slice");
      assertEquals(compactionRequestedTime, fileSlice.getBaseInstantTime(),
          "Compaction requested instant must be base instant");
      logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
      if (preTableVersion8) {
        assertEquals(2, logFiles.size(), "Log files must include only those after compaction request");
      } else {
        assertEquals(3, logFiles.size(), "All log files > compaction requested time to be included.");
      }
      assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
      assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");

      // Check getLatestFileSlicesBeforeOrOn excluding fileIds in pending compaction
      fileSliceList =
          rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime7, false).collect(Collectors.toList());
      assertEquals(0, fileSliceList.size(), "Expect empty list as file-id is in pending compaction");
    });

    assertEquals(3, fsView.getPendingCompactionOperations().count());
    Set<String> partitionsInCompaction = fsView.getPendingCompactionOperations().map(Pair::getValue)
        .map(CompactionOperation::getPartitionPath).collect(Collectors.toSet());
    assertEquals(3, partitionsInCompaction.size());
    assertTrue(partitionsInCompaction.contains(partitionPath1));
    assertTrue(partitionsInCompaction.contains(partitionPath2));
    assertTrue(partitionsInCompaction.contains(partitionPath3));

    Set<String> fileIdsInCompaction = fsView.getPendingCompactionOperations().map(Pair::getValue)
        .map(CompactionOperation::getFileId).collect(Collectors.toSet());
    assertEquals(1, fileIdsInCompaction.size());
    assertTrue(fileIdsInCompaction.contains(fileId));
  }

  private void saveAsComplete(HoodieActiveTimeline timeline, HoodieInstant inflight, HoodieCommitMetadata metadata) {
    if (inflight.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
      timeline.transitionCompactionInflightToComplete(true, inflight, metadata);
    } else {
      HoodieInstant requested = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, inflight.getAction(), inflight.requestedTime());
      timeline.createNewInstant(requested);
      timeline.transitionRequestedToInflight(requested, Option.empty());
      timeline.saveAsComplete(inflight, Option.of(metadata));
    }
  }

  private void saveAsCompleteCluster(HoodieActiveTimeline timeline, HoodieInstant inflight, HoodieCommitMetadata metadata) {
    assertEquals(HoodieTimeline.CLUSTERING_ACTION, inflight.getAction());
    HoodieInstant clusteringInstant = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, inflight.getAction(), inflight.requestedTime());
    HoodieClusteringPlan plan = new HoodieClusteringPlan();
    plan.setExtraMetadata(new HashMap<>());
    plan.setInputGroups(Collections.emptyList());
    plan.setStrategy(HoodieClusteringStrategy.newBuilder().build());
    plan.setVersion(1);
    plan.setPreserveHoodieMetadata(false);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setOperationType(WriteOperationType.CLUSTER.name())
        .setExtraMetadata(Collections.emptyMap())
        .setClusteringPlan(plan)
        .build();
    timeline.saveToPendingClusterCommit(clusteringInstant, requestedReplaceMetadata);
    timeline.transitionRequestedToInflight(clusteringInstant, Option.empty());
    timeline.transitionClusterInflightToComplete(true, inflight, (HoodieReplaceCommitMetadata) metadata);
  }

  @Test
  public void testReplaceWithTimeTravel() throws IOException {
    String partitionPath1 = "2020/06/27";
    new File(basePath + "/" + partitionPath1).mkdirs();

    // create 2 fileId in partition1 - fileId1 is replaced later on.
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath1)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2)),
        "No commit, should not find any data file");
    // Only one commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).count());

    // create commit2 - fileId1 is replaced. new file groups fileId3,fileId4 are created.
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();
    String fileName3 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    String fileName4 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId4, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName4).createNewFile();

    String commitTime2 = "2";
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add(fileId1);
    partitionToReplaceFileIds.put(partitionPath1, replacedFileIds);
    HoodieCommitMetadata commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds,
            Option.empty(), WriteOperationType.INSERT_OVERWRITE, "",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, commitMetadata);

    //make sure view doesn't include fileId1
    refreshFsView();
    assertEquals(0, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId4)).count());

    //exclude commit 2 and make sure fileId1 shows up in view.
    SyncableFileSystemView filteredView = getFileSystemView(metaClient.getActiveTimeline().findInstantsBefore("2"), false);
    assertEquals(1, filteredView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(1, filteredView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).count());
    assertEquals(1, filteredView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());
    assertEquals(1, filteredView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId4)).count());
    filteredView.close();

    // ensure replacedFileGroupsBefore works with all instants
    List<HoodieFileGroup> replacedOnInstant1 = fsView.getReplacedFileGroupsBeforeOrOn("1", partitionPath1).collect(Collectors.toList());
    assertEquals(0, replacedOnInstant1.size());

    List<HoodieFileGroup> allReplaced = fsView.getReplacedFileGroupsBeforeOrOn("2", partitionPath1).collect(Collectors.toList());
    assertEquals(1, allReplaced.size());
    assertEquals(fileId1, allReplaced.get(0).getFileGroupId().getFileId());

    allReplaced = fsView.getReplacedFileGroupsBefore("2", partitionPath1).collect(Collectors.toList());
    assertEquals(0, allReplaced.size());

    allReplaced = fsView.getAllReplacedFileGroups(partitionPath1).collect(Collectors.toList());
    assertEquals(1, allReplaced.size());
    assertEquals(fileId1, allReplaced.get(0).getFileGroupId().getFileId());
  }

  @Test
  public void testReplaceFileIdIsExcludedInView() throws IOException {
    String partitionPath1 = "2020/06/27";
    String partitionPath2 = "2020/07/14";
    new File(basePath + "/" + partitionPath1).mkdirs();
    new File(basePath + "/" + partitionPath2).mkdirs();

    // create 2 fileId in partition1 - fileId1 is replaced later on.
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();

    // create 2 fileId in partition2 - fileId3, fileId4 is replaced later on.
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath1)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2)),
        "No commit, should not find any data file");
    assertFalse(roView.getLatestBaseFiles(partitionPath2)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId3) || dfile.getFileId().equals(fileId4)),
        "No commit, should not find any data file");

    // Only one commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    String fileName3 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    String fileName4 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId4, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();

    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIdsP1 = new ArrayList<>();
    replacedFileIdsP1.add(fileId1);
    partitionToReplaceFileIds.put(partitionPath1, replacedFileIdsP1);
    List<String> replacedFileIdsP2 = new ArrayList<>();
    replacedFileIdsP2.add(fileId3);
    replacedFileIdsP2.add(fileId4);
    partitionToReplaceFileIds.put(partitionPath2, replacedFileIdsP2);
    HoodieCommitMetadata commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds, Option.empty(), WriteOperationType.INSERT_OVERWRITE, "", HoodieTimeline.REPLACE_COMMIT_ACTION);

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, commitMetadata);
    refreshFsView();
    assertEquals(0, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(fileName2, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).findFirst().get().getFileName());
    assertEquals(0, roView.getLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());
    assertEquals(0, roView.getLatestBaseFiles(partitionPath2)
        .filter(dfile -> dfile.getFileId().equals(fileId4)).count());

    // ensure replacedFileGroupsBefore works with all instants
    List<HoodieFileGroup> replacedOnInstant1 = fsView.getReplacedFileGroupsBeforeOrOn("0", partitionPath1).collect(Collectors.toList());
    assertEquals(0, replacedOnInstant1.size());

    List<HoodieFileGroup> allReplaced = fsView.getReplacedFileGroupsBeforeOrOn("2", partitionPath1).collect(Collectors.toList());
    allReplaced.addAll(fsView.getReplacedFileGroupsBeforeOrOn("2", partitionPath2).collect(Collectors.toList()));
    assertEquals(3, allReplaced.size());
    Set<String> allReplacedFileIds = allReplaced.stream().map(fg -> fg.getFileGroupId().getFileId()).collect(Collectors.toSet());
    Set<String> actualReplacedFileIds = Stream.of(fileId1, fileId3, fileId4).collect(Collectors.toSet());
    assertEquals(actualReplacedFileIds, allReplacedFileIds);
  }

  @Test
  public void testPendingClusteringOperations() throws IOException {
    String partitionPath1 = "2020/06/27";
    new File(basePath + "/" + partitionPath1).mkdirs();

    // create 2 fileId in partition1 - fileId1 is replaced later on.
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath1).anyMatch(dfile ->
            dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2)
                || dfile.getFileId().equals(fileId3)),
        "No commit, should not find any data file");
    // Only one commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    String fileName3 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());
    refreshFsView();
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId3)).count());

    List<FileSlice>[] fileSliceGroups = new List[] {
        Collections.singletonList(fsView.getLatestFileSlice(partitionPath1, fileId1).get()),
        Collections.singletonList(fsView.getLatestFileSlice(partitionPath1, fileId2).get())
    };

    // create pending clustering operation - fileId1, fileId2 are being clustered in different groups
    HoodieClusteringPlan plan = ClusteringUtils.createClusteringPlan("strategy", new HashMap<>(),
        fileSliceGroups, Collections.emptyMap());

    String clusterTime = "2";
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.CLUSTERING_ACTION, clusterTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(plan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    metaClient.getActiveTimeline().saveToPendingClusterCommit(instant2, requestedReplaceMetadata);

    //make sure view doesn't include fileId1
    refreshFsView();
    Set<String> fileIds =
        fsView.getFileGroupsInPendingClustering().map(e -> e.getLeft().getFileId()).collect(Collectors.toSet());
    assertTrue(fileIds.contains(fileId1));
    assertTrue(fileIds.contains(fileId2));
    assertFalse(fileIds.contains(fileId3));
  }

  /**
   * create hoodie table like
   * .
   * ├── .hoodie
   * │   ├── .aux
   * │   │   └── .bootstrap
   * │   │       ├── .fileids
   * │   │       └── .partitions
   * │   ├── .temp
   * │   ├── 1.commit
   * │   ├── 1.commit.requested
   * │   ├── 1.inflight
   * │   ├── 2.replacecommit
   * │   ├── 2.replacecommit.inflight
   * │   ├── 2.replacecommit.requested
   * │   ├── 3.commit
   * │   ├── 3.commit.requested
   * │   ├── 3.inflight
   * │   ├── archived
   * │   └── hoodie.properties
   * └── 2020
   *     └── 06
   *         └── 27
   *             ├── 5fe477d2-0150-46d4-833c-1e9cc8da9948_1-0-1_3.parquet
   *             ├── 7e3208c8-fdec-4254-9682-8fff1e51ee8d_1-0-1_2.parquet
   *             ├── e04b0e2d-1467-46b2-8ea6-f4fe950965a5_1-0-1_1.parquet
   *             └── f3936b66-b3db-4fc8-a6d0-b1a7559016e6_1-0-1_1.parquet
   *
   * First test fsView API with finished clustering:
   *  1. getLatestBaseFilesBeforeOrOn
   *  2. getBaseFileOn
   *  3. getLatestBaseFilesInRange
   *  4. getAllBaseFiles
   *  5. getLatestBaseFiles
   *
   * Then remove 2.replacecommit, 1.commit, 1.commit.requested, 1.inflight to simulate
   * pending clustering at the earliest position in the active timeline and test these APIs again.
   *
   * @throws IOException
   */
  @Test
  public void testHoodieTableFileSystemViewWithPendingClustering() throws IOException {
    List<String> latestBaseFilesBeforeOrOn;
    Option<HoodieBaseFile> baseFileOn;
    List<String> latestBaseFilesInRange;
    List<String> allBaseFiles;
    List<String> latestBaseFiles;
    List<String> latestBaseFilesPerPartition;
    String partitionPath = "2020/06/27";
    new File(basePath + "/" + partitionPath).mkdirs();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    // will create 5 fileId in partition.
    // fileId1 and fileId2 will be replaced by fileID3
    // fileId4 and fileId5 will be committed after clustering finished.
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();
    String fileId5 = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1)
                || dfile.getFileId().equals(fileId2)
                || dfile.getFileId().equals(fileId3)
                || dfile.getFileId().equals(fileId4)
                || dfile.getFileId().equals(fileId5)),
        "No commit, should not find any data file");

    // first insert commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1, BASE_FILE_EXTENSION);
    String fileName2 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();

    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime1);

    // build writeStats
    HashMap<String, List<String>> partitionToFile1 = new HashMap<>();
    ArrayList<String> files1 = new ArrayList<>();
    files1.add(fileId1);
    files1.add(fileId2);
    partitionToFile1.put(partitionPath, files1);
    List<HoodieWriteStat> writeStats1 = buildWriteStats(partitionToFile1, commitTime1);

    HoodieCommitMetadata commitMetadata1 =
        CommitUtils.buildMetadata(writeStats1, new HashMap<>(), Option.empty(), WriteOperationType.INSERT, "", HoodieTimeline.COMMIT_ACTION);
    saveAsComplete(commitTimeline, instant1, commitMetadata1);
    commitTimeline.reload();

    // replace commit
    String commitTime2 = "2";
    String fileName3 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId3, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();

    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.CLUSTERING_ACTION, commitTime2);
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add(fileId1);
    replacedFileIds.add(fileId2);
    partitionToReplaceFileIds.put(partitionPath, replacedFileIds);

    HashMap<String, List<String>> partitionToFile2 = new HashMap<>();
    ArrayList<String> files2 = new ArrayList<>();
    files2.add(fileId3);
    partitionToFile2.put(partitionPath, files2);
    List<HoodieWriteStat> writeStats2 = buildWriteStats(partitionToFile2, commitTime2);

    HoodieCommitMetadata commitMetadata2 =
        CommitUtils.buildMetadata(writeStats2, partitionToReplaceFileIds, Option.empty(), WriteOperationType.CLUSTER, "", HoodieTimeline.REPLACE_COMMIT_ACTION);
    saveAsCompleteCluster(
        commitTimeline,
        instant2,
        commitMetadata2);

    // another insert commit
    String commitTime3 = "3";
    String fileName4 = FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId4, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant instant3 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, commitTime3);

    // build writeStats
    HashMap<String, List<String>> partitionToFile3 = new HashMap<>();
    ArrayList<String> files3 = new ArrayList<>();
    files3.add(fileId4);
    partitionToFile3.put(partitionPath, files3);
    List<HoodieWriteStat> writeStats3 = buildWriteStats(partitionToFile3, commitTime3);
    HoodieCommitMetadata commitMetadata3 =
        CommitUtils.buildMetadata(writeStats3, new HashMap<>(), Option.empty(), WriteOperationType.INSERT, "", HoodieTimeline.COMMIT_ACTION);
    saveAsComplete(commitTimeline, instant3, commitMetadata3);

    metaClient.reloadActiveTimeline();
    refreshFsView();

    ArrayList<String> commits = new ArrayList<>();
    commits.add(commitTime1);
    commits.add(commitTime2);
    commits.add(commitTime3);

    // do check
    latestBaseFilesBeforeOrOn = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime3).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, latestBaseFilesBeforeOrOn.size());
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId3));
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId4));

    // could see fileId3 because clustering is committed.
    baseFileOn = fsView.getBaseFileOn(partitionPath, commitTime2, fileId3);
    assertTrue(baseFileOn.isPresent());
    assertEquals(baseFileOn.get().getFileId(), fileId3);

    latestBaseFilesInRange = fsView.getLatestBaseFilesInRange(commits).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, latestBaseFilesInRange.size());
    assertTrue(latestBaseFilesInRange.contains(fileId3));
    assertTrue(latestBaseFilesInRange.contains(fileId4));

    allBaseFiles = fsView.getAllBaseFiles(partitionPath).map(HoodieBaseFile::getFileId)
        .collect(Collectors.toList());
    assertEquals(2, allBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId3));
    assertTrue(allBaseFiles.contains(fileId4));

    // could see fileId3 because clustering is committed.
    latestBaseFiles =
        fsView.getLatestBaseFiles().map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, latestBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId3));
    assertTrue(allBaseFiles.contains(fileId4));

    // could see fileId3 because clustering is committed.
    latestBaseFilesPerPartition =
        fsView.getLatestBaseFiles(partitionPath).map(HoodieBaseFile::getFileId)
            .collect(Collectors.toList());
    assertEquals(2, latestBaseFiles.size());
    assertTrue(latestBaseFilesPerPartition.contains(fileId3));
    assertTrue(latestBaseFilesPerPartition.contains(fileId4));

    HoodieStorage storage = metaClient.getStorage();
    StoragePath instantPath1 = HoodieTestUtils
        .getCompleteInstantPath(storage, metaClient.getTimelinePath(), "1",
            HoodieTimeline.COMMIT_ACTION);
    storage.deleteFile(instantPath1);
    storage.deleteFile(new StoragePath(basePath + "/.hoodie/timeline/", "1.inflight"));
    storage.deleteFile(new StoragePath(basePath + "/.hoodie/timeline/", "1.commit.requested"));
    StoragePath instantPath2 = HoodieTestUtils
        .getCompleteInstantPath(storage, metaClient.getTimelinePath(), "2",
            HoodieTimeline.REPLACE_COMMIT_ACTION);
    storage.deleteFile(instantPath2);

    metaClient.reloadActiveTimeline();
    refreshFsView();
    // do check after delete some commit file
    latestBaseFilesBeforeOrOn = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime3)
        .map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(3, latestBaseFilesBeforeOrOn.size());
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId1));
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId2));
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId4));

    // couldn't see fileId3 because clustering is not committed.
    baseFileOn = fsView.getBaseFileOn(partitionPath, commitTime2, fileId3);
    assertFalse(baseFileOn.isPresent());

    latestBaseFilesInRange =
        fsView.getLatestBaseFilesInRange(commits).map(HoodieBaseFile::getFileId)
            .collect(Collectors.toList());
    assertEquals(3, latestBaseFilesInRange.size());
    assertTrue(latestBaseFilesInRange.contains(fileId1));
    assertTrue(latestBaseFilesInRange.contains(fileId2));
    assertTrue(latestBaseFilesInRange.contains(fileId4));

    allBaseFiles = fsView.getAllBaseFiles(partitionPath).map(HoodieBaseFile::getFileId)
        .collect(Collectors.toList());
    assertEquals(3, allBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId1));
    assertTrue(allBaseFiles.contains(fileId2));
    assertTrue(allBaseFiles.contains(fileId4));

    // couldn't see fileId3 because clustering is not committed.
    latestBaseFiles = fsView.getLatestBaseFiles().map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(3, latestBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId1));
    assertTrue(allBaseFiles.contains(fileId2));
    assertTrue(allBaseFiles.contains(fileId4));

    // couldn't see fileId3 because clustering is not committed.
    latestBaseFilesPerPartition = fsView.getLatestBaseFiles(partitionPath).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(3, latestBaseFiles.size());
    assertTrue(latestBaseFilesPerPartition.contains(fileId1));
    assertTrue(latestBaseFilesPerPartition.contains(fileId2));
    assertTrue(latestBaseFilesPerPartition.contains(fileId4));
  }

  // Generate Hoodie WriteStat For Given Partition
  private List<HoodieWriteStat> buildWriteStats(HashMap<String, List<String>> partitionToFileIds, String commitTime) {
    HashMap<String, List<Pair<String, Integer>>> maps = new HashMap<>();
    for (String partition : partitionToFileIds.keySet()) {
      List<Pair<String, Integer>> list = partitionToFileIds.get(partition).stream().map(fileId -> new ImmutablePair<String, Integer>(fileId, 0)).collect(Collectors.toList());
      maps.put(partition, list);
    }
    return HoodieTestTable.generateHoodieWriteStatForPartition(maps, commitTime, false);
  }

  @Test
  public void testPendingMajorAndMinorCompactionOperations() throws Exception {
    String partitionPath = "2020/06/27";
    new File(basePath + "/" + partitionPath).mkdirs();

    // Generate 2 fileIds
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();

    // This is used for verifying file system view after every commit.
    FileSystemViewExpectedState expectedState = new FileSystemViewExpectedState();

    // First delta commit on partitionPath which creates 2 log files.
    String commitTime1 = "001";
    String logFileName1 = FSUtils.makeLogFileName(fileId1, HoodieFileFormat.HOODIE_LOG.getFileExtension(), commitTime1, 1, TEST_WRITE_TOKEN);
    String logFileName2 = FSUtils.makeLogFileName(fileId2, HoodieFileFormat.HOODIE_LOG.getFileExtension(), commitTime1, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + logFileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + logFileName2).createNewFile();
    expectedState.logFilesCurrentlyPresent.add(logFileName1);
    expectedState.logFilesCurrentlyPresent.add(logFileName2);

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat(partitionPath, getHoodieWriteStat(partitionPath, fileId1, logFileName1));
    commitMetadata.addWriteStat(partitionPath, getHoodieWriteStat(partitionPath, fileId2, logFileName2));
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, commitTime1);
    saveAsComplete(
        commitTimeline,
        instant1,
        commitMetadata);

    SyncableFileSystemView fileSystemView = getFileSystemView(metaClient.reloadActiveTimeline(), true);

    // Verify file system view after 1st commit.
    verifyFileSystemView(partitionPath, expectedState, fileSystemView);

    // Second ingestion commit on partitionPath1
    // First delta commit on partitionPath1 which creates 2 log files.
    String commitTime2 = "002";
    String logFileName3 = FSUtils.makeLogFileName(fileId1, HoodieFileFormat.HOODIE_LOG.getFileExtension(), commitTime2, 2, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + logFileName3).createNewFile();
    expectedState.logFilesCurrentlyPresent.add(logFileName3);

    commitTimeline = metaClient.getActiveTimeline();
    commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addWriteStat(partitionPath, getHoodieWriteStat(partitionPath, fileId1, logFileName3));
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, commitTime2);

    saveAsComplete(
        commitTimeline,
        instant2,
        commitMetadata);

    // Verify file system view after 2nd commit.
    verifyFileSystemView(partitionPath, expectedState, fileSystemView);

    // Create compaction commit
    List<HoodieLogFile> logFiles = Stream.of(
            basePath + "/" + partitionPath + "/" + logFileName1, basePath + "/" + partitionPath + "/" + logFileName3)
        .map(HoodieLogFile::new)
        .collect(Collectors.toList());
    CompactionOperation compactionOperation = new CompactionOperation(Option.empty(), partitionPath, logFiles, Collections.emptyMap());
    HoodieCompactionPlan compactionPlan = getHoodieCompactionPlan(Collections.singletonList(compactionOperation));
    expectedState.pendingCompactionFgIdsCurrentlyPresent.add(fileId1);

    String commitTime3 = "003";
    HoodieInstant compactionInstant =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, commitTime3);
    HoodieInstant compactionRequested = INSTANT_GENERATOR.getCompactionRequestedInstant(compactionInstant.requestedTime());
    metaClient.getActiveTimeline().saveToCompactionRequested(compactionRequested, compactionPlan);
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(compactionRequested);

    // Verify file system view after 3rd commit which is compaction.requested.
    verifyFileSystemView(partitionPath, expectedState, fileSystemView);


    // Create log compaction commit
    logFiles = Collections.singletonList(new HoodieLogFile(basePath + "/" + partitionPath + "/" + logFileName2));
    CompactionOperation logCompactionOperation = new CompactionOperation(Option.empty(), partitionPath, logFiles, Collections.emptyMap());
    HoodieCompactionPlan logCompactionPlan = getHoodieCompactionPlan(Collections.singletonList(logCompactionOperation));
    expectedState.pendingLogCompactionFgIdsCurrentlyPresent.add(fileId2);

    String commitTime4 = "004";
    HoodieInstant logCompactionInstant =
        INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.LOG_COMPACTION_ACTION, commitTime4);
    HoodieInstant logCompactionRequested = INSTANT_GENERATOR.getLogCompactionRequestedInstant(logCompactionInstant.requestedTime());
    metaClient.getActiveTimeline().saveToLogCompactionRequested(logCompactionRequested, logCompactionPlan);
    metaClient.getActiveTimeline().transitionLogCompactionRequestedToInflight(logCompactionRequested);

    // Verify file system view after 4th commit which is logcompaction.requested.
    verifyFileSystemView(partitionPath, expectedState, fileSystemView);

    fileSystemView.close();
  }

  private HoodieCompactionPlan getHoodieCompactionPlan(List<CompactionOperation> operations) {
    return HoodieCompactionPlan.newBuilder()
        .setOperations(operations.stream()
            .map(CompactionUtils::buildHoodieCompactionOperation)
            .collect(Collectors.toList()))
        .setVersion(CompactionUtils.LATEST_COMPACTION_METADATA_VERSION).build();
  }

  private HoodieWriteStat getHoodieWriteStat(String partitionPath, String fileId, String relativeFilePath) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(partitionPath + "/" + relativeFilePath);
    writeStat.setPartitionPath(partitionPath);
    return writeStat;
  }

  @Test
  public void testGetLatestMergedFileSlicesBeforeOrOnIncludingInflight() throws IOException {
    String partitionPath = "2023/01/01";
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "001";
    String instantTime2 = "002";
    String compactionInstant = "003";
    String instantTime4 = "004";
    String instantTime5 = "005";

    new File(basePath + "/" + partitionPath).mkdirs();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    // Create base file at instant1
    String baseFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + baseFileName).createNewFile();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime1);
    saveAsComplete(commitTimeline, instant1, new HoodieCommitMetadata());

    // Add log files at instant2
    HoodieInstant instant2 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime2);
    String logFileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime2, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + logFileName2).createNewFile();
    saveAsComplete(commitTimeline, instant2, new HoodieCommitMetadata());

    refreshFsView();

    List<FileSlice> actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, instantTime2)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    FileSlice fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName());
    assertEquals(1, fileSlice.getLogFiles().count());
    assertEquals(logFileName2, fileSlice.getLogFiles().collect(Collectors.toList()).get(0).getFileName());

    actual = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime2, instantTime2)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName());
    assertEquals(1, fileSlice.getLogFiles().count());
    assertEquals(logFileName2, fileSlice.getLogFiles().collect(Collectors.toList()).get(0).getFileName());

    // Schedule compaction at instant3
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan = CompactionUtils.buildFromFileSlices(
        partitionFileSlicesPairs, Option.empty(), Option.empty());
    HoodieInstant compactionRequestedInstant = INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionInstant);
    commitTimeline.createNewInstant(compactionRequestedInstant);
    commitTimeline.saveToCompactionRequested(compactionRequestedInstant, compactionPlan);
    commitTimeline.transitionRequestedToInflight(compactionRequestedInstant, Option.empty());
    String baseFileName2 = FSUtils.makeBaseFileName(compactionInstant, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + baseFileName2).createNewFile();

    // Add log files at instant4
    HoodieInstant instant4 = INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime4);
    String logFileName4 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime4, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + logFileName4).createNewFile();
    saveAsComplete(commitTimeline, instant4, new HoodieCommitMetadata());

    // Add log files at instant5
    String logFileName5 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime5, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + logFileName5).createNewFile();
    commitTimeline.createNewInstant(INSTANT_GENERATOR.createNewInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime5));
    commitTimeline.createNewInstant(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime5));

    refreshFsView();
    actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, instantTime5)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName());
    assertEquals(2, fileSlice.getLogFiles().count());
    assertEquals(
        Arrays.asList(logFileName2, logFileName4),
        fileSlice.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.toList()));

    actual = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime5, instantTime5)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName());
    assertEquals(3, fileSlice.getLogFiles().count());
    assertEquals(
        Arrays.asList(logFileName2, logFileName4, logFileName5),
        fileSlice.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.toList()));

    actual = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime5, compactionInstant)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName2, fileSlice.getBaseFile().get().getFileName());
    assertEquals(2, fileSlice.getLogFiles().count());
    assertEquals(
        Arrays.asList(logFileName4, logFileName5),
        fileSlice.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.toList()));

    commitTimeline.saveAsComplete(INSTANT_GENERATOR.createNewInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionInstant), Option.empty());
    refreshFsView();
    
    actual = fsView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, instantTime5)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName2, fileSlice.getBaseFile().get().getFileName());
    assertEquals(1, fileSlice.getLogFiles().count());
    assertEquals(logFileName4, fileSlice.getLogFiles().collect(Collectors.toList()).get(0).getFileName());

    actual = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime5, instantTime5)
        .collect(Collectors.toList());
    assertEquals(1, actual.size());
    fileSlice = actual.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    assertEquals(baseFileName2, fileSlice.getBaseFile().get().getFileName());
    assertEquals(2, fileSlice.getLogFiles().count());
    assertEquals(
        Arrays.asList(logFileName4, logFileName5),
        fileSlice.getLogFiles().map(HoodieLogFile::getFileName).sorted().collect(Collectors.toList()));
  }

  @Test
  public void testGetLatestMergedFileSlicesIncludingInflightWithCurrentInstantTime() throws IOException {
    String partitionPath = "2023/01/01";
    String fileId = UUID.randomUUID().toString();
    String instantTime1 = "001";
    String instantTime2 = "002";
    String instantTime3 = "003";
    String instantTime4 = "004";

    new File(basePath + "/" + partitionPath).mkdirs();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    // Create base file at instant1
    String baseFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId, BASE_FILE_EXTENSION);
    new File(basePath + "/" + partitionPath + "/" + baseFileName).createNewFile();
    HoodieInstant instant1 = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.COMMIT_ACTION, instantTime1);
    commitTimeline.createNewInstant(instant1);
    commitTimeline.saveAsComplete(instant1, Option.empty());

    // Add log files at instant2, 3, 4
    for (String instant : Arrays.asList(instantTime2, instantTime3, instantTime4)) {
      HoodieInstant deltaInstant = INSTANT_GENERATOR.createNewInstant(State.COMPLETED, HoodieTimeline.DELTA_COMMIT_ACTION, instant);
      commitTimeline.createNewInstant(deltaInstant);
      commitTimeline.saveAsComplete(deltaInstant, Option.empty());
      String logFileName = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instant, 1, TEST_WRITE_TOKEN);
      new File(basePath + "/" + partitionPath + "/" + logFileName).createNewFile();
    }

    refreshFsView();
    SyncableFileSystemView fsView = getFileSystemView(commitTimeline);

    // Test with maxInstantTime < currentInstantTime - should only include logs up to maxInstantTime
    List<FileSlice> fileSlices = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime2, instantTime4)
        .collect(Collectors.toList());
    assertEquals(1, fileSlices.size());
    FileSlice fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    // Should only have log files up to instant2
    assertEquals(1, fileSlice.getLogFiles().count());

    // Test with maxInstantTime == currentInstantTime
    fileSlices = fsView.getLatestMergedFileSlicesBeforeOrOnIncludingInflight(partitionPath, instantTime4, instantTime4)
        .collect(Collectors.toList());
    assertEquals(1, fileSlices.size());
    fileSlice = fileSlices.get(0);
    assertTrue(fileSlice.getBaseFile().isPresent());
    // Should have all log files
    assertEquals(3, fileSlice.getLogFiles().count());
  }

  static class FileSystemViewExpectedState {
    Set<String> logFilesCurrentlyPresent = new HashSet<>();
    Set<String> baseFilesCurrentlyPresent = new HashSet<>();
    Set<String> pendingCompactionFgIdsCurrentlyPresent = new HashSet<>();
    Set<String> pendingLogCompactionFgIdsCurrentlyPresent = new HashSet<>();
  }

  /**
   * Used to verify file system view on various file systems.
   */
  protected void verifyFileSystemView(String partitionPath, FileSystemViewExpectedState expectedState,
                                      SyncableFileSystemView tableFileSystemView) {
    tableFileSystemView.sync();
    // Verify base files
    assertEquals(expectedState.baseFilesCurrentlyPresent, tableFileSystemView.getLatestBaseFiles(partitionPath)
        .map(HoodieBaseFile::getFileName)
        .collect(Collectors.toSet()));

    // Verify log files
    assertEquals(expectedState.logFilesCurrentlyPresent, tableFileSystemView.getAllFileSlices(partitionPath)
        .flatMap(FileSlice::getLogFiles)
        .map(logFile -> logFile.getPath().getName())
        .collect(Collectors.toSet()));
    // Verify file groups part of pending compaction operations
    assertEquals(expectedState.pendingCompactionFgIdsCurrentlyPresent, tableFileSystemView.getPendingCompactionOperations()
        .map(pair -> pair.getValue().getFileGroupId().getFileId())
        .collect(Collectors.toSet()));

    // Verify file groups part of pending log compaction operations
    assertEquals(expectedState.pendingLogCompactionFgIdsCurrentlyPresent, tableFileSystemView.getPendingLogCompactionOperations()
        .map(pair -> pair.getValue().getFileGroupId().getFileId())
        .collect(Collectors.toSet()));
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
