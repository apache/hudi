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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hudi.avro.model.HoodieClusteringPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieFSPermission;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.avro.model.HoodiePath;
import org.apache.hudi.avro.model.HoodieRequestedReplaceMetadata;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex.IndexWriter;
import org.apache.hudi.common.bootstrap.index.HFileBootstrapIndex;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.BootstrapFileMapping;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie table file system view {@link HoodieTableFileSystemView}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestHoodieTableFileSystemView extends HoodieCommonTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieTableFileSystemView.class);
  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with bootstrap enable={0}";

  private static final String TEST_WRITE_TOKEN = "1-0-1";
  private static final String BOOTSTRAP_SOURCE_PATH = "/usr/warehouse/hive/data/tables/src1/";

  protected SyncableFileSystemView fsView;
  protected BaseFileOnlyView roView;
  protected SliceView rtView;

  public static Stream<Arguments> configParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  @BeforeEach
  public void setup() throws IOException {
    metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType(), BOOTSTRAP_SOURCE_PATH, false);
    basePath = metaClient.getBasePath();
    refreshFsView();
  }

  protected void refreshFsView() throws IOException {
    super.refreshFsView();
    closeFsView();
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
  @Test
  public void testViewForFileSlicesWithNoBaseFile() throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0, "2016/05/01");
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileNonPartitioned() throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0, "");
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
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime2);
    HoodieInstant clusteringInstant3 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringInstantTime3);
    HoodieInstant clusteringInstant4 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, clusteringInstantTime4);
    HoodieCommitMetadata commitMetadata =
            CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds, Option.empty(), WriteOperationType.CLUSTER, "", HoodieTimeline.REPLACE_COMMIT_ACTION);

    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, instant2, Option.empty());
    saveAsComplete(commitTimeline, clusteringInstant3, Option.empty());
    saveAsComplete(commitTimeline, clusteringInstant4, Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    refreshFsView();

    // Now create a scenario where archiving deleted replace commits (requested,inflight and replacecommit)
    boolean deleteReplaceCommit = new File(this.basePath + "/.hoodie/" + clusteringInstantTime3 + ".replacecommit").delete();
    boolean deleteReplaceCommitRequested = new File(this.basePath + "/.hoodie/" + clusteringInstantTime3 + ".replacecommit.requested").delete();
    boolean deleteReplaceCommitInflight = new File(this.basePath + "/.hoodie/" + clusteringInstantTime3 + ".replacecommit.inflight").delete();

    // confirm deleted
    assertTrue(deleteReplaceCommit && deleteReplaceCommitInflight && deleteReplaceCommitRequested);
    assertDoesNotThrow(() -> fsView.close());

  }

  protected void testViewForFileSlicesWithNoBaseFile(int expNumTotalFileSlices, int expNumTotalDataFiles,
      String partitionPath) throws Exception {
    Paths.get(basePath, partitionPath).toFile().mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1, TEST_WRITE_TOKEN);

    Paths.get(basePath, partitionPath, fileName1).toFile().createNewFile();
    Paths.get(basePath, partitionPath, fileName2).toFile().createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();

    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant2, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant3, Option.empty());

    refreshFsView();

    List<HoodieBaseFile> dataFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertTrue(dataFiles.isEmpty(), "No data file expected");
    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(instantTime1, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Correct number of log-files shows up in file-slice");
    assertEquals(fileName2, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName1, logFiles.get(1).getFileName(), "Log File Order check");

    // Check Merged File Slices API
    fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime2).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "File-Id must be set correctly");
    assertFalse(fileSlice.getBaseFile().isPresent(), "Data file for base instant must be present");
    assertEquals(instantTime1, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
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
    assertEquals(instantTime1, fileSlice.getBaseInstantTime(), "Base Instant for file-group set correctly");
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

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction(boolean testBootstrap) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false, 2, 1, true, testBootstrap);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction(boolean testBootstrap) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false, 2, 2, true, testBootstrap);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction(boolean testBootstrap) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true, 2, 1, true, testBootstrap);
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction(boolean testBootstrap) throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true, 2, 2, true, testBootstrap);
  }

  /**
   * Returns all file-slices including uncommitted ones.
   *
   * @param partitionPath
   * @return
   */
  private Stream<FileSlice> getAllRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath).map(HoodieFileGroup::getAllFileSlicesIncludingInflight)
        .flatMap(sliceList -> sliceList);
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

  private void checkExternalFile(HoodieFileStatus srcFileStatus, Option<BaseFile> bootstrapBaseFile, boolean testBootstrap) {
    if (testBootstrap) {
      assertTrue(bootstrapBaseFile.isPresent());
      assertEquals(FileStatusUtils.toPath(srcFileStatus.getPath()), new Path(bootstrapBaseFile.get().getPath()));
      assertEquals(srcFileStatus.getPath(), FileStatusUtils.fromPath(new Path(bootstrapBaseFile.get().getPath())));
      assertEquals(srcFileStatus.getOwner(), bootstrapBaseFile.get().getFileStatus().getOwner());
      assertEquals(srcFileStatus.getGroup(), bootstrapBaseFile.get().getFileStatus().getGroup());
      assertEquals(srcFileStatus.getAccessTime(), new Long(bootstrapBaseFile.get().getFileStatus().getAccessTime()));
      assertEquals(srcFileStatus.getModificationTime(),
          new Long(bootstrapBaseFile.get().getFileStatus().getModificationTime()));
      assertEquals(srcFileStatus.getBlockSize(), new Long(bootstrapBaseFile.get().getFileStatus().getBlockSize()));
      assertEquals(srcFileStatus.getLength(), new Long(bootstrapBaseFile.get().getFileStatus().getLen()));
      assertEquals(srcFileStatus.getBlockReplication(),
          new Integer(bootstrapBaseFile.get().getFileStatus().getReplication()));
      assertEquals(srcFileStatus.getIsDir() == null ? false : srcFileStatus.getIsDir(),
          bootstrapBaseFile.get().getFileStatus().isDirectory());
      assertEquals(FileStatusUtils.toFSPermission(srcFileStatus.getPermission()),
          bootstrapBaseFile.get().getFileStatus().getPermission());
      assertEquals(srcFileStatus.getPermission(),
          FileStatusUtils.fromFSPermission(bootstrapBaseFile.get().getFileStatus().getPermission()));
      assertEquals(srcFileStatus.getSymlink() != null,
          bootstrapBaseFile.get().getFileStatus().isSymlink());
    } else {
      assertFalse(bootstrapBaseFile.isPresent());
    }
  }

  /**
   * Helper method to test Views in the presence of concurrent compaction.
   *
   * @param skipCreatingDataFile if set, first File Slice will not have data-file set. This would simulate inserts going
   *        directly to log files
   * @param isCompactionInFlight if set, compaction was inflight (running) when view was tested first time, otherwise
   *        compaction was in requested state
   * @param expTotalFileSlices Total number of file-slices across file-groups in the partition path
   * @param expTotalDataFiles Total number of data-files across file-groups in the partition path
   * @param includeInvalidAndInflight Whether view includes inflight and invalid file-groups.
   * @param testBootstrap enable Bootstrap and test
   * @throws Exception -
   */
  protected void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingDataFile, boolean isCompactionInFlight,
      int expTotalFileSlices, int expTotalDataFiles, boolean includeInvalidAndInflight, boolean testBootstrap)
      throws Exception {

    if (testBootstrap) {
      metaClient = HoodieTestUtils.init(tempDir.toAbsolutePath().toString(), getTableType(), BOOTSTRAP_SOURCE_PATH, testBootstrap);
    }

    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();
    String srcName = "part_0000" + metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    HoodieFileStatus srcFileStatus = HoodieFileStatus.newBuilder()
        .setPath(HoodiePath.newBuilder().setUri(BOOTSTRAP_SOURCE_PATH + partitionPath + "/" + srcName).build())
        .setLength(256 * 1024 * 1024L)
        .setAccessTime(new Date().getTime())
        .setModificationTime(new Date().getTime() + 99999)
        .setBlockReplication(2)
        .setOwner("hudi")
        .setGroup("hudi")
        .setBlockSize(128 * 1024 * 1024L)
        .setPermission(HoodieFSPermission.newBuilder().setUserAction(FsAction.ALL.name())
            .setGroupAction(FsAction.READ.name()).setOtherAction(FsAction.NONE.name()).setStickyBit(true).build())
        .build();

    // if skipCreatingDataFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = testBootstrap && !skipCreatingDataFile ? HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS : "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String dataFileName = null;
    if (!skipCreatingDataFile) {
      dataFileName = FSUtils.makeDataFileName(instantTime1, TEST_WRITE_TOKEN, fileId);
      new File(basePath + "/" + partitionPath + "/" + dataFileName).createNewFile();
    }
    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);
    String fileName2 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

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
    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant2, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant3, Option.empty());

    refreshFsView();
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSlices.size());
    FileSlice fileSlice = fileSlices.get(0);
    assertEquals(instantTime1, fileSlice.getBaseInstantTime());
    if (!skipCreatingDataFile) {
      assertTrue(fileSlice.getBaseFile().isPresent());
      checkExternalFile(srcFileStatus, fileSlice.getBaseFile().get().getBootstrapBaseFile(), testBootstrap);
    }
    String compactionRequestedTime = "4";
    String compactDataFileName = FSUtils.makeDataFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());
    HoodieInstant compactionInstant;
    if (isCompactionInFlight) {
      // Create a Data-file but this should be skipped by view
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      HoodieInstant requested = HoodieTimeline.getCompactionRequestedInstant(compactionInstant.getTimestamp());
      commitTimeline.saveToCompactionRequested(requested, TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
      commitTimeline.transitionCompactionRequestedToInflight(requested);
    } else {
      compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      commitTimeline.saveToCompactionRequested(compactionInstant, TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
    }

    // View immediately after scheduling compaction
    refreshFsView();
    List<FileSlice> slices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, slices.size(), "Expected latest file-slices");
    assertEquals(compactionRequestedTime, slices.get(0).getBaseInstantTime(),
        "Base-Instant must be compaction Instant");
    assertFalse(slices.get(0).getBaseFile().isPresent(), "Latest File Slice must not have data-file");
    assertEquals(0, slices.get(0).getLogFiles().count(), "Latest File Slice must not have any log-files");

    // Fake delta-ingestion after compaction-requested
    String deltaInstantTime4 = "5";
    String deltaInstantTime5 = "6";
    List<String> allInstantTimes = Arrays.asList(instantTime1, deltaInstantTime1, deltaInstantTime2,
        compactionRequestedTime, deltaInstantTime4, deltaInstantTime5);
    String fileName3 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 0, TEST_WRITE_TOKEN);
    String fileName4 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant deltaInstant4 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);
    HoodieInstant deltaInstant5 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    saveAsComplete(commitTimeline, deltaInstant4, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant5, Option.empty());
    refreshFsView();

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
    assertEquals(instantTime1, fileSlice.getBaseInstantTime(),
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
    String orphanDataFileName = FSUtils.makeDataFileName(invalidInstantId, TEST_WRITE_TOKEN, orphanFileId1);
    new File(basePath + "/" + partitionPath + "/" + orphanDataFileName).createNewFile();
    String orphanLogFileName =
        FSUtils.makeLogFileName(orphanFileId2, HoodieLogFile.DELTA_EXTENSION, invalidInstantId, 0, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + orphanLogFileName).createNewFile();
    String inflightDataFileName = FSUtils.makeDataFileName(inflightDeltaInstantTime, TEST_WRITE_TOKEN, inflightFileId1);
    new File(basePath + "/" + partitionPath + "/" + inflightDataFileName).createNewFile();
    String inflightLogFileName = FSUtils.makeLogFileName(inflightFileId2, HoodieLogFile.DELTA_EXTENSION,
        inflightDeltaInstantTime, 0, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + inflightLogFileName).createNewFile();
    // Mark instant as inflight
    commitTimeline.createNewInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION,
        inflightDeltaInstantTime));
    commitTimeline.transitionRequestedToInflight(new HoodieInstant(State.REQUESTED, HoodieTimeline.DELTA_COMMIT_ACTION,
        inflightDeltaInstantTime), Option.empty());
    refreshFsView();

    List<FileSlice> allRawFileSlices = getAllRawFileSlices(partitionPath).collect(Collectors.toList());
    dataFiles = allRawFileSlices.stream().flatMap(slice -> {
      if (slice.getBaseFile().isPresent()) {
        return Stream.of(slice.getBaseFile().get());
      }
      return Stream.empty();
    }).collect(Collectors.toList());

    if (includeInvalidAndInflight) {
      assertEquals(2 + (isCompactionInFlight ? 1 : 0) + (skipCreatingDataFile ? 0 : 1), dataFiles.size(),
          "Inflight/Orphan data-file is also expected");
      Set<String> fileNames = dataFiles.stream().map(HoodieBaseFile::getFileName).collect(Collectors.toSet());
      assertTrue(fileNames.contains(orphanDataFileName), "Expect orphan data-file to be present");
      assertTrue(fileNames.contains(inflightDataFileName), "Expect inflight data-file to be present");
      if (!skipCreatingDataFile) {
        assertTrue(fileNames.contains(dataFileName), "Expect old committed data-file");
      }

      if (isCompactionInFlight) {
        assertTrue(fileNames.contains(compactDataFileName), "Expect inflight compacted data file to be present");
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

    compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
    // Now simulate Compaction completing - Check the view
    if (!isCompactionInFlight) {
      // For inflight compaction, we already create a data-file to test concurrent inflight case.
      // If we skipped creating data file corresponding to compaction commit, create it now
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      commitTimeline.createNewInstant(compactionInstant);
    }

    commitTimeline.saveAsComplete(compactionInstant, Option.empty());
    refreshFsView();
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
    assertFalse(dataFiles.get(0).getBootstrapBaseFile().isPresent(),"No external data file must be present");

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
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView();
    assertFalse(roView.getLatestBaseFiles(partitionPath).anyMatch(dfile -> dfile.getFileId().equals(fileId)),
        "No commit, should not find any data file");

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    refreshFsView();
    assertEquals(fileName1, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    refreshFsView();
    assertEquals(fileName1, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Make it safe
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, Option.empty());
    refreshFsView();
    assertEquals(fileName2, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());
  }

  @Test
  public void testStreamLatestVersionInPartition() throws IOException {
    testStreamLatestVersionInPartition(false);
  }

  public void testStreamLatestVersionInPartition(boolean isLatestFileSliceOnly) throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String cleanTime1 = "1";
    String commitTime1 = "2";
    String commitTime2 = "3";
    String commitTime3 = "4";
    String commitTime4 = "5";

    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
            .createNewFile();

    // Create commit/clean files
    new File(basePath + "/.hoodie/" + cleanTime1 + ".clean").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    testStreamLatestVersionInPartition(isLatestFileSliceOnly, fullPartitionPath, commitTime1, commitTime2, commitTime3,
        commitTime4, fileId1, fileId2, fileId3, fileId4);

    // Now create a scenario where archiving deleted commits (1,2, and 3) but retained cleaner clean1. Now clean1 is
    // the lowest commit time. Scenario for HUDI-162 - Here clean is the earliest action in active timeline
    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").delete();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").delete();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").delete();
    testStreamLatestVersionInPartition(isLatestFileSliceOnly, fullPartitionPath, commitTime1, commitTime2, commitTime3,
        commitTime4, fileId1, fileId2, fileId3, fileId4);
  }

  private void testStreamLatestVersionInPartition(boolean isLatestFileSliceOnly, String fullPartitionPath,
      String commitTime1, String commitTime2, String commitTime3, String commitTime4, String fileId1, String fileId2,
      String fileId3, String fileId4) throws IOException {

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(11, statuses.length);
    refreshFsView();

    // Check files as of lastest commit.
    List<FileSlice> allSlices = rtView.getAllFileSlices("2016/05/01").collect(Collectors.toList());
    assertEquals(isLatestFileSliceOnly ? 4 : 8, allSlices.size());
    Map<String, Long> fileSliceMap =
        allSlices.stream().collect(Collectors.groupingBy(FileSlice::getFileId, Collectors.counting()));
    assertEquals(isLatestFileSliceOnly ? 1 : 2, fileSliceMap.get(fileId1).longValue());
    assertEquals(isLatestFileSliceOnly ? 1 : 3, fileSliceMap.get(fileId2).longValue());
    assertEquals(isLatestFileSliceOnly ? 1 : 2, fileSliceMap.get(fileId3).longValue());
    assertEquals(1, fileSliceMap.get(fileId4).longValue());

    List<HoodieBaseFile> dataFileList =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime4).collect(Collectors.toList());
    assertEquals(3, dataFileList.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)));

    filenames = new HashSet<>();
    List<HoodieLogFile> logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime4, true)
        .map(FileSlice::getLogFiles).flatMap(logFileList -> logFileList).collect(Collectors.toList());
    assertEquals(logFilesList.size(), 4);
    for (HoodieLogFile logFile : logFilesList) {
      filenames.add(logFile.getFileName());
    }
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1, TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN)));
    assertTrue(filenames
        .contains(FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN)));

    // Reset the max commit time
    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime3).collect(Collectors.toList());
    filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    if (!isLatestFileSliceOnly) {
      assertEquals(3, dataFiles.size());
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)));
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)));
    } else {
      assertEquals(1, dataFiles.size());
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    }

    logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime3, true)
        .map(FileSlice::getLogFiles).flatMap(logFileList -> logFileList).collect(Collectors.toList());
    assertEquals(logFilesList.size(), 1);
    assertEquals(logFilesList.get(0).getFileName(), FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN));
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView();
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups("2016/05/01").collect(Collectors.toList());
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
          expFileNames.add(FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1));
        }
        expFileNames.add(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1));
        assertEquals(expFileNames, filenames);
      } else if (fileId.equals(fileId2)) {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2));
          expFileNames.add(FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2));
        }
        expFileNames.add(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2));
        assertEquals(expFileNames, filenames);
      } else {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3));
        }
        expFileNames.add(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3));
        assertEquals(expFileNames, filenames);
      }
    }
  }

  @Test
  public void testStreamLatestVersionInRange() throws IOException {
    testStreamLatestVersionInRange(false);
  }

  protected void testStreamLatestVersionInRange(boolean isLatestFileSliceOnly) throws IOException {
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId1)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN))
            .createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(9, statuses.length);

    refreshFsView();
    // Populate view for partition
    roView.getAllBaseFiles("2016/05/01/");

    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesInRange(Arrays.asList(commitTime2, commitTime3)).collect(Collectors.toList());
    assertEquals(isLatestFileSliceOnly ? 2 : 3, dataFiles.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : dataFiles) {
      filenames.add(status.getFileName());
    }

    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    if (!isLatestFileSliceOnly) {
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)));
    }

    List<FileSlice> slices =
        rtView.getLatestFileSliceInRange(Arrays.asList(commitTime3, commitTime4)).collect(Collectors.toList());
    assertEquals(3, slices.size());
    for (FileSlice slice : slices) {
      if (slice.getFileId().equals(fileId1)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      } else if (slice.getFileId().equals(fileId2)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 1);
      } else if (slice.getFileId().equals(fileId3)) {
        assertEquals(slice.getBaseInstantTime(), commitTime4);
        assertTrue(slice.getBaseFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      }
    }
  }

  @Test
  public void testStreamLatestVersionsBefore() throws IOException {
    testStreamLatestVersionsBefore(false);
  }

  protected void testStreamLatestVersionsBefore(boolean isLatestFileSliceOnly) throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01/";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView();
    List<HoodieBaseFile> dataFiles =
        roView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime2).collect(Collectors.toList());
    if (!isLatestFileSliceOnly) {
      assertEquals(2, dataFiles.size());
      Set<String> filenames = new HashSet<>();
      for (HoodieBaseFile status : dataFiles) {
        filenames.add(status.getFileName());
      }
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)));
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)));
    } else {
      assertEquals(0, dataFiles.size());
    }
  }

  @Test
  public void testStreamLatestVersions() throws IOException {
    testStreamLatestVersions(false);
  }

  protected void testStreamLatestVersions(boolean isLatestFileSliceOnly) throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
            .createNewFile();

    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();

    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3))
        .createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(10, statuses.length);

    refreshFsView();
    fsView.getAllBaseFiles(partitionPath);
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    assertEquals(3, fileGroups.size());
    for (HoodieFileGroup fileGroup : fileGroups) {
      List<FileSlice> slices = fileGroup.getAllFileSlices().collect(Collectors.toList());
      String fileId = fileGroup.getFileGroupId().getFileId();
      if (fileId.equals(fileId1)) {
        assertEquals(isLatestFileSliceOnly ? 1 : 2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        if (!isLatestFileSliceOnly) {
          assertEquals(commitTime1, slices.get(1).getBaseInstantTime());
        }
      } else if (fileId.equals(fileId2)) {
        assertEquals(isLatestFileSliceOnly ? 1 : 3, slices.size());
        assertEquals(commitTime3, slices.get(0).getBaseInstantTime());
        if (!isLatestFileSliceOnly) {
          assertEquals(commitTime2, slices.get(1).getBaseInstantTime());
          assertEquals(commitTime1, slices.get(2).getBaseInstantTime());
        }
      } else if (fileId.equals(fileId3)) {
        assertEquals(isLatestFileSliceOnly ? 1 : 2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        if (!isLatestFileSliceOnly) {
          assertEquals(commitTime3, slices.get(1).getBaseInstantTime());
        }
      }
    }

    List<HoodieBaseFile> statuses1 = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertEquals(3, statuses1.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : statuses1) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)));
  }

  @Test
  public void testPendingCompactionWithDuplicateFileIdsAcrossPartitions() throws Exception {
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
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
    String fileId = UUID.randomUUID().toString();

    String dataFileName = FSUtils.makeDataFileName(instantTime1, TEST_WRITE_TOKEN, fileId);
    new File(fullPartitionPath1 + dataFileName).createNewFile();

    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);
    new File(fullPartitionPath1 + fileName1).createNewFile();
    new File(fullPartitionPath2 + FSUtils.makeDataFileName(instantTime1, TEST_WRITE_TOKEN, fileId)).createNewFile();
    new File(fullPartitionPath2 + fileName1).createNewFile();
    new File(fullPartitionPath3 + FSUtils.makeDataFileName(instantTime1, TEST_WRITE_TOKEN, fileId)).createNewFile();
    new File(fullPartitionPath3 + fileName1).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant2, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant3, Option.empty());

    // Now we list all partitions
    FileStatus[] statuses = metaClient.getFs().listStatus(
        new Path[] {new Path(fullPartitionPath1), new Path(fullPartitionPath2), new Path(fullPartitionPath3)});
    assertEquals(6, statuses.length);
    refreshFsView();
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3).forEach(p -> fsView.getAllFileGroups(p).count());

    List<HoodieFileGroup> groups = Stream.of(partitionPath1, partitionPath2, partitionPath3)
        .flatMap(p -> fsView.getAllFileGroups(p)).collect(Collectors.toList());
    assertEquals(3, groups.size(), "Expected number of file-groups");
    assertEquals(3, groups.stream().map(HoodieFileGroup::getPartitionPath).collect(Collectors.toSet()).size(),
        "Partitions must be different for file-groups");
    Set<String> fileIds = groups.stream().map(HoodieFileGroup::getFileGroupId).map(HoodieFileGroupId::getFileId)
        .collect(Collectors.toSet());
    assertEquals(1, fileIds.size(), "File Id must be same");
    assertTrue(fileIds.contains(fileId), "Expected FileId");

    // Setup Pending compaction for all of these fileIds.
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath1).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath1, fileSlices.get(0)));
    fileSlices = rtView.getLatestFileSlices(partitionPath2).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath2, fileSlices.get(0)));
    fileSlices = rtView.getLatestFileSlices(partitionPath3).collect(Collectors.toList());
    partitionFileSlicesPairs.add(Pair.of(partitionPath3, fileSlices.get(0)));

    String compactionRequestedTime = "2";
    String compactDataFileName = FSUtils.makeDataFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());

    // Create a Data-file for some of the partitions but this should be skipped by view
    new File(basePath + "/" + partitionPath1 + "/" + compactDataFileName).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + compactDataFileName).createNewFile();

    HoodieInstant compactionInstant =
        new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
    HoodieInstant requested = HoodieTimeline.getCompactionRequestedInstant(compactionInstant.getTimestamp());
    metaClient.getActiveTimeline().saveToCompactionRequested(requested,
        TimelineMetadataUtils.serializeCompactionPlan(compactionPlan));
    metaClient.getActiveTimeline().transitionCompactionRequestedToInflight(requested);

    // Fake delta-ingestion after compaction-requested
    String deltaInstantTime4 = "4";
    String deltaInstantTime5 = "6";
    String fileName3 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 0, TEST_WRITE_TOKEN);
    String fileName4 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 1, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName4).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + fileName4).createNewFile();
    new File(basePath + "/" + partitionPath3 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath3 + "/" + fileName4).createNewFile();

    HoodieInstant deltaInstant4 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);
    HoodieInstant deltaInstant5 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    saveAsComplete(commitTimeline, deltaInstant4, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant5, Option.empty());
    refreshFsView();

    // Test Data Files
    List<HoodieBaseFile> dataFiles = roView.getAllBaseFiles(partitionPath1).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "One data-file is expected as there is only one file-group");
    assertEquals("1", dataFiles.get(0).getCommitTime(), "Expect only valid commit");
    dataFiles = roView.getAllBaseFiles(partitionPath2).collect(Collectors.toList());
    assertEquals(1, dataFiles.size(), "One data-file is expected as there is only one file-group");
    assertEquals("1", dataFiles.get(0).getCommitTime(), "Expect only valid commit");

    // Merge API Tests
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3).forEach(partitionPath -> {
      List<FileSlice> fileSliceList =
          rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
      assertEquals(1, fileSliceList.size(), "Expect file-slice to be merged");
      FileSlice fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertEquals(dataFileName, fileSlice.getBaseFile().get().getFileName(), "Data file must be present");
      assertEquals(instantTime1, fileSlice.getBaseInstantTime(),
          "Base Instant of penultimate file-slice must be base instant");
      List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals(3, logFiles.size(), "Log files must include those after compaction request");
      assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
      assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");
      assertEquals(fileName1, logFiles.get(2).getFileName(), "Log File Order check");

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

      // Check getLatestFileSlicesBeforeOrOn excluding fileIds in pending compaction
      fileSliceList =
          rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5, false).collect(Collectors.toList());
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

  private static void saveAsComplete(HoodieActiveTimeline timeline, HoodieInstant inflight, Option<byte[]> data) {
    if (inflight.getAction().equals(HoodieTimeline.COMPACTION_ACTION)) {
      timeline.transitionCompactionInflightToComplete(inflight, data);
    } else {
      HoodieInstant requested = new HoodieInstant(State.REQUESTED, inflight.getAction(), inflight.getTimestamp());
      timeline.createNewInstant(requested);
      timeline.transitionRequestedToInflight(requested, Option.empty());
      timeline.saveAsComplete(inflight, data);
    }
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
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1);
    String fileName2 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    refreshFsView();
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId1)).count());
    assertEquals(1, roView.getLatestBaseFiles(partitionPath1)
        .filter(dfile -> dfile.getFileId().equals(fileId2)).count());

    // create commit2 - fileId1 is replaced. new file groups fileId3,fileId4 are created.
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();
    String fileName3 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId3);
    String fileName4 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId4);
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName4).createNewFile();

    String commitTime2 = "2";
    Map<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    List<String> replacedFileIds = new ArrayList<>();
    replacedFileIds.add(fileId1);
    partitionToReplaceFileIds.put(partitionPath1, replacedFileIds);
    HoodieCommitMetadata commitMetadata =
        CommitUtils.buildMetadata(Collections.emptyList(), partitionToReplaceFileIds, Option.empty(), WriteOperationType.INSERT_OVERWRITE, "", HoodieTimeline.REPLACE_COMMIT_ACTION);
    commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

    //make sure view doesnt include fileId1
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
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1);
    String fileName2 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2);
    String fileName3 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId3);
    String fileName4 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId4);
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
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.of(commitMetadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
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

    assertFalse(roView.getLatestBaseFiles(partitionPath1)
            .anyMatch(dfile -> dfile.getFileId().equals(fileId1) || dfile.getFileId().equals(fileId2) || dfile.getFileId().equals(fileId3)),
        "No commit, should not find any data file");
    // Only one commit
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1);
    String fileName2 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2);
    String fileName3 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId3);
    new File(basePath + "/" + partitionPath1 + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName2).createNewFile();
    new File(basePath + "/" + partitionPath1 + "/" + fileName3).createNewFile();

    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
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
    HoodieInstant instant2 = new HoodieInstant(State.REQUESTED, HoodieTimeline.REPLACE_COMMIT_ACTION, clusterTime);
    HoodieRequestedReplaceMetadata requestedReplaceMetadata = HoodieRequestedReplaceMetadata.newBuilder()
        .setClusteringPlan(plan).setOperationType(WriteOperationType.CLUSTER.name()).build();
    metaClient.getActiveTimeline().saveToPendingReplaceCommit(instant2, TimelineMetadataUtils.serializeRequestedReplaceMetadata(requestedReplaceMetadata));

    //make sure view doesnt include fileId1
    refreshFsView();
    Set<String> fileIds =
        fsView.getFileGroupsInPendingClustering().map(e -> e.getLeft().getFileId()).collect(Collectors.toSet());
    assertTrue(fileIds.contains(fileId1));
    assertTrue(fileIds.contains(fileId2));
    assertFalse(fileIds.contains(fileId3));
  }

  /**
   *
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
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1);
    String fileName2 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();

    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);

    // build writeStats
    HashMap<String, List<String>> partitionToFile1 = new HashMap<>();
    ArrayList<String> files1 = new ArrayList<>();
    files1.add(fileId1);
    files1.add(fileId2);
    partitionToFile1.put(partitionPath, files1);
    List<HoodieWriteStat> writeStats1 = buildWriteStats(partitionToFile1, commitTime1);

    HoodieCommitMetadata commitMetadata1 =
        CommitUtils.buildMetadata(writeStats1, new HashMap<>(), Option.empty(), WriteOperationType.INSERT, "", HoodieTimeline.COMMIT_ACTION);
    saveAsComplete(commitTimeline, instant1, Option.of(commitMetadata1.toJsonString().getBytes(StandardCharsets.UTF_8)));
    commitTimeline.reload();

    // replace commit
    String commitTime2 = "2";
    String fileName3 = FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId3);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();

    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.REPLACE_COMMIT_ACTION, commitTime2);
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
        CommitUtils.buildMetadata(writeStats2, partitionToReplaceFileIds, Option.empty(), WriteOperationType.INSERT_OVERWRITE, "", HoodieTimeline.REPLACE_COMMIT_ACTION);
    saveAsComplete(commitTimeline, instant2, Option.of(commitMetadata2.toJsonString().getBytes(StandardCharsets.UTF_8)));

    // another insert commit
    String commitTime3 = "3";
    String fileName4 = FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId4);
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant instant3 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime3);

    // build writeStats
    HashMap<String, List<String>> partitionToFile3 = new HashMap<>();
    ArrayList<String> files3 = new ArrayList<>();
    files3.add(fileId4);
    partitionToFile3.put(partitionPath, files3);
    List<HoodieWriteStat> writeStats3 = buildWriteStats(partitionToFile3, commitTime3);
    HoodieCommitMetadata commitMetadata3 =
        CommitUtils.buildMetadata(writeStats3, new HashMap<>(), Option.empty(), WriteOperationType.INSERT, "", HoodieTimeline.COMMIT_ACTION);
    saveAsComplete(commitTimeline, instant3, Option.of(commitMetadata3.toJsonString().getBytes(StandardCharsets.UTF_8)));

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

    allBaseFiles = fsView.getAllBaseFiles(partitionPath).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, allBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId3));
    assertTrue(allBaseFiles.contains(fileId4));

    // could see fileId3 because clustering is committed.
    latestBaseFiles = fsView.getLatestBaseFiles().map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, latestBaseFiles.size());
    assertTrue(allBaseFiles.contains(fileId3));
    assertTrue(allBaseFiles.contains(fileId4));

    // could see fileId3 because clustering is committed.
    latestBaseFilesPerPartition = fsView.getLatestBaseFiles(partitionPath).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(2, latestBaseFiles.size());
    assertTrue(latestBaseFilesPerPartition.contains(fileId3));
    assertTrue(latestBaseFilesPerPartition.contains(fileId4));

    HoodieWrapperFileSystem fs = metaClient.getFs();
    fs.delete(new Path(basePath + "/.hoodie", "1.commit"), false);
    fs.delete(new Path(basePath + "/.hoodie", "1.inflight"), false);
    fs.delete(new Path(basePath + "/.hoodie", "1.commit.requested"), false);
    fs.delete(new Path(basePath + "/.hoodie", "2.replacecommit"), false);

    metaClient.reloadActiveTimeline();
    refreshFsView();
    // do check after delete some commit file
    latestBaseFilesBeforeOrOn = fsView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime3).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(3, latestBaseFilesBeforeOrOn.size());
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId1));
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId2));
    assertTrue(latestBaseFilesBeforeOrOn.contains(fileId4));

    // couldn't see fileId3 because clustering is not committed.
    baseFileOn = fsView.getBaseFileOn(partitionPath, commitTime2, fileId3);
    assertFalse(baseFileOn.isPresent());

    latestBaseFilesInRange = fsView.getLatestBaseFilesInRange(commits).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
    assertEquals(3, latestBaseFilesInRange.size());
    assertTrue(latestBaseFilesInRange.contains(fileId1));
    assertTrue(latestBaseFilesInRange.contains(fileId2));
    assertTrue(latestBaseFilesInRange.contains(fileId4));

    allBaseFiles = fsView.getAllBaseFiles(partitionPath).map(HoodieBaseFile::getFileId).collect(Collectors.toList());
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

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
