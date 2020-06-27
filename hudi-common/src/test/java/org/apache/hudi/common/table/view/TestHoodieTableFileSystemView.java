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

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests hoodie table file system view {@link HoodieTableFileSystemView}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestHoodieTableFileSystemView extends HoodieCommonTestHarness {

  private static final Logger LOG = LogManager.getLogger(TestHoodieTableFileSystemView.class);

  private static String TEST_WRITE_TOKEN = "1-0-1";

  protected SyncableFileSystemView fsView;
  protected BaseFileOnlyView roView;
  protected SliceView rtView;

  @BeforeEach
  public void init() throws IOException {
    initMetaClient();
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
   * Test case for view generation on a file group where the only file-slice does not have base-file. This is the case
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

  protected void testViewForFileSlicesWithNoBaseFile(int expNumTotalFileSlices, int expNumTotalBaseFiles,
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
    assertEquals(expNumTotalBaseFiles, roView.getAllBaseFiles(partitionPath).count(),
        "Total number of base-files in view matches expected");
    assertEquals(1, fsView.getAllFileGroups(partitionPath).count(),
        "Total number of file-groups in view matches expected");
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false, 2, 1, true);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false, 2, 2, true);
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true, 2, 1, true);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true, 2, 2, true);
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

  /**
   * Helper method to test Views in the presence of concurrent compaction.
   * 
   * @param skipCreatingBaseFile if set, first File Slice will not have base-file set. This would simulate inserts going
   *        directly to log files
   * @param isCompactionInFlight if set, compaction was inflight (running) when view was tested first time, otherwise
   *        compaction was in requested state
   * @param expTotalFileSlices Total number of file-slices across file-groups in the partition path
   * @param expTotalBaseFiles Total number of base-files across file-groups in the partition path
   * @param includeInvalidAndInflight Whether view includes inflight and invalid file-groups.
   * @throws Exception -
   */
  protected void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingBaseFile, boolean isCompactionInFlight,
      int expTotalFileSlices, int expTotalBaseFiles, boolean includeInvalidAndInflight) throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    // if skipCreatingBaseFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String baseFileName = null;
    if (!skipCreatingBaseFile) {
      baseFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId);
      new File(basePath + "/" + partitionPath + "/" + baseFileName).createNewFile();
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

    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant2, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant3, Option.empty());

    refreshFsView();
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    String compactionRequestedTime = "4";
    String compactBaseFileName = FSUtils.makeBaseFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());
    HoodieInstant compactionInstant;
    if (isCompactionInFlight) {
      // Create a Base-file but this should be skipped by view
      new File(basePath + "/" + partitionPath + "/" + compactBaseFileName).createNewFile();
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
    assertFalse(slices.get(0).getBaseFile().isPresent(), "Latest File Slice must not have base-file");
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

    List<HoodieBaseFile> baseFiles = roView.getAllBaseFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingBaseFile) {
      assertTrue(baseFiles.isEmpty(), "No base file expected");
    } else {
      assertEquals(1, baseFiles.size(), "One base-file is expected as there is only one file-group");
      assertEquals(baseFileName, baseFiles.get(0).getFileName(), "Expect only valid base-file");
    }

    // Merge API Tests
    List<FileSlice> fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size(), "Expect file-slice to be merged");
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    if (!skipCreatingBaseFile) {
      assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName(), "Base file must be present");
    } else {
      assertFalse(fileSlice.getBaseFile().isPresent(), "No base-file expected as it was not created");
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
    assertFalse(fileSlice.getBaseFile().isPresent(), "No base-file expected in latest file-slice");
    assertEquals(compactionRequestedTime, fileSlice.getBaseInstantTime(),
        "Compaction requested instant must be base instant");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Log files must include only those after compaction request");
    assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");

    // Base Files API tests
    baseFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    if (skipCreatingBaseFile) {
      assertEquals(0, baseFiles.size(), "Expect no base file to be returned");
    } else {
      assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
      baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), instantTime1, "Expect base-file for instant 1 be returned"));
    }
    baseFiles = roView.getLatestBaseFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingBaseFile) {
      assertEquals(0, baseFiles.size(), "Expect no base file to be returned");
    } else {
      assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
      baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), instantTime1, "Expect base-file for instant 1 be returned"));
    }
    baseFiles = roView.getLatestBaseFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    if (skipCreatingBaseFile) {
      assertEquals(0, baseFiles.size(), "Expect no base file to be returned");
    } else {
      assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
      baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), instantTime1, "Expect base-file for instant 1 be returned"));
    }
    baseFiles = roView.getLatestBaseFilesInRange(allInstantTimes).collect(Collectors.toList());
    if (skipCreatingBaseFile) {
      assertEquals(0, baseFiles.size(), "Expect no base file to be returned");
    } else {
      assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
      baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), instantTime1, "Expect base-file for instant 1 be returned"));
    }

    // Inflight/Orphan File-groups needs to be in the view

    // There is a base-file with this inflight file-id
    final String inflightFileId1 = UUID.randomUUID().toString();
    // There is a log-file with this inflight file-id
    final String inflightFileId2 = UUID.randomUUID().toString();
    // There is an orphan base file with this file-id
    final String orphanFileId1 = UUID.randomUUID().toString();
    // There is an orphan log data file with this file-id
    final String orphanFileId2 = UUID.randomUUID().toString();
    final String invalidInstantId = "INVALIDTIME";
    String inflightDeltaInstantTime = "7";
    String orphanBaseFileName = FSUtils.makeBaseFileName(invalidInstantId, TEST_WRITE_TOKEN, orphanFileId1);
    new File(basePath + "/" + partitionPath + "/" + orphanBaseFileName).createNewFile();
    String orphanLogFileName =
        FSUtils.makeLogFileName(orphanFileId2, HoodieLogFile.DELTA_EXTENSION, invalidInstantId, 0, TEST_WRITE_TOKEN);
    new File(basePath + "/" + partitionPath + "/" + orphanLogFileName).createNewFile();
    String inflightBaseFileName = FSUtils.makeBaseFileName(inflightDeltaInstantTime, TEST_WRITE_TOKEN, inflightFileId1);
    new File(basePath + "/" + partitionPath + "/" + inflightBaseFileName).createNewFile();
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
    baseFiles = allRawFileSlices.stream().flatMap(slice -> {
      if (slice.getBaseFile().isPresent()) {
        return Stream.of(slice.getBaseFile().get());
      }
      return Stream.empty();
    }).collect(Collectors.toList());

    if (includeInvalidAndInflight) {
      assertEquals(2 + (isCompactionInFlight ? 1 : 0) + (skipCreatingBaseFile ? 0 : 1), baseFiles.size(),
          "Inflight/Orphan base-file is also expected");
      Set<String> fileNames = baseFiles.stream().map(HoodieBaseFile::getFileName).collect(Collectors.toSet());
      assertTrue(fileNames.contains(orphanBaseFileName), "Expect orphan base-file to be present");
      assertTrue(fileNames.contains(inflightBaseFileName), "Expect inflight base-file to be present");
      if (!skipCreatingBaseFile) {
        assertTrue(fileNames.contains(baseFileName), "Expect old committed base-file");
      }

      if (isCompactionInFlight) {
        assertTrue(fileNames.contains(compactBaseFileName), "Expect inflight compacted base file to be present");
      }

      fileSliceList = getLatestRawFileSlices(partitionPath).collect(Collectors.toList());
      assertEquals(includeInvalidAndInflight ? 5 : 1, fileSliceList.size(),
          "Expect both inflight and orphan file-slice to be included");
      Map<String, FileSlice> fileSliceMap =
          fileSliceList.stream().collect(Collectors.toMap(FileSlice::getFileId, r -> r));
      FileSlice orphanFileSliceWithBaseFile = fileSliceMap.get(orphanFileId1);
      FileSlice orphanFileSliceWithLogFile = fileSliceMap.get(orphanFileId2);
      FileSlice inflightFileSliceWithBaseFile = fileSliceMap.get(inflightFileId1);
      FileSlice inflightFileSliceWithLogFile = fileSliceMap.get(inflightFileId2);

      assertEquals(invalidInstantId, orphanFileSliceWithBaseFile.getBaseInstantTime(),
          "Orphan File Slice with base-file check base-commit");
      assertEquals(orphanBaseFileName, orphanFileSliceWithBaseFile.getBaseFile().get().getFileName(),
          "Orphan File Slice with base-file check base-file");
      assertEquals(0, orphanFileSliceWithBaseFile.getLogFiles().count(),
          "Orphan File Slice with base-file check base-file");
      assertEquals(inflightDeltaInstantTime, inflightFileSliceWithBaseFile.getBaseInstantTime(),
          "Inflight File Slice with base-file check base-commit");
      assertEquals(inflightBaseFileName, inflightFileSliceWithBaseFile.getBaseFile().get().getFileName(),
          "Inflight File Slice with base-file check base-file");
      assertEquals(0, inflightFileSliceWithBaseFile.getLogFiles().count(),
          "Inflight File Slice with base-file check base-file");
      assertEquals(invalidInstantId, orphanFileSliceWithLogFile.getBaseInstantTime(),
          "Orphan File Slice with log-file check base-commit");
      assertFalse(orphanFileSliceWithLogFile.getBaseFile().isPresent(),
          "Orphan File Slice with log-file check base-file");
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
      // For inflight compaction, we already create a base-file to test concurrent inflight case.
      // If we skipped creating base file corresponding to compaction commit, create it now
      new File(basePath + "/" + partitionPath + "/" + compactBaseFileName).createNewFile();
      commitTimeline.createNewInstant(compactionInstant);
    }

    commitTimeline.saveAsComplete(compactionInstant, Option.empty());
    refreshFsView();
    // populate the cache
    roView.getAllBaseFiles(partitionPath);

    fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    LOG.info("FILESLICE LIST=" + fileSliceList);
    baseFiles = fileSliceList.stream().map(FileSlice::getBaseFile).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "Expect only one base-files in latest view as there is only one file-group");
    assertEquals(compactBaseFileName, baseFiles.get(0).getFileName(), "Base Filename must match");
    assertEquals(1, fileSliceList.size(), "Only one latest file-slice in the partition");
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId(), "Check file-Id is set correctly");
    assertEquals(compactBaseFileName, fileSlice.getBaseFile().get().getFileName(),
        "Check base-filename is set correctly");
    assertEquals(compactionRequestedTime, fileSlice.getBaseInstantTime(),
        "Ensure base-instant is now compaction request instant");
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, logFiles.size(), "Only log-files after compaction request shows up");
    assertEquals(fileName4, logFiles.get(0).getFileName(), "Log File Order check");
    assertEquals(fileName3, logFiles.get(1).getFileName(), "Log File Order check");

    // Base Files API tests
    baseFiles = roView.getLatestBaseFiles().collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
    baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), compactionRequestedTime, "Expect base-file created by compaction be returned"));
    baseFiles = roView.getLatestBaseFiles(partitionPath).collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
    baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), compactionRequestedTime, "Expect base-file created by compaction be returned"));
    baseFiles = roView.getLatestBaseFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
    baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), compactionRequestedTime, "Expect base-file created by compaction be returned"));
    baseFiles = roView.getLatestBaseFilesInRange(allInstantTimes).collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "Expect only one base-file to be sent");
    baseFiles.forEach(bf -> assertEquals(bf.getCommitTime(), compactionRequestedTime, "Expect base-file created by compaction be returned"));

    assertEquals(expTotalFileSlices, rtView.getAllFileSlices(partitionPath).count(),
        "Total number of file-slices in partitions matches expected");
    assertEquals(expTotalBaseFiles, roView.getAllBaseFiles(partitionPath).count(),
        "Total number of base-files in partitions matches expected");
    // file-groups includes inflight/invalid file-ids
    assertEquals(5, fsView.getAllFileGroups(partitionPath).count(),
        "Total number of file-groups in partitions matches expected");
  }

  @Test
  public void testGetLatestBaseFilesForFileId() throws IOException {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    assertFalse(roView.getLatestBaseFiles(partitionPath).anyMatch(dfile -> dfile.getFileId().equals(fileId)),
        "No commit, should not find any base file");

    // Only one commit, but is not safe
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView();
    assertFalse(roView.getLatestBaseFiles(partitionPath).anyMatch(dfile -> dfile.getFileId().equals(fileId)),
        "No commit, should not find any base file");

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    refreshFsView();
    assertEquals(fileName1, roView.getLatestBaseFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId);
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

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();
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

    List<HoodieBaseFile> baseFileList =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime4).collect(Collectors.toList());
    assertEquals(3, baseFileList.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : baseFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)));

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
    List<HoodieBaseFile> baseFiles =
        roView.getLatestBaseFilesBeforeOrOn("2016/05/01", commitTime3).collect(Collectors.toList());
    filenames = new HashSet<>();
    for (HoodieBaseFile status : baseFiles) {
      filenames.add(status.getFileName());
    }
    if (!isLatestFileSliceOnly) {
      assertEquals(3, baseFiles.size());
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)));
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)));
    } else {
      assertEquals(1, baseFiles.size());
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
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

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

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
      fileGroup.getAllBaseFiles().forEach(baseFile -> {
        assertEquals(fileId, baseFile.getFileId(), "All same fileId should be grouped");
        filenames.add(baseFile.getFileName());
      });
      Set<String> expFileNames = new HashSet<>();
      if (fileId.equals(fileId1)) {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1));
        assertEquals(expFileNames, filenames);
      } else if (fileId.equals(fileId2)) {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2));
          expFileNames.add(FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2));
        assertEquals(expFileNames, filenames);
      } else {
        if (!isLatestFileSliceOnly) {
          expFileNames.add(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3));
        }
        expFileNames.add(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3));
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

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId1)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0, TEST_WRITE_TOKEN))
            .createNewFile();

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

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

    List<HoodieBaseFile> baseFiles =
        roView.getLatestBaseFilesInRange(Arrays.asList(commitTime2, commitTime3)).collect(Collectors.toList());
    assertEquals(isLatestFileSliceOnly ? 2 : 3, baseFiles.size());
    Set<String> filenames = new HashSet<>();
    for (HoodieBaseFile status : baseFiles) {
      filenames.add(status.getFileName());
    }

    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    if (!isLatestFileSliceOnly) {
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)));
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

    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView();
    List<HoodieBaseFile> baseFiles =
        roView.getLatestBaseFilesBeforeOrOn(partitionPath, commitTime2).collect(Collectors.toList());
    if (!isLatestFileSliceOnly) {
      assertEquals(2, baseFiles.size());
      Set<String> filenames = new HashSet<>();
      for (HoodieBaseFile status : baseFiles) {
        filenames.add(status.getFileName());
      }
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)));
      assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)));
    } else {
      assertEquals(0, baseFiles.size());
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

    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
            .createNewFile();

    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime1, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime2, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0, TEST_WRITE_TOKEN))
            .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2))
        .createNewFile();

    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId3))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3))
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
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeBaseFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)));
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

    String baseFileName = FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId);
    new File(fullPartitionPath1 + baseFileName).createNewFile();

    String fileName1 =
        FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0, TEST_WRITE_TOKEN);
    new File(fullPartitionPath1 + fileName1).createNewFile();
    new File(fullPartitionPath2 + FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId)).createNewFile();
    new File(fullPartitionPath2 + fileName1).createNewFile();
    new File(fullPartitionPath3 + FSUtils.makeBaseFileName(instantTime1, TEST_WRITE_TOKEN, fileId)).createNewFile();
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
    String compactBaseFileName = FSUtils.makeBaseFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());

    // Create a Base-file for some of the partitions but this should be skipped by view
    new File(basePath + "/" + partitionPath1 + "/" + compactBaseFileName).createNewFile();
    new File(basePath + "/" + partitionPath2 + "/" + compactBaseFileName).createNewFile();

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

    // Test Base Files
    List<HoodieBaseFile> baseFiles = roView.getAllBaseFiles(partitionPath1).collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "One base-file is expected as there is only one file-group");
    assertEquals("1", baseFiles.get(0).getCommitTime(), "Expect only valid commit");
    baseFiles = roView.getAllBaseFiles(partitionPath2).collect(Collectors.toList());
    assertEquals(1, baseFiles.size(), "One base-file is expected as there is only one file-group");
    assertEquals("1", baseFiles.get(0).getCommitTime(), "Expect only valid commit");

    // Merge API Tests
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3).forEach(partitionPath -> {
      List<FileSlice> fileSliceList =
          rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
      assertEquals(1, fileSliceList.size(), "Expect file-slice to be merged");
      FileSlice fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertEquals(baseFileName, fileSlice.getBaseFile().get().getFileName(), "Base file must be present");
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
      assertFalse(fileSlice.getBaseFile().isPresent(), "No base-file expected in latest file-slice");
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

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }
}
