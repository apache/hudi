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
import org.apache.hudi.common.HoodieCommonTestHarness;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.TableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests hoodie table file system view {@link HoodieTableFileSystemView}.
 */
@SuppressWarnings("ResultOfMethodCallIgnored")
public class TestHoodieTableFileSystemView extends HoodieCommonTestHarness {

  private static final Logger LOG = LoggerFactory.getLogger(TestHoodieTableFileSystemView.class);

  private static String TEST_WRITE_TOKEN = "1-0-1";

  protected SyncableFileSystemView fsView;
  protected TableFileSystemView.ReadOptimizedView roView;
  protected TableFileSystemView.RealtimeView rtView;

  @Before
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
   * Test case for view generation on a file group where the only file-slice does not have data-file. This is the case
   * where upserts directly go to log-files
   */
  @Test
  public void testViewForFileSlicesWithNoBaseFile() throws Exception {
    testViewForFileSlicesWithNoBaseFile(1, 0);
  }

  protected void testViewForFileSlicesWithNoBaseFile(int expNumTotalFileSlices, int expNumTotalDataFiles)
      throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
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

    List<HoodieDataFile> dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    assertTrue("No data file expected", dataFiles.isEmpty());
    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals("File-Id must be set correctly", fileId, fileSlice.getFileId());
    assertFalse("Data file for base instant must be present", fileSlice.getDataFile().isPresent());
    assertEquals("Base Instant for file-group set correctly", instantTime1, fileSlice.getBaseInstantTime());
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Correct number of log-files shows up in file-slice", 2, logFiles.size());
    assertEquals("Log File Order check", fileName2, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(1).getFileName());

    // Check Merged File Slices API
    fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime2).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals("File-Id must be set correctly", fileId, fileSlice.getFileId());
    assertFalse("Data file for base instant must be present", fileSlice.getDataFile().isPresent());
    assertEquals("Base Instant for file-group set correctly", instantTime1, fileSlice.getBaseInstantTime());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Correct number of log-files shows up in file-slice", 2, logFiles.size());
    assertEquals("Log File Order check", fileName2, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(1).getFileName());

    // Check UnCompacted File Slices API
    fileSliceList = rtView.getLatestUnCompactedFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals("File-Id must be set correctly", fileId, fileSlice.getFileId());
    assertFalse("Data file for base instant must be present", fileSlice.getDataFile().isPresent());
    assertEquals("Base Instant for file-group set correctly", instantTime1, fileSlice.getBaseInstantTime());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Correct number of log-files shows up in file-slice", 2, logFiles.size());
    assertEquals("Log File Order check", fileName2, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(1).getFileName());

    assertEquals("Total number of file-slices in view matches expected", expNumTotalFileSlices,
        rtView.getAllFileSlices(partitionPath).count());
    assertEquals("Total number of data-files in view matches expected", expNumTotalDataFiles,
        roView.getAllDataFiles(partitionPath).count());
    assertEquals("Total number of file-groups in view matches expected", 1,
        fsView.getAllFileGroups(partitionPath).count());
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
    return fsView.getAllFileGroups(partitionPath).map(group -> group.getAllFileSlicesIncludingInflight())
        .flatMap(sliceList -> sliceList);
  }

  /**
   * Returns latest raw file-slices including uncommitted ones.
   * 
   * @param partitionPath
   * @return
   */
  public Stream<FileSlice> getLatestRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath).map(fileGroup -> fileGroup.getLatestFileSlicesIncludingInflight())
        .filter(fileSliceOpt -> fileSliceOpt.isPresent()).map(Option::get);
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
   * @throws Exception
   */
  protected void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingDataFile, boolean isCompactionInFlight,
      int expTotalFileSlices, int expTotalDataFiles, boolean includeInvalidAndInflight) throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    // if skipCreatingDataFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = "1";
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

    saveAsComplete(commitTimeline, instant1, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant2, Option.empty());
    saveAsComplete(commitTimeline, deltaInstant3, Option.empty());

    refreshFsView();
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    String compactionRequestedTime = "4";
    String compactDataFileName = FSUtils.makeDataFileName(compactionRequestedTime, TEST_WRITE_TOKEN, fileId);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs, Option.empty(), Option.empty());
    HoodieInstant compactionInstant = null;
    if (isCompactionInFlight) {
      // Create a Data-file but this should be skipped by view
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      HoodieInstant requested = HoodieTimeline.getCompactionRequestedInstant(compactionInstant.getTimestamp());
      commitTimeline.saveToCompactionRequested(requested, AvroUtils.serializeCompactionPlan(compactionPlan));
      commitTimeline.transitionCompactionRequestedToInflight(requested);
    } else {
      compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      commitTimeline.saveToCompactionRequested(compactionInstant, AvroUtils.serializeCompactionPlan(compactionPlan));
    }

    // View immediately after scheduling compaction
    refreshFsView();
    List<FileSlice> slices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals("Expected latest file-slices", 1, slices.size());
    assertEquals("Base-Instant must be compaction Instant", compactionRequestedTime,
        slices.get(0).getBaseInstantTime());
    assertFalse("Latest File Slice must not have data-file", slices.get(0).getDataFile().isPresent());
    assertEquals("Latest File Slice must not have any log-files", 0, slices.get(0).getLogFiles().count());

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

    List<HoodieDataFile> dataFiles = roView.getAllDataFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertTrue("No data file expected", dataFiles.isEmpty());
    } else {
      assertEquals("One data-file is expected as there is only one file-group", 1, dataFiles.size());
      assertEquals("Expect only valid data-file", dataFileName, dataFiles.get(0).getFileName());
    }

    /** Merge API Tests **/
    List<FileSlice> fileSliceList =
        rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals("Expect file-slice to be merged", 1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    if (!skipCreatingDataFile) {
      assertEquals("Data file must be present", dataFileName, fileSlice.getDataFile().get().getFileName());
    } else {
      assertFalse("No data-file expected as it was not created", fileSlice.getDataFile().isPresent());
    }
    assertEquals("Base Instant of penultimate file-slice must be base instant", instantTime1,
        fileSlice.getBaseInstantTime());
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Log files must include those after compaction request", 4, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());
    assertEquals("Log File Order check", fileName2, logFiles.get(2).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(3).getFileName());

    fileSliceList =
        rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5, true).collect(Collectors.toList());
    assertEquals("Expect only one file-id", 1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    assertFalse("No data-file expected in latest file-slice", fileSlice.getDataFile().isPresent());
    assertEquals("Compaction requested instant must be base instant", compactionRequestedTime,
        fileSlice.getBaseInstantTime());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Log files must include only those after compaction request", 2, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());

    /** Data Files API tests */
    dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.forEach(df -> assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1));
    }
    dataFiles = roView.getLatestDataFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.forEach(df -> assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1));
    }
    dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.forEach(df -> assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1));
    }
    dataFiles = roView.getLatestDataFilesInRange(allInstantTimes).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.forEach(df -> assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1));
    }

    /** Inflight/Orphan File-groups needs to be in the view **/

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
      if (slice.getDataFile().isPresent()) {
        return Stream.of(slice.getDataFile().get());
      }
      return Stream.empty();
    }).collect(Collectors.toList());

    if (includeInvalidAndInflight) {
      assertEquals("Inflight/Orphan data-file is also expected",
          2 + (isCompactionInFlight ? 1 : 0) + (skipCreatingDataFile ? 0 : 1), dataFiles.size());
      Set<String> fileNames = dataFiles.stream().map(HoodieDataFile::getFileName).collect(Collectors.toSet());
      assertTrue("Expect orphan data-file to be present", fileNames.contains(orphanDataFileName));
      assertTrue("Expect inflight data-file to be present", fileNames.contains(inflightDataFileName));
      if (!skipCreatingDataFile) {
        assertTrue("Expect old committed data-file", fileNames.contains(dataFileName));
      }

      if (isCompactionInFlight) {
        assertTrue("Expect inflight compacted data file to be present", fileNames.contains(compactDataFileName));
      }

      fileSliceList = getLatestRawFileSlices(partitionPath).collect(Collectors.toList());
      assertEquals("Expect both inflight and orphan file-slice to be included", includeInvalidAndInflight ? 5 : 1,
          fileSliceList.size());
      Map<String, FileSlice> fileSliceMap =
          fileSliceList.stream().collect(Collectors.toMap(FileSlice::getFileId, r -> r));
      FileSlice orphanFileSliceWithDataFile = fileSliceMap.get(orphanFileId1);
      FileSlice orphanFileSliceWithLogFile = fileSliceMap.get(orphanFileId2);
      FileSlice inflightFileSliceWithDataFile = fileSliceMap.get(inflightFileId1);
      FileSlice inflightFileSliceWithLogFile = fileSliceMap.get(inflightFileId2);

      assertEquals("Orphan File Slice with data-file check base-commit", invalidInstantId,
          orphanFileSliceWithDataFile.getBaseInstantTime());
      assertEquals("Orphan File Slice with data-file check data-file", orphanDataFileName,
          orphanFileSliceWithDataFile.getDataFile().get().getFileName());
      assertEquals("Orphan File Slice with data-file check data-file", 0,
          orphanFileSliceWithDataFile.getLogFiles().count());
      assertEquals("Inflight File Slice with data-file check base-commit", inflightDeltaInstantTime,
          inflightFileSliceWithDataFile.getBaseInstantTime());
      assertEquals("Inflight File Slice with data-file check data-file", inflightDataFileName,
          inflightFileSliceWithDataFile.getDataFile().get().getFileName());
      assertEquals("Inflight File Slice with data-file check data-file", 0,
          inflightFileSliceWithDataFile.getLogFiles().count());
      assertEquals("Orphan File Slice with log-file check base-commit", invalidInstantId,
          orphanFileSliceWithLogFile.getBaseInstantTime());
      assertFalse("Orphan File Slice with log-file check data-file",
          orphanFileSliceWithLogFile.getDataFile().isPresent());
      logFiles = orphanFileSliceWithLogFile.getLogFiles().collect(Collectors.toList());
      assertEquals("Orphan File Slice with log-file check data-file", 1, logFiles.size());
      assertEquals("Orphan File Slice with log-file check data-file", orphanLogFileName, logFiles.get(0).getFileName());
      assertEquals("Inflight File Slice with log-file check base-commit", inflightDeltaInstantTime,
          inflightFileSliceWithLogFile.getBaseInstantTime());
      assertFalse("Inflight File Slice with log-file check data-file",
          inflightFileSliceWithLogFile.getDataFile().isPresent());
      logFiles = inflightFileSliceWithLogFile.getLogFiles().collect(Collectors.toList());
      assertEquals("Inflight File Slice with log-file check data-file", 1, logFiles.size());
      assertEquals("Inflight File Slice with log-file check data-file", inflightLogFileName,
          logFiles.get(0).getFileName());
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
    roView.getAllDataFiles(partitionPath);

    fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    LOG.info("FILESLICE LIST={}", fileSliceList);
    dataFiles = fileSliceList.stream().map(FileSlice::getDataFile).filter(Option::isPresent).map(Option::get)
        .collect(Collectors.toList());
    assertEquals("Expect only one data-files in latest view as there is only one file-group", 1, dataFiles.size());
    assertEquals("Data Filename must match", compactDataFileName, dataFiles.get(0).getFileName());
    assertEquals("Only one latest file-slice in the partition", 1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals("Check file-Id is set correctly", fileId, fileSlice.getFileId());
    assertEquals("Check data-filename is set correctly", compactDataFileName,
        fileSlice.getDataFile().get().getFileName());
    assertEquals("Ensure base-instant is now compaction request instant", compactionRequestedTime,
        fileSlice.getBaseInstantTime());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Only log-files after compaction request shows up", 2, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());

    /** Data Files API tests */
    dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.forEach(df -> assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(), compactionRequestedTime));
    dataFiles = roView.getLatestDataFiles(partitionPath).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.forEach(df -> assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(), compactionRequestedTime));
    dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.forEach(df -> assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(), compactionRequestedTime));
    dataFiles = roView.getLatestDataFilesInRange(allInstantTimes).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.forEach(df -> assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(), compactionRequestedTime));

    assertEquals("Total number of file-slices in partitions matches expected", expTotalFileSlices,
        rtView.getAllFileSlices(partitionPath).count());
    assertEquals("Total number of data-files in partitions matches expected", expTotalDataFiles,
        roView.getAllDataFiles(partitionPath).count());
    // file-groups includes inflight/invalid file-ids
    assertEquals("Total number of file-groups in partitions matches expected", 5,
        fsView.getAllFileGroups(partitionPath).count());
  }

  @Test
  public void testGetLatestDataFilesForFileId() throws IOException {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    assertFalse("No commit, should not find any data file", roView.getLatestDataFiles(partitionPath)
        .anyMatch(dfile -> dfile.getFileId().equals(fileId)));

    // Only one commit, but is not safe
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView();
    assertFalse("No commit, should not find any data file", roView.getLatestDataFiles(partitionPath)
        .anyMatch(dfile -> dfile.getFileId().equals(fileId)));

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    saveAsComplete(commitTimeline, instant1, Option.empty());
    refreshFsView();
    assertEquals("", fileName1, roView.getLatestDataFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    refreshFsView();
    assertEquals("", fileName1, roView.getLatestDataFiles(partitionPath)
        .filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get().getFileName());

    // Make it safe
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime2);
    saveAsComplete(commitTimeline, instant2, Option.empty());
    refreshFsView();
    assertEquals("", fileName2, roView.getLatestDataFiles(partitionPath)
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

    List<HoodieDataFile> dataFileList =
        roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime4).collect(Collectors.toList());
    assertEquals(3, dataFileList.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)));

    filenames = Sets.newHashSet();
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
    List<HoodieDataFile> dataFiles =
        roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime3).collect(Collectors.toList());
    filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
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
      Set<String> filenames = Sets.newHashSet();
      fileGroup.getAllDataFiles().forEach(dataFile -> {
        assertEquals("All same fileId should be grouped", fileId, dataFile.getFileId());
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
    roView.getAllDataFiles("2016/05/01/");

    List<HoodieDataFile> dataFiles =
        roView.getLatestDataFilesInRange(Lists.newArrayList(commitTime2, commitTime3)).collect(Collectors.toList());
    assertEquals(isLatestFileSliceOnly ? 2 : 3, dataFiles.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }

    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)));
    if (!isLatestFileSliceOnly) {
      assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)));
    }

    List<FileSlice> slices =
        rtView.getLatestFileSliceInRange(Lists.newArrayList(commitTime3, commitTime4)).collect(Collectors.toList());
    assertEquals(3, slices.size());
    for (FileSlice slice : slices) {
      if (slice.getFileId().equals(fileId1)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      } else if (slice.getFileId().equals(fileId2)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 1);
      } else if (slice.getFileId().equals(fileId3)) {
        assertEquals(slice.getBaseInstantTime(), commitTime4);
        assertTrue(slice.getDataFile().isPresent());
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
    List<HoodieDataFile> dataFiles =
        roView.getLatestDataFilesBeforeOrOn(partitionPath, commitTime2).collect(Collectors.toList());
    if (!isLatestFileSliceOnly) {
      assertEquals(2, dataFiles.size());
      Set<String> filenames = Sets.newHashSet();
      for (HoodieDataFile status : dataFiles) {
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
    fsView.getAllDataFiles(partitionPath);
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

    List<HoodieDataFile> statuses1 = roView.getLatestDataFiles().collect(Collectors.toList());
    assertEquals(3, statuses1.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : statuses1) {
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
    Assert.assertEquals("Expected number of file-groups", 3, groups.size());
    Assert.assertEquals("Partitions must be different for file-groups", 3,
        groups.stream().map(HoodieFileGroup::getPartitionPath).collect(Collectors.toSet()).size());
    Set<String> fileIds = groups.stream().map(HoodieFileGroup::getFileGroupId).map(HoodieFileGroupId::getFileId)
        .collect(Collectors.toSet());
    Assert.assertEquals("File Id must be same", 1, fileIds.size());
    Assert.assertTrue("Expected FileId", fileIds.contains(fileId));

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
        AvroUtils.serializeCompactionPlan(compactionPlan));
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
    List<HoodieDataFile> dataFiles = roView.getAllDataFiles(partitionPath1).collect(Collectors.toList());
    assertEquals("One data-file is expected as there is only one file-group", 1, dataFiles.size());
    assertEquals("Expect only valid commit", "1", dataFiles.get(0).getCommitTime());
    dataFiles = roView.getAllDataFiles(partitionPath2).collect(Collectors.toList());
    assertEquals("One data-file is expected as there is only one file-group", 1, dataFiles.size());
    assertEquals("Expect only valid commit", "1", dataFiles.get(0).getCommitTime());

    /** Merge API Tests **/
    Arrays.asList(partitionPath1, partitionPath2, partitionPath3).forEach(partitionPath -> {
      List<FileSlice> fileSliceList =
          rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
      assertEquals("Expect file-slice to be merged", 1, fileSliceList.size());
      FileSlice fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertEquals("Data file must be present", dataFileName, fileSlice.getDataFile().get().getFileName());
      assertEquals("Base Instant of penultimate file-slice must be base instant", instantTime1,
          fileSlice.getBaseInstantTime());
      List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals("Log files must include those after compaction request", 3, logFiles.size());
      assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
      assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());
      assertEquals("Log File Order check", fileName1, logFiles.get(2).getFileName());

      fileSliceList =
          rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5, true).collect(Collectors.toList());
      assertEquals("Expect only one file-id", 1, fileSliceList.size());
      fileSlice = fileSliceList.get(0);
      assertEquals(fileId, fileSlice.getFileId());
      assertFalse("No data-file expected in latest file-slice", fileSlice.getDataFile().isPresent());
      assertEquals("Compaction requested instant must be base instant", compactionRequestedTime,
          fileSlice.getBaseInstantTime());
      logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
      assertEquals("Log files must include only those after compaction request", 2, logFiles.size());
      assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
      assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());

      // Check getLatestFileSlicesBeforeOrOn excluding fileIds in pending compaction
      fileSliceList =
          rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5, false).collect(Collectors.toList());
      assertEquals("Expect empty list as file-id is in pending compaction", 0, fileSliceList.size());
    });

    Assert.assertEquals(3, fsView.getPendingCompactionOperations().count());
    Set<String> partitionsInCompaction = fsView.getPendingCompactionOperations().map(Pair::getValue)
        .map(CompactionOperation::getPartitionPath).collect(Collectors.toSet());
    Assert.assertEquals(3, partitionsInCompaction.size());
    Assert.assertTrue(partitionsInCompaction.contains(partitionPath1));
    Assert.assertTrue(partitionsInCompaction.contains(partitionPath2));
    Assert.assertTrue(partitionsInCompaction.contains(partitionPath3));

    Set<String> fileIdsInCompaction = fsView.getPendingCompactionOperations().map(Pair::getValue)
        .map(CompactionOperation::getFileId).collect(Collectors.toSet());
    Assert.assertEquals(1, fileIdsInCompaction.size());
    Assert.assertTrue(fileIdsInCompaction.contains(fileId));
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
