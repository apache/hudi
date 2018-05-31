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

package com.uber.hoodie.common.table.view;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class HoodieTableFileSystemViewTest {

  private HoodieTableMetaClient metaClient;
  private String basePath;
  private HoodieTableFileSystemView fsView;
  private TableFileSystemView.ReadOptimizedView roView;
  private TableFileSystemView.RealtimeView rtView;
  
  @Rule
  public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void init() throws IOException {
    metaClient = HoodieTestUtils.init(tmpFolder.getRoot().getAbsolutePath());;
    basePath = metaClient.getBasePath();
    fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
    roView = (TableFileSystemView.ReadOptimizedView) fsView;
    rtView = (TableFileSystemView.RealtimeView) fsView;
  }

  private void refreshFsView(FileStatus[] statuses) {
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    if (statuses != null) {
      fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants(),
          statuses);
    } else {
      fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants());
    }
    roView = (TableFileSystemView.ReadOptimizedView) fsView;
    rtView = (TableFileSystemView.RealtimeView) fsView;
  }

  /**
   * Test case for view generation on a file group where
   * the only file-slice does not have data-file. This is the case where upserts directly go to log-files
   */
  @Test
  public void testViewForFileSlicesWithNoBaseFile() throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
    String fileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0);
    String fileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    commitTimeline.saveAsComplete(instant1, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant2, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant3, Optional.empty());

    refreshFsView(null);

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
    fileSliceList = rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime2)
        .collect(Collectors.toList());
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
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false);
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true);
  }

  /**
   * Returns all file-slices including uncommitted ones.
   * @param partitionPath
   * @return
   */
  private Stream<FileSlice> getAllRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath)
        .map(group -> group.getAllFileSlicesIncludingInflight())
        .flatMap(sliceList -> sliceList);
  }

  /**
   *  Returns latest raw file-slices including uncommitted ones.
   * @param partitionPath
   * @return
   */
  public Stream<FileSlice> getLatestRawFileSlices(String partitionPath) {
    return fsView.getAllFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSlicesIncludingInflight())
        .filter(fileSliceOpt -> fileSliceOpt.isPresent())
        .map(Optional::get);
  }

  /**
   * Helper method to test Views in the presence of concurrent compaction
   * @param skipCreatingDataFile if set, first File Slice will not have data-file set. This would
   *                             simulate inserts going directly to log files
   * @param isCompactionInFlight if set, compaction was inflight (running) when view was tested first time,
   *                             otherwise compaction was in requested state
   * @throws Exception
   */
  private void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingDataFile,
      boolean isCompactionInFlight) throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    // if skipCreatingDataFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String dataFileName = null;
    if (!skipCreatingDataFile) {
      dataFileName = FSUtils.makeDataFileName(instantTime1, 1, fileId);
      new File(basePath + "/" + partitionPath + "/" + dataFileName).createNewFile();
    }
    String fileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0);
    String fileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    commitTimeline.saveAsComplete(instant1, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant2, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant3, Optional.empty());

    refreshFsView(null);
    List<FileSlice> fileSlices = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    String compactionRequestedTime = "4";
    String compactDataFileName = FSUtils.makeDataFileName(compactionRequestedTime, 1, fileId);
    List<Pair<String, FileSlice>> partitionFileSlicesPairs = new ArrayList<>();
    partitionFileSlicesPairs.add(Pair.of(partitionPath, fileSlices.get(0)));
    HoodieCompactionPlan compactionPlan = CompactionUtils.buildFromFileSlices(partitionFileSlicesPairs,
        Optional.empty(), Optional.empty());
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

    // Fake delta-ingestion after compaction-requested
    String deltaInstantTime4 = "5";
    String deltaInstantTime5 = "6";
    List<String> allInstantTimes = Arrays.asList(instantTime1, deltaInstantTime1, deltaInstantTime2,
        compactionRequestedTime, deltaInstantTime4, deltaInstantTime5);
    String fileName3 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 0);
    String fileName4 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant deltaInstant4 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);
    HoodieInstant deltaInstant5 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    commitTimeline.saveAsComplete(deltaInstant4, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant5, Optional.empty());
    refreshFsView(null);

    List<HoodieDataFile> dataFiles = roView.getAllDataFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertTrue("No data file expected", dataFiles.isEmpty());
    } else {
      assertEquals("One data-file is expected as there is only one file-group", 1, dataFiles.size());
      assertEquals("Expect only valid data-file", dataFileName, dataFiles.get(0).getFileName());
    }

    /** Merge API Tests **/
    List<FileSlice> fileSliceList = rtView.getLatestMergedFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5)
        .collect(Collectors.toList());
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

    fileSliceList = rtView.getLatestFileSlicesBeforeOrOn(partitionPath, deltaInstantTime5)
        .collect(Collectors.toList());
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

    /**  Data Files API tests */
    dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.stream().forEach(df -> {
        assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1);
      });
    }
    dataFiles = roView.getLatestDataFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.stream().forEach(df -> {
        assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1);
      });
    }
    dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.stream().forEach(df -> {
        assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1);
      });
    }
    dataFiles = roView.getLatestDataFilesInRange(allInstantTimes).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertEquals("Expect no data file to be returned", 0, dataFiles.size());
    } else {
      assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
      dataFiles.stream().forEach(df -> {
        assertEquals("Expect data-file for instant 1 be returned", df.getCommitTime(), instantTime1);
      });
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
    String orphanDataFileName = FSUtils.makeDataFileName(invalidInstantId, 1, orphanFileId1);
    new File(basePath + "/" + partitionPath + "/" + orphanDataFileName).createNewFile();
    String orphanLogFileName =
        FSUtils.makeLogFileName(orphanFileId2, HoodieLogFile.DELTA_EXTENSION, invalidInstantId, 0);
    new File(basePath + "/" + partitionPath + "/" + orphanLogFileName).createNewFile();
    String inflightDataFileName = FSUtils.makeDataFileName(inflightDeltaInstantTime, 1, inflightFileId1);
    new File(basePath + "/" + partitionPath + "/" + inflightDataFileName).createNewFile();
    String inflightLogFileName =
        FSUtils.makeLogFileName(inflightFileId2, HoodieLogFile.DELTA_EXTENSION, inflightDeltaInstantTime, 0);
    new File(basePath + "/" + partitionPath + "/" + inflightLogFileName).createNewFile();
    // Mark instant as inflight
    commitTimeline.saveToInflight(new HoodieInstant(State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION,
        inflightDeltaInstantTime), Optional.empty());
    refreshFsView(null);

    List<FileSlice> allRawFileSlices = getAllRawFileSlices(partitionPath).collect(Collectors.toList());
    dataFiles = allRawFileSlices.stream().flatMap(slice -> {
      if (slice.getDataFile().isPresent()) {
        return Stream.of(slice.getDataFile().get());
      }
      return Stream.empty();
    }).collect(Collectors.toList());
    assertEquals("Inflight/Orphan data-file is also expected", 2
        + (isCompactionInFlight ? 1 : 0) + (skipCreatingDataFile ? 0 : 1), dataFiles.size());
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
    assertEquals("Expect both inflight and orphan file-slice to be included",
        5, fileSliceList.size());
    Map<String, FileSlice> fileSliceMap =
        fileSliceList.stream().collect(Collectors.toMap(FileSlice::getFileId, r -> r));
    FileSlice orphanFileSliceWithDataFile =  fileSliceMap.get(orphanFileId1);
    FileSlice orphanFileSliceWithLogFile =  fileSliceMap.get(orphanFileId2);
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
    assertEquals("Orphan File Slice with log-file check data-file", orphanLogFileName,
        logFiles.get(0).getFileName());
    assertEquals("Inflight File Slice with log-file check base-commit", inflightDeltaInstantTime,
        inflightFileSliceWithLogFile.getBaseInstantTime());
    assertFalse("Inflight File Slice with log-file check data-file",
        inflightFileSliceWithLogFile.getDataFile().isPresent());
    logFiles = inflightFileSliceWithLogFile.getLogFiles().collect(Collectors.toList());
    assertEquals("Inflight File Slice with log-file check data-file", 1, logFiles.size());
    assertEquals("Inflight File Slice with log-file check data-file", inflightLogFileName,
        logFiles.get(0).getFileName());

    // Now simulate Compaction completing - Check the view
    if (!isCompactionInFlight) {
      // For inflight compaction, we already create a data-file to test concurrent inflight case.
      // If we skipped creating data file corresponding to compaction commit, create it now
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
    }
    if (isCompactionInFlight) {
      commitTimeline.deleteInflight(compactionInstant);
    } else {
      commitTimeline.deleteCompactionRequested(compactionInstant);
    }
    compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
    commitTimeline.saveAsComplete(compactionInstant, Optional.empty());
    refreshFsView(null);
    // populate the cache
    roView.getAllDataFiles(partitionPath);

    fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    dataFiles = fileSliceList.stream().map(FileSlice::getDataFile)
        .filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
    System.out.println("fileSliceList : " + fileSliceList);
    assertEquals("Expect only one data-files in latest view as there is only one file-group", 1, dataFiles.size());
    assertEquals("Data Filename must match", compactDataFileName, dataFiles.get(0).getFileName());
    assertEquals("Only one latest file-slice in the partition", 1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals("Check file-Id is set correctly", fileId, fileSlice.getFileId());
    assertEquals("Check data-filename is set correctly",
        compactDataFileName, fileSlice.getDataFile().get().getFileName());
    assertEquals("Ensure base-instant is now compaction request instant",
        compactionRequestedTime, fileSlice.getBaseInstantTime());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Only log-files after compaction request shows up", 2, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());

    /**  Data Files API tests */
    dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.stream().forEach(df -> {
      assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(),
          compactionRequestedTime);
    });
    dataFiles = roView.getLatestDataFiles(partitionPath).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.stream().forEach(df -> {
      assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(),
          compactionRequestedTime);
    });
    dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, deltaInstantTime5).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.stream().forEach(df -> {
      assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(),
          compactionRequestedTime);
    });
    dataFiles = roView.getLatestDataFilesInRange(allInstantTimes).collect(Collectors.toList());
    assertEquals("Expect only one data-file to be sent", 1, dataFiles.size());
    dataFiles.stream().forEach(df -> {
      assertEquals("Expect data-file created by compaction be returned", df.getCommitTime(),
          compactionRequestedTime);
    });
  }

  @Test
  public void testGetLatestDataFilesForFileId() throws IOException {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    assertFalse("No commit, should not find any data file",
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst()
            .isPresent());

    // Only one commit, but is not safe
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeDataFileName(commitTime1, 1, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView(null);
    assertFalse("No commit, should not find any data file",
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst()
            .isPresent());

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    commitTimeline.saveAsComplete(instant1, Optional.empty());
    refreshFsView(null);
    assertEquals("", fileName1,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeDataFileName(commitTime2, 1, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    refreshFsView(null);
    assertEquals("", fileName1,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());

    // Make it safe
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime2);
    commitTimeline.saveAsComplete(instant2, Optional.empty());
    refreshFsView(null);
    assertEquals("", fileName2,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());
  }

  @Test
  public void testStreamLatestVersionInPartition() throws IOException {
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
    String fileId4 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(11, statuses.length);
    refreshFsView(null);

    // Check files as of lastest commit.
    List<FileSlice> allSlices = rtView.getAllFileSlices("2016/05/01").collect(Collectors.toList());
    assertEquals(8, allSlices.size());
    Map<String, Long> fileSliceMap = allSlices.stream().collect(
        Collectors.groupingBy(slice -> slice.getFileId(), Collectors.counting()));
    assertEquals(2, fileSliceMap.get(fileId1).longValue());
    assertEquals(3, fileSliceMap.get(fileId2).longValue());
    assertEquals(2, fileSliceMap.get(fileId3).longValue());
    assertEquals(1, fileSliceMap.get(fileId4).longValue());

    List<HoodieDataFile> dataFileList = roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime4)
        .collect(Collectors.toList());
    assertEquals(3, dataFileList.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));

    filenames = Sets.newHashSet();
    List<HoodieLogFile> logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime4)
        .map(slice -> slice.getLogFiles()).flatMap(logFileList -> logFileList)
        .collect(Collectors.toList());
    assertEquals(logFilesList.size(), 4);
    for (HoodieLogFile logFile : logFilesList) {
      filenames.add(logFile.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0)));

    // Reset the max commit time
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime3)
        .collect(Collectors.toList());
    assertEquals(dataFiles.size(), 3);
    filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));

    logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime3).map(slice -> slice.getLogFiles())
        .flatMap(logFileList -> logFileList).collect(Collectors.toList());
    assertEquals(logFilesList.size(), 1);
    assertTrue(logFilesList.get(0).getFileName()
        .equals(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0)));
  }

  @Test
  public void testStreamEveryVersionInPartition() throws IOException {
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView(null);
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups("2016/05/01").collect(Collectors.toList());
    assertEquals(3, fileGroups.size());

    for (HoodieFileGroup fileGroup : fileGroups) {
      String fileId = fileGroup.getId();
      Set<String> filenames = Sets.newHashSet();
      fileGroup.getAllDataFiles().forEach(dataFile -> {
        assertEquals("All same fileId should be grouped", fileId, dataFile.getFileId());
        filenames.add(dataFile.getFileName());
      });
      if (fileId.equals(fileId1)) {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId1),
            FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
      } else if (fileId.equals(fileId2)) {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId2),
            FSUtils.makeDataFileName(commitTime2, 1, fileId2), FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
      } else {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime3, 1, fileId3),
            FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
      }
    }
  }

  @Test
  public void streamLatestVersionInRange() throws IOException {
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId1)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(9, statuses.length);

    refreshFsView(statuses);
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesInRange(Lists.newArrayList(commitTime2, commitTime3))
        .collect(Collectors.toList());
    assertEquals(3, dataFiles.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));

    List<FileSlice> slices = rtView.getLatestFileSliceInRange(Lists.newArrayList(commitTime3, commitTime4))
        .collect(Collectors.toList());
    assertEquals(3, slices.size());
    for (FileSlice slice : slices) {
      if (slice.getFileId().equals(fileId1)) {
        assertEquals(slice.getBaseInstantTime(), commitTime3);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      } else if (slice.getFileId().equals(fileId2)) {
        assertEquals(slice.getBaseInstantTime(), commitTime4);
        assertFalse(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 1);
      } else if (slice.getFileId().equals(fileId3)) {
        assertEquals(slice.getBaseInstantTime(), commitTime4);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      }
    }
  }

  @Test
  public void streamLatestVersionsBefore() throws IOException {
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView(null);
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, commitTime2)
        .collect(Collectors.toList());
    assertEquals(2, dataFiles.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime2, 1, fileId2)));
  }

  @Test
  public void streamLatestVersions() throws IOException {
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

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(10, statuses.length);

    refreshFsView(statuses);

    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    assertEquals(3, fileGroups.size());
    for (HoodieFileGroup fileGroup : fileGroups) {
      List<FileSlice> slices = fileGroup.getAllFileSlices().collect(Collectors.toList());
      if (fileGroup.getId().equals(fileId1)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime1, slices.get(1).getBaseInstantTime());
      } else if (fileGroup.getId().equals(fileId2)) {
        assertEquals(3, slices.size());
        assertEquals(commitTime3, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime2, slices.get(1).getBaseInstantTime());
        assertEquals(commitTime1, slices.get(2).getBaseInstantTime());
      } else if (fileGroup.getId().equals(fileId3)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseInstantTime());
        assertEquals(commitTime3, slices.get(1).getBaseInstantTime());
      }
    }

    List<HoodieDataFile> statuses1 = roView.getLatestDataFiles().collect(Collectors.toList());
    assertEquals(3, statuses1.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : statuses1) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
  }
}
