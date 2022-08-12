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

package org.apache.hudi.cli.commands;

import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.HoodieTableHeaderFields;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.functional.CLIFunctionalTestHarness;
import org.apache.hudi.cli.testutils.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.NumericUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.shell.core.CommandResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link FileSystemViewCommand}.
 */
@Tag("functional")
public class TestFileSystemViewCommand extends CLIFunctionalTestHarness {

  private String nonpartitionedTablePath;
  private String partitionedTablePath;
  private String partitionPath;
  private SyncableFileSystemView nonpartitionedFsView;
  private SyncableFileSystemView partitionedFsView;

  @BeforeEach
  public void init() throws IOException {
    createNonpartitionedTable();
    createPartitionedTable();
  }

  private void createNonpartitionedTable() throws IOException {
    HoodieCLI.conf = hadoopConf();

    // Create table and connect
    String nonpartitionedTableName = "nonpartitioned_" + tableName();
    nonpartitionedTablePath = tablePath(nonpartitionedTableName);
    new TableCommand().createTable(
            nonpartitionedTablePath, nonpartitionedTableName,
            "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    Files.createDirectories(Paths.get(nonpartitionedTablePath));

    // Generate 2 commits
    String commitTime1 = "3";
    String commitTime2 = "4";

    String fileId1 = UUID.randomUUID().toString();

    // Write date files and log file
    String testWriteToken = "2-0-2";
    Files.createFile(Paths.get(nonpartitionedTablePath, FSUtils
        .makeBaseFileName(commitTime1, testWriteToken, fileId1)));
    Files.createFile(Paths.get(nonpartitionedTablePath, FSUtils
        .makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, testWriteToken)));
    Files.createFile(Paths.get(nonpartitionedTablePath, FSUtils
        .makeBaseFileName(commitTime2, testWriteToken, fileId1)));
    Files.createFile(Paths.get(nonpartitionedTablePath, FSUtils
        .makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0, testWriteToken)));

    // Write commit files
    Files.createFile(Paths.get(nonpartitionedTablePath, ".hoodie", commitTime1 + ".commit"));
    Files.createFile(Paths.get(nonpartitionedTablePath, ".hoodie", commitTime2 + ".commit"));

    // Reload meta client and create fsView
    metaClient = HoodieTableMetaClient.reload(metaClient);

    nonpartitionedFsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), true);
  }

  private void createPartitionedTable() throws IOException {
    HoodieCLI.conf = hadoopConf();

    // Create table and connect
    String partitionedTableName = "partitioned_" + tableName();
    partitionedTablePath = tablePath(partitionedTableName);
    new TableCommand().createTable(
            partitionedTablePath, partitionedTableName,
            "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    partitionPath = HoodieTestCommitMetadataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String fullPartitionPath = Paths.get(partitionedTablePath, partitionPath).toString();
    Files.createDirectories(Paths.get(fullPartitionPath));

    // Generate 2 commits
    String commitTime1 = "1";
    String commitTime2 = "2";

    String fileId1 = UUID.randomUUID().toString();

    // Write date files and log file
    String testWriteToken = "1-0-1";
    Files.createFile(Paths.get(fullPartitionPath, FSUtils
        .makeBaseFileName(commitTime1, testWriteToken, fileId1)));
    Files.createFile(Paths.get(fullPartitionPath, FSUtils
        .makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, testWriteToken)));
    Files.createFile(Paths.get(fullPartitionPath, FSUtils
        .makeBaseFileName(commitTime2, testWriteToken, fileId1)));
    Files.createFile(Paths.get(fullPartitionPath, FSUtils
        .makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0, testWriteToken)));

    // Write commit files
    Files.createFile(Paths.get(partitionedTablePath, ".hoodie", commitTime1 + ".commit"));
    Files.createFile(Paths.get(partitionedTablePath, ".hoodie", commitTime2 + ".commit"));

    // Reload meta client and create fsView
    metaClient = HoodieTableMetaClient.reload(metaClient);

    partitionedFsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), true);
  }

  /**
   * Test case for 'show fsview all'.
   */
  @Test
  public void testShowCommits() {
    // Test default show fsview all
    CommandResult cr = shell().executeCommand("show fsview all --pathRegex */*/*");
    assertTrue(cr.isSuccess());

    // Get all file groups
    Stream<HoodieFileGroup> fileGroups = partitionedFsView.getAllFileGroups(partitionPath);

    List<Comparable[]> rows = new ArrayList<>();
    fileGroups.forEach(fg -> fg.getAllFileSlices().forEach(fs -> {
      int idx = 0;
      // For base file only Views, do not display any delta-file related columns
      Comparable[] row = new Comparable[8];
      row[idx++] = fg.getPartitionPath();
      row[idx++] = fg.getFileGroupId().getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      row[idx++] = fs.getLogFiles().count();
      row[idx++] = fs.getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();
      row[idx++] = fs.getLogFiles().collect(Collectors.toList()).toString();
      rows.add(row);
    }));

    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILE_SIZE, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE, converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELTA_FILES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILE_SIZE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES);
    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  /**
   * Test case for 'show fsview all' with specified values.
   */
  @Test
  public void testShowCommitsWithSpecifiedValues() {
    // Test command with options, baseFileOnly and maxInstant is 2
    CommandResult cr = shell().executeCommand("show fsview all --pathRegex */*/* --baseFileOnly true --maxInstant 2");
    assertTrue(cr.isSuccess());

    List<Comparable[]> rows = new ArrayList<>();
    Stream<HoodieFileGroup> fileGroups = partitionedFsView.getAllFileGroups(partitionPath);

    // Only get instant 1, since maxInstant was specified 2
    fileGroups.forEach(fg -> fg.getAllFileSlices().filter(fs -> fs.getBaseInstantTime().equals("1")).forEach(fs -> {
      int idx = 0;
      // For base file only Views, do not display any delta-file related columns.
      Comparable[] row = new Comparable[5];
      row[idx++] = fg.getPartitionPath();
      row[idx++] = fg.getFileGroupId().getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      rows.add(row);
    }));

    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_FILE_SIZE, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE, converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE);

    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    expected = removeNonWordAndStripSpace(expected);
    String got = removeNonWordAndStripSpace(cr.getResult().toString());
    assertEquals(expected, got);
  }

  private List<Comparable[]> fileSlicesToCRList(Stream<FileSlice> fileSlice, String partitionPath) {
    List<Comparable[]> rows = new ArrayList<>();
    fileSlice.forEach(fs -> {
      int idx = 0;
      // For base file only Views, do not display any delta-file related columns
      Comparable[] row = new Comparable[13];
      row[idx++] = partitionPath;
      row[idx++] = fs.getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";

      long dataFileSize = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      row[idx++] = dataFileSize;

      row[idx++] = fs.getLogFiles().count();
      row[idx++] = fs.getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();
      long logFilesScheduledForCompactionTotalSize =
          fs.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
              .mapToLong(HoodieLogFile::getFileSize).sum();
      row[idx++] = logFilesScheduledForCompactionTotalSize;

      long logFilesUnscheduledTotalSize =
          fs.getLogFiles().filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
              .mapToLong(HoodieLogFile::getFileSize).sum();
      row[idx++] = logFilesUnscheduledTotalSize;

      double logSelectedForCompactionToBaseRatio =
          dataFileSize > 0 ? logFilesScheduledForCompactionTotalSize / (dataFileSize * 1.0) : -1;
      row[idx++] = logSelectedForCompactionToBaseRatio;
      double logUnscheduledToBaseRatio = dataFileSize > 0 ? logFilesUnscheduledTotalSize / (dataFileSize * 1.0) : -1;
      row[idx++] = logUnscheduledToBaseRatio;

      row[idx++] = fs.getLogFiles().filter(lf -> lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .collect(Collectors.toList()).toString();
      row[idx++] = fs.getLogFiles().filter(lf -> !lf.getBaseCommitTime().equals(fs.getBaseInstantTime()))
          .collect(Collectors.toList()).toString();
      rows.add(row);
    });
    return rows;
  }

  /**(
   * Test case for command 'show fsview latest'.
   */
  @Test
  public void testShowLatestFileSlices() throws IOException {
    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_SIZE, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DELTA_SIZE_SCHEDULED, converterFunction);
    fieldNameToConverterMap.put(HoodieTableHeaderFields.HEADER_DELTA_SIZE_UNSCHEDULED, converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField(HoodieTableHeaderFields.HEADER_PARTITION)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_FILE_ID)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_BASE_INSTANT)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DATA_FILE_SIZE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_NUM_DELTA_FILES)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_TOTAL_DELTA_SIZE)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_SIZE_SCHEDULED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_SIZE_UNSCHEDULED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_BASE_SCHEDULED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_BASE_UNSCHEDULED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES_SCHEDULED)
        .addTableHeaderField(HoodieTableHeaderFields.HEADER_DELTA_FILES_UNSCHEDULED);

    // Test show with partition path '2016/03/15'
    new TableCommand().connect(partitionedTablePath, null, false, 0, 0, 0);
    CommandResult partitionedTableCR = shell().executeCommand("show fsview latest --partitionPath " + partitionPath);
    assertTrue(partitionedTableCR.isSuccess());

    Stream<FileSlice> partitionedFileSlice = partitionedFsView.getLatestFileSlices(partitionPath);

    List<Comparable[]> partitionedRows = fileSlicesToCRList(partitionedFileSlice, partitionPath);
    String partitionedExpected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, partitionedRows);
    partitionedExpected = removeNonWordAndStripSpace(partitionedExpected);
    String partitionedResults = removeNonWordAndStripSpace(partitionedTableCR.getResult().toString());
    assertEquals(partitionedExpected, partitionedResults);

    // Test show for non-partitioned table
    new TableCommand().connect(nonpartitionedTablePath, null, false, 0, 0, 0);
    CommandResult nonpartitionedTableCR = shell().executeCommand("show fsview latest");
    assertTrue(nonpartitionedTableCR.isSuccess());

    Stream<FileSlice> nonpartitionedFileSlice = nonpartitionedFsView.getLatestFileSlices("");

    List<Comparable[]> nonpartitionedRows = fileSlicesToCRList(nonpartitionedFileSlice, "");

    String nonpartitionedExpected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, nonpartitionedRows);
    nonpartitionedExpected = removeNonWordAndStripSpace(nonpartitionedExpected);
    String nonpartitionedResults = removeNonWordAndStripSpace(nonpartitionedTableCR.getResult().toString());
    assertEquals(nonpartitionedExpected, nonpartitionedResults);
  }
}
