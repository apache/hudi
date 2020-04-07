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

import org.apache.hudi.cli.AbstractShellIntegrationTest;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.common.HoodieTestCommitMetadataGenerator;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.NumericUtils;

import org.junit.Before;
import org.junit.Test;
import org.springframework.shell.core.CommandResult;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link FileSystemViewCommand}.
 */
public class TestFileSystemViewCommand extends AbstractShellIntegrationTest {
  private String tablePath;
  private String partitionPath;
  private SyncableFileSystemView fsView;

  private static String TEST_WRITE_TOKEN = "1-0-1";

  @Before
  public void init() throws IOException {
    HoodieCLI.conf = jsc.hadoopConfiguration();

    // Create table and connect
    String tableName = "test_table";
    tablePath = basePath + File.separator + tableName;
    new TableCommand().createTable(
        tablePath, tableName,
        "COPY_ON_WRITE", "", 1, "org.apache.hudi.common.model.HoodieAvroPayload");

    metaClient = HoodieCLI.getTableMetaClient();

    partitionPath = HoodieTestCommitMetadataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String fullPartitionPath = tablePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();

    // Generate 2 commits
    String commitTime1 = "1";
    String commitTime2 = "2";

    String fileId1 = UUID.randomUUID().toString();

    // Write date files and log file
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0, TEST_WRITE_TOKEN))
        .createNewFile();
    new File(fullPartitionPath + "/" + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId1))
        .createNewFile();
    new File(fullPartitionPath + "/"
        + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // Write commit files
    new File(tablePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(tablePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();

    // Reload meta client and create fsView
    metaClient = HoodieTableMetaClient.reload(metaClient);

    fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline(), true);
  }

  /**
   * Test case for 'show fsview all'.
   */
  @Test
  public void testShowCommits() {
    // Test default show fsview all
    CommandResult cr = getShell().executeCommand("show fsview all");
    assertTrue(cr.isSuccess());

    // Get all file groups
    Stream<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath);

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
    fieldNameToConverterMap.put("Total Delta File Size", converterFunction);
    fieldNameToConverterMap.put("Data-File Size", converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField("Partition").addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant").addTableHeaderField("Data-File").addTableHeaderField("Data-File Size")
        .addTableHeaderField("Num Delta Files").addTableHeaderField("Total Delta File Size")
        .addTableHeaderField("Delta Files");
    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    assertEquals(expected, cr.getResult().toString());

    // Test command with options, baseFileOnly and maxInstant is 2
    cr = getShell().executeCommand("show fsview all --baseFileOnly true --maxInstant 2");
    assertTrue(cr.isSuccess());

    // Clear rows and construct expect result
    rows.clear();

    fileGroups = fsView.getAllFileGroups(partitionPath);
    fileGroups.forEach(fg -> fg.getAllFileSlices().filter(fs -> fs.getBaseInstantTime().equals("1")).forEach(fs -> {
      int idx = 0;
      // For base file only Views, do not display any delta-file related columns
      Comparable[] row = new Comparable[5];
      row[idx++] = fg.getPartitionPath();
      row[idx++] = fg.getFileGroupId().getFileId();
      row[idx++] = fs.getBaseInstantTime();
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getPath() : "";
      row[idx++] = fs.getBaseFile().isPresent() ? fs.getBaseFile().get().getFileSize() : -1;
      rows.add(row);
    }));

    header = new TableHeader().addTableHeaderField("Partition").addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant").addTableHeaderField("Data-File").addTableHeaderField("Data-File Size");

    expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    assertEquals(expected, cr.getResult().toString());
  }

  /**
   * Test case for command 'show fsview latest'.
   */
  @Test
  public void testShowLatestFileSlices() {
    // Test show with partition path '2016/03/15'
    CommandResult cr = getShell().executeCommand("show fsview latest --partitionPath " + partitionPath);
    assertTrue(cr.isSuccess());

    Stream<FileSlice> fileSlice = fsView.getLatestFileSlices(partitionPath);

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

    Function<Object, String> converterFunction =
        entry -> NumericUtils.humanReadableByteCount((Double.parseDouble(entry.toString())));
    Map<String, Function<Object, String>> fieldNameToConverterMap = new HashMap<>();
    fieldNameToConverterMap.put("Data-File Size", converterFunction);
    fieldNameToConverterMap.put("Total Delta Size", converterFunction);
    fieldNameToConverterMap.put("Delta Size - compaction scheduled", converterFunction);
    fieldNameToConverterMap.put("Delta Size - compaction unscheduled", converterFunction);

    TableHeader header = new TableHeader().addTableHeaderField("Partition").addTableHeaderField("FileId")
        .addTableHeaderField("Base-Instant").addTableHeaderField("Data-File").addTableHeaderField("Data-File Size")
        .addTableHeaderField("Num Delta Files").addTableHeaderField("Total Delta Size")
        .addTableHeaderField("Delta Size - compaction scheduled")
        .addTableHeaderField("Delta Size - compaction unscheduled")
        .addTableHeaderField("Delta To Base Ratio - compaction scheduled")
        .addTableHeaderField("Delta To Base Ratio - compaction unscheduled")
        .addTableHeaderField("Delta Files - compaction scheduled")
        .addTableHeaderField("Delta Files - compaction unscheduled");
    String expected = HoodiePrintHelper.print(header, fieldNameToConverterMap, "", false, -1, false, rows);
    assertEquals(expected, cr.getResult().toString());
  }
}
