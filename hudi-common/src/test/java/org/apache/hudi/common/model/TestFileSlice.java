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

package org.apache.hudi.common.model;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.storage.StoragePath;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link FileSlice}.
 */
public class TestFileSlice {
  private static final String PARTITION_PATH = "test_partition";
  private static final String FILE_ID = "test_file_id";
  private static final String BASE_INSTANT = "001";
  private static final String BASE_FILE_NAME =
      "b5068208-e1a4-11e6-bf01-fe55135034f3_1-0-1_" + BASE_INSTANT + ".parquet";

  @Test
  void testGetLatestInstantTime() {
    String baseInstant = "003";
    String deltaInstant2 = "002";
    String deltaInstant4 = "004";

    FileSlice fileSlice = new FileSlice("par1", baseInstant, "fg1");
    assertThat(fileSlice.getLatestInstantTime(), is(baseInstant));

    fileSlice.addLogFile(new HoodieLogFile(new StoragePath(getLogFileName(deltaInstant2))));
    assertThat(fileSlice.getLatestInstantTime(), is(baseInstant));

    fileSlice.addLogFile(new HoodieLogFile(new StoragePath(getLogFileName(deltaInstant4))));
    assertThat(fileSlice.getLatestInstantTime(), is(deltaInstant4));
  }

  private static String getLogFileName(String instantTime) {
    return ".fg1_" + instantTime + ".log.1_1-0-1";
  }

  @Test
  public void testGetAllFilesWithBaseFileOnly() {
    // Create a FileSlice with only a base file and no log files
    HoodieBaseFile baseFile = new HoodieBaseFile(
        "file://" + PARTITION_PATH + "/" + BASE_FILE_NAME);
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(PARTITION_PATH, FILE_ID),
        BASE_INSTANT,
        baseFile,
        Collections.emptyList()
    );

    List<String> allFiles = fileSlice.getAllFileNames();
    assertEquals(1, allFiles.size());
    assertTrue(allFiles.contains(baseFile.getFileName()));
  }

  @Test
  public void testGetAllFilesWithLogFilesOnly() {
    // Create a FileSlice with no base file but with log files
    // Log files must follow the proper naming convention: .{fileId}_{instant}.log.{version}
    HoodieLogFile logFile1 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_002.log.1"));
    HoodieLogFile logFile2 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_003.log.2"));

    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(PARTITION_PATH, FILE_ID),
        BASE_INSTANT,
        null, // No base file
        Arrays.asList(logFile1, logFile2)
    );

    List<String> allFiles = fileSlice.getAllFileNames();
    assertEquals(2, allFiles.size());
    assertTrue(allFiles.contains(logFile1.getFileName()));
    assertTrue(allFiles.contains(logFile2.getFileName()));
  }

  @Test
  public void testGetAllFilesWithBaseFileAndLogFiles() {
    // Create a FileSlice with both base file and log files
    HoodieBaseFile baseFile = new HoodieBaseFile(
        "file://" + PARTITION_PATH + "/" + BASE_FILE_NAME);
    // Log files must follow the proper naming convention: .{fileId}_{instant}.log.{version}
    HoodieLogFile logFile1 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_004.log.1"));
    HoodieLogFile logFile2 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_005.log.2"));

    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(PARTITION_PATH, FILE_ID),
        BASE_INSTANT,
        baseFile,
        Arrays.asList(logFile1, logFile2)
    );

    List<String> allFiles = fileSlice.getAllFileNames();
    assertEquals(3, allFiles.size());
    assertTrue(allFiles.contains(baseFile.getFileName()));
    assertTrue(allFiles.contains(logFile1.getFileName()));
    assertTrue(allFiles.contains(logFile2.getFileName()));
  }

  @Test
  public void testGetAllFilesEmptyFileSlice() {
    // Test with an empty file slice (no files and no base file)
    FileSlice fileSlice = new FileSlice(PARTITION_PATH, BASE_INSTANT, FILE_ID);

    List<String> allFiles = fileSlice.getAllFileNames();
    assertEquals(0, allFiles.size());
  }

  @Test
  public void testFilterLogFiles() {
    // Create a FileSlice with both base file and log files
    HoodieBaseFile baseFile = new HoodieBaseFile(
        "file://" + PARTITION_PATH + "/" + BASE_FILE_NAME);
    // Log files must follow the proper naming convention: .{fileId}_{instant}.log.{version}
    HoodieLogFile logFile1 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_004.log.1"));
    HoodieLogFile logFile2 = new HoodieLogFile(new StoragePath(PARTITION_PATH + "/." + FILE_ID + "_005.log.2"));
    FileSlice fileSlice = new FileSlice(
        new HoodieFileGroupId(PARTITION_PATH, FILE_ID),
        BASE_INSTANT,
        baseFile,
        Arrays.asList(logFile1, logFile2)
    );
    // test filter log files that matches one log file
    FileSlice fileSliceAfterFilterPart = fileSlice.filterLogFiles(logFile -> logFile.getFileName().endsWith("_004.log.1"));
    assertEquals(1, fileSliceAfterFilterPart.getLogFiles().count());
    assertEquals(fileSlice.getFileGroupId(), fileSliceAfterFilterPart.getFileGroupId());
    assertEquals(fileSlice.getBaseFile(), fileSliceAfterFilterPart.getBaseFile());
    assertEquals(fileSlice.getBaseInstantTime(), fileSliceAfterFilterPart.getBaseInstantTime());

    // test filter log files that matches all log files
    FileSlice fileSliceAfterFilterAll = fileSlice.filterLogFiles(logFile -> true);
    assertEquals(fileSlice, fileSliceAfterFilterAll);
    // if all log files matches, the new file slice will share the same object references with the original file slice
    assertTrue(fileSlice == fileSliceAfterFilterAll);

    // test filter log files that matches no log files
    FileSlice fileSliceAfterFilterNone = fileSlice.filterLogFiles(logFile -> false);
    assertEquals(0, fileSliceAfterFilterNone.getLogFiles().count());
    assertEquals(fileSlice.getFileGroupId(), fileSliceAfterFilterPart.getFileGroupId());
    assertEquals(fileSlice.getBaseFile(), fileSliceAfterFilterPart.getBaseFile());
    assertEquals(fileSlice.getBaseInstantTime(), fileSliceAfterFilterPart.getBaseInstantTime());
  }

  @Test
  void testGetTotalFileSizeAsParquetFormat() {
    FileSlice fileSlice = new FileSlice(PARTITION_PATH, BASE_INSTANT, "file-id");
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(PARTITION_PATH + "/.file-id_002.log.1_1-0-1"), 1000L));
    fileSlice.addLogFile(new HoodieLogFile(
        new StoragePath(PARTITION_PATH + "/file-id_1-0-1_002_2.log.parquet"), 2000L));

    assertEquals(2100L, fileSlice.getTotalFileSizeAsParquetFormat(createLogConfig("avro", 0.1)));
    assertEquals(3000L, fileSlice.getTotalFileSizeAsParquetFormat(createLogConfig("parquet", 0.1)));
    assertEquals(3000L, fileSlice.getTotalFileSizeAsParquetFormat(createLogConfig("hfile", 0.1)));
  }

  private static HoodieConfig createLogConfig(String logDataBlockFormat, double logFileFraction) {
    HoodieConfig config = new HoodieConfig();
    config.setValue(HoodieStorageConfig.LOGFILE_DATA_BLOCK_FORMAT, logDataBlockFormat);
    config.setValue(
        HoodieStorageConfig.LOGFILE_TO_PARQUET_COMPRESSION_RATIO_FRACTION,
        String.valueOf(logFileFraction));
    return config;
  }
}
