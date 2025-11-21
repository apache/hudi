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
        "file://" + PARTITION_PATH + "/test_base_file.parquet");
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
        "file://" + PARTITION_PATH + "/test_base_file.parquet");
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
}
