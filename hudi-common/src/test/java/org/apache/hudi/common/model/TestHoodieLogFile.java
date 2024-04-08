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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieLogFile {
  private final String pathStr = "file:///tmp/hoodie/2021/01/01/.136281f3-c24e-423b-a65a-95dbfbddce1d_100.log.2_1-0-1";
  private final String partitionPath = "2021/01/01";
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int logVersion = 2;
  private final String writeToken = "1-0-1";
  private final String fileExtension = "log";

  private final int length = 10;

  @Test
  void createFromLogFile() {
    FileStatus fileStatus = new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(pathStr));
    HoodieLogFile hoodieLogFile = new HoodieLogFile(fileStatus, partitionPath);
    assertFileGetters(fileStatus, new HoodieLogFile(hoodieLogFile), length);
  }

  @Test
  void createFromFileStatus() {
    FileStatus fileStatus = new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(pathStr));
    HoodieLogFile hoodieLogFile = new HoodieLogFile(fileStatus, partitionPath);
    assertFileGetters(fileStatus, hoodieLogFile, length);
  }

  @Test
  void createFromPath() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new Path(pathStr), partitionPath);
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromPathAndLength() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(new Path(pathStr), length, partitionPath);
    assertFileGetters(null, hoodieLogFile, length);
  }

  @Test
  void createFromString() {
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathStr, partitionPath);
    assertFileGetters(null, hoodieLogFile, -1);
  }

  @Test
  void createFromStringWithSuffix() {
    String suffix = ".cdc";
    String pathWithSuffix = pathStr + suffix;
    HoodieLogFile hoodieLogFile = new HoodieLogFile(pathWithSuffix, partitionPath);
    assertFileGetters(pathWithSuffix, null, hoodieLogFile, -1, suffix);
  }

  private void assertFileGetters(FileStatus fileStatus, HoodieLogFile hoodieLogFile, long fileLength) {
    assertFileGetters(pathStr, fileStatus, hoodieLogFile, fileLength, "");
  }

  private void assertFileGetters(String pathStr, FileStatus fileStatus, HoodieLogFile hoodieLogFile, long fileLength, String suffix) {
    assertEquals(fileId, hoodieLogFile.getFileId());
    assertEquals(baseCommitTime, hoodieLogFile.getDeltaCommitTime());
    assertEquals(logVersion, hoodieLogFile.getLogVersion());
    assertEquals(writeToken, hoodieLogFile.getLogWriteToken());
    assertEquals(fileExtension, hoodieLogFile.getFileExtension());
    assertEquals(new Path(pathStr), hoodieLogFile.getPath());
    assertEquals(fileLength, hoodieLogFile.getFileSize());
    assertEquals(fileStatus, hoodieLogFile.getFileStatus());
    assertEquals(suffix, hoodieLogFile.getSuffix());
    assertEquals(partitionPath, hoodieLogFile.getPartitionPath());
  }
}
