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

import org.apache.hudi.common.util.Option;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieBaseFile {
  private final String fileName = "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_100.parquet";
  private final String pathStr = "file:/tmp/hoodie/2021/01/01/" + fileName;
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int length = 10;

  @Test
  void createFromHoodieBaseFile() {
    FileStatus fileStatus = new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(pathStr));
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(fileStatus);
    assertFileGetters(fileStatus, new HoodieBaseFile(hoodieBaseFile), length, Option.empty());
  }

  @Test
  void createFromFileStatus() {
    FileStatus fileStatus = new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(pathStr));
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(fileStatus);
    assertFileGetters(fileStatus, hoodieBaseFile, length, Option.empty());
  }

  @Test
  void createFromFileStatusAndBootstrapBaseFile() {
    HoodieBaseFile bootstrapBaseFile = new HoodieBaseFile(pathStr);
    FileStatus fileStatus = new FileStatus(length, false, 0, 0, 0, 0, null, null, null, new Path(pathStr));
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(fileStatus, bootstrapBaseFile);
    assertFileGetters(fileStatus, hoodieBaseFile, length, Option.of(bootstrapBaseFile));
  }

  @Test
  void createFromFilePath() {
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(pathStr);
    assertFileGetters(null, hoodieBaseFile, -1, Option.empty());
  }

  @Test
  void createFromFilePathAndBootstrapBaseFile() {
    HoodieBaseFile bootstrapBaseFile = new HoodieBaseFile(pathStr);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(pathStr, bootstrapBaseFile);
    assertFileGetters(null, hoodieBaseFile, -1, Option.of(bootstrapBaseFile));
  }

  private void assertFileGetters(FileStatus fileStatus, HoodieBaseFile hoodieBaseFile, long fileLength, Option<HoodieBaseFile> bootstrapBaseFile) {
    assertEquals(fileId, hoodieBaseFile.getFileId());
    assertEquals(baseCommitTime, hoodieBaseFile.getCommitTime());
    assertEquals(bootstrapBaseFile, hoodieBaseFile.getBootstrapBaseFile());
    assertEquals(fileName, hoodieBaseFile.getFileName());
    assertEquals(pathStr, hoodieBaseFile.getPath());
    assertEquals(new Path(pathStr), hoodieBaseFile.getHadoopPath());
    assertEquals(fileLength, hoodieBaseFile.getFileSize());
    assertEquals(fileStatus, hoodieBaseFile.getFileStatus());
  }
}
