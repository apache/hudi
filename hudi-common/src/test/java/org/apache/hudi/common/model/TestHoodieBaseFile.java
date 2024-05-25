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
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieBaseFile {
  private final String fileName = "136281f3-c24e-423b-a65a-95dbfbddce1d_1-0-1_100.parquet";
  private final String pathStr = "file:/tmp/hoodie/2021/01/01/" + fileName;
  private final String fileId = "136281f3-c24e-423b-a65a-95dbfbddce1d";
  private final String baseCommitTime = "100";
  private final int length = 10;
  private final short blockReplication = 2;
  private final long blockSize = 1000000L;

  @Test
  void createFromHoodieBaseFile() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(pathInfo);
    assertFileGetters(pathInfo, new HoodieBaseFile(hoodieBaseFile), length, Option.empty());
  }

  @Test
  void createFromFileStatus() {
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(pathInfo);
    assertFileGetters(pathInfo, hoodieBaseFile, length, Option.empty());
  }

  @Test
  void createFromFileStatusAndBootstrapBaseFile() {
    HoodieBaseFile bootstrapBaseFile = new HoodieBaseFile(pathStr);
    StoragePathInfo pathInfo = new StoragePathInfo(new StoragePath(pathStr), length, false, blockReplication, blockSize, 0);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(pathInfo, bootstrapBaseFile);
    assertFileGetters(pathInfo, hoodieBaseFile, length, Option.of(bootstrapBaseFile));
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

  @Test
  void createFromExternalFileStatus() {
    String fileName = "parquet_file_1.parquet";
    String storedPathString =
        "file:/tmp/hoodie/2021/01/01/" + fileName + "_" + baseCommitTime + "_hudiext";
    String expectedPathString = "file:/tmp/hoodie/2021/01/01/" + fileName;
    StoragePathInfo inputPathInfo = new StoragePathInfo(
        new StoragePath(storedPathString), length, false, blockReplication, blockSize, 0);
    StoragePathInfo expectedPathInfo = new StoragePathInfo(
        new StoragePath(expectedPathString), length, false, blockReplication, blockSize, 0);
    HoodieBaseFile hoodieBaseFile = new HoodieBaseFile(inputPathInfo);

    assertFileGetters(expectedPathInfo, hoodieBaseFile, length, Option.empty(), fileName,
        expectedPathString, fileName);
  }

  private void assertFileGetters(StoragePathInfo pathInfo, HoodieBaseFile hoodieBaseFile,
                                 long fileLength, Option<HoodieBaseFile> bootstrapBaseFile) {
    assertFileGetters(pathInfo, hoodieBaseFile, fileLength, bootstrapBaseFile, fileId, pathStr,
        fileName);
  }

  private void assertFileGetters(StoragePathInfo pathInfo, HoodieBaseFile hoodieBaseFile,
                                 long fileLength, Option<HoodieBaseFile> bootstrapBaseFile,
                                 String fileId, String pathStr, String fileName) {
    assertEquals(fileId, hoodieBaseFile.getFileId());
    assertEquals(baseCommitTime, hoodieBaseFile.getCommitTime());
    assertEquals(bootstrapBaseFile, hoodieBaseFile.getBootstrapBaseFile());
    assertEquals(fileName, hoodieBaseFile.getFileName());
    assertEquals(pathStr, hoodieBaseFile.getPath());
    assertEquals(new StoragePath(pathStr), hoodieBaseFile.getStoragePath());
    assertEquals(fileLength, hoodieBaseFile.getFileSize());
    assertEquals(pathInfo, hoodieBaseFile.getPathInfo());
  }
}
