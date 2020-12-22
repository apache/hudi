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

package org.apache.hudi.common.table;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieSnapshotMetadataClient {
  private static String TEST_WRITE_TOKEN = "1-0-1";
  
  @TempDir
  public java.nio.file.Path folder;
  
  private String basePath;
  private String partitionPath;
  private String fullPartitionPath;
  private HoodieSnapshotMetadataClient snapshotMetadataClient;
  private HoodieTableMetaClient metaClient;

  private String fileId1 = UUID.randomUUID().toString();
  private String fileId2 = UUID.randomUUID().toString();
  private String fileId3 = UUID.randomUUID().toString();
  private String fileId4 = UUID.randomUUID().toString();

  @BeforeEach
  public void setUp() throws IOException {
    basePath = folder.resolve("dataset").toString();
    partitionPath = "2016/05/01/";
    fullPartitionPath = basePath + "/" + partitionPath;
    HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    setupDataFiles();
    snapshotMetadataClient = new HoodieSnapshotMetadataClient(HoodieTestUtils.getDefaultHadoopConf(), basePath);
    metaClient = snapshotMetadataClient.getMetaClient();
    
  }
  
  private void setupDataFiles() throws IOException {
    // Put some files in the partition
    new File(fullPartitionPath).mkdirs();
    String cleanTime1 = "0";
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
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
  }

  @Test
  public void testSnapshotMetadata() throws IOException {
    assertEquals("4", snapshotMetadataClient.getLatestInstant().get());
    
    Set<String> fileIds = snapshotMetadataClient.getLatestSnapshotFiles(partitionPath).map(FSUtils::getFileIdFromFilePath)
        .collect(Collectors.toSet());
    
    //fileId4 has only log file. so ensure it doesnt show up in results.
    assertEquals(Stream.of(fileId1, fileId2, fileId3).collect(Collectors.toSet()), fileIds);
    
    Set<String> fileIdsAt2 = snapshotMetadataClient.getSnapshotFilesAt("2", partitionPath).map(FSUtils::getFileIdFromFilePath)
        .collect(Collectors.toSet());

    // only fileId1/fileId2 exist at instant 2.
    assertEquals(2, fileIdsAt2.size());
    assertEquals(Stream.of(fileId1, fileId2).collect(Collectors.toSet()), fileIdsAt2);
  }
}