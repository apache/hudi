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

import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieIncrementalMetadataClient {
  private static String TEST_WRITE_TOKEN = "1-0-1";
  
  @TempDir
  public java.nio.file.Path folder;
  
  private String basePath;
  private HoodieIncrementalMetadataClient incrementalMetadataClient;
  private HoodieTableMetaClient metaClient;

  private String fileId1 = UUID.randomUUID().toString();
  private String fileId2 = UUID.randomUUID().toString();
  private String fileId3 = UUID.randomUUID().toString();
  private String fileId4 = UUID.randomUUID().toString();

  @BeforeEach
  public void setUp() throws IOException {
    basePath = folder.resolve("dataset").toString();
    HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE);
    setupDataFiles();
    incrementalMetadataClient = new HoodieIncrementalMetadataClient(HoodieTestUtils.getDefaultHadoopConf(), basePath);
    metaClient = incrementalMetadataClient.getMetaClient();
    
  }
  
  private void setupDataFiles() throws IOException {
    // Put some files in the partition
    new File(basePath).mkdirs();
    String cleanTime1 = "0";
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String replaceCommit5 = "5";
    String partition1 = "p1";
    String partition2 = "p2";
    
    String fullPartition1 = new Path(basePath, partition1).toString();
    String fullPartition2 = new Path(basePath, partition2).toString();

    new File(fullPartition1 + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    new File(fullPartition1 + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId1)).createNewFile();
    
    new File(fullPartition2 + FSUtils.makeDataFileName(commitTime1, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartition2 + FSUtils.makeDataFileName(commitTime2, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    
    new File(fullPartition1 + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId2)).createNewFile();
    new File(fullPartition1 + FSUtils.makeDataFileName(commitTime3, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartition1 + FSUtils.makeDataFileName(commitTime4, TEST_WRITE_TOKEN, fileId3)).createNewFile();
    new File(fullPartition1
        + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0, TEST_WRITE_TOKEN))
        .createNewFile();

    // Create commit/clean files
    FileCreateUtils.createCommit(basePath, commitTime1, buildCommitMetadata(partition1, partition2));
    FileCreateUtils.createCommit(basePath, commitTime2, buildCommitMetadata(partition2));
    FileCreateUtils.createCommit(basePath, commitTime3, buildCommitMetadata(partition1));
    FileCreateUtils.createCommit(basePath, commitTime4, buildCommitMetadata(partition1));

    FileCreateUtils.createReplaceCommit(basePath, replaceCommit5, 
        buildReplaceCommitMetadata(new String[] {partition1}, new String[] {partition2, "p3"}));
  }

  private HoodieReplaceCommitMetadata buildReplaceCommitMetadata(String[] partitionsReplaced, String[] partitionsAdded) {
    HoodieReplaceCommitMetadata metadata = new HoodieReplaceCommitMetadata();
    for (String p : partitionsAdded) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(p);
      writeStat.setFileId("f1"); // fileId is not used in incremental metadata client
      writeStat.setPrevCommit("");
      writeStat.setNumInserts(10L);
      writeStat.setNumDeletes(0L);
      writeStat.setPath(p);
      metadata.addWriteStat(p, writeStat);
    }
    
    for (String p : partitionsReplaced) {
      metadata.getPartitionToReplaceFileIds().putIfAbsent(p, new ArrayList<>());
      metadata.getPartitionToReplaceFileIds().get(p).add("fReplacedId");
    }
    metadata.setCompacted(false);
    metadata.setOperationType(WriteOperationType.INSERT);
    return metadata;
  }

  private HoodieCommitMetadata buildCommitMetadata(String... partitions) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    for (String p : partitions) {
      HoodieWriteStat writeStat = new HoodieWriteStat();
      writeStat.setPartitionPath(p);
      writeStat.setFileId("f1"); // fileId is not used in incremental metadata client
      writeStat.setPrevCommit("");
      writeStat.setNumInserts(10L);
      writeStat.setNumDeletes(0L);
      writeStat.setPath(p);
      metadata.addWriteStat(p, writeStat);
    }
    metadata.setCompacted(false);
    metadata.setOperationType(WriteOperationType.INSERT);
    return metadata;
  }

  @Test
  public void testIncremental() throws IOException {

    List<String> partitions = incrementalMetadataClient.getPartitionsMutatedBetween("0", "4");
    assertEquals(2, partitions.size());
    assertEquals(Stream.of("p1", "p2").collect(Collectors.toList()), partitions);


    partitions = incrementalMetadataClient.getPartitionsMutatedBetween("1", "2");
    assertEquals(1, partitions.size());
    assertEquals(Stream.of("p2").collect(Collectors.toList()), partitions); //only p2 is modified in commitTime2

    partitions = incrementalMetadataClient.getPartitionsMutatedBetween("2", "4");
    assertEquals(1, partitions.size());
    assertEquals(Stream.of("p1").collect(Collectors.toList()), partitions); //only p1 is modified between commitTime2, commitTime4

    partitions = incrementalMetadataClient.getPartitionsMutatedSince("2");
    assertEquals(3, partitions.size());
    assertEquals(Stream.of("p1", "p2", "p3").collect(Collectors.toList()), partitions);
  }
}