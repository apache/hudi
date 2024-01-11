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

package org.apache.hudi.common.table.timeline;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.InstantOffsetRange.InstantOffset;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestInstantOffsetRange extends HoodieCommonTestHarness {

  private HoodieActiveTimeline timeline;

  @BeforeEach
  public void setUp() throws Exception {
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanMetaClient();
  }

  @Override
  protected HoodieTableType getTableType() {
    // We need compaction commits to perform tests
    return HoodieTableType.MERGE_ON_READ;
  }

  /**
   * Create commit with 100 rows & 3 files
   */
  private void createCommit(String instantTime, String... fileIds) throws Exception {
    ValidationUtils.checkArgument(fileIds.length == 3);
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        buildWriteStat(fileIds[0], 10, 10, 10));
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        buildWriteStat(fileIds[1], 20, 20, 20));
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        buildWriteStat(fileIds[2], 10, 0, 0));
    commitMetadata.setOperationType(WriteOperationType.INSERT);
    HoodieTestTable.of(metaClient)
        .addCommit(instantTime, Option.of(commitMetadata));
  }

  private void createReplaceCommit(String instantTime, WriteOperationType writeOperationType,
                                   String writtenFileId, Map<String, List<String>> replacedFileIds) throws Exception {
    HoodieReplaceCommitMetadata commitMetadata = new HoodieReplaceCommitMetadata();
    commitMetadata.addMetadata("test", "test");
    commitMetadata.addWriteStat(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        buildWriteStat(writtenFileId, 30, 30, 30));
    commitMetadata.setPartitionToReplaceFileIds(replacedFileIds);
    commitMetadata.setOperationType(writeOperationType);
    HoodieTestTable.of(metaClient)
        .addReplaceCommit(instantTime, Option.empty(), Option.empty(), commitMetadata);
  }

  private HoodieWriteStat buildWriteStat(String fileId, int numInsert, int numUpdate, int numDelete) {
    HoodieWriteStat writeStat = new HoodieWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setNumInserts(numInsert);
    writeStat.setNumUpdateWrites(numUpdate);
    writeStat.setNumDeletes(numDelete);
    // Directly use file id here to identify path
    writeStat.setPath(fileId);
    return writeStat;
  }

  @Test
  public void testRangeWithOnlyInstantTime() throws Exception {
    String[] firstPartition1 = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    String[] firstPartition2 = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    String[] secondPartition = {UUID.randomUUID().toString(), UUID.randomUUID().toString(), UUID.randomUUID().toString()};
    String[] commitTimes = {metaClient.createNewInstantTime(), metaClient.createNewInstantTime(), metaClient.createNewInstantTime()};
    for (int i = 0; i < firstPartition1.length; i++) {
      createCommit(commitTimes[i], firstPartition1[i], firstPartition2[i], secondPartition[i]);
    }
    // Create a clustering commit
    HashMap<String, List<String>> partitionToReplaceFileIds = new HashMap<>();
    partitionToReplaceFileIds.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        Arrays.asList(firstPartition1));
    String clusteringFileId = UUID.randomUUID().toString();
    String clusteringTime = metaClient.createNewInstantTime();
    createReplaceCommit(clusteringTime, WriteOperationType.CLUSTER,
        clusteringFileId, partitionToReplaceFileIds);

    // Create an insert overwrite commit
    HashMap<String, List<String>> overwritePartitionToReplaceFileId = new HashMap<>();
    overwritePartitionToReplaceFileId.put(HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH,
        Collections.singletonList(clusteringFileId));
    partitionToReplaceFileIds.put(HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH,
        Arrays.asList(firstPartition2));
    partitionToReplaceFileIds.put(HoodieTestDataGenerator.DEFAULT_THIRD_PARTITION_PATH,
        Arrays.asList(secondPartition));
    String insertOverwriteFileId = UUID.randomUUID().toString();
    String overwriteTime = metaClient.createNewInstantTime();
    createReplaceCommit(overwriteTime, WriteOperationType.INSERT_OVERWRITE,
        insertOverwriteFileId, overwritePartitionToReplaceFileId);

    //1. Read commits, should return files btw (startOffset, endOffset] returns
    InstantOffset startOffset = InstantOffset.fromString(commitTimes[0]);
    InstantOffset endOffset = InstantOffset.fromString(commitTimes[2]);
    Stream<String> filesBetween = InstantOffsetRange.newBuilder().build(startOffset, endOffset, metaClient.getActiveTimeline())
        .getWriteStatsBetween()
        .stream()
        .map(HoodieWriteStat::getPath);

    Assertions.assertEquals(Stream.of(firstPartition1[1], firstPartition2[1], secondPartition[1],
            firstPartition1[2], firstPartition2[2], secondPartition[2]).sorted().collect(Collectors.toList()),
        filesBetween.sorted().collect(Collectors.toList()));

    //4. If maxFiles/maxRows is not specified, should directly return endInstantTime
    startOffset = InstantOffset.fromString(commitTimes[1]);
    endOffset = InstantOffsetRange.newBuilder()
        .build(startOffset, metaClient.getActiveTimeline())
        .getEndOffset();
    Assertions.assertEquals(overwriteTime, endOffset.toString());
    filesBetween = InstantOffsetRange.newBuilder().build(startOffset, endOffset, metaClient.getActiveTimeline())
        .getWriteStatsBetween()
        .stream()
        .map(HoodieWriteStat::getPath);
    // Should read insert_overwrite commits while ignore clustering commit
    Assertions.assertEquals(Stream.of(firstPartition1[2], firstPartition2[2], secondPartition[2], insertOverwriteFileId).sorted().collect(Collectors.toList()),
        filesBetween.sorted().collect(Collectors.toList()));

    //5. Should respect maxFiles & maxRows
    startOffset = InstantOffset.fromString(commitTimes[1]);
    endOffset = InstantOffsetRange.newBuilder().withMaxFiles(1)
        .build(startOffset, metaClient.getActiveTimeline())
        .getEndOffset();
    Assertions.assertEquals(commitTimes[2] + "_0" + "_0", endOffset.toString());

    endOffset = InstantOffsetRange.newBuilder().withMaxRows(100)
        .build(startOffset, metaClient.getActiveTimeline())
        .getEndOffset();
    Assertions.assertEquals(commitTimes[2] + "_1" + "_0", endOffset.toString());
    // Match the smaller one if both being set
    endOffset = InstantOffsetRange.newBuilder().withMaxRows(100)
        .withMaxFiles(2)
        .build(startOffset, metaClient.getActiveTimeline())
        .getEndOffset();
    Assertions.assertEquals(commitTimes[2] + "_0" + "_1", endOffset.toString());

    //6. getFilesBetween should return consistent results
    List<String> filesBtwInCache = InstantOffsetRange.newBuilder()
        .build(startOffset, endOffset, metaClient.getActiveTimeline())
        .getWriteStatsBetween()
        .stream()
        .map(HoodieWriteStat::getPath)
        .sorted()
        .collect(Collectors.toList());
    // Call it twice to do the real calculation
    filesBetween = InstantOffsetRange.newBuilder()
        .build(startOffset, endOffset, metaClient.getActiveTimeline())
        .getWriteStatsBetween()
        .stream()
        .map(HoodieWriteStat::getPath);
    Assertions.assertEquals(Stream.of(firstPartition1[2], firstPartition2[2]).sorted().collect(Collectors.toList()),
        filesBtwInCache);
    Assertions.assertEquals(filesBtwInCache, filesBetween.sorted().collect(Collectors.toList()));
  }
}
