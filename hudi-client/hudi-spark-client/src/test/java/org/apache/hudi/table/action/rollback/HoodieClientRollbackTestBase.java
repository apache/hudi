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

package org.apache.hudi.table.action.rollback;

import org.apache.hudi.client.HoodieWriteResult;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.Assertions;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HoodieClientRollbackTestBase extends HoodieClientTestBase {
  protected void twoUpsertCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
                                                      List<FileSlice> secondPartitionCommit2FileSlices,
                                                      HoodieWriteConfig cfg,
                                                      boolean commitSecondUpsert) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    //1. prepare data
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(fs, new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    client.commit(newCommitTime, statuses);

    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    if (commitSecondUpsert) {
      client.commit(newCommitTime, statuses);
    }


    //2. assert file group and get the first partition file slice
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    SyncableFileSystemView fsView = getFileSystemViewWithUnCommittedSlices(table.getMetaClient());
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    firstPartitionCommit2FileSlices.addAll(firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));
    //3. assert file group and get the second partition file slice
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileGroups.size());
    secondPartitionCommit2FileSlices.addAll(secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));

    //4. assert file slice
    HoodieTableType tableType = this.getTableType();
    if (tableType.equals(HoodieTableType.COPY_ON_WRITE)) {
      assertEquals(2, firstPartitionCommit2FileSlices.size());
      assertEquals(2, secondPartitionCommit2FileSlices.size());
    } else {
      assertEquals(1, firstPartitionCommit2FileSlices.size());
      assertEquals(1, secondPartitionCommit2FileSlices.size());
    }
  }

  protected void insertOverwriteCommitDataWithTwoPartitions(List<FileSlice> firstPartitionCommit2FileSlices,
                                                            List<FileSlice> secondPartitionCommit2FileSlices,
                                                            HoodieWriteConfig cfg,
                                                            boolean commitSecondInsertOverwrite) throws IOException {
    //just generate two partitions
    dataGen = new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    HoodieTestDataGenerator.writePartitionMetadataDeprecated(fs, new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    SparkRDDWriteClient client = getHoodieWriteClient(cfg);
    /**
     * Write 1 (upsert)
     */
    String newCommitTime = "001";
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    client.startCommitWithTime(newCommitTime);
    JavaRDD<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime);
    Assertions.assertNoWriteErrors(statuses.collect());
    client.commit(newCommitTime, statuses);

    // get fileIds written
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    SyncableFileSystemView fsView = getFileSystemViewWithUnCommittedSlices(table.getMetaClient());
    List<HoodieFileGroup> firstPartitionCommit1FileGroups = fsView.getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit1FileGroups.size());
    Set<String> partition1Commit1FileIds = firstPartitionCommit1FileGroups.get(0).getAllFileSlices().map(FileSlice::getFileId).collect(Collectors.toSet());
    List<HoodieFileGroup> secondPartitionCommit1FileGroups = fsView.getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit1FileGroups.size());
    Set<String> partition2Commit1FileIds = secondPartitionCommit1FileGroups.get(0).getAllFileSlices().map(FileSlice::getFileId).collect(Collectors.toSet());

    /**
     * Write 2 (one insert_overwrite)
     */
    String commitActionType = HoodieTimeline.REPLACE_COMMIT_ACTION;
    newCommitTime = "002";
    records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    writeRecords = jsc.parallelize(records, 1);
    client.startCommitWithTime(newCommitTime, commitActionType);
    HoodieWriteResult result = client.insertOverwrite(writeRecords, newCommitTime);
    statuses = result.getWriteStatuses();
    Assertions.assertNoWriteErrors(statuses.collect());
    if (commitSecondInsertOverwrite) {
      client.commit(newCommitTime, statuses, Option.empty(), commitActionType, result.getPartitionToReplaceFileIds());
    }
    metaClient.reloadActiveTimeline();
    // get new fileIds written as part of insert_overwrite
    fsView = getFileSystemViewWithUnCommittedSlices(metaClient);
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH)
        .filter(fg -> !partition1Commit1FileIds.contains(fg.getFileGroupId().getFileId())).collect(Collectors.toList());
    firstPartitionCommit2FileSlices.addAll(firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = fsView.getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH)
        .filter(fg -> !partition2Commit1FileIds.contains(fg.getFileGroupId().getFileId())).collect(Collectors.toList());
    secondPartitionCommit2FileSlices.addAll(secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList()));

    assertEquals(1, firstPartitionCommit2FileSlices.size());
    assertEquals(1, secondPartitionCommit2FileSlices.size());
  }
}
