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

import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieRollbackStat;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class TestMergeOnReadRollbackActionExecutor extends HoodieClientTestBase {
  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    //just generate tow partitions
    dataGen = new HoodieTestDataGenerator(new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private void twoUpsertCommitDataRollBack(boolean isUsingMarkers) throws IOException, InterruptedException {
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).build();
    // prepare data
    List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
    List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[]{DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH}, basePath);
    /**
     * Write 1 (only inserts)
     */
    String newCommitTime = "001";
    client.startCommitWithTime(newCommitTime);
    List<HoodieRecord> records = dataGen.generateInsertsContainsAllPartitions(newCommitTime, 2);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);
    List<WriteStatus> statuses = client.upsert(writeRecords, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    /**
     * Write 2 (updates)
     */
    newCommitTime = "002";
    client.startCommitWithTime(newCommitTime);
    records = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
    assertNoWriteErrors(statuses);
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    //assert and get the first partition fileslice
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    List<FileSlice> firstPartitionCommit2FileSlices = firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileSlices.size());
    FileSlice firstPartitionCommit2FileSlice = firstPartitionCommit2FileSlices.get(0);
    firstPartitionCommit2FileSlice.getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, firstPartitionCommit2LogFiles.size());

    //assert and get the first partition fileslice
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileGroups.size());
    List<FileSlice> secondPartitionCommit2FileSlices = secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileSlices.size());
    FileSlice secondPartitionCommit2FileSlice = secondPartitionCommit2FileSlices.get(0);
    secondPartitionCommit2FileSlice.getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, secondPartitionCommit2LogFiles.size());


    // use MergeOnReadRollbackActionExecutor to rollback with filelist mode
    HoodieInstant rollBackInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002");
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        jsc,
        cfg,
        table,
        "003",
        rollBackInstant,
        true);
    // assert is filelist mode
    if (!isUsingMarkers) {
      assertFalse(mergeOnReadRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    } else {
      assertTrue(mergeOnReadRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    }

    // assert rollbackstates
    List<HoodieRollbackStat> hoodieRollbackStats = mergeOnReadRollbackActionExecutor.executeRollback();
    assertEquals(2, hoodieRollbackStats.size());
    HoodieRollbackStat rollBack1FirstPartitionStat = null;
    HoodieRollbackStat rollBack1SecondPartitionStat = null;
    for (int i = 0; i < hoodieRollbackStats.size(); i++) {
      HoodieRollbackStat hoodieRollbackStat = hoodieRollbackStats.get(i);

      if (!isUsingMarkers) {
        assertTrue(hoodieRollbackStat.getCommandBlocksCount() == null
            || hoodieRollbackStat.getCommandBlocksCount().size() == 1);
      } else {
        assertTrue(hoodieRollbackStat.getCommandBlocksCount() == null
            || hoodieRollbackStat.getCommandBlocksCount().size() == 0);
      }
      assertTrue(hoodieRollbackStat.getFailedDeleteFiles() == null
          || hoodieRollbackStat.getFailedDeleteFiles().size() == 0);
      assertTrue(hoodieRollbackStat.getSuccessDeleteFiles() == null
          || hoodieRollbackStat.getSuccessDeleteFiles().size() == 0);
      if (hoodieRollbackStat.getPartitionPath().equals(DEFAULT_FIRST_PARTITION_PATH)) {
        rollBack1FirstPartitionStat = hoodieRollbackStat;
      } else if (hoodieRollbackStat.getPartitionPath().equals(DEFAULT_SECOND_PARTITION_PATH)) {
        rollBack1SecondPartitionStat = hoodieRollbackStat;
      }
    }

    // assert the first partition data and log file size
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());
    FileSlice firstPartitionRollBack1FileSlice = firstPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> firstPartitionRollBackLogFiles = firstPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, firstPartitionRollBackLogFiles.size());

    firstPartitionRollBackLogFiles.removeAll(firstPartitionCommit2LogFiles);
    if (!isUsingMarkers) {
      // assert the first partition new log file equals the hoodieRollbackStat append log file
      assertEquals(1, firstPartitionRollBackLogFiles.size());
      assertEquals(rollBack1FirstPartitionStat.getCommandBlocksCount().keySet().iterator().next(),
          firstPartitionRollBackLogFiles.get(0).getFileStatus());
    } else {
      // assert the add log files
      assertEquals(1, firstPartitionRollBackLogFiles.size());
    }


    // assert the second partition data and log file size
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());
    FileSlice secondPartitionRollBack1FileSlice = secondPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> secondPartitionRollBackLogFiles = secondPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, secondPartitionRollBackLogFiles.size());

    secondPartitionRollBackLogFiles.removeAll(secondPartitionCommit2LogFiles);
    if (!isUsingMarkers) {
      // assert the second partition new log file equals the hoodieRollbackStat append log file
      assertEquals(1, secondPartitionRollBackLogFiles.size());
      assertEquals(rollBack1SecondPartitionStat.getCommandBlocksCount().keySet().iterator().next(),
          secondPartitionRollBackLogFiles.get(0).getFileStatus());
    } else {
      // assert the add log files
      assertEquals(1, secondPartitionRollBackLogFiles.size());
    }
  }

  @Test
  public void testMergeOnReadRollbackActionExecutorForFileListing() throws IOException, InterruptedException {
    twoUpsertCommitDataRollBack(false);
  }

  @Test
  public void testMergeOnReadRollbackActionExecutorForMarkerFiles() throws IOException, InterruptedException {
    twoUpsertCommitDataRollBack(true);
  }
}
