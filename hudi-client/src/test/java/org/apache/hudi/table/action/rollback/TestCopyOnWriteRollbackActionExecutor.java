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

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCopyOnWriteRollbackActionExecutor extends HoodieClientTestBase {
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

  @Test
  public void testCopyOnWriteRollbackActionExecutorForFileListingAsGenerateFile() throws IOException {
    // Let's create some commit files and parquet files
    String commitTime1 = "001";
    String commitTime2 = "002";
    new File(basePath + "/.hoodie").mkdirs();
    HoodieTestDataGenerator.writePartitionMetadata(fs, new String[]{"2015/03/16", "2015/03/17", "2016/03/15"},
        basePath);
    HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2);

    // Make commit1
    String file11 = HoodieTestUtils.createDataFile(basePath, "2015/03/16", commitTime1, "id11");
    HoodieTestUtils.createNewLogFile(fs, basePath, "2015/03/16",
        commitTime1, "id11", Option.of(3));
    String file12 = HoodieTestUtils.createDataFile(basePath, "2015/03/17", commitTime1, "id12");

    // Make commit2
    String file21 = HoodieTestUtils.createDataFile(basePath, "2015/03/16", commitTime2, "id21");
    String file22 = HoodieTestUtils.createDataFile(basePath, "2015/03/17", commitTime2, "id22");
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
    HoodieTable table = this.getHoodieTable(metaClient, config);
    HoodieInstant needRollBackInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "002");

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(jsc, config, table, "003", needRollBackInstant, true);
    assertFalse(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    List<HoodieRollbackStat> hoodieRollbackStats = copyOnWriteRollbackActionExecutor.executeRollback();

    // assert hoodieRollbackStats
    assertEquals(hoodieRollbackStats.size(), 3);
    hoodieRollbackStats.forEach(stat -> {
      if (stat.getPartitionPath().equals("2015/03/16")) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(null, stat.getCommandBlocksCount());
        assertEquals("file:" + HoodieTestUtils.getDataFilePath(basePath, "2015/03/16", commitTime2, file21),
            stat.getSuccessDeleteFiles().get(0));
      } else if (stat.getPartitionPath().equals("2015/03/17")) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(null, stat.getCommandBlocksCount());
        assertEquals("file:" + HoodieTestUtils.getDataFilePath(basePath, "2015/03/17", commitTime2, file22),
            stat.getSuccessDeleteFiles().get(0));
      } else if (stat.getPartitionPath().equals("2015/03/17")) {
        assertEquals(0, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(null, stat.getCommandBlocksCount());
      }
    });

    assertTrue(HoodieTestUtils.doesCommitExist(basePath, "001"));
    assertTrue(HoodieTestUtils.doesInflightExist(basePath, "001"));
    assertFalse(HoodieTestUtils.doesCommitExist(basePath, "002"));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, "002"));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2015/03/16", commitTime1, file11)
        && HoodieTestUtils.doesDataFileExist(basePath, "2015/03/17", commitTime1, file12));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2015/03/16", commitTime2, file21)
        || HoodieTestUtils.doesDataFileExist(basePath, "2015/03/17", commitTime2, file22));
  }

  private void twoUpsertCommitDataRollBack(boolean isUsingMarkers) throws IOException {
    //1. prepare data
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).build();
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
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

    //2. assert and get the first partition fileslice
    List<HoodieFileGroup> firstPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionCommit2FileGroups.size());
    firstPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList())
        .forEach(fileSlice -> firstPartitionCommit2FileSlices.add(fileSlice));
    assertEquals(2, firstPartitionCommit2FileSlices.size());
    //assert and get the first partition fileslice
    List<HoodieFileGroup> secondPartitionCommit2FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionCommit2FileGroups.size());
    secondPartitionCommit2FileGroups.get(0).getAllFileSlices().collect(Collectors.toList())
        .forEach(fileSlice -> secondPartitionCommit2FileSlices.add(fileSlice));
    assertEquals(2, secondPartitionCommit2FileSlices.size());


    //3.  rollback
    List<HoodieInstant> commitInstants = table.getCompletedCommitTimeline().getInstants().collect(Collectors.toList());
    HoodieInstant commitInstant = commitInstants.get(commitInstants.size() - 1);

    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(jsc, cfg, table, "003", commitInstant, true);
    if (!isUsingMarkers) {
      assertFalse(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    } else {
      assertTrue(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    }

    List<HoodieRollbackStat> hoodieRollbackStats = copyOnWriteRollbackActionExecutor.executeRollback();

    //assert the rollback stat
    assertEquals(2, hoodieRollbackStats.size());
    HoodieRollbackStat rollBack1FirstPartitionStat = null;
    HoodieRollbackStat rollBack1SecondPartitionStat = null;
    for (int i = 0; i < hoodieRollbackStats.size(); i++) {
      HoodieRollbackStat hoodieRollbackStat = hoodieRollbackStats.get(i);
      assertTrue(hoodieRollbackStat.getCommandBlocksCount() == null
          || hoodieRollbackStat.getCommandBlocksCount().size() == 0);
      assertTrue(hoodieRollbackStat.getFailedDeleteFiles() == null
          || hoodieRollbackStat.getFailedDeleteFiles().size() == 0);
      assertTrue(hoodieRollbackStat.getSuccessDeleteFiles() == null
          || hoodieRollbackStat.getSuccessDeleteFiles().size() == 1);
      if (hoodieRollbackStat.getPartitionPath().equals(DEFAULT_FIRST_PARTITION_PATH)) {
        rollBack1FirstPartitionStat = hoodieRollbackStat;
      } else if (hoodieRollbackStat.getPartitionPath().equals(DEFAULT_SECOND_PARTITION_PATH)) {
        rollBack1SecondPartitionStat = hoodieRollbackStat;
      }
    }

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition file group and file slice
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());
    firstPartitionCommit2FileSlices.removeAll(firstPartitionRollBack1FileSlices);
    assertEquals(1, firstPartitionCommit2FileSlices.size());
    // assert the first partition rollback file is equals rollBack1FirstPartitionStat
    if (!isUsingMarkers) {
      assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          rollBack1FirstPartitionStat.getSuccessDeleteFiles().get(0));
    } else {
      assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          String.format("%s:%s/%s", this.fs.getScheme(), basePath, rollBack1FirstPartitionStat.getSuccessDeleteFiles().get(0)));
    }

    // assert the second partition file group and file slice
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());
    secondPartitionCommit2FileSlices.removeAll(secondPartitionRollBack1FileSlices);
    assertEquals(1, secondPartitionCommit2FileSlices.size());
    // assert the second partition rollback file is equals rollBack1SecondPartitionStat
    if (!isUsingMarkers) {
      assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          rollBack1SecondPartitionStat.getSuccessDeleteFiles().get(0));
    } else {
      assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          String.format("%s:%s/%s", this.fs.getScheme(), basePath, rollBack1SecondPartitionStat.getSuccessDeleteFiles().get(0)));
    }
  }

  @Test
  public void testCopyOnWriteRollbackActionExecutorForFileListing() throws IOException {
    twoUpsertCommitDataRollBack(false);
  }

  @Test
  public void testCopyOnWriteRollbackActionExecutorForMarkFiles() throws IOException {
    twoUpsertCommitDataRollBack(true);
  }
}
