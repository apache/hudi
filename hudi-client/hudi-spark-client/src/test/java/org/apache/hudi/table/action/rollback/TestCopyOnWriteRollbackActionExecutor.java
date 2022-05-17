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

import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestCopyOnWriteRollbackActionExecutor extends HoodieClientRollbackTestBase {
  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testCopyOnWriteRollbackActionExecutorForFileListingAsGenerateFile() throws Exception {
    final String p1 = "2015/03/16";
    final String p2 = "2015/03/17";
    final String p3 = "2016/03/15";
    // Let's create some commit files and base files
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit("001")
        .withBaseFilesInPartition(p1, "id11")
        .withBaseFilesInPartition(p2, "id12")
        .withLogFile(p1, "id11", 3)
        .addCommit("002")
        .withBaseFilesInPartition(p1, "id21")
        .withBaseFilesInPartition(p2, "id22");

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(false).build();
    HoodieTable table = this.getHoodieTable(metaClient, writeConfig);
    HoodieInstant needRollBackInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "002");
    String rollbackInstant = "003";
    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, rollbackInstant, needRollBackInstant, false,
            table.getConfig().shouldRollbackUsingMarkers());
    HoodieRollbackPlan rollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, table.getConfig(), table, "003", needRollBackInstant, true,
        false);
    List<HoodieRollbackStat> hoodieRollbackStats = copyOnWriteRollbackActionExecutor.executeRollback(rollbackPlan);

    // assert hoodieRollbackStats
    assertEquals(hoodieRollbackStats.size(), 3);
    for (HoodieRollbackStat stat : hoodieRollbackStats) {
      switch (stat.getPartitionPath()) {
        case p1:
          assertEquals(1, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          assertEquals(testTable.forCommit("002").getBaseFilePath(p1, "id21").toString(),
              this.fs.getScheme() + ":" + stat.getSuccessDeleteFiles().get(0));
          break;
        case p2:
          assertEquals(1, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          assertEquals(testTable.forCommit("002").getBaseFilePath(p2, "id22").toString(),
              this.fs.getScheme() + ":" + stat.getSuccessDeleteFiles().get(0));
          break;
        case p3:
          assertEquals(0, stat.getSuccessDeleteFiles().size());
          assertEquals(0, stat.getFailedDeleteFiles().size());
          assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
          break;
        default:
          fail("Unexpected partition: " + stat.getPartitionPath());
      }
    }

    assertTrue(testTable.inflightCommitExists("001"));
    assertTrue(testTable.commitExists("001"));
    assertTrue(testTable.baseFileExists(p1, "001", "id11"));
    assertTrue(testTable.baseFileExists(p2, "001", "id12"));
    // Note that executeRollback() does not delete inflight instant files
    // The deletion is done in finishRollback() called by runRollback()
    assertTrue(testTable.inflightCommitExists("002"));
    assertFalse(testTable.commitExists("002"));
    assertFalse(testTable.baseFileExists(p1, "002", "id21"));
    assertFalse(testTable.baseFileExists(p2, "002", "id22"));
  }

  // Verify that rollback works with replacecommit
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCopyOnWriteRollbackWithReplaceCommits(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).withAutoCommit(false).build();
    this.insertOverwriteCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers);
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    performRollbackAndValidate(isUsingMarkers, cfg, table, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCopyOnWriteRollbackActionExecutor(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).withAutoCommit(false).build();
    this.twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers);
    metaClient.reloadActiveTimeline();
    HoodieTable table = this.getHoodieTable(metaClient, cfg);
    performRollbackAndValidate(isUsingMarkers, cfg, table, firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices);
  }

  private void performRollbackAndValidate(boolean isUsingMarkers, HoodieWriteConfig cfg, HoodieTable table,
                                          List<FileSlice> firstPartitionCommit2FileSlices,
                                          List<FileSlice> secondPartitionCommit2FileSlices) throws IOException {
    //2. rollback
    HoodieInstant commitInstant;
    if (isUsingMarkers) {
      commitInstant = table.getActiveTimeline().getCommitTimeline().filterInflights().lastInstant().get();
    } else {
      commitInstant = table.getCompletedCommitTimeline().lastInstant().get();
    }

    BaseRollbackPlanActionExecutor copyOnWriteRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, table.getConfig(), table, "003", commitInstant, false,
            table.getConfig().shouldRollbackUsingMarkers());
    HoodieRollbackPlan hoodieRollbackPlan = (HoodieRollbackPlan) copyOnWriteRollbackPlanActionExecutor.execute().get();
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(context, cfg, table, "003", commitInstant, false,
        false);
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = copyOnWriteRollbackActionExecutor.execute().getPartitionMetadata();

    //3. assert the rollback stat
    assertEquals(2, rollbackMetadata.size());
    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
      HoodieRollbackPartitionMetadata meta = entry.getValue();
      assertTrue(meta.getFailedDeleteFiles() == null
          || meta.getFailedDeleteFiles().size() == 0);
      assertTrue(meta.getSuccessDeleteFiles() == null
          || meta.getSuccessDeleteFiles().size() == 1);
    }

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition file group and file slice
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());

    firstPartitionCommit2FileSlices.removeAll(firstPartitionRollBack1FileSlices);
    assertEquals(1, firstPartitionCommit2FileSlices.size());
    assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
        this.fs.getScheme() + ":" + rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().get(0));


    // assert the second partition file group and file slice
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());

    // assert the second partition rollback file is equals rollBack1SecondPartitionStat
    secondPartitionCommit2FileSlices.removeAll(secondPartitionRollBack1FileSlices);
    assertEquals(1, secondPartitionCommit2FileSlices.size());
    assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
        this.fs.getScheme() + ":" + rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().get(0));

    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, commitInstant.getTimestamp()).doesMarkerDirExist());
  }
}
