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
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.MarkerFiles;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
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
    HoodieTable table = this.getHoodieTable(metaClient, getConfig());
    HoodieInstant needRollBackInstant = new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, "002");

    // execute CopyOnWriteRollbackActionExecutor with filelisting mode
    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(jsc, table.getConfig(), table, "003", needRollBackInstant, true);
    assertFalse(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    List<HoodieRollbackStat> hoodieRollbackStats = copyOnWriteRollbackActionExecutor.executeRollback();

    // assert hoodieRollbackStats
    assertEquals(hoodieRollbackStats.size(), 3);
    hoodieRollbackStats.forEach(stat -> {
      if (stat.getPartitionPath().equals("2015/03/16")) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
        assertEquals("file:" + HoodieTestUtils.getDataFilePath(basePath, "2015/03/16", commitTime2, file21),
            stat.getSuccessDeleteFiles().get(0));
      } else if (stat.getPartitionPath().equals("2015/03/17")) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
        assertEquals("file:" + HoodieTestUtils.getDataFilePath(basePath, "2015/03/17", commitTime2, file22),
            stat.getSuccessDeleteFiles().get(0));
      } else if (stat.getPartitionPath().equals("2016/03/15")) {
        assertEquals(0, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(Collections.EMPTY_MAP, stat.getCommandBlocksCount());
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testCopyOnWriteRollbackActionExecutor(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).withAutoCommit(false).build();
    this.twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers);
    HoodieTable<?> table = this.getHoodieTable(metaClient, cfg);

    //2. rollback
    HoodieInstant commitInstant;
    if (isUsingMarkers) {
      commitInstant = table.getActiveTimeline().getCommitTimeline().filterInflights().lastInstant().get();
    } else {
      commitInstant = table.getCompletedCommitTimeline().lastInstant().get();
    }

    CopyOnWriteRollbackActionExecutor copyOnWriteRollbackActionExecutor = new CopyOnWriteRollbackActionExecutor(jsc, cfg, table, "003", commitInstant, false);
    if (!isUsingMarkers) {
      assertFalse(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    } else {
      assertTrue(copyOnWriteRollbackActionExecutor.getRollbackStrategy() instanceof MarkerBasedRollbackStrategy);
    }
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

    if (!isUsingMarkers) {
      firstPartitionCommit2FileSlices.removeAll(firstPartitionRollBack1FileSlices);
      assertEquals(1, firstPartitionCommit2FileSlices.size());
      assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().get(0));
    } else {
      assertEquals(firstPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          String.format("%s:%s/%s", this.fs.getScheme(), basePath, rollbackMetadata.get(DEFAULT_FIRST_PARTITION_PATH).getSuccessDeleteFiles().get(0)));
    }

    // assert the second partition file group and file slice
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());

    // assert the second partition rollback file is equals rollBack1SecondPartitionStat
    if (!isUsingMarkers) {
      secondPartitionCommit2FileSlices.removeAll(secondPartitionRollBack1FileSlices);
      assertEquals(1, secondPartitionCommit2FileSlices.size());
      assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().get(0));
    } else {
      assertEquals(secondPartitionCommit2FileSlices.get(0).getBaseFile().get().getPath(),
          String.format("%s:%s/%s", this.fs.getScheme(), basePath, rollbackMetadata.get(DEFAULT_SECOND_PARTITION_PATH).getSuccessDeleteFiles().get(0)));
    }

    assertFalse(new MarkerFiles(table, commitInstant.getTimestamp()).doesMarkerDirExist());
  }
}
