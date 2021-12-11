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
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.marker.WriteMarkersFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.DEFAULT_SECOND_PARTITION_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMergeOnReadRollbackActionExecutor extends HoodieClientRollbackTestBase {
  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    //just generate tow partitions
    dataGen = new HoodieTestDataGenerator(new String[] {DEFAULT_FIRST_PARTITION_PATH, DEFAULT_SECOND_PARTITION_PATH});
    initFileSystem();
    initMetaClient();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testMergeOnReadRollbackActionExecutor(boolean isUsingMarkers) throws IOException {
    //1. prepare data and assert data result
    List<FileSlice> firstPartitionCommit2FileSlices = new ArrayList<>();
    List<FileSlice> secondPartitionCommit2FileSlices = new ArrayList<>();
    HoodieWriteConfig cfg = getConfigBuilder().withRollbackUsingMarkers(isUsingMarkers).withAutoCommit(false).build();
    twoUpsertCommitDataWithTwoPartitions(firstPartitionCommit2FileSlices, secondPartitionCommit2FileSlices, cfg, !isUsingMarkers);
    List<HoodieLogFile> firstPartitionCommit2LogFiles = new ArrayList<>();
    List<HoodieLogFile> secondPartitionCommit2LogFiles = new ArrayList<>();
    firstPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> firstPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, firstPartitionCommit2LogFiles.size());
    secondPartitionCommit2FileSlices.get(0).getLogFiles().collect(Collectors.toList()).forEach(logFile -> secondPartitionCommit2LogFiles.add(logFile));
    assertEquals(1, secondPartitionCommit2LogFiles.size());
    HoodieTable table = this.getHoodieTable(metaClient, cfg);

    //2. rollback
    HoodieInstant rollBackInstant = new HoodieInstant(isUsingMarkers, HoodieTimeline.DELTA_COMMIT_ACTION, "002");
    BaseRollbackPlanActionExecutor mergeOnReadRollbackPlanActionExecutor =
        new BaseRollbackPlanActionExecutor(context, cfg, table, "003", rollBackInstant, false,
            cfg.shouldRollbackUsingMarkers());
    mergeOnReadRollbackPlanActionExecutor.execute().get();
    MergeOnReadRollbackActionExecutor mergeOnReadRollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context,
        cfg,
        table,
        "003",
        rollBackInstant,
        true,
        false);
    //3. assert the rollback stat
    Map<String, HoodieRollbackPartitionMetadata> rollbackMetadata = mergeOnReadRollbackActionExecutor.execute().getPartitionMetadata();
    assertEquals(2, rollbackMetadata.size());

    for (Map.Entry<String, HoodieRollbackPartitionMetadata> entry : rollbackMetadata.entrySet()) {
      HoodieRollbackPartitionMetadata meta = entry.getValue();
      assertTrue(meta.getFailedDeleteFiles() == null || meta.getFailedDeleteFiles().size() == 0);
      assertTrue(meta.getSuccessDeleteFiles() == null || meta.getSuccessDeleteFiles().size() == 0);
    }

    //4. assert filegroup after rollback, and compare to the rollbackstat
    // assert the first partition data and log file size
    List<HoodieFileGroup> firstPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_FIRST_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileGroups.size());
    List<FileSlice> firstPartitionRollBack1FileSlices = firstPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, firstPartitionRollBack1FileSlices.size());
    FileSlice firstPartitionRollBack1FileSlice = firstPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> firstPartitionRollBackLogFiles = firstPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, firstPartitionRollBackLogFiles.size());

    firstPartitionRollBackLogFiles.removeAll(firstPartitionCommit2LogFiles);
    assertEquals(1, firstPartitionRollBackLogFiles.size());

    // assert the second partition data and log file size
    List<HoodieFileGroup> secondPartitionRollBack1FileGroups = table.getFileSystemView().getAllFileGroups(DEFAULT_SECOND_PARTITION_PATH).collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileGroups.size());
    List<FileSlice> secondPartitionRollBack1FileSlices = secondPartitionRollBack1FileGroups.get(0).getAllFileSlices().collect(Collectors.toList());
    assertEquals(1, secondPartitionRollBack1FileSlices.size());
    FileSlice secondPartitionRollBack1FileSlice = secondPartitionRollBack1FileSlices.get(0);
    List<HoodieLogFile> secondPartitionRollBackLogFiles = secondPartitionRollBack1FileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals(2, secondPartitionRollBackLogFiles.size());

    secondPartitionRollBackLogFiles.removeAll(secondPartitionCommit2LogFiles);
    assertEquals(1, secondPartitionRollBackLogFiles.size());

    assertFalse(WriteMarkersFactory.get(cfg.getMarkersType(), table, "002").doesMarkerDirExist());
  }

  @Test
  public void testFailForCompletedInstants() {
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      HoodieInstant rollBackInstant = new HoodieInstant(false, HoodieTimeline.DELTA_COMMIT_ACTION, "002");
      new MergeOnReadRollbackActionExecutor(context, getConfigBuilder().build(),
          getHoodieTable(metaClient, getConfigBuilder().build()),
          "003",
          rollBackInstant,
          true,
          true,
          true,
          false).execute();
    });
  }

  /**
   * Test Cases for rolling back when there is no base file.
   */
  @Test
  public void testRollbackWhenFirstCommitFail() throws Exception {

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
        .withRollbackUsingMarkers(false)
        .withPath(basePath).withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build()).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      client.startCommitWithTime("001");
      client.insert(jsc.emptyRDD(), "001");
      client.rollback("001");
    }
  }
}
