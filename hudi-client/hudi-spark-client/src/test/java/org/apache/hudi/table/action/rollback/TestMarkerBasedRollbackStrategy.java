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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestMarkerBasedRollbackStrategy extends HoodieClientTestBase {

  private static final String TEST_NAME_WITH_PARAMS = "[{index}] Test with listing metadata enable={0}";

  public static Stream<Arguments> configParams() {
    return Arrays.stream(new Boolean[][] {{true}, {false}}).map(Arguments::of);
  }

  private HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initFileSystem();
    initMetaClient(tableType);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testCopyOnWriteRollback(boolean useFileListingMetadata) throws Exception {
    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true).withAutoCommit(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withPath(basePath).build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // rollback 2nd commit and ensure stats reflect the info.
      List<HoodieRollbackStat> stats = testRun(useFileListingMetadata, writeConfig, writeClient);

      assertEquals(3, stats.size());
      for (HoodieRollbackStat stat : stats) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(0, stat.getCommandBlocksCount().size());
        assertEquals(0, stat.getWrittenLogFileSizeMap().size());
      }
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testMergeOnReadRollback(boolean useFileListingMetadata) throws Exception {
    // init MERGE_ON_READ_TABLE
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true).withAutoCommit(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withPath(basePath).build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {

      // rollback 2nd commit and ensure stats reflect the info.
      List<HoodieRollbackStat> stats = testRun(useFileListingMetadata, writeConfig, writeClient);

      assertEquals(3, stats.size());
      for (HoodieRollbackStat stat : stats) {
        assertEquals(0, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        if (useFileListingMetadata) {
          assertEquals(1, stat.getCommandBlocksCount().size());
          stat.getCommandBlocksCount().forEach((fileStatus, len) -> assertTrue(fileStatus.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())));
          assertEquals(1, stat.getWrittenLogFileSizeMap().size());
          stat.getWrittenLogFileSizeMap().forEach((fileStatus, len) -> assertTrue(fileStatus.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())));
        } else {
          assertEquals(0, stat.getCommandBlocksCount().size());
          assertEquals(0, stat.getWrittenLogFileSizeMap().size());
        }
      }
    }
  }

  private List<HoodieRollbackStat> testRun(boolean useFileListingMetadata, HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient) {
    String newCommitTime = "001";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = writeClient.insert(jsc.parallelize(records, 1), newCommitTime);
    writeClient.commit(newCommitTime, writeStatuses);

    // Updates
    newCommitTime = "002";
    writeClient.startCommitWithTime(newCommitTime);
    records = dataGen.generateUniqueUpdates(newCommitTime, 50);
    writeStatuses = writeClient.upsert(jsc.parallelize(records, 1), newCommitTime);
    writeStatuses.collect();

    // rollback 2nd commit and ensure stats reflect the info.
    return new SparkMarkerBasedRollbackStrategy(HoodieSparkTable.create(writeConfig, context, metaClient), context, writeConfig, "003")
        .execute(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"));
  }

}
