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

package org.apache.hudi.table.functional;

import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.BaseRollbackHelper;
import org.apache.hudi.table.action.rollback.MarkerBasedRollbackStrategy;
import org.apache.hudi.table.marker.DirectWriteMarkers;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

@Tag("functional")
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
    initHoodieStorage();
    initMetaClient(tableType);
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testMarkerBasedRollbackAppend() throws Exception {
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("000")
        .getFileIdsWithBaseFilesInPartitions("partA").get("partA");
    testTable.forCommit("001")
        .withLogMarkerFile("000", "partA", f0, IOType.APPEND, 1);

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfig(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(),
        "002").getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
  }

  @ParameterizedTest
  @CsvSource(value = {"APPEND,true,true", "APPEND,true,false", "APPEND,false,true", "APPEND,false,false"})
  public void testMarkerBasedRollbackAppendWithLogFileMarkers(IOType testIOType,
                                                              boolean logFileInNewFileGroup,
                                                              boolean logFileExists) throws Exception {
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();
    String partitionPath = "partA";
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = logFileInNewFileGroup ? UUID.randomUUID().toString()
        : testTable.addDeltaCommit("000").getFileIdWithLogFile(partitionPath);
    String logFileName = EMPTY_STRING;
    int logFileVersion = 1;
    int logFileSize = 13042;
    String logFileBaseInstantTime = logFileInNewFileGroup ? "001" : "000";
    // log file name should still use the base instant time
    testTable.addInflightDeltaCommit("001")
        .withLogMarkerFile(logFileBaseInstantTime, partitionPath, f0, testIOType, logFileVersion);
    if (logFileExists) {
      testTable.withLogFilesAndBaseInstantTimeInPartition(
          partitionPath, Collections.singletonList(
              Pair.of(Pair.of(logFileBaseInstantTime, f0), new Integer[] {logFileVersion, logFileSize})));
      logFileName = testTable.getLogFileNameById(logFileBaseInstantTime, f0, logFileVersion);
    }

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfig(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(), "002")
        .getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
    HoodieRollbackRequest rollbackRequest = rollbackRequests.get(0);
    assertEquals(partitionPath, rollbackRequest.getPartitionPath());
    assertEquals((logFileInNewFileGroup && logFileExists) ? EMPTY_STRING : f0, rollbackRequest.getFileId());
    if (logFileExists) {
      if (logFileInNewFileGroup) {
        // log file is written to a new file group in the failed instant; the rollback plan
        // should include it in the "filesToBeDeleted" field so it is going to be deleted
        assertEquals(1, rollbackRequest.getFilesToBeDeleted().size());
        assertEquals(
            new StoragePath(new StoragePath(basePath, partitionPath), logFileName).toString(),
            rollbackRequest.getFilesToBeDeleted().get(0));
        assertEquals(0, rollbackRequest.getLogBlocksToBeDeleted().size());
      } else {
        // log file is written to an existing file group in the failed instant; the rollback plan
        // should include it in the "logBlocksToBeDeleted" field, so it is kept on storage rolled
        // back through a command log block
        assertEquals(0, rollbackRequest.getFilesToBeDeleted().size());
        assertEquals(1, rollbackRequest.getLogBlocksToBeDeleted().size());
        assertTrue(rollbackRequest.getLogBlocksToBeDeleted().containsKey(logFileName));
        assertEquals(logFileSize, rollbackRequest.getLogBlocksToBeDeleted().get(logFileName));
      }
    } else {
      // log file marker exists but the log file is not written to the storage, so the rollback
      // plan should not include it.
      assertEquals(0, rollbackRequest.getFilesToBeDeleted().size());
      assertEquals(0, rollbackRequest.getLogBlocksToBeDeleted().size());
    }
  }

  @Test
  public void testCopyOnWriteRollbackWithTestTable() throws Exception {
    // given: wrote some base files and corresponding markers
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("000")
        .getFileIdsWithBaseFilesInPartitions("partA").get("partA");
    String f1 = testTable.addCommit("001")
        .withBaseFilesInPartition("partA", f0).getLeft()
        .getFileIdsWithBaseFilesInPartitions("partB").get("partB");
    String f2 = "f2";
    testTable.forCommit("001")
        .withMarkerFile("partA", f0, IOType.MERGE)
        .withMarkerFile("partB", f1, IOType.CREATE)
        .withMarkerFile("partA", f2, IOType.CREATE);

    // when
    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder().withEmbeddedTimelineServerEnabled(false).build(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(),
        "002").getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));

    List<HoodieRollbackStat> stats = new BaseRollbackHelper(hoodieTable, getConfig()).performRollback(context, "002",
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"),
        rollbackRequests);

    // then: ensure files are deleted correctly, non-existent files reported as failed deletes
    assertEquals(2, stats.size());

    FileStatus[] partAFiles = testTable.listAllFilesInPartition("partA");
    FileStatus[] partBFiles = testTable.listAllFilesInPartition("partB");

    assertEquals(0, partBFiles.length);
    assertEquals(1, partAFiles.length);
    assertEquals(2, stats.stream().mapToInt(r -> r.getSuccessDeleteFiles().size()).sum());
    assertEquals(1, stats.stream().mapToInt(r -> r.getFailedDeleteFiles().size()).sum());
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testCopyOnWriteRollback(boolean useFileListingMetadata) throws Exception {
    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true).withAutoCommit(false)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withPath(basePath).build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // rollback 2nd commit and ensure stats reflect the info.
      List<HoodieRollbackStat> stats = testUpdateAndRollback(useFileListingMetadata, writeConfig, writeClient);

      assertEquals(3, stats.size());
      for (HoodieRollbackStat stat : stats) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(0, stat.getCommandBlocksCount().size());
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

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true)
        .withEmbeddedTimelineServerEnabled(false).withAutoCommit(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withPath(basePath).build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {

      // rollback 2nd commit and ensure stats reflect the info.
      List<HoodieRollbackStat> stats = testUpdateAndRollback(useFileListingMetadata, writeConfig, writeClient);

      assertEquals(3, stats.size());
      for (HoodieRollbackStat stat : stats) {
        assertEquals(0, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(1, stat.getCommandBlocksCount().size());
        stat.getCommandBlocksCount().forEach((fileStatus, len) -> assertTrue(fileStatus.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())));
      }
    }
  }

  @ParameterizedTest(name = TEST_NAME_WITH_PARAMS)
  @MethodSource("configParams")
  public void testMergeOnReadRollbackDeletesFirstAppendFiles(boolean useFileListingMetadata) throws Exception {
    // init MERGE_ON_READ_TABLE
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true).withAutoCommit(false)
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(useFileListingMetadata).build())
        .withPath(basePath).build();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    try (SparkRDDWriteClient writeClient = new SparkRDDWriteClient(engineContext, writeConfig)) {

      // rollback 2nd commit and ensure stats reflect the info.
      List<HoodieRollbackStat> stats = testInsertAndRollback(writeClient);

      assertEquals(3, stats.size());
      for (HoodieRollbackStat stat : stats) {
        assertEquals(1, stat.getSuccessDeleteFiles().size());
        assertEquals(0, stat.getFailedDeleteFiles().size());
        assertEquals(0, stat.getCommandBlocksCount().size());
        stat.getCommandBlocksCount().forEach((fileStatus, len) -> assertTrue(fileStatus.getPath().getName().contains(HoodieFileFormat.HOODIE_LOG.getFileExtension())));
      }
    }
  }

  private List<HoodieRollbackStat> testInsertAndRollback(SparkRDDWriteClient writeClient) {
    String newCommitTime = "001";
    writeClient.startCommitWithTime(newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = writeClient.insert(jsc.parallelize(records, 1), newCommitTime);
    writeClient.commit(newCommitTime, writeStatuses);

    writeStatuses.collect();

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder().withEmbeddedTimelineServerEnabled(false).build(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context,
        getConfigBuilder().withEmbeddedTimelineServerEnabled(false).build(),
        "002").getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));

    // rollback 1st commit and ensure stats reflect the info.
    return new BaseRollbackHelper(hoodieTable, getConfig()).performRollback(context, "002",
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"),
        rollbackRequests);
  }

  private List<HoodieRollbackStat> testUpdateAndRollback(boolean useFileListingMetadata, HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient) {
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

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder().withEmbeddedTimelineServerEnabled(false).build(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfigBuilder()
        .withEmbeddedTimelineServerEnabled(false).build(),
        "003").getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"));

    // rollback 2nd commit and ensure stats reflect the info.
    return new BaseRollbackHelper(hoodieTable, getConfig()).performRollback(context, "003",
        new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
        rollbackRequests);
  }

  @Test
  public void testMarkerBasedRollbackFallbackToTimelineServerWhenDirectMarkerFails() throws Exception {
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("000")
        .getFileIdsWithBaseFilesInPartitions("partA").get("partA");
    testTable.forCommit("001")
        .withLogMarkerFile("000", "partA", f0, IOType.APPEND, 1);

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfig(), context, metaClient);

    DirectWriteMarkers writeMarkers = mock(DirectWriteMarkers.class);
    initMocks(this);
    when(writeMarkers.allMarkerFilePaths()).thenThrow(new IOException("Markers.type file not present"));
    MarkerBasedRollbackStrategy rollbackStrategy = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(), "002");
    List<HoodieRollbackRequest> rollbackRequests = rollbackStrategy.getRollbackRequests(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
  }
}
