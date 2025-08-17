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

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.client.embedded.EmbeddedTimelineServerHelper;
import org.apache.hudi.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.IOType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.rollback.MarkerBasedRollbackStrategy;
import org.apache.hudi.table.action.rollback.MergeOnReadRollbackActionExecutor;
import org.apache.hudi.table.action.rollback.RollbackHelper;
import org.apache.hudi.table.marker.DirectWriteMarkers;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieSparkWriteableTestTable;

import org.apache.hadoop.fs.FileStatus;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.hudi.avro.HoodieAvroUtils.addMetadataFields;
import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.createSimpleRecord;
import static org.apache.hudi.config.HoodieWriteConfig.ROLLBACK_PARALLELISM_VALUE;
import static org.apache.hudi.table.action.rollback.BaseRollbackPlanActionExecutor.LATEST_ROLLBACK_PLAN_VERSION;
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
        "002").getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
  }

  @ParameterizedTest
  @EnumSource(names = {"APPEND", "CREATE"})
  public void testMarkerBasedRollbackAppendWithLogFileMarkers(IOType testIOType) throws Exception {
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();
    HoodieTestTable testTable = HoodieTestTable.of(metaClient);
    String f0 = testTable.addRequestedCommit("000")
        .getFileIdWithLogFile("partA");
    testTable.forCommit("001")
        .withLogMarkerFile("partA", f0, testIOType);

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfig(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(),
        "002").getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
    HoodieRollbackRequest rollbackRequest = rollbackRequests.get(0);
    assertEquals("partA", rollbackRequest.getPartitionPath());
    assertEquals(f0, rollbackRequest.getFileId());
    assertEquals(testIOType.equals(IOType.CREATE) ? 1 : 0, rollbackRequest.getFilesToBeDeleted().size());
    assertEquals(testIOType.equals(IOType.CREATE) ? 0 : 1, rollbackRequest.getLogBlocksToBeDeleted().size());
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
    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder().build(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(),
        "002").getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));

    //  "002" argument performRollback older API
    List<HoodieRollbackStat> stats = new RollbackHelper(hoodieTable, getConfig()).performRollback(context, "002",
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"),
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

  @Test
  public void testCopyOnWriteRollbackNoMdt() throws Exception {
    testCopyOnWriteRollback(false);
  }

  @Test
  public void testCopyOnWriteRollback() throws Exception {
    testCopyOnWriteRollback(true);
  }

  private void testCopyOnWriteRollback(boolean useFileListingMetadata) throws Exception {
    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true)
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
  public void testMergeOnReadRollbackDeletesFirstAppendFiles(boolean useFileListingMetadata) throws Exception {
    // init MERGE_ON_READ_TABLE
    tearDown();
    tableType = HoodieTableType.MERGE_ON_READ;
    setUp();

    HoodieWriteConfig writeConfig = getConfigBuilder().withRollbackUsingMarkers(true)
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
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = writeClient.insert(jsc.parallelize(records, 1), newCommitTime);
    writeClient.commit(newCommitTime, writeStatuses);

    writeStatuses.collect();

    HoodieTable hoodieTable = HoodieSparkTable.create(getConfigBuilder().build(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, getConfigBuilder().build(),
        "002").getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"));

    // rollback 1st commit and ensure stats reflect the info.
    // 002 argument older API might be needed
    return new RollbackHelper(hoodieTable, getConfig()).performRollback(context, "002",
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "001"),
        rollbackRequests);
  }

  private List<HoodieRollbackStat> testUpdateAndRollback(boolean useFileListingMetadata, HoodieWriteConfig writeConfig, SparkRDDWriteClient writeClient) {
    String newCommitTime = "001";
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);

    List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 100);
    JavaRDD<WriteStatus> writeStatuses = writeClient.insert(jsc.parallelize(records, 1), newCommitTime);
    writeClient.commit(newCommitTime, writeStatuses);

    // Updates
    newCommitTime = "002";
    WriteClientTestUtils.startCommitWithTime(writeClient, newCommitTime);
    records = dataGen.generateUniqueUpdates(newCommitTime, 50);
    writeStatuses = writeClient.upsert(jsc.parallelize(records, 1), newCommitTime);
    writeStatuses.collect();

    HoodieTable hoodieTable = HoodieSparkTable.create(writeClient.getConfig(), context, metaClient);
    List<HoodieRollbackRequest> rollbackRequests = new MarkerBasedRollbackStrategy(hoodieTable, context, writeClient.getConfig(),
        "003").getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"));

    // rollback 2nd commit and ensure stats reflect the info.
    // 003 argument older API might be needed
    return new RollbackHelper(hoodieTable, getConfig()).performRollback(context, "003",
        INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, "002"),
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
    List<HoodieRollbackRequest> rollbackRequests = rollbackStrategy.getRollbackRequests(INSTANT_GENERATOR.createNewInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "001"));
    assertEquals(1, rollbackRequests.size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"SIX", "EIGHT"})
  void testRollbackMultipleAppendLogFilesInOneFileGroupInMOR(HoodieTableVersion tableVersion) throws Exception {
    Properties props = new Properties();
    props.put(HoodieTableConfig.VERSION.key(), tableVersion.versionCode());
    initMetaClient(tableType, props);
    String partition = "partA";
    HoodieSparkWriteableTestTable testTable = HoodieSparkWriteableTestTable.of(
        metaClient, addMetadataFields(HoodieTestUtils.SIMPLE_RECORD_SCHEMA));
    String fileId = UUID.randomUUID().toString();
    HoodieRecord tripRecord = createSimpleRecord("key1", "2016-01-31T03:16:41.415Z", 123);
    String instantTime1 = "001";
    testTable.forCommit(instantTime1);
    StoragePath baseFilePath = testTable.withInserts(partition, fileId, Collections.singletonList(tripRecord));
    testTable.addDeltaCommit(instantTime1);
    assertTrue(storage.exists(baseFilePath));

    String instantTime2 = "002";
    testTable.forDeltaCommit(instantTime2)
        .withLogFile(partition, fileId,
            tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
                ? instantTime2 : instantTime1, 1);
    testTable.addDeltaCommit(instantTime2);

    String instantTime3 = "003";
    int numLogFiles = 199;
    Set<String> logFilePathSet = new HashSet<>();
    int[] logVersions = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
        ? IntStream.rangeClosed(1, numLogFiles).toArray()
        : IntStream.rangeClosed(2, numLogFiles + 1).toArray();
    String logFileInstantTime = tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)
        ? instantTime3 : instantTime1;
    testTable.forDeltaCommit(instantTime3)
        .withLogFile(partition, fileId, logFileInstantTime, logVersions);
    for (int version : logVersions) {
      String logFileName = FileCreateUtils.logFileName(logFileInstantTime, fileId, version);
      StoragePath logFilePath = new StoragePath(new StoragePath(basePath, partition), logFileName);
      assertTrue(storage.exists(logFilePath));
      logFilePathSet.add(logFilePath.toString());
      testTable.withLogMarkerFile(partition, logFileName);
    }
    testTable.addInflightDeltaCommit(instantTime3);

    metaClient.reloadActiveTimeline();
    HoodieWriteConfig writeConfig = getConfig();
    writeConfig.setValue(ROLLBACK_PARALLELISM_VALUE, String.valueOf(logVersions.length));
    HoodieTable hoodieTable = HoodieSparkTable.create(getConfig(), context, metaClient);

    DirectWriteMarkers writeMarkers = mock(DirectWriteMarkers.class);
    MockitoAnnotations.openMocks(this);
    when(writeMarkers.allMarkerFilePaths()).thenThrow(new IOException("Markers.type file not present"));
    MarkerBasedRollbackStrategy rollbackStrategy =
        new MarkerBasedRollbackStrategy(hoodieTable, context, getConfig(), "004");
    HoodieInstant instantToRollback = INSTANT_GENERATOR.createNewInstant(
        HoodieInstant.State.INFLIGHT, HoodieTimeline.DELTA_COMMIT_ACTION, instantTime3);
    List<HoodieRollbackRequest> rollbackRequests = rollbackStrategy.getRollbackRequests(instantToRollback);
    assertEquals(
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? numLogFiles : 1,
        rollbackRequests.size());
    HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan(
        new HoodieInstantInfo(instantTime3, HoodieTimeline.DELTA_COMMIT_ACTION),
        rollbackRequests, LATEST_ROLLBACK_PLAN_VERSION);
    EmbeddedTimelineService timelineServer =
        EmbeddedTimelineServerHelper.createEmbeddedTimelineService(context, writeConfig);
    writeConfig.setViewStorageConfig(timelineServer.getRemoteFileSystemViewConfig(writeConfig));
    hoodieTable = HoodieSparkTable.create(writeConfig, context, metaClient);
    MergeOnReadRollbackActionExecutor rollbackActionExecutor = new MergeOnReadRollbackActionExecutor(
        context, writeConfig, hoodieTable, "004", instantToRollback, true, false);
    List<HoodieRollbackStat> rollbackStats = rollbackActionExecutor.doRollbackAndGetStats(rollbackPlan);
    timelineServer.stopForBasePath(basePath);
    assertEquals(1, rollbackStats.size());
    HoodieRollbackStat rollbackStat = rollbackStats.get(0);
    if (!tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      StoragePath rollbackLogPath = new StoragePath(new StoragePath(basePath, partition),
          FileCreateUtils.logFileName(instantTime1, fileId, numLogFiles + 2));
      assertTrue(storage.exists(rollbackLogPath));
      assertEquals(rollbackLogPath.getPathWithoutSchemeAndAuthority(),
          rollbackStat.getCommandBlocksCount().entrySet().stream().findFirst().get()
              .getKey().getPath().getPathWithoutSchemeAndAuthority());
    }
    assertEquals(partition, rollbackStat.getPartitionPath());
    assertEquals(
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? numLogFiles : 0,
        rollbackStat.getSuccessDeleteFiles().size());
    if (tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT)) {
      rollbackStat.getSuccessDeleteFiles().forEach(logFilePathSet::contains);
    }
    assertEquals(0, rollbackStat.getFailedDeleteFiles().size());
    assertEquals(
        tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 0 : 1,
        rollbackStat.getCommandBlocksCount().size());
    assertEquals(tableVersion.greaterThanOrEquals(HoodieTableVersion.EIGHT) ? 0 : numLogFiles, rollbackStat.getLogFilesFromFailedCommit().size());
  }
}
