/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.client.functional;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.RemoteHoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.metadata.HoodieBackedTestDelayedTableMetadata;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.timeline.service.TimelineService;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.HoodieTimeline.COMMIT_ACTION;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.DELTA_COMMIT_ACTION;
import static org.apache.hudi.common.table.view.FileSystemViewStorageConfig.REMOTE_PORT_NUM;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the {@link RemoteHoodieTableFileSystemView} with metadata table enabled, using
 * {@link org.apache.hudi.common.table.view.HoodieTableFileSystemView} on the timeline server.
 */
@Slf4j
public class TestRemoteFileSystemViewWithMetadataTable extends HoodieSparkClientTestHarness {

  @BeforeEach
  public void setUp() throws Exception {
    initPath();
    initSparkContexts();
    initHoodieStorage();
    dataGen = new HoodieTestDataGenerator(0x1f86);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupTimelineService();
    cleanupClients();
    cleanupSparkContexts();
    cleanupFileSystem();
    cleanupExecutorService();
    dataGen = null;
    System.gc();
  }

  @Override
  public void initTimelineService() {
    // Start a timeline server that are running across multiple commits
    HoodieLocalEngineContext localEngineContext = new HoodieLocalEngineContext(storageConf);

    try {
      HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
          .withPath(basePath)
          .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
              .withRemoteServerPort(incrementTimelineServicePortToUse()).build())
          .build();
      timelineService = new TimelineService(HadoopFSUtils.getStorageConf(),
          TimelineService.Config.builder().enableMarkerRequests(true)
              .serverPort(config.getViewStorageConfig().getRemoteViewServerPort()).build(),
          FileSystemViewManager.createViewManager(
              context, config.getMetadataConfig(), config.getViewStorageConfig(),
              config.getCommonConfig(),
              metaClient -> new HoodieBackedTestDelayedTableMetadata(
                  context, metaClient.getStorage(), config.getMetadataConfig(), metaClient.getBasePath().toString(), true)));
      timelineService.startService();
      timelineServicePort = timelineService.getServerPort();
      log.info("Started timeline server on port: {}", timelineServicePort);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private enum TestCase {
    USE_EXISTING_TIMELINE_SERVER(true, false),
    EMBEDDED_TIMELINE_SERVER_PER_TABLE(false, false),
    SINGLE_EMBEDDED_TIMELINE_SERVER(false, true);

    private final boolean useExistingTimelineServer;
    private final boolean reuseTimelineServer;

    TestCase(boolean useExistingTimelineServer, boolean reuseTimelineServer) {
      this.useExistingTimelineServer = useExistingTimelineServer;
      this.reuseTimelineServer = reuseTimelineServer;
    }
  }

  @ParameterizedTest
  @EnumSource(value = TestCase.class)
  public void testMORGetLatestFileSliceWithMetadataTable(TestCase testCase) throws IOException {
    if (testCase.useExistingTimelineServer) {
      initTimelineService();
    }
    // This test utilizes the `HoodieBackedTestDelayedTableMetadata` to make sure the
    // synced file system view is always served.

    // Create two tables to guarantee the timeline server can properly handle multiple base paths with metadata table enabled
    String basePathStr1 = initializeTable("dataset1");
    String basePathStr2 = initializeTable("dataset2");
    try (SparkRDDWriteClient writeClient1 = createWriteClient(basePathStr1, "test_mor_table1", testCase.reuseTimelineServer,
        testCase.useExistingTimelineServer ? Option.of(timelineService) : Option.empty());
         SparkRDDWriteClient writeClient2 = createWriteClient(basePathStr2, "test_mor_table2", testCase.reuseTimelineServer,
             testCase.useExistingTimelineServer ? Option.of(timelineService) : Option.empty())) {
      for (int i = 0; i < 3; i++) {
        writeToTable(i, writeClient1);
      }

      for (int i = 0; i < 3; i++) {
        writeToTable(i, writeClient2);
      }

      runAssertionsForBasePath(testCase.useExistingTimelineServer, basePathStr1, writeClient1);
      runAssertionsForBasePath(testCase.useExistingTimelineServer, basePathStr2, writeClient2);
    }
  }

  private void runAssertionsForBasePath(boolean useExistingTimelineServer, String basePathStr, SparkRDDWriteClient writeClient) throws IOException {
    // At this point, there are three deltacommits and one compaction commit in the Hudi timeline,
    // and the file system view of timeline server is not yet synced
    HoodieTableMetaClient newMetaClient = createMetaClient(basePathStr);
    HoodieActiveTimeline timeline = newMetaClient.getActiveTimeline();
    HoodieInstant compactionCommit = timeline.lastInstant().get();
    assertTrue(timeline.lastInstant().get().getAction().equals(COMMIT_ACTION));


    // For all the file groups compacted by the compaction commit, the file system view
    // should return the latest file slices which is written by the latest commit
    HoodieCommitMetadata commitMetadata = timeline.readCommitMetadata(compactionCommit);
    List<Pair<String, String>> partitionFileIdPairList =
        commitMetadata.getPartitionToWriteStats().entrySet().stream().flatMap(
            entry -> {
              String partitionPath = entry.getKey();
              return entry.getValue().stream().map(
                  writeStat -> Pair.of(partitionPath, writeStat.getFileId()));
            }
        ).collect(Collectors.toList());
    List<Pair<String, String>> lookupList = new ArrayList<>();
    // Accumulate enough threads to test read-write concurrency while syncing the file system
    // view at the timeline server
    while (lookupList.size() < 128) {
      lookupList.addAll(partitionFileIdPairList);
    }

    int timelineServerPort = useExistingTimelineServer
        ? timelineService.getServerPort()
        : writeClient.getTimelineServer().get().getRemoteFileSystemViewConfig(writeClient.getConfig()).getRemoteViewServerPort();

    log.info("Connecting to Timeline Server: {}", timelineServerPort);
    RemoteHoodieTableFileSystemView view =
        new RemoteHoodieTableFileSystemView("localhost", timelineServerPort, newMetaClient);

    List<TestViewLookUpCallable> callableList = lookupList.stream()
        .map(pair -> new TestViewLookUpCallable(view, pair, compactionCommit.requestedTime(), basePathStr))
        .collect(Collectors.toList());
    List<Future<Boolean>> resultList = new ArrayList<>();

    ExecutorService pool = Executors.newCachedThreadPool();
    callableList.forEach(callable -> {
      resultList.add(pool.submit(callable));
    });

    assertTrue(resultList.stream().map(future -> {
      try {
        return future.get();
      } catch (Exception e) {
        log.error("Get result error", e);
        return false;
      }
    }).reduce((a, b) -> a && b).get());
    pool.shutdown();
  }

  private String initializeTable(String dataset) throws IOException {
    java.nio.file.Path basePath = tempDir.resolve(dataset);
    Files.createDirectories(basePath);
    String basePathStr = basePath.toAbsolutePath().toString();
    HoodieTestUtils.init(storageConf, basePathStr, HoodieTableType.MERGE_ON_READ, new Properties());
    return basePathStr;
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private SparkRDDWriteClient createWriteClient(String basePath, String tableName, boolean reuseTimelineServer, Option<TimelineService> timelineService) {
    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(basePath)
        .withSchema(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withBulkInsertParallelism(2)
        .withFinalizeWriteParallelism(2)
        .withDeleteParallelism(2)
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withMergeSmallFileGroupCandidatesLimit(0)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(3)
            .build())
        .withFileSystemViewConfig(FileSystemViewStorageConfig.newBuilder()
            .withStorageType(FileSystemViewStorageType.REMOTE_ONLY)
            .withRemoteServerPort(timelineService.isPresent()
                ? timelineService.get().getServerPort() : REMOTE_PORT_NUM.defaultValue())
            .build())
        .withEmbeddedTimelineServerReuseEnabled(reuseTimelineServer)
        .forTable(tableName)
        .build();
    return new SparkRDDWriteClient(context, writeConfig, timelineService);
  }

  private void writeToTable(int round, SparkRDDWriteClient writeClient) throws IOException {
    String instantTime = writeClient.startCommit();
    List<HoodieRecord> records = round == 0
        ? dataGen.generateInserts(instantTime, 100)
        : dataGen.generateUpdates(instantTime, 100);

    JavaRDD<WriteStatus> writeStatusRDD = writeClient.upsert(jsc.parallelize(records, 1), instantTime);
    writeClient.commit(instantTime, writeStatusRDD, Option.empty(), DELTA_COMMIT_ACTION, Collections.emptyMap());
    // Triggers compaction
    writeClient.scheduleCompaction(Option.empty());
    writeClient.runAnyPendingCompactions();
  }

  /**
   * Test callable to send lookup request to the timeline server for the latest file slice
   * based on the partition path and file ID.
   */
  static class TestViewLookUpCallable implements Callable<Boolean> {
    private final RemoteHoodieTableFileSystemView view;
    private final Pair<String, String> partitionFileIdPair;
    private final String expectedCommitTime;
    private final String expectedBasePath;

    public TestViewLookUpCallable(
        RemoteHoodieTableFileSystemView view,
        Pair<String, String> partitionFileIdPair,
        String expectedCommitTime,
        String expectedBasePath) {
      this.view = view;
      this.partitionFileIdPair = partitionFileIdPair;
      this.expectedCommitTime = expectedCommitTime;
      this.expectedBasePath = expectedBasePath;
    }

    @Override
    public Boolean call() throws Exception {
      Option<FileSlice> latestFileSlice = view.getLatestFileSlice(
          partitionFileIdPair.getLeft(), partitionFileIdPair.getRight());
      String latestBaseFilePath = latestFileSlice.get().getBaseFile().get().getPath();
      boolean result = latestFileSlice.isPresent() && latestBaseFilePath.startsWith(expectedBasePath)
          && expectedCommitTime.equals(FSUtils.getCommitTime(new Path(latestBaseFilePath).getName()));
      if (!result) {
        log.error("The timeline server does not return the correct result: latestFileSliceReturned={} expectedCommitTime={}",
            latestFileSlice, expectedCommitTime);
      }
      return result;
    }
  }
}