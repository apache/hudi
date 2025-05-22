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

package org.apache.hudi.functional;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("functional")
class TestHoodieSparkMergeOnReadTableClustering extends SparkClientFunctionalTestHarness {

  private static Stream<Arguments> testClustering() {
    // enableClusteringAsRow, doUpdates, populateMetaFields, preserveCommitMetadata
    return Stream.of(
        Arguments.of(false, true, true),
        Arguments.of(true, true, false),
        Arguments.of(true, false, true),
        Arguments.of(true, false, false),
        Arguments.of(false, true, true),
        Arguments.of(false, true, false),
        Arguments.of(false, false, true),
        Arguments.of(false, false, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void testClustering(boolean clusteringAsRow, boolean doUpdates, boolean populateMetaFields) throws Exception {
    // set low compaction small File Size to generate more file groups.
    HoodieWriteConfig.Builder cfgBuilder = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(10L)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024)
            .parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.BLOOM).build())
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1)
            .build())
        .withRollbackUsingMarkers(false);
    addConfigsForPopulateMetaFields(cfgBuilder, populateMetaFields);
    HoodieWriteConfig cfg = cfgBuilder.build();
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, cfg.getProps());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {

      /*
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 400);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records.subList(0, 200), client, cfg, newCommitTime);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      /*
       * Write 2 (more inserts to create new files)
       */
      // we already set small file size to small number to force inserts to go into new file.
      newCommitTime = "002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      dataFiles = insertRecordsToMORTable(metaClient, records.subList(200, 400), client, cfg, newCommitTime);
      assertTrue(dataFiles.findAny().isPresent(), "should list the base files we wrote in the delta commit");

      if (doUpdates) {
        /*
         * Write 3 (updates)
         */
        newCommitTime = "003";
        WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
        records = dataGen.generateUpdates(newCommitTime, 100);
        updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime, false);
      }

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      hoodieTable.getHoodieView().sync();
      List<StoragePathInfo> allFiles = listAllBaseFilesInPath(hoodieTable);
      // expect 2 base files for each partition
      assertEquals(dataGen.getPartitionPaths().length * 2, allFiles.size());

      String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      // verify all files are included in clustering plan.
      assertEquals(allFiles.size(),
          hoodieTable.getFileSystemView().getFileGroupsInPendingClustering().map(Pair::getLeft).count());

      // Do the clustering and validate
      doClusteringAndValidate(client, clusteringCommitTime, metaClient, cfg, dataGen, clusteringAsRow);
    }
  }

  private static Stream<Arguments> testClusteringWithNoBaseFiles() {
    return Stream.of(
        Arguments.of(true, true, false),
        Arguments.of(true, false, false),
        Arguments.of(false, true, false),
        Arguments.of(false, false, false),
        // do updates with file slice having no base files and write record positions in log blocks
        Arguments.of(true, true, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void testClusteringWithNoBaseFiles(boolean clusteringAsRow, boolean doUpdates, boolean shouldWriteRecordPositions) throws Exception {
    // set low compaction small File Size to generate more file groups.
    HoodieWriteConfig.Builder cfgBuilder = HoodieWriteConfig.newBuilder()
        .forTable("test-trip-table")
        .withPath(basePath())
        .withSchema(TRIP_EXAMPLE_SCHEMA)
        .withParallelism(2, 2)
        .withDeleteParallelism(2)
        .withAutoCommit(true)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .compactionSmallFileSize(10L)
            .withInlineCompaction(false).withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withStorageConfig(HoodieStorageConfig.newBuilder()
            .hfileMaxFileSize(1024 * 1024 * 1024)
            .parquetMaxFileSize(1024 * 1024 * 1024).build())
        .withEmbeddedTimelineServerEnabled(true)
        .withFileSystemViewConfig(new FileSystemViewStorageConfig.Builder()
            .withEnableBackupForRemoteFileSystemView(false).build())
        // set index type to INMEMORY so that log files can be indexed, and it is safe to send
        // inserts straight to the log to produce file slices with only log files and no data files
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withWriteRecordPositionsEnabled(shouldWriteRecordPositions)
        .withClusteringConfig(HoodieClusteringConfig.newBuilder()
            .withClusteringMaxNumGroups(10)
            .withClusteringTargetPartitions(0)
            .withInlineClustering(true)
            .withInlineClusteringNumCommits(1).build())
        .withRollbackUsingMarkers(false);
    HoodieWriteConfig cfg = cfgBuilder.build();
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, cfg.getProps());
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();

    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      // test 2 inserts
      String newCommitTime = "001";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 400);
      Stream<HoodieBaseFile> dataFiles = insertRecordsToMORTable(metaClient, records.subList(0, 200), client, cfg, newCommitTime);
      assertTrue(!dataFiles.findAny().isPresent(), "should not have any base files");
      newCommitTime = "002";
      WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
      dataFiles = insertRecordsToMORTable(metaClient, records.subList(200, 400), client, cfg, newCommitTime);
      assertTrue(!dataFiles.findAny().isPresent(), "should not have any base files");
      // run updates
      if (doUpdates) {
        newCommitTime = "003";
        WriteClientTestUtils.startCommitWithTime(client, newCommitTime);
        records = dataGen.generateUpdates(newCommitTime, 100);
        updateRecordsInMORTable(metaClient, records, client, cfg, newCommitTime, false);
      }

      HoodieTable hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      hoodieTable.getHoodieView().sync();
      List<StoragePathInfo> allBaseFiles = listAllBaseFilesInPath(hoodieTable);
      // expect 0 base files for each partition
      assertEquals(0, allBaseFiles.size());

      String clusteringCommitTime = client.scheduleClustering(Option.empty()).get().toString();
      metaClient = HoodieTableMetaClient.reload(metaClient);
      hoodieTable = HoodieSparkTable.create(cfg, context(), metaClient);
      // verify log files are included in clustering plan for each partition.
      assertEquals(dataGen.getPartitionPaths().length, hoodieTable.getFileSystemView().getFileGroupsInPendingClustering().map(Pair::getLeft).count());

      // do the clustering and validate
      doClusteringAndValidate(client, clusteringCommitTime, metaClient, cfg, dataGen, clusteringAsRow);
    }
  }

  private void doClusteringAndValidate(SparkRDDWriteClient client,
                                       String clusteringCommitTime,
                                       HoodieTableMetaClient metaClient,
                                       HoodieWriteConfig cfg,
                                       HoodieTestDataGenerator dataGen,
                                       boolean clusteringAsRow) {
    client.getConfig().setValue(DataSourceWriteOptions.ENABLE_ROW_WRITER(), Boolean.toString(clusteringAsRow));

    client.cluster(clusteringCommitTime, true);
    metaClient = HoodieTableMetaClient.reload(metaClient);
    final HoodieTable clusteredTable = HoodieSparkTable.create(cfg, context(), metaClient);
    clusteredTable.getHoodieView().sync();
    Stream<HoodieBaseFile> dataFilesToRead = Arrays.stream(dataGen.getPartitionPaths())
        .flatMap(p -> clusteredTable.getBaseFileOnlyView().getLatestBaseFiles(p));
    assertEquals(dataGen.getPartitionPaths().length, dataFilesToRead.count());
    HoodieTimeline timeline = metaClient.getCommitTimeline().filterCompletedInstants();
    assertEquals(1, timeline.findInstantsAfter("003", Integer.MAX_VALUE).countInstants(),
        "Expecting a single commit.");
    assertEquals(clusteringCommitTime, timeline.lastInstant().get().requestedTime());
    assertEquals(HoodieTimeline.REPLACE_COMMIT_ACTION, timeline.lastInstant().get().getAction());
    if (cfg.populateMetaFields()) {
      assertEquals(400, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.of("000")),
          "Must contain 200 records");
    } else {
      assertEquals(400, HoodieClientTestUtils.countRecordsOptionallySince(jsc(), basePath(), sqlContext(), timeline, Option.empty()));
    }
  }
}
