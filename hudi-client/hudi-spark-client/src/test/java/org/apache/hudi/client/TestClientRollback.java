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

package org.apache.hudi.client;

import org.apache.hudi.avro.model.HoodieInstantInfo;
import org.apache.hudi.avro.model.HoodieRollbackPlan;
import org.apache.hudi.avro.model.HoodieRollbackRequest;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.FileCreateUtils;
import org.apache.hudi.common.testutils.HoodieMetadataTestTable;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.metadata.SparkHoodieBackedTableMetadataWriter;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.EMPTY_STRING;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test Cases for rollback of snapshots and commits.
 */
public class TestClientRollback extends HoodieClientTestBase {

  /**
   * Test case for rollback-savepoint interaction.
   */
  @Test
  public void testSavepointAndRollback() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(HoodieCompactionConfig.newBuilder()
        .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1).build()).build();
    try (SparkRDDWriteClient client = getHoodieWriteClient(cfg)) {
      HoodieTestDataGenerator.writePartitionMetadataDeprecated(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

      /**
       * Write 1 (only inserts)
       */
      String newCommitTime = "001";
      client.startCommitWithTime(newCommitTime);

      List<HoodieRecord> records = dataGen.generateInserts(newCommitTime, 200);
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
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      client.savepoint("hoodie-unit-test", "test");

      /**
       * Write 3 (updates)
       */
      newCommitTime = "003";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);
      HoodieWriteConfig config = getConfig();
      List<String> partitionPaths =
          FSUtils.getAllPartitionPaths(context, config.getMetadataConfig(), cfg.getBasePath());
      metaClient = HoodieTableMetaClient.reload(metaClient);
      HoodieSparkTable table = HoodieSparkTable.create(getConfig(), context, metaClient);
      final BaseFileOnlyView view1 = table.getBaseFileOnlyView();

      List<HoodieBaseFile> dataFiles = partitionPaths.stream().flatMap(s -> {
        return view1.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("003"));
      }).collect(Collectors.toList());
      assertEquals(3, dataFiles.size(), "The data files for commit 003 should be present");

      dataFiles = partitionPaths.stream().flatMap(s -> {
        return view1.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("002"));
      }).collect(Collectors.toList());
      assertEquals(3, dataFiles.size(), "The data files for commit 002 should be present");

      /**
       * Write 4 (updates)
       */
      newCommitTime = "004";
      client.startCommitWithTime(newCommitTime);

      records = dataGen.generateUpdates(newCommitTime, records);
      statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
      // Verify there are no errors
      assertNoWriteErrors(statuses);

      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(getConfig(), context, metaClient);
      final BaseFileOnlyView view2 = table.getBaseFileOnlyView();

      dataFiles = partitionPaths.stream().flatMap(s -> view2.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("004"))).collect(Collectors.toList());
      assertEquals(3, dataFiles.size(), "The data files for commit 004 should be present");

      // rolling back to a non existent savepoint must not succeed
      assertThrows(HoodieRollbackException.class, () -> {
        client.restoreToSavepoint("001");
      }, "Rolling back to non-existent savepoint should not be allowed");

      // rollback to savepoint 002
      HoodieInstant savepoint = table.getCompletedSavepointTimeline().getInstants().findFirst().get();
      client.restoreToSavepoint(savepoint.getTimestamp());

      metaClient = HoodieTableMetaClient.reload(metaClient);
      table = HoodieSparkTable.create(getConfig(), context, metaClient);
      final BaseFileOnlyView view3 = table.getBaseFileOnlyView();
      dataFiles = partitionPaths.stream().flatMap(s -> view3.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("002"))).collect(Collectors.toList());
      assertEquals(3, dataFiles.size(), "The data files for commit 002 be available");

      dataFiles = partitionPaths.stream().flatMap(s -> view3.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("003"))).collect(Collectors.toList());
      assertEquals(0, dataFiles.size(), "The data files for commit 003 should be rolled back");

      dataFiles = partitionPaths.stream().flatMap(s -> view3.getAllBaseFiles(s).filter(f -> f.getCommitTime().equals("004"))).collect(Collectors.toList());
      assertEquals(0, dataFiles.size(), "The data files for commit 004 should be rolled back");
    }
  }

  /**
   * Test Cases for effects of rolling back completed/inflight commits.
   */
  @Test
  public void testRollbackCommit() throws Exception {
    // Let's create some commit files and base files
    final String p1 = "2016/05/01";
    final String p2 = "2016/05/02";
    final String p3 = "2016/05/06";
    final String commitTime1 = "20160501010101";
    final String commitTime2 = "20160502020601";
    final String commitTime3 = "20160506030611";
    Map<String, String> partitionAndFileId1 = new HashMap<String, String>() {
      {
        put(p1, "id11");
        put(p2, "id12");
        put(p3, "id13");
      }
    };
    Map<String, String> partitionAndFileId2 = new HashMap<String, String>() {
      {
        put(p1, "id21");
        put(p2, "id22");
        put(p3, "id23");
      }
    };
    Map<String, String> partitionAndFileId3 = new HashMap<String, String>() {
      {
        put(p1, "id31");
        put(p2, "id32");
        put(p3, "id33");
      }
    };

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withRollbackUsingMarkers(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap1 = new HashMap<>();
    partitionAndFileId1.forEach((k, v) -> partitionToFilesNameLengthMap1.put(k, Collections.singletonList(Pair.of(v, 100))));
    testTable.doWriteOperation(commitTime1, WriteOperationType.INSERT, Arrays.asList(p1, p2, p3), partitionToFilesNameLengthMap1,
        false, false);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap2 = new HashMap<>();
    partitionAndFileId2.forEach((k, v) -> partitionToFilesNameLengthMap2.put(k, Collections.singletonList(Pair.of(v, 200))));
    testTable.doWriteOperation(commitTime2, WriteOperationType.INSERT, Collections.emptyList(), partitionToFilesNameLengthMap2,
        false, false);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap3 = new HashMap<>();
    partitionAndFileId3.forEach((k, v) -> partitionToFilesNameLengthMap3.put(k, Collections.singletonList(Pair.of(v, 300))));
    testTable.doWriteOperation(commitTime3, WriteOperationType.INSERT, Collections.emptyList(), partitionToFilesNameLengthMap3,
        false, true);

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {

      // Rollback commit3
      client.rollback(commitTime3);
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertFalse(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));

      // simulate partial failure, where .inflight was not deleted, but data files were.
      testTable.addInflightCommit(commitTime3);
      client.rollback(commitTime3);
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));

      // Rollback commit2
      client.rollback(commitTime2);
      assertFalse(testTable.commitExists(commitTime2));
      assertFalse(testTable.inflightCommitExists(commitTime2));
      assertFalse(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));

      // simulate partial failure, where only .commit => .inflight renaming succeeded, leaving a
      // .inflight commit and a bunch of data files around.
      testTable.addInflightCommit(commitTime2).withBaseFilesInPartitions(partitionAndFileId2);

      client.rollback(commitTime2);
      assertFalse(testTable.commitExists(commitTime2));
      assertFalse(testTable.inflightCommitExists(commitTime2));
      assertFalse(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));

      // Let's rollback commit1, Check results
      client.rollback(commitTime1);
      assertFalse(testTable.commitExists(commitTime1));
      assertFalse(testTable.inflightCommitExists(commitTime1));
      assertFalse(testTable.baseFilesExist(partitionAndFileId1, commitTime1));
    }
  }

  private static Stream<Arguments> testFailedRollbackCommitParams() {
    return Arrays.stream(new Boolean[][] {
        {true, true}, {true, false}, {false, true}, {false, false},
    }).map(Arguments::of);
  }

  /**
   * Test Cases for effects of rollbacking completed/inflight commits.
   */
  @ParameterizedTest
  @MethodSource("testFailedRollbackCommitParams")
  public void testFailedRollbackCommit(
      boolean enableMetadataTable, boolean instantToRollbackExists) throws Exception {
    // Let's create some commit files and base files
    final String p1 = "2016/05/01";
    final String p2 = "2016/05/02";
    final String p3 = "2016/05/06";
    final String commitTime1 = "20160501010101";
    final String commitTime2 = "20160502020601";
    final String commitTime3 = "20160506030611";
    Map<String, String> partitionAndFileId1 = new HashMap<String, String>() {
      {
        put(p1, "id11");
        put(p2, "id12");
        put(p3, "id13");
      }
    };
    Map<String, String> partitionAndFileId2 = new HashMap<String, String>() {
      {
        put(p1, "id21");
        put(p2, "id22");
        put(p3, "id23");
      }
    };
    Map<String, String> partitionAndFileId3 = new HashMap<String, String>() {
      {
        put(p1, "id31");
        put(p2, "id32");
        put(p3, "id33");
      }
    };

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withRollbackUsingMarkers(false)
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                // Column Stats Index is disabled, since these tests construct tables which are
                // not valid (empty commit metadata, invalid parquet files)
                .withMetadataIndexColumnStats(false)
                .enable(enableMetadataTable)
                .build()
        )
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    HoodieTestTable testTable = enableMetadataTable
        ? HoodieMetadataTestTable.of(metaClient, SparkHoodieBackedTableMetadataWriter.create(
        metaClient.getHadoopConf(), config, context))
        : HoodieTestTable.of(metaClient);

    testTable.withPartitionMetaFiles(p1, p2, p3)
        .addCommit(commitTime1)
        .withBaseFilesInPartitions(partitionAndFileId1)
        .addCommit(commitTime2)
        .withBaseFilesInPartitions(partitionAndFileId2)
        .addInflightCommit(commitTime3)
        .withBaseFilesInPartitions(partitionAndFileId3);

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {

      // Rollback commit3
      client.rollback(commitTime3);
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertFalse(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));

      metaClient.reloadActiveTimeline();
      List<HoodieInstant> rollbackInstants = metaClient.getActiveTimeline().getRollbackTimeline().getInstants().collect(Collectors.toList());
      assertEquals(rollbackInstants.size(), 1);
      HoodieInstant rollbackInstant = rollbackInstants.get(0);

      // delete rollback completed meta file and retry rollback.
      FileCreateUtils.deleteRollbackCommit(basePath, rollbackInstant.getTimestamp());

      if (instantToRollbackExists) {
        // recreate actual commit files if needed
        testTable.addInflightCommit(commitTime3).withBaseFilesInPartitions(partitionAndFileId3);
      }

      // retry rolling back the commit again.
      client.rollback(commitTime3);

      // verify there are no extra rollback instants
      metaClient.reloadActiveTimeline();
      rollbackInstants = metaClient.getActiveTimeline().getRollbackTimeline().getInstants().collect(Collectors.toList());
      assertEquals(rollbackInstants.size(), 1);
      assertEquals(rollbackInstants.get(0), rollbackInstant);

      final String commitTime4 = "20160507040601";
      final String commitTime5 = "20160507050611";

      // add inflight compaction then rolls it back
      testTable.addInflightCompaction(commitTime4, new HoodieCommitMetadata());
      HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();
      rollbackPlan.setRollbackRequests(Collections.emptyList());
      rollbackPlan.setInstantToRollback(new HoodieInstantInfo(commitTime4, HoodieTimeline.COMPACTION_ACTION));
      testTable.addRequestedRollback(commitTime5, rollbackPlan);

      // the compaction instants should be excluded
      metaClient.reloadActiveTimeline();
      assertEquals(0, client.getPendingRollbackInfos(metaClient).size());

      // verify there is no extra rollback instants
      client.rollback(commitTime4);

      metaClient.reloadActiveTimeline();
      rollbackInstants = metaClient.reloadActiveTimeline().getRollbackTimeline().getInstants().collect(Collectors.toList());
      assertEquals(2, rollbackInstants.size());
    }
  }

  /**
   * Test auto-rollback of commits which are in flight.
   */
  @Test
  public void testAutoRollbackInflightCommit() throws Exception {
    // Let's create some commit files and base files
    final String p1 = "2016/05/01";
    final String p2 = "2016/05/02";
    final String p3 = "2016/05/06";
    final String commitTime1 = "20160501010101";
    final String commitTime2 = "20160502020601";
    final String commitTime3 = "20160506030611";
    Map<String, String> partitionAndFileId1 = new HashMap<String, String>() {
      {
        put(p1, "id11");
        put(p2, "id12");
        put(p3, "id13");
      }
    };
    Map<String, String> partitionAndFileId2 = new HashMap<String, String>() {
      {
        put(p1, "id21");
        put(p2, "id22");
        put(p3, "id23");
      }
    };
    Map<String, String> partitionAndFileId3 = new HashMap<String, String>() {
      {
        put(p1, "id31");
        put(p2, "id32");
        put(p3, "id33");
      }
    };

    // Set Failed Writes rollback to LAZY
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build()).build();

    HoodieTableMetadataWriter metadataWriter = SparkHoodieBackedTableMetadataWriter.create(hadoopConf, config, context);
    HoodieTestTable testTable = HoodieMetadataTestTable.of(metaClient, metadataWriter);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap1 = new HashMap<>();
    partitionAndFileId1.forEach((k, v) -> partitionToFilesNameLengthMap1.put(k, Collections.singletonList(Pair.of(v, 100))));
    testTable.doWriteOperation(commitTime1, WriteOperationType.INSERT, Arrays.asList(p1, p2, p3), partitionToFilesNameLengthMap1,
        false, false);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap2 = new HashMap<>();
    partitionAndFileId2.forEach((k, v) -> partitionToFilesNameLengthMap2.put(k, Collections.singletonList(Pair.of(v, 200))));
    testTable.doWriteOperation(commitTime2, WriteOperationType.INSERT, Collections.emptyList(), partitionToFilesNameLengthMap2,
        false, true);

    Map<String, List<Pair<String, Integer>>> partitionToFilesNameLengthMap3 = new HashMap<>();
    partitionAndFileId3.forEach((k, v) -> partitionToFilesNameLengthMap3.put(k, Collections.singletonList(Pair.of(v, 300))));
    testTable.doWriteOperation(commitTime3, WriteOperationType.INSERT, Collections.emptyList(), partitionToFilesNameLengthMap3,
        false, true);

    final String commitTime4 = "20160506030621";
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      client.startCommitWithTime(commitTime4);
      // Check results, nothing changed
      assertTrue(testTable.commitExists(commitTime1));
      assertTrue(testTable.inflightCommitExists(commitTime2));
      assertTrue(testTable.inflightCommitExists(commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
    }

    // Set Failed Writes rollback to EAGER
    config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withRollbackUsingMarkers(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();
    final String commitTime5 = "20160506030631";
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      client.startCommitWithTime(commitTime5);
      assertTrue(testTable.commitExists(commitTime1));
      assertFalse(testTable.inflightCommitExists(commitTime2));
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));
      assertFalse(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertFalse(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
    }
  }

  private static Stream<Arguments> testRollbackWithRequestedRollbackPlanParams() {
    return Arrays.stream(new Boolean[][] {
        {true, true}, {true, false}, {false, true}, {false, false},
    }).map(Arguments::of);
  }

  @ParameterizedTest
  @MethodSource("testRollbackWithRequestedRollbackPlanParams")
  public void testRollbackWithRequestedRollbackPlan(boolean enableMetadataTable, boolean isRollbackPlanCorrupted) throws Exception {
    // Let's create some commit files and base files
    final String p1 = "2022/04/05";
    final String p2 = "2022/04/06";
    final String commitTime1 = "20220406010101002";
    final String commitTime2 = "20220406020601002";
    final String commitTime3 = "20220406030611002";
    final String rollbackInstantTime = "20220406040611002";
    Map<String, String> partitionAndFileId1 = new HashMap<String, String>() {
      {
        put(p1, "id11");
        put(p2, "id12");
      }
    };
    Map<String, String> partitionAndFileId2 = new HashMap<String, String>() {
      {
        put(p1, "id21");
        put(p2, "id22");
      }
    };
    Map<String, String> partitionAndFileId3 = new HashMap<String, String>() {
      {
        put(p1, "id31");
        put(p2, "id32");
      }
    };

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withRollbackUsingMarkers(false)
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                // Column Stats Index is disabled, since these tests construct tables which are
                // not valid (empty commit metadata, invalid parquet files)
                .withMetadataIndexColumnStats(false)
                .enable(enableMetadataTable)
                .build()
        )
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withFailedWritesCleaningPolicy(HoodieFailedWritesCleaningPolicy.LAZY).build())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    HoodieTestTable testTable = enableMetadataTable
        ? HoodieMetadataTestTable.of(metaClient, SparkHoodieBackedTableMetadataWriter.create(
        metaClient.getHadoopConf(), config, context))
        : HoodieTestTable.of(metaClient);

    testTable.withPartitionMetaFiles(p1, p2)
        .addCommit(commitTime1)
        .withBaseFilesInPartitions(partitionAndFileId1)
        .addCommit(commitTime2)
        .withBaseFilesInPartitions(partitionAndFileId2)
        .addInflightCommit(commitTime3)
        .withBaseFilesInPartitions(partitionAndFileId3);

    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      if (isRollbackPlanCorrupted) {
        // Add a corrupted requested rollback plan
        FileCreateUtils.createRequestedRollbackFile(metaClient.getBasePath(), rollbackInstantTime, new byte[] {0, 1, 2});
      } else {
        // Add a valid requested rollback plan to roll back commitTime3
        HoodieRollbackPlan rollbackPlan = new HoodieRollbackPlan();
        List<HoodieRollbackRequest> rollbackRequestList = partitionAndFileId3.keySet().stream()
            .map(partition -> new HoodieRollbackRequest(partition, EMPTY_STRING, EMPTY_STRING,
                Collections.singletonList(metaClient.getBasePath() + "/" + partition + "/"
                    + FileCreateUtils.baseFileName(commitTime3, partitionAndFileId3.get(p1))),
                Collections.emptyMap()))
            .collect(Collectors.toList());
        rollbackPlan.setRollbackRequests(rollbackRequestList);
        rollbackPlan.setInstantToRollback(new HoodieInstantInfo(commitTime3, HoodieTimeline.COMMIT_ACTION));
        FileCreateUtils.createRequestedRollbackFile(metaClient.getBasePath(), rollbackInstantTime, rollbackPlan);
      }

      // Rollback commit3
      client.rollback(commitTime3);
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertFalse(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));

      metaClient.reloadActiveTimeline();
      List<HoodieInstant> rollbackInstants = metaClient.getActiveTimeline().getRollbackTimeline().getInstants().collect(Collectors.toList());
      // Corrupted requested rollback plan should be deleted before scheduling a new one
      assertEquals(rollbackInstants.size(), 1);
      HoodieInstant rollbackInstant = rollbackInstants.get(0);
      assertTrue(rollbackInstant.isCompleted());

      if (isRollbackPlanCorrupted) {
        // Should create a new rollback instant
        assertNotEquals(rollbackInstantTime, rollbackInstant.getTimestamp());
      } else {
        // Should reuse the rollback instant
        assertEquals(rollbackInstantTime, rollbackInstant.getTimestamp());
      }
    }
  }
}
