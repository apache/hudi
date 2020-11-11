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

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
      HoodieTestDataGenerator.writePartitionMetadata(fs, HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS, basePath);

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
      List<String> partitionPaths =
          FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(), getConfig().shouldAssumeDatePartitioning());
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
   * Test Cases for effects of rollbacking completed/inflight commits.
   */
  @Test
  public void testRollbackCommit() throws Exception {
    // Let's create some commit files and parquet files
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
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit(commitTime1)
        .withBaseFilesInPartitions(partitionAndFileId1)
        .addCommit(commitTime2)
        .withBaseFilesInPartitions(partitionAndFileId2)
        .addInflightCommit(commitTime3)
        .withBaseFilesInPartitions(partitionAndFileId3);

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    try (SparkRDDWriteClient client = getHoodieWriteClient(config, false)) {

      // Rollback commit 1 (this should fail, since commit2 is still around)
      assertThrows(HoodieRollbackException.class, () -> {
        client.rollback(commitTime1);
      }, "Should have thrown an exception ");

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

  /**
   * Test auto-rollback of commits which are in flight.
   */
  @Test
  public void testAutoRollbackInflightCommit() throws Exception {
    // Let's create some commit files and parquet files
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
    HoodieTestTable testTable = HoodieTestTable.of(metaClient)
        .withPartitionMetaFiles(p1, p2, p3)
        .addCommit(commitTime1)
        .withBaseFilesInPartitions(partitionAndFileId1)
        .addInflightCommit(commitTime2)
        .withBaseFilesInPartitions(partitionAndFileId2)
        .addInflightCommit(commitTime3)
        .withBaseFilesInPartitions(partitionAndFileId3);

    // Turn auto rollback off
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    final String commitTime4 = "20160506030621";
    try (SparkRDDWriteClient client = getHoodieWriteClient(config, false)) {
      client.startCommitWithTime(commitTime4);
      // Check results, nothing changed
      assertTrue(testTable.commitExists(commitTime1));
      assertTrue(testTable.inflightCommitExists(commitTime2));
      assertTrue(testTable.inflightCommitExists(commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));
      assertTrue(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertTrue(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
    }

    // Turn auto rollback on
    final String commitTime5 = "20160506030631";
    try (SparkRDDWriteClient client = getHoodieWriteClient(config, true)) {
      client.startCommitWithTime(commitTime5);
      assertTrue(testTable.commitExists(commitTime1));
      assertFalse(testTable.inflightCommitExists(commitTime2));
      assertFalse(testTable.inflightCommitExists(commitTime3));
      assertTrue(testTable.baseFilesExist(partitionAndFileId1, commitTime1));
      assertFalse(testTable.baseFilesExist(partitionAndFileId2, commitTime2));
      assertFalse(testTable.baseFilesExist(partitionAndFileId3, commitTime3));
    }
  }
}
