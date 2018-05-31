/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCleaningPolicy;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieIndexConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieRollbackException;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.File;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

/**
 * Test Cases for rollback of snapshots and commits
 */
public class TestClientRollback extends TestHoodieClientBase {

  /**
   * Test case for rollback-savepoint interaction
   */
  @Test
  public void testSavepointAndRollback() throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder().withCompactionConfig(
        HoodieCompactionConfig.newBuilder().withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS).retainCommits(1)
            .build()).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
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
    List<String> partitionPaths = FSUtils.getAllPartitionPaths(fs, cfg.getBasePath(),
        getConfig().shouldAssumeDatePartitioning());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
    final TableFileSystemView.ReadOptimizedView view1 = table.getROFileSystemView();

    List<HoodieDataFile> dataFiles = partitionPaths.stream().flatMap(s -> {
      return view1.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("003"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 003 should be present", 3, dataFiles.size());

    dataFiles = partitionPaths.stream().flatMap(s -> {
      return view1.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("002"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 002 should be present", 3, dataFiles.size());

    /**
     * Write 4 (updates)
     */
    newCommitTime = "004";
    client.startCommitWithTime(newCommitTime);

    records = dataGen.generateUpdates(newCommitTime, records);
    statuses = client.upsert(jsc.parallelize(records, 1), newCommitTime).collect();
    // Verify there are no errors
    assertNoWriteErrors(statuses);

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
    final TableFileSystemView.ReadOptimizedView view2 = table.getROFileSystemView();

    dataFiles = partitionPaths.stream().flatMap(s -> {
      return view2.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("004"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 004 should be present", 3, dataFiles.size());

    // rolling back to a non existent savepoint must not succeed
    try {
      client.rollbackToSavepoint("001");
      fail("Rolling back to non-existent savepoint should not be allowed");
    } catch (HoodieRollbackException e) {
      // this is good
    }

    // rollback to savepoint 002
    HoodieInstant savepoint = table.getCompletedSavepointTimeline().getInstants().findFirst().get();
    client.rollbackToSavepoint(savepoint.getTimestamp());

    metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    table = HoodieTable.getHoodieTable(metaClient, getConfig(), jsc);
    final TableFileSystemView.ReadOptimizedView view3 = table.getROFileSystemView();
    dataFiles = partitionPaths.stream().flatMap(s -> {
      return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("002"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 002 be available", 3, dataFiles.size());

    dataFiles = partitionPaths.stream().flatMap(s -> {
      return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("003"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 003 should be rolled back", 0, dataFiles.size());

    dataFiles = partitionPaths.stream().flatMap(s -> {
      return view3.getAllDataFiles(s).filter(f -> f.getCommitTime().equals("004"));
    }).collect(Collectors.toList());
    assertEquals("The data files for commit 004 should be rolled back", 0, dataFiles.size());
  }

  /**
   * Test Cases for effects of rollbacking completed/inflight commits
   */
  @Test
  public void testRollbackCommit() throws Exception {
    // Let's create some commit files and parquet files
    String commitTime1 = "20160501010101";
    String commitTime2 = "20160502020601";
    String commitTime3 = "20160506030611";
    new File(basePath + "/.hoodie").mkdirs();
    HoodieTestDataGenerator
        .writePartitionMetadata(fs, new String[]{"2016/05/01", "2016/05/02", "2016/05/06"}, basePath);

    // Only first two have commit files
    HoodieTestUtils.createCommitFiles(basePath, commitTime1, commitTime2);
    // Third one has a .inflight intermediate commit file
    HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);

    // Make commit1
    String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
    String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
    String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

    // Make commit2
    String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
    String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
    String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

    // Make commit3
    String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
    String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
    String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    HoodieWriteClient client = new HoodieWriteClient(jsc, config, false);

    // Rollback commit 1 (this should fail, since commit2 is still around)
    try {
      client.rollback(commitTime1);
      assertTrue("Should have thrown an exception ", false);
    } catch (HoodieRollbackException hrbe) {
      // should get here
    }

    // Rollback commit3
    client.rollback(commitTime3);
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));

    // simulate partial failure, where .inflight was not deleted, but data files were.
    HoodieTestUtils.createInflightCommitFiles(basePath, commitTime3);
    client.rollback(commitTime3);
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));

    // Rollback commit2
    client.rollback(commitTime2);
    assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));

    // simulate partial failure, where only .commit => .inflight renaming succeeded, leaving a
    // .inflight commit and a bunch of data files around.
    HoodieTestUtils.createInflightCommitFiles(basePath, commitTime2);
    file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
    file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
    file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

    client.rollback(commitTime2);
    assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime2));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));

    // Let's rollback commit1, Check results
    client.rollback(commitTime1);
    assertFalse(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime1));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
  }

  /**
   * Test auto-rollback of commits which are in flight
   */
  @Test
  public void testAutoRollbackInflightCommit() throws Exception {
    // Let's create some commit files and parquet files
    String commitTime1 = "20160501010101";
    String commitTime2 = "20160502020601";
    String commitTime3 = "20160506030611";
    new File(basePath + "/.hoodie").mkdirs();
    HoodieTestDataGenerator
        .writePartitionMetadata(fs, new String[]{"2016/05/01", "2016/05/02", "2016/05/06"}, basePath);

    // One good commit
    HoodieTestUtils.createCommitFiles(basePath, commitTime1);
    // Two inflight commits
    HoodieTestUtils.createInflightCommitFiles(basePath, commitTime2, commitTime3);

    // Make commit1
    String file11 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime1, "id11");
    String file12 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime1, "id12");
    String file13 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime1, "id13");

    // Make commit2
    String file21 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime2, "id21");
    String file22 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime2, "id22");
    String file23 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime2, "id23");

    // Make commit3
    String file31 = HoodieTestUtils.createDataFile(basePath, "2016/05/01", commitTime3, "id31");
    String file32 = HoodieTestUtils.createDataFile(basePath, "2016/05/02", commitTime3, "id32");
    String file33 = HoodieTestUtils.createDataFile(basePath, "2016/05/06", commitTime3, "id33");

    // Turn auto rollback off
    HoodieWriteConfig config = HoodieWriteConfig.newBuilder().withPath(basePath).withIndexConfig(
        HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build()).build();

    new HoodieWriteClient(jsc, config, false);

    // Check results, nothing changed
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
    assertTrue(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
    assertTrue(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));

    // Turn auto rollback on
    new HoodieWriteClient(jsc, config, true).startCommit();
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, commitTime1));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime2));
    assertFalse(HoodieTestUtils.doesInflightExist(basePath, commitTime3));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime3, file31)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime3, file32)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime3, file33));
    assertFalse(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime2, file21)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime2, file22)
        || HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime2, file23));
    assertTrue(HoodieTestUtils.doesDataFileExist(basePath, "2016/05/01", commitTime1, file11)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/02", commitTime1, file12)
        && HoodieTestUtils.doesDataFileExist(basePath, "2016/05/06", commitTime1, file13));
  }
}
