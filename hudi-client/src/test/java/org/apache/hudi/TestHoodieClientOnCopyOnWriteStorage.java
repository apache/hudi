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

package org.apache.hudi;

import org.apache.hudi.common.HoodieClientTestUtils;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDataFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieTestUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.TableFileSystemView.ReadOptimizedView;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.ConsistencyGuardConfig;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieStorageConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.table.HoodieTable;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.HoodieTestDataGenerator.NULL_SCHEMA;
import static org.apache.hudi.common.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.apache.hudi.common.util.ParquetUtils.readRowKeysFromParquet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestHoodieClientOnCopyOnWriteStorage extends TestHoodieClientBase {

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testAutoCommitOnInsert() throws Exception {
    testAutoCommit(HoodieWriteClient::insert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API.
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit(HoodieWriteClient::insertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testAutoCommitOnUpsert() throws Exception {
    testAutoCommit(HoodieWriteClient::upsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API.
   */
  @Test
  public void testAutoCommitOnUpsertPrepped() throws Exception {
    testAutoCommit(HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testAutoCommitOnBulkInsert() throws Exception {
    testAutoCommit(HoodieWriteClient::bulkInsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API.
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, commitTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, commitTime,
        Option.empty()), true);
  }

  /**
   * Test auto-commit by applying write function.
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  private void testAutoCommit(Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      boolean isPrepped) throws Exception {
    // Set autoCommit false
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    try (HoodieWriteClient client = getHoodieWriteClient(cfg);) {

      String prevCommitTime = "000";
      String newCommitTime = "001";
      int numRecords = 200;
      JavaRDD<WriteStatus> result = insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, writeFn,
          isPrepped, false, numRecords);

      assertFalse("If Autocommit is false, then commit should not be made automatically",
          HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
      assertTrue("Commit should succeed", client.commit(newCommitTime, result));
      assertTrue("After explicit commit, commit file should be created",
          HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
    }
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API.
   */
  @Test
  public void testDeduplicationOnInsert() throws Exception {
    testDeduplication(HoodieWriteClient::insert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API.
   */
  @Test
  public void testDeduplicationOnBulkInsert() throws Exception {
    testDeduplication(HoodieWriteClient::bulkInsert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API.
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication(HoodieWriteClient::upsert);
  }

  /**
   * Test Deduplication Logic for write function.
   *
   * @param writeFn One of HoddieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  private void testDeduplication(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws Exception {
    String newCommitTime = "001";

    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord recordOne =
        new HoodieRecord(keyOne, HoodieTestDataGenerator.generateRandomValue(keyOne, newCommitTime));

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
        new HoodieRecord(keyTwo, HoodieTestDataGenerator.generateRandomValue(keyTwo, newCommitTime));

    // Same key and partition as keyTwo
    HoodieRecord recordThree =
        new HoodieRecord(keyTwo, HoodieTestDataGenerator.generateRandomValue(keyTwo, newCommitTime));

    JavaRDD<HoodieRecord> records = jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);

    // dedup should be done based on recordKey only
    HoodieWriteClient clientWithDummyGlobalIndex = getWriteClientWithDummyIndex(true);
    List<HoodieRecord> dedupedRecs = clientWithDummyGlobalIndex.deduplicateRecords(records, 1).collect();
    assertEquals(1, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // dedup should be done based on both recordKey and partitionPath
    HoodieWriteClient clientWithDummyNonGlobalIndex = getWriteClientWithDummyIndex(false);
    dedupedRecs = clientWithDummyNonGlobalIndex.deduplicateRecords(records, 1).collect();
    assertEquals(2, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // Perform write-action and check
    try (HoodieWriteClient client = getHoodieWriteClient(getConfigBuilder().combineInput(true, true).build(), false);) {
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, records, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNodupesWithinPartition(statuses.stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Build a test Hoodie WriteClient with dummy index to configure isGlobal flag.
   *
   * @param isGlobal Flag to control HoodieIndex.isGlobal
   * @return Hoodie Write Client
   * @throws Exception in case of error
   */
  private HoodieWriteClient getWriteClientWithDummyIndex(final boolean isGlobal) throws Exception {
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(isGlobal);
    return getHoodieWriteClient(getConfigBuilder().build(), false, index);
  }

  /**
   * Test Upsert API.
   */
  @Test
  public void testUpserts() throws Exception {
    testUpsertsInternal(getConfig(), HoodieWriteClient::upsert, false);
  }

  /**
   * Test UpsertPrepped API.
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal(getConfig(), HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test one of HoodieWriteClient upsert(Prepped) APIs.
   *
   * @param hoodieWriteConfig Write Config
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  private void testUpsertsInternal(HoodieWriteConfig hoodieWriteConfig,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPrepped)
      throws Exception {
    HoodieWriteClient client = getHoodieWriteClient(hoodieWriteConfig, false);

    // Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig, client, newCommitTime, initCommitTime, numRecords, HoodieWriteClient::insert,
        isPrepped, true, numRecords);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        Option.of(Arrays.asList(commitTimeBetweenPrevAndNew)), initCommitTime, numRecords, writeFn, isPrepped, true,
        numRecords, 200, 2);

    // Delete 1
    prevCommitTime = newCommitTime;
    newCommitTime = "005";
    numRecords = 50;

    deleteBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, HoodieWriteClient::delete, isPrepped, true,
        0, 150);
  }

  /**
   * Tesst deletion of records.
   */
  @Test
  public void testDeletes() throws Exception {
    HoodieWriteClient client = getHoodieWriteClient(getConfig(), false);

    /**
     * Write 1 (inserts and deletes) Write actual 200 insert records and ignore 100 delete records
     */
    String initCommitTime = "000";
    String newCommitTime = "001";

    final List<HoodieRecord> recordsInFirstBatch = new ArrayList<>();
    Function2<List<HoodieRecord>, String, Integer> recordGenFunction =
        (String commitTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(commitTime, 200);
          List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(commitTime, 100);

          recordsInFirstBatch.addAll(fewRecordsForInsert);
          recordsInFirstBatch.addAll(fewRecordsForDelete);
          return recordsInFirstBatch;
        };
    writeBatch(client, newCommitTime, initCommitTime, Option.empty(), initCommitTime,
        // unused as genFn uses hard-coded number of inserts/updates/deletes
        -1, recordGenFunction, HoodieWriteClient::upsert, true, 200, 200, 1);

    /**
     * Write 2 (deletes+writes).
     */
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    final List<HoodieRecord> recordsInSecondBatch = new ArrayList<>();

    recordGenFunction = (String commitTime, Integer numRecordsInThisCommit) -> {
      List<HoodieRecord> fewRecordsForDelete = recordsInFirstBatch.subList(0, 50);
      List<HoodieRecord> fewRecordsForUpdate = recordsInFirstBatch.subList(50, 100);
      recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
      recordsInSecondBatch.addAll(fewRecordsForUpdate);
      return recordsInSecondBatch;
    };
    writeBatch(client, newCommitTime, prevCommitTime, Option.empty(), initCommitTime, 100, recordGenFunction,
        HoodieWriteClient::upsert, true, 50, 150, 2);
  }

  /**
   * Test scenario of new file-group getting added during upsert().
   */
  @Test
  public void testSmallInsertHandlingForUpserts() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);

    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals("Just 1 file needs to be added.", 1, statuses.size());
    String file1 = statuses.get(0).getFileId();
    Assert.assertEquals("file should contain 100 records",
        readRowKeysFromParquet(jsc.hadoopConfiguration(), new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(),
        100);

    // Update + Inserts such that they just expand file1
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
    List<HoodieRecord> insertsAndUpdates2 = new ArrayList<>();
    insertsAndUpdates2.addAll(inserts2);
    insertsAndUpdates2.addAll(dataGen.generateUpdates(commitTime2, inserts1));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD2 = jsc.parallelize(insertsAndUpdates2, 1);
    statuses = client.upsert(insertAndUpdatesRDD2, commitTime2).collect();
    assertNoWriteErrors(statuses);

    assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
    assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
    assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals("file should contain 140 records", readRowKeysFromParquet(jsc.hadoopConfiguration(), newFile).size(),
        140);

    List<GenericRecord> records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertEquals("only expect commit2", commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
      assertTrue("key expected to be part of commit2", keys2.contains(recordKey) || keys1.contains(recordKey));
    }

    // update + inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    client.startCommitWithTime(commitTime3);
    List<HoodieRecord> insertsAndUpdates3 = dataGen.generateInserts(commitTime3, 200);
    Set<String> keys3 = HoodieClientTestUtils.getRecordKeys(insertsAndUpdates3);
    List<HoodieRecord> updates3 = dataGen.generateUpdates(commitTime3, inserts2);
    insertsAndUpdates3.addAll(updates3);

    JavaRDD<HoodieRecord> insertAndUpdatesRDD3 = jsc.parallelize(insertsAndUpdates3, 1);
    statuses = client.upsert(insertAndUpdatesRDD3, commitTime3).collect();
    assertNoWriteErrors(statuses);

    assertEquals("2 files needs to be committed.", 2, statuses.size());
    HoodieTableMetaClient metadata = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);

    HoodieTable table = getHoodieTable(metadata, config);
    ReadOptimizedView fileSystemView = table.getROFileSystemView();
    List<HoodieDataFile> files =
        fileSystemView.getLatestDataFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    int numTotalInsertsInCommit3 = 0;
    int numTotalUpdatesInCommit3 = 0;
    for (HoodieDataFile file : files) {
      if (file.getFileName().contains(file1)) {
        assertEquals("Existing file should be expanded", commitTime3, file.getCommitTime());
        records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), new Path(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          String recordCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
          if (recordCommitTime.equals(commitTime3)) {
            if (keys2.contains(recordKey)) {
              keys2.remove(recordKey);
              numTotalUpdatesInCommit3++;
            } else {
              numTotalInsertsInCommit3++;
            }
          }
        }
        assertEquals("All keys added in commit 2 must be updated in commit3 correctly", 0, keys2.size());
      } else {
        assertEquals("New file must be written for commit 3", commitTime3, file.getCommitTime());
        records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), new Path(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          assertEquals("only expect commit3", commitTime3,
              record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
          assertTrue("key expected to be part of commit3", keys3.contains(recordKey));
        }
        numTotalInsertsInCommit3 += records.size();
      }
    }
    assertEquals("Total updates in commit3 must add up", inserts2.size(), numTotalUpdatesInCommit3);
    assertEquals("Total inserts in commit3 must add up", keys3.size(), numTotalInsertsInCommit3);
  }

  /**
   * Test scenario of new file-group getting added during insert().
   */
  @Test
  public void testSmallInsertHandlingForInserts() throws Exception {

    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});
    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.insert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);
    assertPartitionMetadata(new String[]{testPartitionPath}, fs);

    assertEquals("Just 1 file needs to be added.", 1, statuses.size());
    String file1 = statuses.get(0).getFileId();
    assertEquals("file should contain 100 records",
        readRowKeysFromParquet(jsc.hadoopConfiguration(), new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(),
        100);

    // Second, set of Inserts should just expand file1
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
    JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(inserts2, 1);
    statuses = client.insert(insertRecordsRDD2, commitTime2).collect();
    assertNoWriteErrors(statuses);

    assertEquals("Just 1 file needs to be updated.", 1, statuses.size());
    assertEquals("Existing file should be expanded", file1, statuses.get(0).getFileId());
    assertEquals("Existing file should be expanded", commitTime1, statuses.get(0).getStat().getPrevCommit());
    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals("file should contain 140 records", readRowKeysFromParquet(jsc.hadoopConfiguration(), newFile).size(),
        140);

    List<GenericRecord> records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
      assertTrue("Record expected to be part of commit 1 or commit2",
          commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime));
      assertTrue("key expected to be part of commit 1 or commit2",
          keys2.contains(recordKey) || keys1.contains(recordKey));
    }

    // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    client.startCommitWithTime(commitTime3);
    List<HoodieRecord> insert3 = dataGen.generateInserts(commitTime3, 200);
    JavaRDD<HoodieRecord> insertRecordsRDD3 = jsc.parallelize(insert3, 1);
    statuses = client.insert(insertRecordsRDD3, commitTime3).collect();
    assertNoWriteErrors(statuses);
    assertEquals("2 files needs to be committed.", 2, statuses.size());

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = getHoodieTable(metaClient, config);
    List<HoodieDataFile> files = table.getROFileSystemView()
        .getLatestDataFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    assertEquals("Total of 2 valid data files", 2, files.size());

    int totalInserts = 0;
    for (HoodieDataFile file : files) {
      assertEquals("All files must be at commit 3", commitTime3, file.getCommitTime());
      records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), new Path(file.getPath()));
      totalInserts += records.size();
    }
    assertEquals("Total number of records must add up", totalInserts,
        inserts1.size() + inserts2.size() + insert3.size());
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithDeleteApi() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    List<String> keysSoFar = new ArrayList<>();
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
    keysSoFar.addAll(keys1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals("Just 1 file needs to be added.", 1, statuses.size());
    String file1 = statuses.get(0).getFileId();
    Assert.assertEquals("file should contain 100 records",
        readRowKeysFromParquet(jsc.hadoopConfiguration(), new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(),
        100);

    // Delete 20 among 100 inserted
    testDeletes(client, inserts1, 20, file1, "002", 80, keysSoFar);

    // Insert and update 40 records
    Pair<Set<String>, List<HoodieRecord>> updateBatch2 = testUpdates("003", client, 40, 120);
    keysSoFar.addAll(updateBatch2.getLeft());

    // Delete 10 records among 40 updated
    testDeletes(client, updateBatch2.getRight(), 10, file1, "004", 110, keysSoFar);

    // do another batch of updates
    Pair<Set<String>, List<HoodieRecord>> updateBatch3 = testUpdates("005", client, 40, 150);
    keysSoFar.addAll(updateBatch3.getLeft());

    // delete non existent keys
    String commitTime6 = "006";
    client.startCommitWithTime(commitTime6);

    List<HoodieRecord> dummyInserts3 = dataGen.generateInserts(commitTime6, 20);
    List<HoodieKey> hoodieKeysToDelete3 = HoodieClientTestUtils
        .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(dummyInserts3), 20);
    JavaRDD<HoodieKey> deleteKeys3 = jsc.parallelize(hoodieKeysToDelete3, 1);
    statuses = client.delete(deleteKeys3, commitTime6).collect();
    assertNoWriteErrors(statuses);
    assertEquals("Just 0 write status for delete.", 0, statuses.size());

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals("Must contain " + 150 + " records", 150,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count());

    // delete another batch. previous delete commit should have persisted the schema. If not,
    // this will throw exception
    testDeletes(client, updateBatch3.getRight(), 10, file1, "007", 140, keysSoFar);
  }

  private Pair<Set<String>, List<HoodieRecord>> testUpdates(String commitTime, HoodieWriteClient client,
      int sizeToInsertAndUpdate, int expectedTotalRecords)
      throws IOException {
    client.startCommitWithTime(commitTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(commitTime, sizeToInsertAndUpdate);
    Set<String> keys = HoodieClientTestUtils.getRecordKeys(inserts);
    List<HoodieRecord> insertsAndUpdates = new ArrayList<>();
    insertsAndUpdates.addAll(inserts);
    insertsAndUpdates.addAll(dataGen.generateUpdates(commitTime, inserts));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD = jsc.parallelize(insertsAndUpdates, 1);
    List<WriteStatus> statuses = client.upsert(insertAndUpdatesRDD, commitTime).collect();
    assertNoWriteErrors(statuses);

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals("Must contain " + expectedTotalRecords + " records", expectedTotalRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count());
    return Pair.of(keys, inserts);
  }

  private void testDeletes(HoodieWriteClient client, List<HoodieRecord> previousRecords, int sizeToDelete,
      String existingFile, String commitTime, int exepctedRecords, List<String> keys) {
    client.startCommitWithTime(commitTime);

    List<HoodieKey> hoodieKeysToDelete = HoodieClientTestUtils
        .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(previousRecords), sizeToDelete);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    List<WriteStatus> statuses = client.delete(deleteKeys, commitTime).collect();

    assertNoWriteErrors(statuses);

    assertEquals("Just 1 file needs to be added.", 1, statuses.size());
    assertEquals("Existing file should be expanded", existingFile, statuses.get(0).getFileId());

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals("Must contain " + exepctedRecords + " records", exepctedRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count());

    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals("file should contain 110 records", readRowKeysFromParquet(jsc.hadoopConfiguration(), newFile).size(),
        exepctedRecords);

    List<GenericRecord> records = ParquetUtils.readAvroRecords(jsc.hadoopConfiguration(), newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertTrue("key expected to be part of " + commitTime, keys.contains(recordKey));
      assertFalse("Key deleted", hoodieKeysToDelete.contains(recordKey));
    }
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithoutInserts() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit, true); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // delete non existent keys
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> dummyInserts = dataGen.generateInserts(commitTime1, 20);
    List<HoodieKey> hoodieKeysToDelete = HoodieClientTestUtils
        .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(dummyInserts), 20);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    try {
      client.delete(deleteKeys, commitTime1).collect();
      fail("Should have thrown Exception");
    } catch (HoodieIOException e) {
      // ignore
    }
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    try (HoodieWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
      HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

      String commitTime = "000";
      client.startCommitWithTime(commitTime);

      List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, commitTime);

      assertTrue("Commit should succeed", client.commit(commitTime, result));
      assertTrue("After explicit commit, commit file should be created",
          HoodieTestUtils.doesCommitExist(basePath, commitTime));

      // Get parquet file paths from commit metadata
      String actionType = metaClient.getCommitActionType();
      HoodieInstant commitInstant = new HoodieInstant(false, actionType, commitTime);
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
          .fromBytes(commitTimeline.getInstantDetails(commitInstant).get(), HoodieCommitMetadata.class);
      String basePath = table.getMetaClient().getBasePath();
      Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

      // Read from commit file
      String filename = HoodieTestUtils.getCommitFilePath(basePath, commitTime);
      FileInputStream inputStream = new FileInputStream(filename);
      String everything = FileIOUtils.readAsUTFString(inputStream);
      HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything, HoodieCommitMetadata.class);
      HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
      inputStream.close();

      // Compare values in both to make sure they are equal.
      for (String pathName : paths.values()) {
        assertTrue(commitPathNames.contains(pathName));
      }
    }
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testRollingStatsInMetadata() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, cfg, jsc);

    String commitTime = "000";
    client.startCommitWithTime(commitTime);

    List<HoodieRecord> records = dataGen.generateInserts(commitTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, commitTime);

    assertTrue("Commit should succeed", client.commit(commitTime, result));
    assertTrue("After explicit commit, commit file should be created",
        HoodieTestUtils.doesCommitExist(basePath, commitTime));

    // Read from commit file
    String filename = HoodieTestUtils.getCommitFilePath(basePath, commitTime);
    FileInputStream inputStream = new FileInputStream(filename);
    String everything = FileIOUtils.readAsUTFString(inputStream);
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(everything.toString(), HoodieCommitMetadata.class);
    HoodieRollingStatMetadata rollingStatMetadata = HoodieCommitMetadata.fromJsonString(
        metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY),
        HoodieRollingStatMetadata.class);
    int inserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
      }
    }
    Assert.assertEquals(inserts, 200);

    // Update + Inserts such that they just expand file1
    commitTime = "001";
    client.startCommitWithTime(commitTime);

    records = dataGen.generateUpdates(commitTime, records);
    writeRecords = jsc.parallelize(records, 1);
    result = client.upsert(writeRecords, commitTime);

    assertTrue("Commit should succeed", client.commit(commitTime, result));
    assertTrue("After explicit commit, commit file should be created",
        HoodieTestUtils.doesCommitExist(basePath, commitTime));

    // Read from commit file
    filename = HoodieTestUtils.getCommitFilePath(basePath, commitTime);
    inputStream = new FileInputStream(filename);
    everything = FileIOUtils.readAsUTFString(inputStream);
    metadata = HoodieCommitMetadata.fromJsonString(everything.toString(), HoodieCommitMetadata.class);
    rollingStatMetadata = HoodieCommitMetadata.fromJsonString(
        metadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY),
        HoodieRollingStatMetadata.class);
    inserts = 0;
    int upserts = 0;
    for (Map.Entry<String, Map<String, HoodieRollingStat>> pstat : rollingStatMetadata.getPartitionToRollingStats()
        .entrySet()) {
      for (Map.Entry<String, HoodieRollingStat> stat : pstat.getValue().entrySet()) {
        inserts += stat.getValue().getInserts();
        upserts += stat.getValue().getUpserts();
      }
    }
    Assert.assertEquals(inserts, 200);
    Assert.assertEquals(upserts, 200);

  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @Test
  public void testConsistencyCheckDuringFinalize() throws Exception {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    String commitTime = "000";
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    Pair<Path, JavaRDD<WriteStatus>> result = testConsistencyCheck(metaClient, commitTime);

    // Delete orphan marker and commit should succeed
    metaClient.getFs().delete(result.getKey(), false);
    assertTrue("Commit should succeed", client.commit(commitTime, result.getRight()));
    assertTrue("After explicit commit, commit file should be created",
        HoodieTestUtils.doesCommitExist(basePath, commitTime));
    // Marker directory must be removed
    assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(commitTime))));
  }

  @Test
  public void testRollbackAfterConsistencyCheckFailure() throws Exception {
    String commitTime = "000";
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), basePath);
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    testConsistencyCheck(metaClient, commitTime);

    // Rollback of this commit should succeed
    client.rollback(commitTime);
    assertFalse("After explicit rollback, commit file should not be present",
        HoodieTestUtils.doesCommitExist(basePath, commitTime));
    // Marker directory must be removed after rollback
    assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(commitTime))));
  }

  private Pair<Path, JavaRDD<WriteStatus>> testConsistencyCheck(HoodieTableMetaClient metaClient, String commitTime)
      throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).build())
        .build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);

    client.startCommitWithTime(commitTime);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(commitTime, 200), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, commitTime);
    result.collect();

    // Create a dummy marker file to simulate the case that a marker file was created without data file.
    // This should fail the commit
    String partitionPath = Arrays
        .stream(fs.globStatus(new Path(String.format("%s/*/*/*/*", metaClient.getMarkerFolderPath(commitTime))),
            path -> path.toString().endsWith(HoodieTableMetaClient.MARKER_EXTN)))
        .limit(1).map(status -> status.getPath().getParent().toString()).collect(Collectors.toList()).get(0);
    Path markerFilePath = new Path(String.format("%s/%s", partitionPath,
        FSUtils.makeMarkerFile(commitTime, "1-0-1", UUID.randomUUID().toString())));
    metaClient.getFs().create(markerFilePath);
    logger.info("Created a dummy marker path=" + markerFilePath);

    try {
      client.commit(commitTime, result);
      fail("Commit should fail due to consistency check");
    } catch (HoodieCommitException cme) {
      assertTrue(cme.getCause() instanceof HoodieIOException);
    }
    return Pair.of(markerFilePath, result);
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize) {
    return getSmallInsertWriteConfig(insertSplitSize, false);
  }

  /**
   * Build Hoodie Write Config for small data file sizes.
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize, boolean useNullSchema) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(useNullSchema ? NULL_SCHEMA : TRIP_EXAMPLE_SCHEMA);
    return builder
        .withCompactionConfig(
            HoodieCompactionConfig.newBuilder().compactionSmallFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 15)
                .insertSplitSize(insertSplitSize).build()) // tolerate upto 15 records
        .withStorageConfig(
            HoodieStorageConfig.newBuilder().limitFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 20).build())
        .build();
  }
}
