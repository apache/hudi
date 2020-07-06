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

import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.testutils.HoodieTestUtils;
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
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.commit.WriteHelper;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieClientTestUtils;
import org.apache.hudi.testutils.HoodieTestDataGenerator;
import org.apache.hudi.testutils.TestRawTripPayload;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion.VERSION_0;
import static org.apache.hudi.common.util.ParquetUtils.readRowKeysFromParquet;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.apache.hudi.testutils.HoodieTestDataGenerator.NULL_SCHEMA;
import static org.apache.hudi.testutils.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TestHoodieClientOnCopyOnWriteStorage extends HoodieClientTestBase {

  private static final Logger LOG = LogManager.getLogger(TestHoodieClientOnCopyOnWriteStorage.class);

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
    testAutoCommit((writeClient, recordRDD, instantTime) -> writeClient.bulkInsertPreppedRecords(recordRDD, instantTime,
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

      assertFalse(HoodieTestUtils.doesCommitExist(basePath, newCommitTime),
          "If Autocommit is false, then commit should not be made automatically");
      assertTrue(client.commit(newCommitTime, result), "Commit should succeed");
      assertTrue(HoodieTestUtils.doesCommitExist(basePath, newCommitTime),
          "After explicit commit, commit file should be created");
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
    HoodieRecord<TestRawTripPayload> recordOne =
        new HoodieRecord(keyOne, dataGen.generateRandomValue(keyOne, newCommitTime));

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo =
        new HoodieRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    // Same key and partition as keyTwo
    HoodieRecord recordThree =
        new HoodieRecord(keyTwo, dataGen.generateRandomValue(keyTwo, newCommitTime));

    JavaRDD<HoodieRecord<TestRawTripPayload>> records =
        jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);

    // Global dedup should be done based on recordKey only
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(true);
    List<HoodieRecord<TestRawTripPayload>> dedupedRecs = WriteHelper.deduplicateRecords(records, index, 1).collect();
    assertEquals(1, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // non-Global dedup should be done based on both recordKey and partitionPath
    index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(false);
    dedupedRecs = WriteHelper.deduplicateRecords(records, index, 1).collect();
    assertEquals(2, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // Perform write-action and check
    JavaRDD<HoodieRecord> recordList = jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);
    try (HoodieWriteClient client = getHoodieWriteClient(getConfigBuilder().combineInput(true, true).build(), false);) {
      client.startCommitWithTime(newCommitTime);
      List<WriteStatus> statuses = writeFn.apply(client, recordList, newCommitTime).collect();
      assertNoWriteErrors(statuses);
      assertEquals(2, statuses.size());
      assertNodupesInPartition(statuses.stream().map(WriteStatus::getWrittenRecords).flatMap(Collection::stream)
          .collect(Collectors.toList()));
    }
  }

  /**
   * Assert that there is no duplicate key at the partition level.
   *
   * @param records List of Hoodie records
   */
  void assertNodupesInPartition(List<HoodieRecord> records) {
    Map<String, Set<String>> partitionToKeys = new HashMap<>();
    for (HoodieRecord r : records) {
      String key = r.getRecordKey();
      String partitionPath = r.getPartitionPath();
      if (!partitionToKeys.containsKey(partitionPath)) {
        partitionToKeys.put(partitionPath, new HashSet<>());
      }
      assertFalse(partitionToKeys.get(partitionPath).contains(key), "key " + key + " is duplicate within partition " + partitionPath);
      partitionToKeys.get(partitionPath).add(key);
    }
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
   * @param config  Write Config
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  private void testUpsertsInternal(HoodieWriteConfig config,
                                   Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn, boolean isPrepped)
      throws Exception {
    // Force using older timeline layout
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder().withProps(config.getProps()).withTimelineLayoutVersion(
        VERSION_0).build();
    HoodieTableMetaClient.initTableType(metaClient.getHadoopConf(), metaClient.getBasePath(), metaClient.getTableType(),
        metaClient.getTableConfig().getTableName(), metaClient.getArchivePath(),
        metaClient.getTableConfig().getPayloadClass(), VERSION_0);
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

    // Now simulate an upgrade and perform a restore operation
    HoodieWriteConfig newConfig = getConfigBuilder().withProps(config.getProps()).withTimelineLayoutVersion(
        TimelineLayoutVersion.CURR_VERSION).build();
    client = getHoodieWriteClient(newConfig, false);
    client.restoreToInstant("004");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(200, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + 200 + " records");

    // Perform Delete again on upgraded dataset.
    prevCommitTime = newCommitTime;
    newCommitTime = "006";
    numRecords = 50;

    deleteBatch(newConfig, client, newCommitTime, prevCommitTime,
        initCommitTime, numRecords, HoodieWriteClient::delete, isPrepped, true,
        0, 150);

    HoodieActiveTimeline activeTimeline = new HoodieActiveTimeline(metaClient, false);
    List<HoodieInstant> instants = activeTimeline.getCommitTimeline().getInstants().collect(Collectors.toList());
    assertEquals(5, instants.size());
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "001"),
        instants.get(0));
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "004"),
        instants.get(1));
    // New Format should have all states of instants
    assertEquals(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(2));
    assertEquals(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(3));
    assertEquals(new HoodieInstant(HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, "006"),
        instants.get(4));
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
        (String instantTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForInsert = dataGen.generateInserts(instantTime, 200);
          List<HoodieRecord> fewRecordsForDelete = dataGen.generateDeletes(instantTime, 100);

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

    recordGenFunction = (String instantTime, Integer numRecordsInThisCommit) -> {
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
   * Test update of a record to different partition with Global Index.
   */
  @Test
  public void testUpsertToDiffPartitionGlobalIndex() throws Exception {
    HoodieWriteClient client = getHoodieWriteClient(getConfig(IndexType.GLOBAL_BLOOM), false);
    /**
     * Write 1 (inserts and deletes) Write actual 200 insert records and ignore 100 delete records
     */
    String newCommitTime = "001";
    List<HoodieRecord> inserts1 = dataGen.generateInserts(newCommitTime, 100);

    // Write 1 (only inserts)
    client.startCommitWithTime(newCommitTime);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(inserts1, 1);

    JavaRDD<WriteStatus> result = client.insert(writeRecords, newCommitTime);
    List<WriteStatus> statuses = result.collect();
    assertNoWriteErrors(statuses);

    // check the partition metadata is written out
    assertPartitionMetadataForRecords(inserts1, fs);
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(100, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain 100 records");

    /**
     * Write 2. Updates with different partition
     */
    newCommitTime = "004";
    client.startCommitWithTime(newCommitTime);

    List<HoodieRecord> updates1 = dataGen.generateUpdatesWithDiffPartition(newCommitTime, inserts1);
    JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(updates1, 1);

    JavaRDD<WriteStatus> result1 = client.upsert(updateRecords, newCommitTime);
    List<WriteStatus> statuses1 = result1.collect();
    assertNoWriteErrors(statuses1);

    // check the partition metadata is written out
    assertPartitionMetadataForRecords(updates1, fs);
    // Check the entire dataset has all records still
    fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(100, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain 100 records");
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
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);

    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        readRowKeysFromParquet(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

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

    assertEquals(1, statuses.size(), "Just 1 file needs to be updated.");
    assertEquals(file1, statuses.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statuses.get(0).getStat().getPrevCommit(), "Existing file should be expanded");
    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(140, readRowKeysFromParquet(hadoopConf, newFile).size(),
        "file should contain 140 records");

    List<GenericRecord> records = ParquetUtils.readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertEquals(commitTime2, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(), "only expect commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey), "key expected to be part of commit2");
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

    assertEquals(2, statuses.size(), "2 files needs to be committed.");
    HoodieTableMetaClient metadata = new HoodieTableMetaClient(hadoopConf, basePath);

    HoodieTable table = getHoodieTable(metadata, config);
    BaseFileOnlyView fileSystemView = table.getBaseFileOnlyView();
    List<HoodieBaseFile> files =
        fileSystemView.getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    int numTotalInsertsInCommit3 = 0;
    int numTotalUpdatesInCommit3 = 0;
    for (HoodieBaseFile file : files) {
      if (file.getFileName().contains(file1)) {
        assertEquals(commitTime3, file.getCommitTime(), "Existing file should be expanded");
        records = ParquetUtils.readAvroRecords(hadoopConf, new Path(file.getPath()));
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
        assertEquals(0, keys2.size(), "All keys added in commit 2 must be updated in commit3 correctly");
      } else {
        assertEquals(commitTime3, file.getCommitTime(), "New file must be written for commit 3");
        records = ParquetUtils.readAvroRecords(hadoopConf, new Path(file.getPath()));
        for (GenericRecord record : records) {
          String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
          assertEquals(commitTime3, record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString(),
              "only expect commit3");
          assertTrue(keys3.contains(recordKey), "key expected to be part of commit3");
        }
        numTotalInsertsInCommit3 += records.size();
      }
    }
    assertEquals(numTotalUpdatesInCommit3, inserts2.size(), "Total updates in commit3 must add up");
    assertEquals(numTotalInsertsInCommit3, keys3.size(), "Total inserts in commit3 must add up");
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
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});
    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.insert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);
    assertPartitionMetadata(new String[] {testPartitionPath}, fs);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        readRowKeysFromParquet(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

    // Second, set of Inserts should just expand file1
    String commitTime2 = "002";
    client.startCommitWithTime(commitTime2);
    List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime2, 40);
    Set<String> keys2 = HoodieClientTestUtils.getRecordKeys(inserts2);
    JavaRDD<HoodieRecord> insertRecordsRDD2 = jsc.parallelize(inserts2, 1);
    statuses = client.insert(insertRecordsRDD2, commitTime2).collect();
    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be updated.");
    assertEquals(file1, statuses.get(0).getFileId(), "Existing file should be expanded");
    assertEquals(commitTime1, statuses.get(0).getStat().getPrevCommit(), "Existing file should be expanded");
    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(140, readRowKeysFromParquet(hadoopConf, newFile).size(),
        "file should contain 140 records");

    List<GenericRecord> records = ParquetUtils.readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      String recCommitTime = record.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString();
      assertTrue(commitTime1.equals(recCommitTime) || commitTime2.equals(recCommitTime),
          "Record expected to be part of commit 1 or commit2");
      assertTrue(keys2.contains(recordKey) || keys1.contains(recordKey),
          "key expected to be part of commit 1 or commit2");
    }

    // Lots of inserts such that file1 is updated and expanded, a new file2 is created.
    String commitTime3 = "003";
    client.startCommitWithTime(commitTime3);
    List<HoodieRecord> insert3 = dataGen.generateInserts(commitTime3, 200);
    JavaRDD<HoodieRecord> insertRecordsRDD3 = jsc.parallelize(insert3, 1);
    statuses = client.insert(insertRecordsRDD3, commitTime3).collect();
    assertNoWriteErrors(statuses);
    assertEquals(2, statuses.size(), "2 files needs to be committed.");

    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
    HoodieTable table = getHoodieTable(metaClient, config);
    List<HoodieBaseFile> files = table.getBaseFileOnlyView()
        .getLatestBaseFilesBeforeOrOn(testPartitionPath, commitTime3).collect(Collectors.toList());
    assertEquals(2, files.size(), "Total of 2 valid data files");

    int totalInserts = 0;
    for (HoodieBaseFile file : files) {
      assertEquals(commitTime3, file.getCommitTime(), "All files must be at commit 3");
      records = ParquetUtils.readAvroRecords(hadoopConf, new Path(file.getPath()));
      totalInserts += records.size();
    }
    assertEquals(totalInserts, inserts1.size() + inserts2.size() + insert3.size(),
        "Total number of records must add up");
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithDeleteApi() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // Inserts => will write file1
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);
    List<HoodieRecord> inserts1 = dataGen.generateInserts(commitTime1, insertSplitLimit); // this writes ~500kb
    Set<String> keys1 = HoodieClientTestUtils.getRecordKeys(inserts1);
    List<String> keysSoFar = new ArrayList<>(keys1);
    JavaRDD<HoodieRecord> insertRecordsRDD1 = jsc.parallelize(inserts1, 1);
    List<WriteStatus> statuses = client.upsert(insertRecordsRDD1, commitTime1).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    String file1 = statuses.get(0).getFileId();
    assertEquals(100,
        readRowKeysFromParquet(hadoopConf, new Path(basePath, statuses.get(0).getStat().getPath()))
            .size(), "file should contain 100 records");

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
    assertEquals(0, statuses.size(), "Just 0 write status for delete.");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(150,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + 150 + " records");

    // delete another batch. previous delete commit should have persisted the schema. If not,
    // this will throw exception
    testDeletes(client, updateBatch3.getRight(), 10, file1, "007", 140, keysSoFar);
  }

  private Pair<Set<String>, List<HoodieRecord>> testUpdates(String instantTime, HoodieWriteClient client,
                                                            int sizeToInsertAndUpdate, int expectedTotalRecords)
      throws IOException {
    client.startCommitWithTime(instantTime);
    List<HoodieRecord> inserts = dataGen.generateInserts(instantTime, sizeToInsertAndUpdate);
    Set<String> keys = HoodieClientTestUtils.getRecordKeys(inserts);
    List<HoodieRecord> insertsAndUpdates = new ArrayList<>();
    insertsAndUpdates.addAll(inserts);
    insertsAndUpdates.addAll(dataGen.generateUpdates(instantTime, inserts));

    JavaRDD<HoodieRecord> insertAndUpdatesRDD = jsc.parallelize(insertsAndUpdates, 1);
    List<WriteStatus> statuses = client.upsert(insertAndUpdatesRDD, instantTime).collect();
    assertNoWriteErrors(statuses);

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(expectedTotalRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + expectedTotalRecords + " records");
    return Pair.of(keys, inserts);
  }

  private void testDeletes(HoodieWriteClient client, List<HoodieRecord> previousRecords, int sizeToDelete,
                           String existingFile, String instantTime, int exepctedRecords, List<String> keys) {
    client.startCommitWithTime(instantTime);

    List<HoodieKey> hoodieKeysToDelete = HoodieClientTestUtils
        .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(previousRecords), sizeToDelete);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    List<WriteStatus> statuses = client.delete(deleteKeys, instantTime).collect();

    assertNoWriteErrors(statuses);

    assertEquals(1, statuses.size(), "Just 1 file needs to be added.");
    assertEquals(existingFile, statuses.get(0).getFileId(), "Existing file should be expanded");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(exepctedRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + exepctedRecords + " records");

    Path newFile = new Path(basePath, statuses.get(0).getStat().getPath());
    assertEquals(exepctedRecords,
        readRowKeysFromParquet(hadoopConf, newFile).size(),
        "file should contain 110 records");

    List<GenericRecord> records = ParquetUtils.readAvroRecords(hadoopConf, newFile);
    for (GenericRecord record : records) {
      String recordKey = record.get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
      assertTrue(keys.contains(recordKey), "key expected to be part of " + instantTime);
      assertFalse(hoodieKeysToDelete.contains(recordKey), "Key deleted");
    }
  }

  /**
   * Test delete with delete api.
   */
  @Test
  public void testDeletesWithoutInserts() {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit, true); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[] {testPartitionPath});

    HoodieWriteClient client = getHoodieWriteClient(config, false);

    // delete non existent keys
    String commitTime1 = "001";
    client.startCommitWithTime(commitTime1);

    List<HoodieRecord> dummyInserts = dataGen.generateInserts(commitTime1, 20);
    List<HoodieKey> hoodieKeysToDelete = HoodieClientTestUtils
        .getKeysToDelete(HoodieClientTestUtils.getHoodieKeys(dummyInserts), 20);
    JavaRDD<HoodieKey> deleteKeys = jsc.parallelize(hoodieKeysToDelete, 1);
    assertThrows(HoodieIOException.class, () -> {
      client.delete(deleteKeys, commitTime1).collect();
    }, "Should have thrown Exception");
  }

  /**
   * Test to ensure commit metadata points to valid files.
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    try (HoodieWriteClient client = getHoodieWriteClient(cfg);) {
      HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
      HoodieTable table = HoodieTable.create(metaClient, cfg, hadoopConf);

      String instantTime = "000";
      client.startCommitWithTime(instantTime);

      List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
      JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

      JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);

      assertTrue(client.commit(instantTime, result), "Commit should succeed");
      assertTrue(HoodieTestUtils.doesCommitExist(basePath, instantTime),
          "After explicit commit, commit file should be created");

      // Get parquet file paths from commit metadata
      String actionType = metaClient.getCommitActionType();
      HoodieInstant commitInstant = new HoodieInstant(false, actionType, instantTime);
      HoodieTimeline commitTimeline = metaClient.getCommitTimeline().filterCompletedInstants();
      HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
          .fromBytes(commitTimeline.getInstantDetails(commitInstant).get(), HoodieCommitMetadata.class);
      String basePath = table.getMetaClient().getBasePath();
      Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

      // Read from commit file
      String filename = HoodieTestUtils.getCommitFilePath(basePath, instantTime);
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
   * Test to ensure commit metadata points to valid files.10.
   */
  @Test
  public void testMetadataStatsOnCommit() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);

    String instantTime = "000";
    client.startCommitWithTime(instantTime);

    List<HoodieRecord> records = dataGen.generateInserts(instantTime, 200);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(records, 1);

    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);

    assertTrue(client.commit(instantTime, result), "Commit should succeed");
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, instantTime),
        "After explicit commit, commit file should be created");

    // Read from commit file
    String filename = HoodieTestUtils.getCommitFilePath(basePath, instantTime);
    FileInputStream inputStream = new FileInputStream(filename);
    String everything = FileIOUtils.readAsUTFString(inputStream);
    HoodieCommitMetadata metadata =
        HoodieCommitMetadata.fromJsonString(everything.toString(), HoodieCommitMetadata.class);
    int inserts = 0;
    for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : pstat.getValue()) {
        inserts += stat.getNumInserts();
      }
    }
    assertEquals(200, inserts);

    // Update + Inserts such that they just expand file1
    instantTime = "001";
    client.startCommitWithTime(instantTime);

    records = dataGen.generateUpdates(instantTime, records);
    writeRecords = jsc.parallelize(records, 1);
    result = client.upsert(writeRecords, instantTime);

    assertTrue(client.commit(instantTime, result), "Commit should succeed");
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, instantTime),
        "After explicit commit, commit file should be created");

    // Read from commit file
    filename = HoodieTestUtils.getCommitFilePath(basePath, instantTime);
    inputStream = new FileInputStream(filename);
    everything = FileIOUtils.readAsUTFString(inputStream);
    metadata = HoodieCommitMetadata.fromJsonString(everything.toString(), HoodieCommitMetadata.class);
    inserts = 0;
    int upserts = 0;
    for (Map.Entry<String, List<HoodieWriteStat>> pstat : metadata.getPartitionToWriteStats().entrySet()) {
      for (HoodieWriteStat stat : pstat.getValue()) {
        inserts += stat.getNumInserts();
        upserts += stat.getNumUpdateWrites();
      }
    }
    assertEquals(0, inserts);
    assertEquals(200, upserts);

  }

  /**
   * Tests behavior of committing only when consistency is verified.
   */
  @Test
  public void testConsistencyCheckDuringFinalize() throws Exception {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
    String instantTime = "000";
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    Pair<Path, JavaRDD<WriteStatus>> result = testConsistencyCheck(metaClient, instantTime);

    // Delete orphan marker and commit should succeed
    metaClient.getFs().delete(result.getKey(), false);
    assertTrue(client.commit(instantTime, result.getRight()), "Commit should succeed");
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, instantTime),
        "After explicit commit, commit file should be created");
    // Marker directory must be removed
    assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
  }

  @Test
  public void testRollbackAfterConsistencyCheckFailure() throws Exception {
    String instantTime = "000";
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(hadoopConf, basePath);
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    testConsistencyCheck(metaClient, instantTime);

    // Rollback of this commit should succeed
    client.rollback(instantTime);
    assertFalse(HoodieTestUtils.doesCommitExist(basePath, instantTime),
        "After explicit rollback, commit file should not be present");
    // Marker directory must be removed after rollback
    assertFalse(metaClient.getFs().exists(new Path(metaClient.getMarkerFolderPath(instantTime))));
  }

  private Pair<Path, JavaRDD<WriteStatus>> testConsistencyCheck(HoodieTableMetaClient metaClient, String instantTime)
      throws Exception {
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder().withConsistencyCheckEnabled(true)
            .withMaxConsistencyCheckIntervalMs(1).withInitialConsistencyCheckIntervalMs(1).build())
        .build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);

    client.startCommitWithTime(instantTime);
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(instantTime, 200), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, instantTime);
    result.collect();

    // Create a dummy marker file to simulate the case that a marker file was created without data file.
    // This should fail the commit
    String partitionPath = Arrays
        .stream(fs.globStatus(new Path(String.format("%s/*/*/*/*", metaClient.getMarkerFolderPath(instantTime))),
            path -> path.toString().endsWith(HoodieTableMetaClient.MARKER_EXTN)))
        .limit(1).map(status -> status.getPath().getParent().toString()).collect(Collectors.toList()).get(0);
    Path markerFilePath = new Path(String.format("%s/%s", partitionPath,
        FSUtils.makeMarkerFile(instantTime, "1-0-1", UUID.randomUUID().toString())));
    metaClient.getFs().create(markerFilePath);
    LOG.info("Created a dummy marker path=" + markerFilePath);

    Exception e = assertThrows(HoodieCommitException.class, () -> {
      client.commit(instantTime, result);
    }, "Commit should fail due to consistency check");
    assertTrue(e.getCause() instanceof HoodieIOException);
    return Pair.of(markerFilePath, result);
  }

  @Test
  public void testMultiOperationsPerCommit() throws IOException {
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false)
        .withAllowMultiWriteOnSameInstant(true)
        .build();
    HoodieWriteClient client = getHoodieWriteClient(cfg);
    String firstInstantTime = "0000";
    client.startCommitWithTime(firstInstantTime);
    int numRecords = 200;
    JavaRDD<HoodieRecord> writeRecords = jsc.parallelize(dataGen.generateInserts(firstInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> result = client.bulkInsert(writeRecords, firstInstantTime);
    assertTrue(client.commit(firstInstantTime, result), "Commit should succeed");
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, firstInstantTime),
        "After explicit commit, commit file should be created");

    // Check the entire dataset has all records still
    String[] fullPartitionPaths = new String[dataGen.getPartitionPaths().length];
    for (int i = 0; i < fullPartitionPaths.length; i++) {
      fullPartitionPaths[i] = String.format("%s/%s/*", basePath, dataGen.getPartitionPaths()[i]);
    }
    assertEquals(numRecords,
        HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + numRecords + " records");

    String nextInstantTime = "0001";
    client.startCommitWithTime(nextInstantTime);
    JavaRDD<HoodieRecord> updateRecords = jsc.parallelize(dataGen.generateUpdates(nextInstantTime, numRecords), 1);
    JavaRDD<HoodieRecord> insertRecords = jsc.parallelize(dataGen.generateInserts(nextInstantTime, numRecords), 1);
    JavaRDD<WriteStatus> inserts = client.bulkInsert(insertRecords, nextInstantTime);
    JavaRDD<WriteStatus> upserts = client.upsert(updateRecords, nextInstantTime);
    assertTrue(client.commit(nextInstantTime, inserts.union(upserts)), "Commit should succeed");
    assertTrue(HoodieTestUtils.doesCommitExist(basePath, firstInstantTime),
        "After explicit commit, commit file should be created");
    int totalRecords = 2 * numRecords;
    assertEquals(totalRecords, HoodieClientTestUtils.read(jsc, basePath, sqlContext, fs, fullPartitionPaths).count(),
        "Must contain " + totalRecords + " records");
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
            HoodieCompactionConfig.newBuilder()
                .compactionSmallFileSize(dataGen.getEstimatedFileSizeInBytes(150))
                .insertSplitSize(insertSplitSize).build())
        .withStorageConfig(
            HoodieStorageConfig.newBuilder()
                .limitFileSize(dataGen.getEstimatedFileSizeInBytes(200)).build())
        .build();
  }
}
