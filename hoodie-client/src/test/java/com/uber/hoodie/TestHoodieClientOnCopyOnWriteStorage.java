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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.uber.hoodie.common.HoodieClientTestUtils;
import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.ParquetUtils;
import com.uber.hoodie.config.HoodieCompactionConfig;
import com.uber.hoodie.config.HoodieStorageConfig;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.index.HoodieIndex;
import com.uber.hoodie.table.HoodieTable;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import scala.Option;

@SuppressWarnings("unchecked")
public class TestHoodieClientOnCopyOnWriteStorage extends TestHoodieClientBase {

  /**
   * Test Auto Commit behavior for HoodieWriteClient insert API
   */
  @Test
  public void testAutoCommitOnInsert() throws Exception {
    testAutoCommit(HoodieWriteClient::insert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient insertPrepped API
   */
  @Test
  public void testAutoCommitOnInsertPrepped() throws Exception {
    testAutoCommit(HoodieWriteClient::insertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert API
   */
  @Test
  public void testAutoCommitOnUpsert() throws Exception {
    testAutoCommit(HoodieWriteClient::upsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient upsert Prepped API
   */
  @Test
  public void testAutoCommitOnUpsertPrepped() throws Exception {
    testAutoCommit(HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert API
   */
  @Test
  public void testAutoCommitOnBulkInsert() throws Exception {
    testAutoCommit(HoodieWriteClient::bulkInsert, false);
  }

  /**
   * Test Auto Commit behavior for HoodieWriteClient bulk-insert prepped API
   */
  @Test
  public void testAutoCommitOnBulkInsertPrepped() throws Exception {
    testAutoCommit((writeClient, recordRDD, commitTime)
        -> writeClient.bulkInsertPreppedRecords(recordRDD, commitTime, Option.empty()), true);
  }

  /**
   * Test auto-commit by applying write function
   *
   * @param writeFn One of HoodieWriteClient Write API
   * @throws Exception in case of failure
   */
  private void testAutoCommit(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      boolean isPrepped) throws Exception {
    // Set autoCommit false
    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);

    String prevCommitTime = "000";
    String newCommitTime = "001";
    int numRecords = 200;
    JavaRDD<WriteStatus> result =
        insertFirstBatch(cfg, client, newCommitTime, prevCommitTime, numRecords, writeFn, isPrepped, false, numRecords);

    assertFalse("If Autocommit is false, then commit should not be made automatically",
        HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
    assertTrue("Commit should succeed", client.commit(newCommitTime, result));
    assertTrue("After explicit commit, commit file should be created",
        HoodieTestUtils.doesCommitExist(basePath, newCommitTime));
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient insert API
   */
  @Test
  public void testDeduplicationOnInsert() throws Exception {
    testDeduplication(HoodieWriteClient::insert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient bulk-insert API
   */
  @Test
  public void testDeduplicationOnBulkInsert() throws Exception {
    testDeduplication(HoodieWriteClient::bulkInsert);
  }

  /**
   * Test De-duplication behavior for HoodieWriteClient upsert API
   */
  @Test
  public void testDeduplicationOnUpsert() throws Exception {
    testDeduplication(HoodieWriteClient::upsert);
  }

  /**
   * Test Deduplication Logic for write function
   *
   * @param writeFn One of HoddieWriteClient non-prepped write APIs
   * @throws Exception in case of failure
   */
  private void testDeduplication(
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn) throws Exception {
    String newCommitTime = "001";

    String recordKey = UUID.randomUUID().toString();
    HoodieKey keyOne = new HoodieKey(recordKey, "2018-01-01");
    HoodieRecord recordOne = new HoodieRecord(keyOne,
        HoodieTestDataGenerator.generateRandomValue(keyOne, newCommitTime));

    HoodieKey keyTwo = new HoodieKey(recordKey, "2018-02-01");
    HoodieRecord recordTwo = new HoodieRecord(keyTwo,
        HoodieTestDataGenerator.generateRandomValue(keyTwo, newCommitTime));

    // Same key and partition as keyTwo
    HoodieRecord recordThree = new HoodieRecord(keyTwo,
        HoodieTestDataGenerator.generateRandomValue(keyTwo, newCommitTime));

    JavaRDD<HoodieRecord> records = jsc.parallelize(Arrays.asList(recordOne, recordTwo, recordThree), 1);

    // dedup should be done based on recordKey only
    HoodieWriteClient clientWithDummyGlobalIndex = getWriteClientWithDummyIndex(true);
    List<HoodieRecord> dedupedRecs = clientWithDummyGlobalIndex.deduplicateRecords(records, 1).collect();
    assertEquals(1, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // dedup should be done based on both recordKey and partitionPath
    HoodieWriteClient clientWithDummyNonGlobalIndex = getWriteClientWithDummyIndex(false);
    dedupedRecs =
        clientWithDummyNonGlobalIndex.deduplicateRecords(records, 1).collect();
    assertEquals(2, dedupedRecs.size());
    assertNodupesWithinPartition(dedupedRecs);

    // Perform write-action and check
    HoodieWriteClient client = new HoodieWriteClient(jsc,
        getConfigBuilder().combineInput(true, true).build());
    client.startCommitWithTime(newCommitTime);
    List<WriteStatus> statuses = writeFn.apply(client, records, newCommitTime).collect();
    assertNoWriteErrors(statuses);
    assertEquals(2, statuses.size());
    assertNodupesWithinPartition(
        statuses.stream().map(WriteStatus::getWrittenRecords)
            .flatMap(Collection::stream).collect(Collectors.toList()));
  }

  /**
   * Build a test Hoodie WriteClient with dummy index to configure isGlobal flag
   *
   * @param isGlobal Flag to control HoodieIndex.isGlobal
   * @return Hoodie Write Client
   * @throws Exception in case of error
   */
  private HoodieWriteClient getWriteClientWithDummyIndex(final boolean isGlobal) throws Exception {
    HoodieIndex index = mock(HoodieIndex.class);
    when(index.isGlobal()).thenReturn(isGlobal);
    return new HoodieWriteClient(jsc, getConfigBuilder().build(), false, index);
  }

  /**
   * Test Upsert API
   */
  @Test
  public void testUpserts() throws Exception {
    testUpsertsInternal(getConfig(),
        HoodieWriteClient::upsert, false);
  }

  /**
   * Test Upsert API using temporary folders.
   */
  @Test
  public void testUpsertsWithFinalizeWrite() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withUseTempFolderCopyOnWriteForCreate(true)
        .withUseTempFolderCopyOnWriteForMerge(true)
        .build();
    testUpsertsInternal(hoodieWriteConfig,
        HoodieWriteClient::upsert, false);
  }

  /**
   * Test UpsertPrepped API
   */
  @Test
  public void testUpsertsPrepped() throws Exception {
    testUpsertsInternal(getConfig(),
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test UpsertPrepped API using temporary folders.
   */
  @Test
  public void testUpsertsPreppedWithFinalizeWrite() throws Exception {
    HoodieWriteConfig hoodieWriteConfig = getConfigBuilder()
        .withUseTempFolderCopyOnWriteForCreate(true)
        .withUseTempFolderCopyOnWriteForMerge(true)
        .build();
    testUpsertsInternal(hoodieWriteConfig,
        HoodieWriteClient::upsertPreppedRecords, true);
  }

  /**
   * Test one of HoodieWriteClient upsert(Prepped) APIs
   *
   * @param hoodieWriteConfig Write Config
   * @param writeFn One of Hoodie Write Function API
   * @throws Exception in case of error
   */
  private void testUpsertsInternal(HoodieWriteConfig hoodieWriteConfig,
      Function3<JavaRDD<WriteStatus>, HoodieWriteClient, JavaRDD<HoodieRecord>, String> writeFn,
      boolean isPrepped) throws Exception {
    HoodieWriteClient client = new HoodieWriteClient(jsc, hoodieWriteConfig);

    //Write 1 (only inserts)
    String newCommitTime = "001";
    String initCommitTime = "000";
    int numRecords = 200;
    insertFirstBatch(hoodieWriteConfig,
        client, newCommitTime, initCommitTime, numRecords, HoodieWriteClient::insert, isPrepped, true, numRecords);

    // Write 2 (updates)
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    numRecords = 100;
    String commitTimeBetweenPrevAndNew = "002";
    updateBatch(hoodieWriteConfig, client, newCommitTime, prevCommitTime,
        Optional.of(Arrays.asList(commitTimeBetweenPrevAndNew)),
        initCommitTime, numRecords, writeFn, isPrepped, true, numRecords, 200, 2);
  }

  /**
   * Tesst deletion of records
   */
  @Test
  public void testDeletes() throws Exception {
    HoodieWriteClient client = new HoodieWriteClient(jsc, getConfig());

    /**
     * Write 1 (inserts and deletes)
     * Write actual 200 insert records and ignore 100 delete records
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
    writeBatch(client, newCommitTime, initCommitTime, Optional.empty(), initCommitTime,
        //unused as genFn uses hard-coded number of inserts/updates/deletes
        -1,
        recordGenFunction, HoodieWriteClient::upsert, true,
        200, 200, 1);

    /**
     * Write 2 (deletes+writes)
     */
    String prevCommitTime = newCommitTime;
    newCommitTime = "004";
    final List<HoodieRecord> recordsInSecondBatch = new ArrayList<>();

    recordGenFunction =
        (String commitTime, Integer numRecordsInThisCommit) -> {
          List<HoodieRecord> fewRecordsForDelete = recordsInFirstBatch.subList(0, 50);
          List<HoodieRecord> fewRecordsForUpdate = recordsInFirstBatch.subList(50, 100);
          recordsInSecondBatch.addAll(dataGen.generateDeletesFromExistingRecords(fewRecordsForDelete));
          recordsInSecondBatch.addAll(fewRecordsForUpdate);
          return recordsInSecondBatch;
        };
    writeBatch(client, newCommitTime, prevCommitTime, Optional.empty(), initCommitTime,
        100, recordGenFunction, HoodieWriteClient::upsert, true,
        50, 150, 2);
  }

  /**
   * Test scenario of new file-group getting added during upsert()
   */
  @Test
  public void testSmallInsertHandlingForUpserts() throws Exception {
    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});

    HoodieWriteClient client = new HoodieWriteClient(jsc, config);

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
    assertEquals("file should contain 100 records", ParquetUtils.readRowKeysFromParquet(jsc.hadoopConfiguration(),
        new Path(basePath, testPartitionPath + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(), 100);

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
    Path newFile = new Path(basePath, testPartitionPath + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
    assertEquals("file should contain 140 records",
        ParquetUtils.readRowKeysFromParquet(jsc.hadoopConfiguration(), newFile).size(), 140);

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
    HoodieTable table = HoodieTable.getHoodieTable(metadata, config, jsc);
    TableFileSystemView.ReadOptimizedView fileSystemView = table.getROFileSystemView();
    List<HoodieDataFile> files = fileSystemView.getLatestDataFilesBeforeOrOn(testPartitionPath, commitTime3)
        .collect(Collectors.toList());
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
   * Test scenario of new file-group getting added during insert()
   */
  @Test
  public void testSmallInsertHandlingForInserts() throws Exception {

    final String testPartitionPath = "2016/09/26";
    final int insertSplitLimit = 100;
    // setup the small file handling params
    HoodieWriteConfig config = getSmallInsertWriteConfig(insertSplitLimit); // hold upto 200 records max
    dataGen = new HoodieTestDataGenerator(new String[]{testPartitionPath});
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);

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
    assertEquals("file should contain 100 records", ParquetUtils.readRowKeysFromParquet(jsc.hadoopConfiguration(),
        new Path(basePath, testPartitionPath + "/" + FSUtils.makeDataFileName(commitTime1, 0, file1))).size(), 100);

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
    Path newFile = new Path(basePath, testPartitionPath + "/" + FSUtils.makeDataFileName(commitTime2, 0, file1));
    assertEquals("file should contain 140 records",
        ParquetUtils.readRowKeysFromParquet(jsc.hadoopConfiguration(), newFile).size(), 140);

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
    HoodieTable table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    List<HoodieDataFile> files = table.getROFileSystemView()
        .getLatestDataFilesBeforeOrOn(testPartitionPath, commitTime3)
        .collect(Collectors.toList());
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
   * Test to ensure commit metadata points to valid files
   */
  @Test
  public void testCommitWritesRelativePaths() throws Exception {

    HoodieWriteConfig cfg = getConfigBuilder().withAutoCommit(false).build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, cfg);
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
        .fromBytes(commitTimeline.getInstantDetails(commitInstant).get());
    String basePath = table.getMetaClient().getBasePath();
    Collection<String> commitPathNames = commitMetadata.getFileIdAndFullPaths(basePath).values();

    // Read from commit file
    String filename = HoodieTestUtils.getCommitFilePath(basePath, commitTime);
    FileInputStream inputStream = new FileInputStream(filename);
    String everything = IOUtils.toString(inputStream);
    HoodieCommitMetadata metadata = HoodieCommitMetadata.fromJsonString(everything.toString());
    HashMap<String, String> paths = metadata.getFileIdAndFullPaths(basePath);
    inputStream.close();

    // Compare values in both to make sure they are equal.
    for (String pathName : paths.values()) {
      assertTrue(commitPathNames.contains(pathName));
    }
  }

  /**
   * Build Hoodie Write Config for small data file sizes
   */
  private HoodieWriteConfig getSmallInsertWriteConfig(int insertSplitSize) {
    HoodieWriteConfig.Builder builder = getConfigBuilder();
    return builder.withCompactionConfig(
        HoodieCompactionConfig.newBuilder().compactionSmallFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 15)
            .insertSplitSize(insertSplitSize).build()) // tolerate upto 15 records
        .withStorageConfig(
            HoodieStorageConfig.newBuilder().limitFileSize(HoodieTestDataGenerator.SIZE_PER_RECORD * 20).build())
        .build();
  }
}
