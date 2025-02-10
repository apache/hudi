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

import org.apache.hudi.avro.model.HoodieSecondaryIndexInfo;
import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.convertMetadataToRecordIndexRecords;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getRecordKeys;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getRevivedAndDeletedKeysFromMergedLogs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.reduceByKeys;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.constructSecondaryIndexKey;
import static org.apache.hudi.metadata.SecondaryIndexKeyUtils.getRecordKeyFromSecondaryIndexKey;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.convertWriteStatsToSecondaryIndexRecords;
import static org.apache.hudi.metadata.SecondaryIndexRecordGenerationUtils.readSecondaryKeysFromFileSlices;
import static org.apache.hudi.testutils.Assertions.assertNoWriteErrors;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestMetadataUtilRLIandSIRecordGeneration extends HoodieClientTestBase {

  /**
   * Tests various methods used for RLI and SI record generation flows.
   * We test below methods
   * BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(...). This is used for RLI record generation.
   * BaseFileRecordParsingUtils.getRecordKeyStatuses(...) // This is used in both RLI and SI flow.
   *
   * @throws IOException
   */
  @Test
  public void testRecordGenerationAPIsForCOW() throws IOException {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.createNewInstantTime();
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 100);
      client.startCommitWithTime(commitTime);
      List<WriteStatus> writeStatuses1 = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses1);

      // assert RLI records for a base file from 1st commit
      String finalCommitTime = commitTime;
      Map<String, String> recordKeyToPartitionMapping1 = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatuses1.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
          fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
        }

        // poll into generateRLIMetadataHoodieRecordsForBaseFile to fetch MDT RLI records for inserts and deletes.
        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
            writeStatus.getStat(), writeConfig.getWritesFileIdEncoding(), finalCommitTime, metaClient.getStorage());
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          String partition = ((HoodieMetadataPayload) rliRecord.getData()).getRecordGlobalLocation().getPartitionPath();
          recordKeyToPartitionMapping1.put(key, partition);
        }
      });

      Map<String, String> expectedRecordToPartitionMapping1 = new HashMap<>();
      records1.forEach(record -> expectedRecordToPartitionMapping1.put(record.getRecordKey(), record.getPartitionPath()));

      assertEquals(expectedRecordToPartitionMapping1, recordKeyToPartitionMapping1);

      // lets update some records and assert RLI records.
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      String finalCommitTime2 = commitTime;
      List<HoodieRecord> deletes2 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      records2.addAll(deletes2);

      List<WriteStatus> writeStatuses2 = client.upsert(jsc.parallelize(records2, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses2);

      List<String> expectedInserts = inserts2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> expectedDeletes = deletes2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      List<String> actualInserts = new ArrayList<>();
      List<String> actualDeletes = new ArrayList<>();
      // only inserts and deletes will result in RLI records. lets validate that.
      generateRliRecordsAndAssert(writeStatuses2, fileIdToFileNameMapping1, finalCommitTime2, writeConfig, actualInserts, actualDeletes);

      assertListEquality(expectedInserts, actualInserts);
      assertListEquality(expectedDeletes, actualDeletes);

      // lets validate APIs in BaseFileParsingUtils directly
      actualInserts = new ArrayList<>();
      actualDeletes = new ArrayList<>();
      List<String> actualUpdates = new ArrayList<>();
      List<String> expectedUpdates = updates2.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
      parseRecordKeysFromBaseFiles(writeStatuses2, fileIdToFileNameMapping1, finalCommitTime2, writeConfig, actualInserts, actualDeletes, actualUpdates);
      assertListEquality(expectedInserts, actualInserts);
      assertListEquality(expectedDeletes, actualDeletes);
      // we can't really assert equality for updates. bcoz, w/ COW, we might just rewrite an existing parquet file. So, more records will be deduced as updates.
      // And so, we are validating using contains.
      expectedUpdates.forEach(entry -> assertTrue(actualUpdates.contains(entry)));
    }
  }

  /**
   * Tests various methods used for RLI and SI record generation flows w/ MOR table. here emphasis are given to log files.
   * We test below methods
   * BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(...). This is used for RLI record generation.
   * HoodieTableMetadataUtil.getRecordKeys() // This is used in both RLI and SI flow.
   * HoodieTableMetadataUtil.getRecordKeysDeletedOrUpdated() for HoodieCommitMetadata.
   * <p>
   * We also test few adhoc scenarios.
   * - if any log files contains inserts, RLI and SI record generation should throw exception.
   * - RLI do no generate any records for compaction operation.
   *
   * @throws IOException
   */
  @Test
  public void testRecordGenerationAPIsForMOR() throws IOException {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    cleanupClients();
    Properties props = new Properties();
    props.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");
    initMetaClient(tableType, props);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(2)
            .withInlineCompaction(true)
            .compactionSmallFileSize(0).build()).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.createNewInstantTime();
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, 100);
      client.startCommitWithTime(commitTime);
      List<WriteStatus> writeStatuses1 = client.insert(jsc.parallelize(records1, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses1);

      // assert RLI records for a base file from 1st commit
      String finalCommitTime = commitTime;
      Map<String, String> recordKeyToPartitionMapping1 = new HashMap<>();
      Map<String, String> fileIdToFileNameMapping1 = new HashMap<>();
      writeStatuses1.forEach(writeStatus -> {
        assertEquals(writeStatus.getStat().getNumDeletes(), 0);
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping1.containsKey(writeStatFileId)) {
          fileIdToFileNameMapping1.put(writeStatFileId, writeStatus.getStat().getPath().substring(writeStatus.getStat().getPath().lastIndexOf("/") + 1));
        }

        // poll into generateRLIMetadataHoodieRecordsForBaseFile to fetch MDT RLI records for inserts and deletes.
        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(),
            writeStatus.getStat(), writeConfig.getWritesFileIdEncoding(), finalCommitTime, metaClient.getStorage());
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          String partition = ((HoodieMetadataPayload) rliRecord.getData()).getRecordGlobalLocation().getPartitionPath();
          recordKeyToPartitionMapping1.put(key, partition);
        }
      });

      Map<String, String> expectedRecordToPartitionMapping1 = new HashMap<>();
      records1.forEach(record -> expectedRecordToPartitionMapping1.put(record.getRecordKey(), record.getPartitionPath()));

      assertEquals(expectedRecordToPartitionMapping1, recordKeyToPartitionMapping1);

      // lets update some records and assert RLI records.
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      List<HoodieRecord> deletes2 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      records2.addAll(deletes2);

      List<WriteStatus> writeStatuses2 = client.upsert(jsc.parallelize(records2, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses2);
      assertRLIandSIRecordGenerationAPIs(inserts2, updates2, deletes2, writeStatuses2, commitTime, writeConfig);

      // trigger 2nd commit.
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      String finalCommitTime3 = commitTime;
      List<HoodieRecord> deletes3 = dataGen.generateUniqueDeleteRecords(commitTime, 30);
      List<HoodieRecord> updates3 = dataGen.generateUniqueUpdates(commitTime, 30);
      List<HoodieRecord> inserts3 = dataGen.generateInserts(commitTime, 30);
      List<HoodieRecord> records3 = new ArrayList<>();
      records3.addAll(inserts3);
      records3.addAll(updates3);
      records3.addAll(deletes3);

      List<WriteStatus> writeStatuses3 = client.upsert(jsc.parallelize(records3, 1), commitTime).collect();
      assertNoWriteErrors(writeStatuses3);
      assertRLIandSIRecordGenerationAPIs(inserts3, updates3, deletes3, writeStatuses3, finalCommitTime3, writeConfig);

      // trigger compaction
      Option<String> compactionInstantOpt = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantOpt.isPresent());
      HoodieWriteMetadata compactionWriteMetadata = client.compact(compactionInstantOpt.get());
      HoodieCommitMetadata compactionCommitMetadata = (HoodieCommitMetadata) compactionWriteMetadata.getCommitMetadata().get();
      // no RLI records should be generated for compaction operation.
      assertTrue(convertMetadataToRecordIndexRecords(context, compactionCommitMetadata, writeConfig.getMetadataConfig(),
          metaClient, writeConfig.getWritesFileIdEncoding(), compactionInstantOpt.get(), EngineType.SPARK).isEmpty());
    }
  }

  /**
   * Tests methods used for Secondary Index record generation flows with MOR table during index initialization as well as update.
   */
  @Test
  public void testSecondaryIndexRecordGenerationForMOR() throws IOException {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    cleanupClients();
    Properties props = new Properties();
    props.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), "timestamp");
    initMetaClient(tableType, props);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).withAutoCommit(false)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(3)
            .withInlineCompaction(false)
            .compactionSmallFileSize(0).build()).build();

    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      // Insert
      String commitTime = client.createNewInstantTime();
      int initialRecordsCount = 10;
      List<HoodieRecord> records1 = dataGen.generateInserts(commitTime, initialRecordsCount);
      client.startCommitWithTime(commitTime);
      JavaRDD<WriteStatus> writeStatuses1 = client.insert(jsc.parallelize(records1, 1), commitTime);
      assertNoWriteErrors(writeStatuses1.collect());
      client.commit(commitTime, writeStatuses1);

      // assert SI records from 1st commit
      List<String> expectedSecondaryIndexKeys = records1.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      String firstCommitTime = commitTime;
      HoodieIndexDefinition indexDefinition = HoodieIndexDefinition.newBuilder()
          .withIndexName("secondary_index_idx_rider")
          .withIndexType("")
          .withIndexFunction("")
          .withSourceFields(Collections.singletonList("rider"))
          .withIndexOptions(Collections.emptyMap())
          .build();
      HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).withSecondaryIndexParallelism(2).build();
      HoodieMetadataFileSystemView metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      metadataView.loadAllPartitions();
      List<Pair<String, FileSlice>> partitionFileSlicePairs = new ArrayList<>();
      HoodieMetadataFileSystemView finalMetadataView = metadataView;
      Arrays.asList(dataGen.getPartitionPaths()).forEach(partition -> finalMetadataView.getLatestMergedFileSlicesBeforeOrOn(partition, firstCommitTime)
          .forEach(fs -> partitionFileSlicePairs.add(Pair.of(partition, fs))));
      List<HoodieRecord> secondaryIndexRecords = readSecondaryKeysFromFileSlices(
          engineContext, partitionFileSlicePairs, metadataConfig.getSecondaryIndexParallelism(), this.getClass().getSimpleName(), metaClient, EngineType.SPARK, indexDefinition).collectAsList();
      assertListEquality(expectedSecondaryIndexKeys, secondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));

      // update and insert some records and assert SI records
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      List<HoodieRecord> updates2 = dataGen.generateUniqueUpdates(commitTime, 1);
      List<String> expectedUpdatedIndexKeys = updates2.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> inserts2 = dataGen.generateInserts(commitTime, 1);
      List<String> expectedInsertedIndexKeys = inserts2.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records2 = new ArrayList<>();
      records2.addAll(inserts2);
      records2.addAll(updates2);
      JavaRDD<WriteStatus> writeStatuses2 = client.upsert(jsc.parallelize(records2, 1), commitTime);
      assertNoWriteErrors(writeStatuses2.collect());

      // assert SI
      String secondCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      List<HoodieWriteStat> allWriteStats = writeStatuses2.collect().stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, secondCommitTime, indexDefinition, metadataConfig, metadataView, metaClient, engineContext, EngineType.SPARK).collectAsList();
      client.commit(secondCommitTime, writeStatuses2);

      // There should be 3 SI records:
      // a) 1 insert due to inserts2,
      // b) 1 delete + 1 insert due to updates2,
      assertEquals(3, secondaryIndexRecords.size());
      List<String> validSecondaryIndexKeys = new ArrayList<>(expectedInsertedIndexKeys);
      validSecondaryIndexKeys.addAll(expectedUpdatedIndexKeys);
      // filter delete records from secondaryIndexRecords
      List<HoodieRecord> deletedSecondaryIndexRecords = new ArrayList<>();
      List<HoodieRecord> validSecondaryIndexRecords = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords, validSecondaryIndexRecords);
      });
      assertListEquality(validSecondaryIndexKeys, validSecondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList()));
      assertTrue(expectedSecondaryIndexKeys.containsAll(deletedSecondaryIndexRecords.stream().map(HoodieRecord::getRecordKey).collect(Collectors.toList())));

      // let us create one more file slice with delete
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      List<HoodieRecord> deletes = dataGen.generateUniqueDeleteRecords(commitTime, 1);
      List<String> expectedDeletedIndexKeys = deletes.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records3 = new ArrayList<>();
      records3.addAll(deletes);
      JavaRDD<WriteStatus> writeStatuses3 = client.upsert(jsc.parallelize(records3, 1), commitTime);
      assertNoWriteErrors(writeStatuses3.collect());

      // assert SI
      String thirdCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      allWriteStats = writeStatuses3.collect().stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, thirdCommitTime, indexDefinition, metadataConfig, metadataView, metaClient, engineContext, EngineType.SPARK).collectAsList();
      client.commit(thirdCommitTime, writeStatuses3);

      // There should be 1 SI records: 1 delete due to deletes3
      assertEquals(1, secondaryIndexRecords.size());
      // filter insert records from secondaryIndexRecords
      List<HoodieRecord> validSecondaryIndexRecords2 = new ArrayList<>();
      List<HoodieRecord> deletedSecondaryIndexRecords2 = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords2, validSecondaryIndexRecords2);
      });
      assertTrue(validSecondaryIndexRecords2.isEmpty());
      assertEquals(1, deletedSecondaryIndexRecords2.size());
      assertEquals(getRecordKeyFromSecondaryIndexKey(expectedDeletedIndexKeys.get(0)), getRecordKeyFromSecondaryIndexKey(deletedSecondaryIndexRecords2.get(0).getRecordKey()));

      // revive the deleted keys
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      List<HoodieRecord> inserts4 = dataGen.generateSameKeyInserts(commitTime, deletes);
      List<String> expectedRevivedIndexKeys = inserts4.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records4 = new ArrayList<>();
      records4.addAll(inserts4);
      JavaRDD<WriteStatus> writeStatuses4 = client.upsert(jsc.parallelize(records4, 1), commitTime);
      assertNoWriteErrors(writeStatuses4.collect());

      // assert SI
      String fourthCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      allWriteStats = writeStatuses4.collect().stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, fourthCommitTime, indexDefinition, metadataConfig, metadataView, metaClient, engineContext, EngineType.SPARK).collectAsList();
      client.commit(fourthCommitTime, writeStatuses4);

      // There should be 1 SI records: 1 insert due to inserts4
      assertEquals(1, secondaryIndexRecords.size());
      assertEquals(expectedRevivedIndexKeys.get(0), secondaryIndexRecords.get(0).getRecordKey());

      // generate update for the same key, but with the same rider value
      commitTime = client.createNewInstantTime();
      client.startCommitWithTime(commitTime);
      List<HoodieRecord> updates5 = dataGen.generateUpdatesWithTimestamp(fourthCommitTime, inserts4, Long.parseLong(commitTime));
      List<String> expectedUpdatedIndexKeys2 = updates5.stream().map(TestMetadataUtilRLIandSIRecordGeneration::getSecondaryIndexKey).collect(Collectors.toList());
      List<HoodieRecord> records5 = new ArrayList<>();
      records5.addAll(updates5);
      JavaRDD<WriteStatus> writeStatuses5 = client.upsert(jsc.parallelize(records5, 1), commitTime);
      assertNoWriteErrors(writeStatuses5.collect());

      // assert SI
      String fifthCommitTime = commitTime;
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      allWriteStats = writeStatuses5.collect().stream().map(WriteStatus::getStat).collect(Collectors.toList());
      secondaryIndexRecords =
          convertWriteStatsToSecondaryIndexRecords(allWriteStats, fifthCommitTime, indexDefinition, metadataConfig, metadataView, metaClient, engineContext, EngineType.SPARK).collectAsList();
      client.commit(fifthCommitTime, writeStatuses5);

      // There should be 0 SI records because the secondary key field "rider" value has not changed.
      assertEquals(0, secondaryIndexRecords.size());

      // trigger compaction
      Option<String> compactionInstantOpt = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstantOpt.isPresent());
      HoodieWriteMetadata compactionWriteMetadata = client.compact(compactionInstantOpt.get());
      HoodieCommitMetadata compactionCommitMetadata = (HoodieCommitMetadata) compactionWriteMetadata.getCommitMetadata().get();
      // assert SI records
      metaClient = HoodieTableMetaClient.reload(metaClient);
      metadataView = new HoodieMetadataFileSystemView(engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
      allWriteStats = compactionCommitMetadata.getWriteStats();
      secondaryIndexRecords = convertWriteStatsToSecondaryIndexRecords(
          allWriteStats, compactionInstantOpt.get(), indexDefinition, metadataConfig, metadataView, metaClient, engineContext, EngineType.SPARK).collectAsList();
      // Get valid and deleted secondary index records
      List<HoodieRecord> validSecondaryIndexRecords3 = new ArrayList<>();
      List<HoodieRecord> deletedSecondaryIndexRecords3 = new ArrayList<>();
      secondaryIndexRecords.forEach(record -> {
        populateValidAndDeletedSecondaryIndexRecords(record, deletedSecondaryIndexRecords3, validSecondaryIndexRecords3);
      });
      // There should 0 deleted records because compaction does not update any records.
      assertEquals(0, deletedSecondaryIndexRecords3.size());
      assertEquals(initialRecordsCount, dataGen.getNumExistingKeys(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
    }
  }

  private static void populateValidAndDeletedSecondaryIndexRecords(HoodieRecord record, List<HoodieRecord> deletedSecondaryIndexRecords, List<HoodieRecord> validSecondaryIndexRecords) {
    try {
      if (record.isDelete(HoodieSecondaryIndexInfo.getClassSchema(), new Properties())) {
        deletedSecondaryIndexRecords.add(record);
      } else {
        validSecondaryIndexRecords.add(record);
      }
    } catch (IOException e) {
      fail("Failed to check if record is deleted.", e);
    }
  }

  private static String getSecondaryIndexKey(HoodieRecord record) {
    try {
      return constructSecondaryIndexKey(((RawTripTestPayload) record.getData()).getJsonDataAsMap().get("rider").toString(), record.getRecordKey());
    } catch (IOException e) {
      throw new HoodieException("Failed to construct secondary index key for record key: " + record.getRecordKey(), e);
    }
  }

  private void assertRLIandSIRecordGenerationAPIs(List<HoodieRecord> inserts3, List<HoodieRecord> updates3, List<HoodieRecord> deletes3,
                                                  List<WriteStatus> writeStatuses3, String finalCommitTime3, HoodieWriteConfig writeConfig) {
    List<String> expectedRLIInserts = inserts3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedUpdates = updates3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedRLIDeletes = deletes3.stream().map(record -> record.getKey().getRecordKey()).collect(Collectors.toList());
    List<String> expectedUpatesAndDeletes = new ArrayList<>(expectedRLIDeletes);
    expectedUpatesAndDeletes.addAll(expectedUpdates);

    // lets validate RLI record generation.
    List<String> actualInserts = new ArrayList<>();
    List<String> actualDeletes = new ArrayList<>();
    List<String> actualUpdatesAndDeletes = new ArrayList<>();
    generateRliRecordsAndAssert(writeStatuses3.stream().filter(writeStatus -> !FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath())))
        .collect(Collectors.toList()), Collections.emptyMap(), finalCommitTime3, writeConfig, actualInserts, actualDeletes);

    // lets also test HoodieTableMetadataUtil.getRecordKeys() for each individual log file touched as part of HoodieCommitMetadata.
    // lets test only deletes and also test both validat and deleted keys for log files.
    // we have disabled small file handling. And so, updates and deletes will definitely go into log files.
    String latestCommitTimestamp = metaClient.reloadActiveTimeline().getCommitsTimeline().lastInstant().get().requestedTime();
    Option<Schema> writerSchemaOpt = tryResolveSchemaForTable(metaClient);
    List<String> finalActualDeletes = actualDeletes;
    writeStatuses3.stream().filter(writeStatus -> FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath())))
        .forEach(writeStatus -> {
          try {
            StoragePath fullFilePath = new StoragePath(basePath, writeStatus.getStat().getPath());
            // used for RLI
            finalActualDeletes.addAll(getRevivedAndDeletedKeysFromMergedLogs(metaClient, latestCommitTimestamp, EngineType.SPARK, Collections.singletonList(fullFilePath.toString()), writerSchemaOpt,
                Collections.singletonList(fullFilePath.toString())).getValue());

            // used in SI flow
            actualUpdatesAndDeletes.addAll(getRecordKeys(Collections.singletonList(fullFilePath.toString()), metaClient, writerSchemaOpt,
                writeConfig.getMetadataConfig().getMaxReaderBufferSize(), latestCommitTimestamp, true, true));
          } catch (IOException e) {
            throw new HoodieIOException("Failed w/ IOException ", e);
          }
        });

    assertListEquality(expectedRLIInserts, actualInserts);
    assertListEquality(expectedRLIDeletes, actualDeletes);
    assertListEquality(expectedUpatesAndDeletes, actualUpdatesAndDeletes);
  }

  @Test
  public void testReducedByKeysForRLIRecords() throws IOException {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    cleanupClients();
    initMetaClient(tableType);
    cleanupTimelineService();
    initTimelineService();

    HoodieSparkEngineContext engineContext = new HoodieSparkEngineContext(jsc);
    HoodieWriteConfig writeConfig = getConfigBuilder(HoodieFailedWritesCleaningPolicy.EAGER).build();
    try (SparkRDDWriteClient client = new SparkRDDWriteClient(engineContext, writeConfig)) {
      String commitTime = client.createNewInstantTime();
      List<HoodieRecord> inserts = dataGen.generateInserts(commitTime, 100);
      List<HoodieRecord> deletes = dataGen.generateUniqueDeleteRecords(commitTime, 20);
      String randomFileId = UUID.randomUUID().toString() + "-0";
      List<String> deletedRecordKeys = deletes.stream().map(record -> record.getRecordKey()).collect(Collectors.toList());
      List<HoodieRecord> adjustedInserts = inserts.stream().filter(record -> !deletedRecordKeys.contains(record.getRecordKey())).collect(Collectors.toList());

      List<HoodieRecord> insertRecords =
          inserts.stream().map(record -> HoodieMetadataPayload.createRecordIndexUpdate(record.getRecordKey(), "abc", randomFileId, commitTime, 0))
              .collect(Collectors.toList());
      List<HoodieRecord> deleteRecords = inserts.stream().map(record -> HoodieMetadataPayload.createRecordIndexDelete(record.getRecordKey()))
          .collect(Collectors.toList());

      List<HoodieRecord> recordsToTest = new ArrayList<>();
      recordsToTest.addAll(adjustedInserts);
      recordsToTest.addAll(deleteRecords);
      // happy paths. no dups. in and out are same.
      List<HoodieRecord> actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2).collectAsList();
      assertHoodieRecordListEquality(actualRecords, recordsToTest);

      // few records has both inserts and deletes.
      recordsToTest = new ArrayList<>();
      recordsToTest.addAll(insertRecords);
      recordsToTest.addAll(deleteRecords);
      actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2).collectAsList();
      List<HoodieRecord> expectedList = new ArrayList<>();
      expectedList.addAll(insertRecords);
      assertHoodieRecordListEquality(actualRecords, expectedList);

      // few deletes are duplicates. we are allowed to have duplicate deletes.
      recordsToTest = new ArrayList<>();
      recordsToTest.addAll(adjustedInserts);
      recordsToTest.addAll(deleteRecords);
      recordsToTest.addAll(deleteRecords.subList(0, 10));
      actualRecords = reduceByKeys(context.parallelize(recordsToTest, 2), 2).collectAsList();
      expectedList = new ArrayList<>();
      expectedList.addAll(adjustedInserts);
      expectedList.addAll(deleteRecords);
      assertHoodieRecordListEquality(actualRecords, expectedList);

      // test failure case. same record having 2 inserts should fail.
      recordsToTest = new ArrayList<>();
      recordsToTest.addAll(adjustedInserts);
      recordsToTest.addAll(adjustedInserts.subList(0, 5));
      try {
        reduceByKeys(context.parallelize(recordsToTest, 2), 2).collectAsList();
        fail("Should not have reached here");
      } catch (Exception e) {
        // expected. no-op
        assertTrue(e.getCause() instanceof HoodieIOException);
      }
    }
  }

  private void assertHoodieRecordListEquality(List<HoodieRecord> actualList, List<HoodieRecord> expectedList) {
    assertEquals(expectedList.size(), actualList.size());
    List<String> expectedInsertRecordKeys = expectedList.stream().filter(record -> !(record.getData() instanceof EmptyHoodieRecordPayload))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());
    List<String> expectedDeletedRecordKeys = expectedList.stream().filter(record -> (record.getData() instanceof EmptyHoodieRecordPayload))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());

    List<String> actualInsertRecordKeys = actualList.stream().filter(record -> !(record.getData() instanceof EmptyHoodieRecordPayload))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());
    List<String> actualDeletedRecordKeys = actualList.stream().filter(record -> (record.getData() instanceof EmptyHoodieRecordPayload))
        .map(record -> record.getRecordKey()).collect(Collectors.toList());

    assertListEquality(expectedInsertRecordKeys, actualInsertRecordKeys);
    assertListEquality(expectedDeletedRecordKeys, actualDeletedRecordKeys);
  }

  private void assertListEquality(List<String> list1, List<String> list2) {
    Collections.sort(list1);
    Collections.sort(list2);
    assertEquals(list1, list2);
  }

  private static Option<Schema> tryResolveSchemaForTable(HoodieTableMetaClient dataTableMetaClient) {
    if (dataTableMetaClient.getCommitsTimeline().filterCompletedInstants().countInstants() == 0) {
      return Option.empty();
    }

    try {
      TableSchemaResolver schemaResolver = new TableSchemaResolver(dataTableMetaClient);
      return Option.of(schemaResolver.getTableAvroSchema());
    } catch (Exception e) {
      throw new HoodieException("Failed to get latest columns for " + dataTableMetaClient.getBasePath(), e);
    }
  }

  private void generateRliRecordsAndAssert(List<WriteStatus> writeStatuses, Map<String, String> fileIdToFileNameMapping, String commitTime,
                                           HoodieWriteConfig writeConfig, List<String> actualInserts,
                                           List<String> actualDeletes) {
    writeStatuses.forEach(writeStatus -> {
      if (!FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath()))) {
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping.isEmpty()) {
          assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping.get(writeStatFileId));
        }

        Iterator<HoodieRecord> rliRecordsItr = BaseFileRecordParsingUtils.generateRLIMetadataHoodieRecordsForBaseFile(metaClient.getBasePath().toString(), writeStatus.getStat(),
            writeConfig.getWritesFileIdEncoding(), commitTime, metaClient.getStorage());
        while (rliRecordsItr.hasNext()) {
          HoodieRecord rliRecord = rliRecordsItr.next();
          String key = rliRecord.getRecordKey();
          if (rliRecord.getData() instanceof EmptyHoodieRecordPayload) {
            actualDeletes.add(key);
          } else {
            actualInserts.add(key);
          }
        }
      }
    });
  }

  private void parseRecordKeysFromBaseFiles(List<WriteStatus> writeStatuses, Map<String, String> fileIdToFileNameMapping, String commitTime,
                                            HoodieWriteConfig writeConfig, List<String> actualInserts,
                                            List<String> actualDeletes, List<String> actualUpdates) {
    writeStatuses.forEach(writeStatus -> {
      if (!FSUtils.isLogFile(FSUtils.getFileName(writeStatus.getStat().getPath(), writeStatus.getPartitionPath()))) {
        // Fetch record keys for all
        String writeStatFileId = writeStatus.getFileId();
        if (!fileIdToFileNameMapping.isEmpty()) {
          assertEquals(writeStatus.getStat().getPrevBaseFile(), fileIdToFileNameMapping.get(writeStatFileId));
        }

        String partition = writeStatus.getStat().getPartitionPath();
        String latestFileName = FSUtils.getFileNameFromPath(writeStatus.getStat().getPath());

        Set<BaseFileRecordParsingUtils.RecordStatus> recordStatusSet = new HashSet<>();
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.INSERT);
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.UPDATE);
        recordStatusSet.add(BaseFileRecordParsingUtils.RecordStatus.DELETE);

        Map<BaseFileRecordParsingUtils.RecordStatus, List<String>> recordKeyMappings = BaseFileRecordParsingUtils.getRecordKeyStatuses(metaClient.getBasePath().toString(), partition, latestFileName,
            writeStatus.getStat().getPrevBaseFile(), storage, recordStatusSet);
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.INSERT)) {
          actualInserts.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.INSERT));
        }
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.UPDATE)) {
          actualUpdates.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.UPDATE));
        }
        if (recordKeyMappings.containsKey(BaseFileRecordParsingUtils.RecordStatus.DELETE)) {
          actualDeletes.addAll(recordKeyMappings.get(BaseFileRecordParsingUtils.RecordStatus.DELETE));
        }
      }
    });
  }
}
