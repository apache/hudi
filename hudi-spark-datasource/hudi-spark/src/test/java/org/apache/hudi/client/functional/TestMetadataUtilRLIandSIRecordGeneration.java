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
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.EmptyHoodieRecordPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFailedWritesCleaningPolicy;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.CommitUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.metadata.BaseFileRecordParsingUtils;
import org.apache.hudi.metadata.HoodieMetadataPayload;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.HoodieClientTestBase;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
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
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getDeletedKeysFromMergedLogs;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getRecordKeys;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.getRecordKeysDeletedOrUpdated;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.reduceByKeys;
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
        try {
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
        } catch (IOException e) {
          throw new HoodieException("Should not have failed ", e);
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
        try {
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
        } catch (IOException e) {
          throw new HoodieException("Should not have failed ", e);
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

      // lets validate that if any log file contains inserts, fetching keys will fail.
      HoodieWriteStat writeStat = writeStatuses3.get(1).getStat();
      writeStat.setNumInserts(5);
      HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(Collections.singletonList(writeStat), Collections.emptyMap(),
          Option.empty(), WriteOperationType.UPSERT, writeConfig.getSchema(), "commit");

      try {
        getRecordKeysDeletedOrUpdated(context, commitMetadata, writeConfig.getMetadataConfig(), metaClient, finalCommitTime3);
        fail("Should not have reached here");
      } catch (Exception e) {
        // no op
      }

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
            finalActualDeletes.addAll(
                getDeletedKeysFromMergedLogs(metaClient, latestCommitTimestamp, EngineType.SPARK, Collections.singletonList(fullFilePath.toString()), writerSchemaOpt, fullFilePath));

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
    HoodieCommitMetadata commitMetadata = CommitUtils.buildMetadata(writeStatuses3.stream().map(writeStatus -> writeStatus.getStat()).collect(Collectors.toList()), Collections.emptyMap(),
        Option.empty(), WriteOperationType.UPSERT, writeConfig.getSchema(), "commit");

    // validate HoodieTableMetadataUtil.getRecordKeysDeletedOrUpdated for entire CommitMetadata which is used in SI code path.
    List<String> updatedOrDeletedKeys =
        new ArrayList<>(getRecordKeysDeletedOrUpdated(context, commitMetadata, writeConfig.getMetadataConfig(), metaClient, finalCommitTime3).collectAsList());
    List<String> expectedUpdatesOrDeletes = new ArrayList<>(expectedUpdates);
    expectedUpdatesOrDeletes.addAll(expectedRLIDeletes);
    assertListEquality(expectedUpatesAndDeletes, updatedOrDeletedKeys);
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
        try {
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
        } catch (IOException e) {
          throw new HoodieException("Should not have failed ", e);
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
        try {
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
        } catch (IOException e) {
          throw new HoodieException("Should not have failed ", e);
        }
      }
    });
  }
}
