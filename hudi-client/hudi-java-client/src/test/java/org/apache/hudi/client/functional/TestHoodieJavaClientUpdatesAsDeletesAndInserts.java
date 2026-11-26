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

package org.apache.hudi.client.functional;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteClientTestUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.index.HoodieInMemoryHashIndex;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.table.action.commit.BaseWriteHelper;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
import static org.apache.hudi.common.testutils.HoodieTestUtils.TIMELINE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Functional tests for {@link HoodieWriteConfig#WRITE_UPDATES_AS_DELETES_AND_INSERTS} on
 * merge-on-read storage: an update becomes a positional delete in the record's current file group
 * plus an insert of the new version into a different file group of the same partition.
 */
public class TestHoodieJavaClientUpdatesAsDeletesAndInserts extends HoodieJavaClientTestHarness {

  @BeforeEach
  public void setUpTestTable() {
    testTable = HoodieTestTable.of(metaClient);
  }

  @Override
  protected HoodieTableType getTableType() {
    return HoodieTableType.MERGE_ON_READ;
  }

  private HoodieWriteConfig buildConfig(boolean updatesAsDeletesAndInserts) {
    return configBuilder(updatesAsDeletesAndInserts, 10).build();
  }

  private HoodieWriteConfig buildConfig(boolean updatesAsDeletesAndInserts, HoodieTableVersion tableVersion,
                                        int maxDeltaCommitsBeforeCompaction) {
    return configBuilder(updatesAsDeletesAndInserts, maxDeltaCommitsBeforeCompaction)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
  }

  private HoodieWriteConfig.Builder configBuilder(boolean updatesAsDeletesAndInserts, int maxDeltaCommitsBeforeCompaction) {
    return getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withProperties(keyGenProps())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(maxDeltaCommitsBeforeCompaction).build())
        .withWriteUpdatesAsDeletesAndInserts(updatesAsDeletesAndInserts);
  }

  /**
   * Re-creates the test table at the given table version; versions 8 and 9 write inline log files and
   * version 10 writes native log files.
   */
  private void initTable(HoodieTableVersion tableVersion) throws IOException {
    Properties properties = new Properties();
    properties.setProperty(HoodieWriteConfig.WRITE_TABLE_VERSION.key(), String.valueOf(tableVersion.versionCode()));
    metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType(), properties);
    assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion());
    testTable = HoodieTestTable.of(metaClient);
  }

  private void assertTableVersion(HoodieTableVersion tableVersion) {
    assertEquals(tableVersion, HoodieTestUtils.createMetaClient(storageConf, basePath).getTableConfig().getTableVersion(),
        "the writes must keep the table at its version");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testUpdatesBecomePositionalDeletesAndInserts(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = buildConfig(true, tableVersion, 10);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    Set<String> fileIdsAfterInsert = collectFileIds();

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    assertTableVersion(tableVersion);

    assertEquals(50, assertOnlyPositionalDeletes(config, tableVersion), "each update must produce exactly one positional delete");

    // The new versions must land in new file groups of the same partitions.
    Set<String> newFileIds = new HashSet<>(collectFileIds());
    newFileIds.removeAll(fileIdsAfterInsert);
    assertFalse(newFileIds.isEmpty(), "updates must insert the new version into new file groups");

    // The merged view must expose all 100 records with the 50 new versions served from the new
    // file groups (their file name meta field points outside the original file groups).
    Map<String, GenericRecord> recordMap =
        GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, recordMap.size());
    long updatedRecords = recordMap.values().stream()
        .filter(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString().equals(updateTime))
        .peek(r -> {
          String fileName = r.get(HoodieRecord.FILENAME_METADATA_FIELD).toString();
          assertTrue(newFileIds.stream().anyMatch(fileName::contains),
              "an updated record must be served from a new file group but came from " + fileName);
        }).count();
    assertEquals(50, updatedRecords);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testRepeatedUpdatesOfSameKeys(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = buildConfig(true, tableVersion, 10);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String commitTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, commitTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    // Update every key repeatedly: from the second round on, the base-file-scanning index finds
    // each key in the tombstoned old file group as well as its current one, and only the latest
    // location must be updated.
    String prevCommitTime = commitTime;
    for (int round = 2; round <= 4; round++) {
      String updateTime = WriteClientTestUtils.createNewInstantTime();
      List<WriteStatus> statuses = updateBatch(config, client, updateTime, prevCommitTime,
          Option.of(Arrays.asList(prevCommitTime)), "000", 100, HoodieJavaWriteClient::upsert,
          false, false, 100, 100, round, config.populateMetaFields(), INSTANT_GENERATOR);
      long deletes = statuses.stream().mapToLong(status -> status.getStat().getNumDeletes()).sum();
      long inserts = statuses.stream().mapToLong(status -> status.getStat().getNumInserts()).sum();
      assertEquals(100, deletes, "each key must be deleted exactly once per round");
      assertEquals(100, inserts, "each key must be re-inserted exactly once per round");

      Map<String, GenericRecord> recordMap =
          GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
      assertEquals(100, recordMap.size());
      assertEquals(100, recordMap.values().stream()
          .filter(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString().equals(updateTime))
          .count(), "every record must be served from the latest round");
      prevCommitTime = updateTime;
    }
    assertTableVersion(tableVersion);
  }

  /**
   * With the SIMPLE index, an insert writes base files of new file groups only, and a delete appends
   * positional deletes to the file groups of the deleted records.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testInsertsAndDeletesWithSimpleIndex(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = buildConfig(true, tableVersion, 10);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String firstInsertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, firstInsertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    Set<String> fileIdsAfterFirstInsert = collectFileIds();

    String secondInsertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, secondInsertTime, "000", 50, HoodieJavaWriteClient::insert,
        false, false, 50, 150, 2, Option.empty(), INSTANT_GENERATOR);
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      List<FileSlice> slices = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices).collect(Collectors.toList());
      for (FileSlice slice : slices) {
        assertFalse(slice.hasLogFiles(), "an insert must not write log files: " + slice);
        assertEquals(fileIdsAfterFirstInsert.contains(slice.getFileId()) ? firstInsertTime : secondInsertTime,
            slice.getBaseInstantTime(), "an insert must write new file groups only: " + slice);
      }
      assertTrue(slices.size() > fileIdsAfterFirstInsert.size(), "the second insert must write new file groups");
    }

    String deleteTime = WriteClientTestUtils.createNewInstantTime();
    deleteBatch(config, client, deleteTime, secondInsertTime, "000", 30, false, false, 30, 120,
        TIMELINE_FACTORY, INSTANT_GENERATOR);
    assertEquals(30, assertOnlyPositionalDeletes(config, tableVersion), "each deleted key must produce one positional delete");
    assertEquals(120, GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen).size());
    assertTableVersion(tableVersion);
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testDefaultBehaviorKeepsUpdatesInLogFiles(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = buildConfig(false, tableVersion, 10);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    Set<String> fileIdsAfterInsert = collectFileIds();

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    assertTableVersion(tableVersion);

    // Updates stay in their file groups as data blocks of their logs; no new file groups appear.
    assertEquals(fileIdsAfterInsert, collectFileIds());
    AtomicLong updateRecordCount = new AtomicLong();
    forEachLatestLogBlock(config, (slice, logFile, block) -> {
      assertTrue(block instanceof HoodieDataBlock,
          "expected only data blocks but got: " + block.getClass().getSimpleName() + " in " + logFile.getFileName());
      try (ClosableIterator<HoodieRecord<Object>> records =
               ((HoodieDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
        records.forEachRemaining(record -> updateRecordCount.incrementAndGet());
      }
    });
    assertEquals(50, updateRecordCount.get(), "every update must be appended to its file group's log by default");
  }

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testCompactionAfterDecomposedUpdates(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = buildConfig(true, tableVersion, 2);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    Option<String> compactionTime = client.scheduleCompaction(Option.empty());
    assertTrue(compactionTime.isPresent(), "expected a compaction plan for the delete logs");
    HoodieWriteMetadata writeMetadata = client.compact(compactionTime.get());
    client.commitCompaction(compactionTime.get(), writeMetadata, Option.empty());
    assertTrue(metaClient.reloadActiveTimeline().filterCompletedInstants()
        .containsInstant(compactionTime.get()));

    // Compaction applies the positional deletes: the merged view is unchanged and no log files remain.
    Map<String, GenericRecord> recordMap =
        GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, recordMap.size());
    assertEquals(50, recordMap.values().stream()
        .filter(r -> r.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString().equals(updateTime))
        .count());
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      long remainingLogFiles = Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices)
          .flatMap(FileSlice::getLogFiles)
          .count();
      assertEquals(0, remainingLogFiles, "compaction must consume all delete log files");
    }
    assertTableVersion(tableVersion);
  }

  /**
   * With a global index, an update within a partition, an update that moves a key to another partition
   * and a delete each tombstone the row of the key's current copy by position, so every key keeps a
   * single live copy across partitions.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testGlobalIndexUpdatesWithinAndAcrossPartitions(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.GLOBAL_SIMPLE)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.GLOBAL_SIMPLE)
            .withGlobalSimpleIndexUpdatePartitionPath(true).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
    assertDeletesAndInserts(0, 100, writeAndCommit(client, insertTime, () -> client.insert(inserts, insertTime)));

    String updateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 30);
    assertDeletesAndInserts(30, 30, writeAndCommit(client, updateTime, () -> client.upsert(updates, updateTime)));

    // Some moved keys were updated above, so their old partition also holds a tombstoned copy
    String newPartition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
    String moveTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> moves = dataGen.generateUpdatesForDifferentPartition(moveTime,
        inserts.stream().filter(r -> !r.getPartitionPath().equals(newPartition)).limit(20).collect(Collectors.toList()),
        0L, newPartition);
    assertDeletesAndInserts(20, 20, writeAndCommit(client, moveTime, () -> client.upsert(moves, moveTime)));

    String deleteTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieKey> deletedKeys = moves.stream().limit(10).map(HoodieRecord::getKey).collect(Collectors.toList());
    assertDeletesAndInserts(10, 0, writeAndCommit(client, deleteTime, () -> client.delete(deletedKeys, deleteTime)));

    assertEquals(60, assertOnlyPositionalDeletes(config, tableVersion),
        "every update, move and delete must produce exactly one positional delete");
    // Collecting the records by key fails on a key with copies in two partitions
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(90, recordMap.size());
    deletedKeys.forEach(key -> assertFalse(recordMap.containsKey(key.getRecordKey()), "deleted key is still live: " + key));
    moves.stream().skip(10).forEach(record -> {
      GenericRecord live = recordMap.get(record.getRecordKey());
      assertEquals(newPartition, live.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString());
      assertEquals(moveTime, live.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
    });
    assertTableVersion(tableVersion);
  }

  private static List<WriteStatus> writeAndCommit(HoodieJavaWriteClient client, String instantTime,
                                                  Supplier<List<WriteStatus>> write) {
    WriteClientTestUtils.startCommitWithTime(client, instantTime);
    List<WriteStatus> statuses = write.get();
    client.commit(instantTime, statuses);
    return statuses;
  }

  private static void assertDeletesAndInserts(long expectedDeletes, long expectedInserts, List<WriteStatus> statuses) {
    assertEquals(expectedDeletes, statuses.stream().mapToLong(status -> status.getStat().getNumDeletes()).sum());
    assertEquals(expectedInserts, statuses.stream().mapToLong(status -> status.getStat().getNumInserts()).sum());
    assertEquals(0, statuses.stream().mapToLong(status -> status.getStat().getNumUpdateWrites()).sum(),
        "no update record must be written in this mode");
  }

  private static Stream<Arguments> indexAndUpdateRequestedBeforeCompaction() {
    return Stream.of(HoodieIndex.IndexType.SIMPLE, HoodieIndex.IndexType.GLOBAL_SIMPLE)
        .flatMap(indexType -> Stream.of(Arguments.of(indexType, false), Arguments.of(indexType, true)));
  }

  /**
   * Updates keys of a file group around a compaction and then again after the compaction. An update
   * requested first holds off the compaction, so the compacted base file cannot keep the rows it
   * tombstones. An update of a file group whose compaction is already pending is refused, since its
   * positional deletes would land in the file slice of the compaction with positions in the base file
   * being compacted; it succeeds once the compaction completes.
   */
  @ParameterizedTest
  @MethodSource("indexAndUpdateRequestedBeforeCompaction")
  void testUpdatesAroundPendingCompaction(HoodieIndex.IndexType indexType, boolean updateRequestedBeforeCompaction) throws Exception {
    initTable(HoodieTableVersion.TEN);
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .withGlobalSimpleIndexUpdatePartitionPath(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .withWriteTableVersion(HoodieTableVersion.TEN.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
    writeAndCommit(client, insertTime, () -> client.insert(inserts, insertTime));
    String firstUpdateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> firstUpdates = dataGen.generateUpdates(firstUpdateTime, inserts.subList(0, 30));
    writeAndCommit(client, firstUpdateTime, () -> client.upsert(firstUpdates, firstUpdateTime));

    List<HoodieRecord> keysUnderCompaction = inserts.subList(30, 60);
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    if (updateRequestedBeforeCompaction) {
      WriteClientTestUtils.startCommitWithTime(client, updateTime);
      assertFalse(client.scheduleCompaction(Option.empty()).isPresent(),
          "compaction must not be scheduled while the update is pending");
      List<HoodieRecord> updates = dataGen.generateUpdates(updateTime, keysUnderCompaction);
      client.commit(updateTime, client.upsert(updates, updateTime));
      Option<String> compactionTime = client.scheduleCompaction(Option.empty());
      assertTrue(compactionTime.isPresent());
      client.commitCompaction(compactionTime.get(), client.compact(compactionTime.get()), Option.empty());
    } else {
      Option<String> compactionTime = client.scheduleCompaction(Option.empty());
      assertTrue(compactionTime.isPresent());
      String refusedUpdateTime = updateTime;
      WriteClientTestUtils.startCommitWithTime(client, refusedUpdateTime);
      List<HoodieRecord> refusedUpdates = dataGen.generateUpdates(refusedUpdateTime, keysUnderCompaction);
      Exception exception = assertThrows(Exception.class, () -> client.upsert(refusedUpdates, refusedUpdateTime));
      assertTrue(exceptionChainContains(exception, "while compaction " + compactionTime.get() + " is pending"),
          "an update of a file group under pending compaction must be refused but got: " + exception);
      String refusedDeleteTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, refusedDeleteTime);
      List<HoodieKey> refusedDeletes = keysUnderCompaction.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
      exception = assertThrows(Exception.class, () -> client.delete(refusedDeletes, refusedDeleteTime));
      assertTrue(exceptionChainContains(exception, "while compaction " + compactionTime.get() + " is pending"),
          "a delete from a file group under pending compaction must be refused but got: " + exception);
      client.commitCompaction(compactionTime.get(), client.compact(compactionTime.get()), Option.empty());
      updateTime = WriteClientTestUtils.createNewInstantTime();
      String retriedUpdateTime = updateTime;
      List<HoodieRecord> updates = dataGen.generateUpdates(retriedUpdateTime, keysUnderCompaction);
      writeAndCommit(client, retriedUpdateTime, () -> client.upsert(updates, retriedUpdateTime));
    }
    assertOnlyPositionalDeletes(config, HoodieTableVersion.TEN);
    String committedUpdateTime = updateTime;
    Map<String, GenericRecord> afterCompaction = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, afterCompaction.size());
    keysUnderCompaction.forEach(record -> assertEquals(committedUpdateTime,
        afterCompaction.get(record.getRecordKey()).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()));

    String nextUpdateTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> nextUpdates = dataGen.generateUpdates(nextUpdateTime, keysUnderCompaction);
    writeAndCommit(client, nextUpdateTime, () -> client.upsert(nextUpdates, nextUpdateTime));
    Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf, dataGen);
    assertEquals(100, recordMap.size());
    keysUnderCompaction.forEach(record -> assertEquals(nextUpdateTime,
        recordMap.get(record.getRecordKey()).get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString()));
    assertOnlyPositionalDeletes(config, HoodieTableVersion.TEN);
  }

  @Test
  public void testLogCompactionRejectedInDeleteInsertMode() throws Exception {
    HoodieWriteConfig config = buildConfig(true);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
        "000", 50, HoodieJavaWriteClient::upsert, false, false, 50, 100, 2,
        config.populateMetaFields(), INSTANT_GENERATOR);

    Exception exception =
        assertThrows(Exception.class, () -> client.scheduleLogCompaction(Option.empty()));
    assertTrue(
        exceptionChainContains(exception, HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()),
        "log compaction must be rejected in this write mode but got: " + exception);
  }

  /**
   * Prepped upserts arrive already tagged and skip the write helper, so they would append the
   * updates to the log as data blocks.
   */
  @Test
  void testPreppedUpsertRejectedInDeleteInsertMode() throws Exception {
    HoodieWriteConfig config = buildConfig(true);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);
    String updateTime = WriteClientTestUtils.createNewInstantTime();
    Exception exception = assertThrows(Exception.class, () ->
        updateBatch(config, client, updateTime, insertTime, Option.of(Arrays.asList(insertTime)),
            "000", 50, (writeClient, records, instantTime) -> {
              // The merge-on-read prepped executor expects the caller to move the instant to inflight
              writeClient.transitionInflight(instantTime);
              return writeClient.upsertPreppedRecords(records, instantTime);
            }, true, false, 50, 100, 2,
            config.populateMetaFields(), INSTANT_GENERATOR));
    assertTrue(
        exceptionChainContains(exception, HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()),
        "prepped upserts must be rejected in this write mode but got: " + exception);
  }

  /**
   * Prepped inserts and deletes reach the log writer with the caller's locations, which carry no base
   * file position when built the way Spark SQL builds them, from the file name meta field only.
   */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"INSERT_PREPPED", "DELETE_PREPPED"})
  void testPreppedInsertAndDeleteRejectedInDeleteInsertMode(WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = buildConfig(true);
    HoodieJavaWriteClient client = getHoodieWriteClient(config);

    String insertTime = WriteClientTestUtils.createNewInstantTime();
    insertBatch(config, client, insertTime, "000", 100, HoodieJavaWriteClient::insert,
        false, false, 100, 100, 1, Option.empty(), INSTANT_GENERATOR);

    Map<String, HoodieRecordLocation> locations = locationsWithoutPositions(config);
    String writeTime = WriteClientTestUtils.createNewInstantTime();
    List<HoodieRecord> records = operation == WriteOperationType.DELETE_PREPPED
        ? dataGen.generateUniqueDeleteRecords(writeTime, 10) : dataGen.generateUniqueUpdates(writeTime, 10);
    records.forEach(record -> {
      record.unseal();
      record.setCurrentLocation(locations.get(record.getRecordKey()));
      record.seal();
    });
    WriteClientTestUtils.startCommitWithTime(client, writeTime);
    client.transitionInflight(writeTime);
    Exception exception = assertThrows(Exception.class, () -> {
      if (operation == WriteOperationType.DELETE_PREPPED) {
        client.deletePrepped(records, writeTime);
      } else {
        client.insertPreppedRecords(records, writeTime);
      }
    });
    assertTrue(
        exceptionChainContains(exception, HoodieWriteConfig.WRITE_UPDATES_AS_DELETES_AND_INSERTS.key()),
        operation + " must be rejected in this write mode but got: " + exception);
  }

  @Test
  public void testEventTimeOrderingRejectedInDeleteInsertMode() throws Exception {
    String eventTimeBasePath = basePath + "_event_time";
    Properties properties = new Properties();
    properties.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(),
        RecordMergeMode.EVENT_TIME_ORDERING.name());
    properties.setProperty("hoodie.table.ordering.fields", "timestamp");
    HoodieTestUtils.init(storageConf, eventTimeBasePath, HoodieTableType.MERGE_ON_READ, properties);
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.SIMPLE)
        .withPath(eventTimeBasePath)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    try (HoodieJavaWriteClient client =
        new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConf), config)) {
      String writeTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, writeTime);
      List<HoodieRecord> records = (List<HoodieRecord>) (List<?>) dataGen.generateInserts(writeTime, 10);
      Exception exception =
          assertThrows(Exception.class, () -> client.upsert(records, writeTime));
      assertTrue(exceptionChainContains(exception, "commit-time ordering"),
          "event-time ordering must be rejected in this write mode but got: " + exception);
    }
  }

  private static Stream<Arguments> indexWithoutPositionsAndOperation() {
    return Stream.of(HoodieIndex.IndexType.INMEMORY)
        .flatMap(indexType -> Stream.of(WriteOperationType.UPSERT, WriteOperationType.INSERT, WriteOperationType.DELETE)
            .map(operation -> Arguments.of(indexType, operation)));
  }

  /**
   * The in-memory index tags records without positions and writes inserts to log files.
   */
  @ParameterizedTest
  @MethodSource("indexWithoutPositionsAndOperation")
  void testIndexWithoutPositionsRejectedInDeleteInsertMode(HoodieIndex.IndexType indexType,
                                                           WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType)
        .withProperties(keyGenProps())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertWriteRejected(config, operation, "produces record positions");
  }

  /**
   * A custom index class takes precedence over the index type, so the index type no longer describes
   * the index in use.
   */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"UPSERT", "INSERT", "DELETE"})
  void testCustomIndexClassRejectedInDeleteInsertMode(WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.SIMPLE)
            .withIndexClass(HoodieInMemoryHashIndex.class.getName()).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertWriteRejected(config, operation, HoodieIndexConfig.INDEX_CLASS_NAME.key());
  }

  @Test
  public void testDisabledRecordPositionsRejectedInDeleteInsertMode() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.SIMPLE)
        .withWriteRecordPositionsEnabled(false)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertWriteRejected(config, WriteOperationType.UPSERT, HoodieWriteConfig.WRITE_RECORD_POSITIONS.key());
  }

  @Test
  void testPartialUpdatesRejectedInDeleteInsertMode() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.SIMPLE)
        .withProps(Collections.singletonMap(HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key(), HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA))
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertWriteRejected(config, WriteOperationType.UPSERT, HoodieWriteConfig.WRITE_PARTIAL_UPDATE_SCHEMA.key());
  }

  /**
   * An update or delete of a table without a primary key whose writer generates the record keys cannot locate
   * the current version of a row by its key.
   */
  @ParameterizedTest
  @EnumSource(value = WriteOperationType.class, names = {"UPSERT", "DELETE"})
  void testKeylessTableRejectedInDeleteInsertMode(WriteOperationType operation) throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertWriteRejected(config, operation, "requires a table with a primary key");
  }

  /**
   * A record key declared by the table or by the writer lets updates and deletes through, and an insert locates
   * no existing row, so it needs none.
   */
  @ParameterizedTest
  @CsvSource({"true,false,UPSERT", "false,true,DELETE", "false,false,INSERT"})
  void testRecordKeyInDeleteInsertMode(boolean keyDeclaredByTable, boolean keyDeclaredByWriter,
                                       WriteOperationType operation) throws Exception {
    if (keyDeclaredByTable) {
      Properties properties = new Properties();
      properties.setProperty(HoodieTableConfig.RECORDKEY_FIELDS.key(), "_row_key");
      metaClient = HoodieTestUtils.init(storageConf, basePath, getTableType(), properties);
    }
    HoodieWriteConfig.Builder builder = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, HoodieIndex.IndexType.SIMPLE)
        .withWriteUpdatesAsDeletesAndInserts(true);
    HoodieWriteConfig config = (keyDeclaredByWriter ? builder.withProperties(keyGenProps()) : builder).build();
    assertDoesNotThrow(() -> BaseWriteHelper.validateUpdatesAsDeletesAndInserts(config, metaClient, operation));
  }

  /**
   * A version 6 writer does not write record positions, whether the table is at version 6 or at a
   * higher version it may still write to.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testWriteTableVersionSixRejectedInDeleteInsertMode(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    assertWriteRejected(buildConfig(true, HoodieTableVersion.SIX, 10), WriteOperationType.UPSERT,
        "write table version 8 or higher");
  }

  /**
   * Asserts that the write mode rejects the operation; a delete targets records written beforehand
   * with the write mode off.
   */
  private void assertWriteRejected(HoodieWriteConfig config, WriteOperationType operation, String expectedMessage) {
    List<HoodieKey> existingKeys = operation == WriteOperationType.DELETE
        ? insertWithoutDeleteInsertMode(config) : Collections.emptyList();
    try (HoodieJavaWriteClient client =
        new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConf), config)) {
      String writeTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, writeTime);
      List<HoodieRecord> records = (List<HoodieRecord>) (List<?>) dataGen.generateInserts(writeTime, 10);
      Exception exception = assertThrows(Exception.class, () -> {
        if (operation == WriteOperationType.UPSERT) {
          client.upsert(records, writeTime);
        } else if (operation == WriteOperationType.INSERT) {
          client.insert(records, writeTime);
        } else {
          client.delete(existingKeys, writeTime);
        }
      });
      assertTrue(exceptionChainContains(exception, expectedMessage),
          operation + " must be rejected in this write mode but got: " + exception);
    }
  }

  private List<HoodieKey> insertWithoutDeleteInsertMode(HoodieWriteConfig config) {
    HoodieWriteConfig insertConfig = HoodieWriteConfig.newBuilder().withProperties(config.getProps())
        .withWriteUpdatesAsDeletesAndInserts(false).build();
    try (HoodieJavaWriteClient client =
        new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConf), insertConfig)) {
      String insertTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, insertTime);
      List<HoodieRecord> records = (List<HoodieRecord>) (List<?>) dataGen.generateInserts(insertTime, 10);
      client.commit(insertTime, client.insert(records, insertTime));
      return records.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
    }
  }

  /**
   * Asserts that every log block of the latest file slices is a delete block whose record positions
   * point at the rows of the deleted keys in the slice's base file, and returns the number of deletes.
   * Version 10 writes native delete log files, earlier versions blocks of inline log files.
   */
  private long assertOnlyPositionalDeletes(HoodieWriteConfig config, HoodieTableVersion tableVersion) throws Exception {
    AtomicLong deleteRecordCount = new AtomicLong();
    forEachLatestLogBlock(config, (slice, logFile, block) -> {
      assertTrue(slice.getBaseFile().isPresent(), "a file group with delete logs must have a base file");
      assertEquals(tableVersion.greaterThanOrEquals(HoodieTableVersion.TEN),
          FSUtils.isNativeDeleteLogFile(logFile.getFileName()), "unexpected log file layout: " + logFile.getFileName());
      assertTrue(block instanceof HoodieDeleteBlock,
          "expected only delete blocks but got: " + block.getClass().getSimpleName() + " in " + logFile.getFileName());
      HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
      DeleteRecord[] deleteRecords = deleteBlock.getRecordsToDelete();
      deleteRecordCount.addAndGet(deleteRecords.length);
      List<Long> positions = deleteBlock.getRecordPositionList();
      assertEquals(deleteRecords.length, positions.size(),
          "every delete record must carry a position");
      // The i-th position belongs to the i-th delete record: inline delete blocks store both sorted by
      // position, and native delete log files store positions in delete-row order.
      List<String> baseFileKeys = readRecordKeys(config, slice.getBaseFile().get());
      for (int i = 0; i < positions.size(); i++) {
        long position = positions.get(i);
        assertTrue(position >= 0 && position < baseFileKeys.size(), "delete record position out of range: " + position);
        assertEquals(deleteRecords[i].getRecordKey(), baseFileKeys.get((int) position),
            "the base file row at a delete position must hold the deleted record key");
      }
      assertEquals(slice.getBaseFile().get().getCommitTime(),
          deleteBlock.getBaseFileInstantTimeOfPositions(),
          "positions must reference the file slice's base file");
    });
    return deleteRecordCount.get();
  }

  /**
   * Maps each record key to its file group the way Spark SQL tags prepped records, without a base
   * file position.
   */
  private Map<String, HoodieRecordLocation> locationsWithoutPositions(HoodieWriteConfig config) throws Exception {
    Map<String, HoodieRecordLocation> locations = new HashMap<>();
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      for (String partition : dataGen.getPartitionPaths()) {
        for (HoodieBaseFile baseFile : fsView.getLatestBaseFiles(partition).collect(Collectors.toList())) {
          HoodieRecordLocation location = new HoodieRecordLocation(baseFile.getCommitTime(), baseFile.getFileId());
          readRecordKeys(config, baseFile).forEach(key -> locations.put(key, location));
        }
      }
    }
    return locations;
  }

  private static Properties keyGenProps() {
    Properties properties = new Properties();
    properties.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    properties.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    return properties;
  }

  private static boolean exceptionChainContains(Throwable throwable, String text) {
    while (throwable != null) {
      if (throwable.getMessage() != null && throwable.getMessage().contains(text)) {
        return true;
      }
      throwable = throwable.getCause();
    }
    return false;
  }

  @FunctionalInterface
  private interface LogBlockVisitor {
    void visit(FileSlice slice, HoodieLogFile logFile, HoodieLogBlock block) throws Exception;
  }

  /**
   * Visits every block of every log file in the latest file slices through the log reader, which reads
   * inline log files and native log files alike.
   */
  private void forEachLatestLogBlock(HoodieWriteConfig config, LogBlockVisitor visitor) throws Exception {
    HoodieSchema schema = HoodieSchema.parse(config.getSchema());
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      for (String partition : dataGen.getPartitionPaths()) {
        for (FileSlice slice : fsView.getLatestFileSlices(partition).collect(Collectors.toList())) {
          for (HoodieLogFile logFile : slice.getLogFiles().collect(Collectors.toList())) {
            int blocks = 0;
            try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, schema)) {
              while (reader.hasNext()) {
                visitor.visit(slice, logFile, reader.next());
                blocks++;
              }
            }
            assertTrue(blocks > 0, "log file must hold at least one block: " + logFile.getFileName());
          }
        }
      }
    }
  }

  private Set<String> collectFileIds() throws Exception {
    try (SyncableFileSystemView fsView = getFileSystemView(metaClient.reloadActiveTimeline())) {
      return Arrays.stream(dataGen.getPartitionPaths())
          .flatMap(fsView::getLatestFileSlices)
          .map(FileSlice::getFileId)
          .collect(Collectors.toSet());
    }
  }

  /**
   * Returns the record keys of a base file in row order, so that a key's index is its row position.
   */
  private List<String> readRecordKeys(HoodieWriteConfig config, HoodieBaseFile baseFile) throws IOException {
    List<String> keys = new ArrayList<>();
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(storage)
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(config, new StoragePath(baseFile.getPath()));
         ClosableIterator<String> keyIterator = reader.getRecordKeyIterator()) {
      keyIterator.forEachRemaining(keys::add);
    }
    return keys;
  }
}
