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
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.HoodieJavaClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.hudi.common.testutils.HoodieTestUtils.INSTANT_GENERATOR;
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

    // Every log block written by the update must be a delete block carrying valid record positions
    // that reference the file slice's base file instant; no update record lands in a log. Version 10
    // writes the deletes as native delete log files, earlier versions as blocks of inline log files.
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
    assertEquals(50, deleteRecordCount.get(), "each update must produce exactly one positional delete");

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

  /**
   * Global indexes tag merge-on-read records through a lookup that drops the base file position.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"INMEMORY", "GLOBAL_SIMPLE"})
  void testIndexWithoutPositionsRejectedInDeleteInsertMode(HoodieIndex.IndexType indexType) throws Exception {
    Properties keyGenProps = new Properties();
    keyGenProps.setProperty(KeyGeneratorOptions.RECORDKEY_FIELD_NAME.key(), "_row_key");
    keyGenProps.setProperty(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), "partition_path");
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA, indexType)
        .withProperties(keyGenProps)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertUpsertRejected(config, "produces record positions");
  }

  @Test
  public void testDisabledRecordPositionsRejectedInDeleteInsertMode() throws Exception {
    HoodieWriteConfig config = getConfigBuilder(HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA,
        HoodieIndex.IndexType.SIMPLE)
        .withWriteRecordPositionsEnabled(false)
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    assertUpsertRejected(config, HoodieWriteConfig.WRITE_RECORD_POSITIONS.key());
  }

  /**
   * A version 6 writer does not write record positions, whether the table is at version 6 or at a
   * higher version it may still write to.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"SIX", "EIGHT"})
  void testWriteTableVersionSixRejectedInDeleteInsertMode(HoodieTableVersion tableVersion) throws Exception {
    initTable(tableVersion);
    assertUpsertRejected(buildConfig(true, HoodieTableVersion.SIX, 10), "write table version 8 or higher");
  }

  private void assertUpsertRejected(HoodieWriteConfig config, String expectedMessage) {
    try (HoodieJavaWriteClient client =
        new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(storageConf), config)) {
      String writeTime = WriteClientTestUtils.createNewInstantTime();
      WriteClientTestUtils.startCommitWithTime(client, writeTime);
      List<HoodieRecord> records = (List<HoodieRecord>) (List<?>) dataGen.generateInserts(writeTime, 10);
      Exception exception =
          assertThrows(Exception.class, () -> client.upsert(records, writeTime));
      assertTrue(exceptionChainContains(exception, expectedMessage),
          "upsert must be rejected in this write mode but got: " + exception);
    }
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
