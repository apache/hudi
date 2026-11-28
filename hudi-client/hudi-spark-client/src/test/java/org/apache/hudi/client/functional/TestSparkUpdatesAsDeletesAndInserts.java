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

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.core.io.storage.HoodieFileReader;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.testutils.GenericRecordValidationTestUtils;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies hoodie.write.updates.as.deletes.and.inserts through the Spark write path: updates on a
 * merge-on-read table decompose into positional deletes plus inserts, repeated updates of the same
 * keys resolve to a single live copy, and log files carry only deletes.
 */
@Tag("functional")
public class TestSparkUpdatesAsDeletesAndInserts extends SparkClientFunctionalTestHarness {

  @ParameterizedTest
  @EnumSource(value = HoodieTableVersion.class, names = {"EIGHT", "NINE", "TEN"})
  void testMergeOnReadUpdatesDecomposeIntoDeletesAndInserts(HoodieTableVersion tableVersion) throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion.versionCode()));
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, tableProperties);
    HoodieWriteConfig config = getConfigBuilder(true, HoodieIndex.IndexType.SIMPLE)
        .withProperties(keyGenProps())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
      JavaRDD<WriteStatus> insertStatuses = client.upsert(jsc().parallelize(inserts, 1), insertTime);
      client.commit(insertTime, insertStatuses);

      // Repeated rounds update every key: from the second round on, the simple index sees a key
      // in its tombstoned old file group and its current one, and each round must still produce
      // exactly one positional delete and one insert per key.
      for (int round = 0; round < 2; round++) {
        String updateTime = client.startCommit();
        List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 100);
        List<WriteStatus> statuses = client.upsert(jsc().parallelize(updates, 1), updateTime).collect();
        client.commit(updateTime, jsc().parallelize(statuses, 1));
        long deletes = statuses.stream().mapToLong(status -> status.getStat().getNumDeletes()).sum();
        long insertCount = statuses.stream().mapToLong(status -> status.getStat().getNumInserts()).sum();
        long updateWrites = statuses.stream().mapToLong(status -> status.getStat().getNumUpdateWrites()).sum();
        assertEquals(100, deletes, "each key must be deleted exactly once per round");
        assertEquals(100, insertCount, "each key must be re-inserted exactly once per round");
        assertEquals(0, updateWrites, "no update record must be written in this mode");
      }

      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion());
      // Both rounds tombstone 100 records each in file groups that remain the latest slices.
      assertEquals(200, assertOnlyPositionalDeletes(config, metaClient, tableVersion),
          "each update must produce exactly one positional delete");
    }
  }

  private static Stream<Arguments> globalIndexAndTableVersion() {
    return Stream.of(HoodieIndex.IndexType.GLOBAL_SIMPLE, HoodieIndex.IndexType.GLOBAL_BLOOM, HoodieIndex.IndexType.RECORD_INDEX)
        .flatMap(indexType -> Stream.of(HoodieTableVersion.EIGHT, HoodieTableVersion.NINE, HoodieTableVersion.TEN)
            .map(tableVersion -> Arguments.of(indexType, tableVersion)));
  }

  /**
   * With a global index, an update within a partition, an update that moves a key to another partition
   * and a delete each tombstone the row of the key's current copy by position, so every key keeps a
   * single live copy across partitions.
   */
  @ParameterizedTest
  @MethodSource("globalIndexAndTableVersion")
  void testGlobalIndexUpdatesWithinAndAcrossPartitions(HoodieIndex.IndexType indexType, HoodieTableVersion tableVersion)
      throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.VERSION.key(), String.valueOf(tableVersion.versionCode()));
    // A global index moves a key to its new partition only on a partitioned table
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, tableProperties);
    HoodieWriteConfig config = getConfigBuilder(true, indexType)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(keyGenProps()).withIndexType(indexType)
            .withGlobalSimpleIndexUpdatePartitionPath(true)
            .withGlobalBloomIndexUpdatePartitionPath(true)
            .withRecordIndexUpdatePartitionPath(true).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withEnableGlobalRecordLevelIndex(indexType == HoodieIndex.IndexType.RECORD_INDEX).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .withWriteTableVersion(tableVersion.versionCode())
        .withAutoUpgradeVersion(false)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
      assertDeletesAndInserts(0, 100, writeAndCommit(client, insertTime, client.insert(jsc().parallelize(inserts, 1), insertTime)));

      String updateTime = client.startCommit();
      List<HoodieRecord> updates = dataGen.generateUniqueUpdates(updateTime, 30);
      assertDeletesAndInserts(30, 30, writeAndCommit(client, updateTime, client.upsert(jsc().parallelize(updates, 1), updateTime)));

      // Some moved keys were updated above, so their old partition also holds a tombstoned copy
      String newPartition = HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH;
      String moveTime = client.startCommit();
      List<HoodieRecord> moves = dataGen.generateUpdatesForDifferentPartition(moveTime,
          inserts.stream().filter(r -> !r.getPartitionPath().equals(newPartition)).limit(20).collect(Collectors.toList()),
          0L, newPartition);
      assertDeletesAndInserts(20, 20, writeAndCommit(client, moveTime, client.upsert(jsc().parallelize(moves, 1), moveTime)));

      String deleteTime = client.startCommit();
      List<HoodieKey> deletedKeys = moves.stream().limit(10).map(HoodieRecord::getKey).collect(Collectors.toList());
      assertDeletesAndInserts(10, 0, writeAndCommit(client, deleteTime, client.delete(jsc().parallelize(deletedKeys, 1), deleteTime)));

      metaClient = HoodieTableMetaClient.reload(metaClient);
      assertEquals(tableVersion, metaClient.getTableConfig().getTableVersion());
      assertEquals(60, assertOnlyPositionalDeletes(config, metaClient, tableVersion),
          "every update, move and delete must produce exactly one positional delete");
      // Collecting the records by key fails on a key with copies in two partitions
      Map<String, GenericRecord> recordMap = GenericRecordValidationTestUtils.getRecordsMap(config, storageConf(), dataGen);
      assertEquals(90, recordMap.size());
      deletedKeys.forEach(key -> assertFalse(recordMap.containsKey(key.getRecordKey()), "deleted key is still live: " + key));
      moves.stream().skip(10).forEach(record -> {
        GenericRecord live = recordMap.get(record.getRecordKey());
        assertEquals(newPartition, live.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString());
        assertEquals(moveTime, live.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
      });
    }
  }

  /**
   * An upsert or delete that locates records in a file group with a pending compaction is refused,
   * since its positional deletes would land in the file slice of the compaction with positions in the
   * base file being compacted.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"SIMPLE", "GLOBAL_SIMPLE"})
  void testWriteToFileGroupUnderPendingCompactionRejected(HoodieIndex.IndexType indexType) throws IOException {
    getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());
    HoodieWriteConfig config = getConfigBuilder(true, indexType)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(indexType)
            .withGlobalSimpleIndexUpdatePartitionPath(false).build())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder().withMaxNumDeltaCommitsBeforeCompaction(1).build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
      writeAndCommit(client, insertTime, client.insert(jsc().parallelize(inserts, 1), insertTime));
      String firstUpdateTime = client.startCommit();
      List<HoodieRecord> firstUpdates = dataGen.generateUpdates(firstUpdateTime, inserts.subList(0, 30));
      writeAndCommit(client, firstUpdateTime, client.upsert(jsc().parallelize(firstUpdates, 1), firstUpdateTime));

      Option<String> compactionTime = client.scheduleCompaction(Option.empty());
      assertTrue(compactionTime.isPresent());
      List<HoodieRecord> keysUnderCompaction = inserts.subList(30, 60);
      String refusedUpdateTime = client.startCommit();
      List<HoodieRecord> refusedUpdates = dataGen.generateUpdates(refusedUpdateTime, keysUnderCompaction);
      Exception exception = assertThrows(Exception.class,
          () -> client.upsert(jsc().parallelize(refusedUpdates, 1), refusedUpdateTime).collect());
      assertTrue(exceptionChainContains(exception, "while compaction " + compactionTime.get() + " is pending"),
          "an update of a file group under pending compaction must be refused but got: " + exception);
      String refusedDeleteTime = client.startCommit();
      List<HoodieKey> refusedDeletes = keysUnderCompaction.stream().map(HoodieRecord::getKey).collect(Collectors.toList());
      exception = assertThrows(Exception.class,
          () -> client.delete(jsc().parallelize(refusedDeletes, 1), refusedDeleteTime).collect());
      assertTrue(exceptionChainContains(exception, "while compaction " + compactionTime.get() + " is pending"),
          "a delete from a file group under pending compaction must be refused but got: " + exception);
    }
  }

  private static Stream<Arguments> indexAndWriteMode() {
    Stream<Arguments> deleteInsertMode = Stream.of("SIMPLE", "BLOOM", "GLOBAL_SIMPLE", "GLOBAL_BLOOM", "RECORD_INDEX")
        .map(index -> Arguments.of(index, true));
    // Outside the mode a global index tags the position of every record it locates only without the merged lookup of
    // partition path updates, which the global bloom index then skips
    Stream<Arguments> updateMode = Stream.of("SIMPLE", "BLOOM", "GLOBAL_BLOOM")
        .map(index -> Arguments.of(index, false));
    return Stream.concat(deleteInsertMode, updateMode);
  }

  /**
   * Each index type the update-as-delete-insert write mode allows writes the base file position of each record an
   * upsert updates or deletes, and of each key a delete removes, to the log of a merge-on-read table, and so do the
   * bloom, simple and global bloom indexes outside the mode.
   */
  @ParameterizedTest
  @MethodSource("indexAndWriteMode")
  void testUpdatesAndDeletesCarryRecordPositions(String index, boolean updatesAsDeletesAndInserts) throws IOException {
    Properties tableProperties = new Properties();
    tableProperties.setProperty(HoodieTableConfig.PARTITION_FIELDS.key(), "partition_path");
    HoodieTableMetaClient metaClient = getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, tableProperties);
    HoodieIndex.IndexType indexType = HoodieIndex.IndexType.valueOf(index);
    HoodieWriteConfig config = getConfigBuilder(true, HoodieIndex.IndexType.SIMPLE)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(keyGenProps()).withIndexType(indexType)
            .withGlobalBloomIndexUpdatePartitionPath(updatesAsDeletesAndInserts).build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder()
            .withEnableGlobalRecordLevelIndex(indexType == HoodieIndex.IndexType.RECORD_INDEX).build())
        .withWriteUpdatesAsDeletesAndInserts(updatesAsDeletesAndInserts)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 100);
      writeAndCommit(client, insertTime, client.insert(jsc().parallelize(inserts, 1), insertTime));

      // Each key is updated or deleted once, so each write locates a single copy of it
      String upsertTime = client.startCommit();
      List<HoodieRecord> upserts = new ArrayList<>(dataGen.generateDeletesFromExistingRecords(inserts.subList(0, 10)));
      upserts.addAll(dataGen.generateUpdates(upsertTime, inserts.subList(10, 40)));
      writeAndCommit(client, upsertTime, client.upsert(jsc().parallelize(upserts, 1), upsertTime));

      String deleteTime = client.startCommit();
      List<HoodieKey> deletes = inserts.subList(40, 50).stream().map(HoodieRecord::getKey).collect(Collectors.toList());
      writeAndCommit(client, deleteTime, client.delete(jsc().parallelize(deletes, 1), deleteTime));

      metaClient = HoodieTableMetaClient.reload(metaClient);
      long[] deletesAndDataRecords = assertLogRecordsCarryPositions(config, metaClient);
      assertEquals(updatesAsDeletesAndInserts ? 50 : 20, deletesAndDataRecords[0]);
      assertEquals(updatesAsDeletesAndInserts ? 0 : 30, deletesAndDataRecords[1]);
      assertEquals(80, GenericRecordValidationTestUtils.getRecordsMap(config, storageConf(), dataGen).size());
    }
  }

  private List<WriteStatus> writeAndCommit(SparkRDDWriteClient client, String instantTime, JavaRDD<WriteStatus> writeStatuses) {
    List<WriteStatus> statuses = writeStatuses.collect();
    client.commit(instantTime, jsc().parallelize(statuses, 1));
    return statuses;
  }

  private static void assertDeletesAndInserts(long expectedDeletes, long expectedInserts, List<WriteStatus> statuses) {
    assertEquals(expectedDeletes, statuses.stream().mapToLong(status -> status.getStat().getNumDeletes()).sum());
    assertEquals(expectedInserts, statuses.stream().mapToLong(status -> status.getStat().getNumInserts()).sum());
    assertEquals(0, statuses.stream().mapToLong(status -> status.getStat().getNumUpdateWrites()).sum(),
        "no update record must be written in this mode");
  }

  /**
   * Indexes that can index log files write merge-on-read inserts to logs, and the bucket index also
   * routes inserts into the existing file group of their bucket.
   */
  @ParameterizedTest
  @EnumSource(value = HoodieIndex.IndexType.class, names = {"BUCKET", "INMEMORY"})
  void testInsertWithIndexWritingLogsRejectedInDeleteInsertMode(HoodieIndex.IndexType indexType) throws IOException {
    getHoodieMetaClient(HoodieTableType.MERGE_ON_READ, new Properties());
    HoodieWriteConfig config = getConfigBuilder(true, HoodieIndex.IndexType.SIMPLE)
        .withProperties(keyGenProps())
        .withIndexConfig(HoodieIndexConfig.newBuilder().fromProperties(keyGenProps()).withIndexType(indexType).withBucketNum("1").build())
        .withWriteUpdatesAsDeletesAndInserts(true)
        .build();
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    try (SparkRDDWriteClient client = getHoodieWriteClient(config)) {
      String insertTime = client.startCommit();
      List<HoodieRecord> inserts = dataGen.generateInserts(insertTime, 10);
      Exception exception = assertThrows(Exception.class,
          () -> client.commit(insertTime, client.insert(jsc().parallelize(inserts, 1), insertTime)));
      assertTrue(exceptionChainContains(exception, "produces record positions"),
          "inserts with the " + indexType + " index must be rejected in this write mode but got: " + exception);
    }
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

  /**
   * Asserts that every log file of the latest file slices belongs to a slice with a base file and holds
   * only delete blocks whose positions point at the rows of the deleted keys in that base file, and
   * returns the number of deletes. Version 10 writes native delete log files, earlier versions inline
   * log files, so the blocks are read through the log reader.
   */
  private long assertOnlyPositionalDeletes(HoodieWriteConfig config, HoodieTableMetaClient metaClient,
                                           HoodieTableVersion tableVersion) throws IOException {
    HoodieSchema schema = HoodieSchema.parse(config.getSchema());
    long deleteRecords = 0;
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(), metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
    try {
      List<FileSlice> slicesWithLogs = Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
          .flatMap(fsView::getLatestFileSlices)
          .filter(FileSlice::hasLogFiles)
          .collect(Collectors.toList());
      assertTrue(!slicesWithLogs.isEmpty(), "the deletes must land in log files");
      for (FileSlice slice : slicesWithLogs) {
        assertTrue(slice.getBaseFile().isPresent(),
            "a file group with delete logs must have a base file");
        List<String> baseFileKeys = readRecordKeys(config, slice.getBaseFile().get());
        for (HoodieLogFile logFile : slice.getLogFiles().collect(Collectors.toList())) {
          assertEquals(tableVersion.greaterThanOrEquals(HoodieTableVersion.TEN),
              FSUtils.isNativeDeleteLogFile(logFile.getFileName()), "unexpected log file layout: " + logFile.getFileName());
          long logFileDeletes = deleteRecords;
          try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, schema)) {
            while (reader.hasNext()) {
              HoodieLogBlock block = reader.next();
              assertTrue(block instanceof HoodieDeleteBlock,
                  "expected only delete blocks but got: " + block.getClass().getSimpleName() + " in " + logFile.getFileName());
              HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
              DeleteRecord[] deletes = deleteBlock.getRecordsToDelete();
              List<Long> positions = deleteBlock.getRecordPositionList();
              assertEquals(deletes.length, positions.size(),
                  "every delete record must carry a position");
              for (int i = 0; i < positions.size(); i++) {
                long position = positions.get(i);
                assertTrue(position >= 0 && position < baseFileKeys.size(), "delete record position out of range: " + position);
                assertEquals(deletes[i].getRecordKey(), baseFileKeys.get((int) position),
                    "the base file row at a delete position must hold the deleted record key");
              }
              assertEquals(slice.getBaseFile().get().getCommitTime(), deleteBlock.getBaseFileInstantTimeOfPositions(),
                  "positions must reference the file slice's base file");
              deleteRecords += positions.size();
            }
          }
          assertTrue(deleteRecords > logFileDeletes, "log file must hold deletes: " + logFile.getFileName());
        }
      }
    } finally {
      fsView.close();
    }
    return deleteRecords;
  }

  /**
   * Asserts that every log block of the latest file slices carries the position of each of its records in the base
   * file of its slice, and returns the number of records the delete blocks and the data blocks hold.
   */
  private long[] assertLogRecordsCarryPositions(HoodieWriteConfig config, HoodieTableMetaClient metaClient) throws IOException {
    HoodieSchema schema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(config.getSchema()));
    long[] deletesAndDataRecords = new long[2];
    HoodieTableFileSystemView fsView = HoodieTableFileSystemView.fileListingBasedFileSystemView(
        context(), metaClient, metaClient.getActiveTimeline().filterCompletedInstants());
    try {
      List<FileSlice> slicesWithLogs = Arrays.stream(HoodieTestDataGenerator.DEFAULT_PARTITION_PATHS)
          .flatMap(fsView::getLatestFileSlices)
          .filter(FileSlice::hasLogFiles)
          .collect(Collectors.toList());
      assertFalse(slicesWithLogs.isEmpty(), "the updates and deletes must land in log files");
      for (FileSlice slice : slicesWithLogs) {
        assertTrue(slice.getBaseFile().isPresent(), "a file group with logs must have a base file");
        List<String> baseFileKeys = readRecordKeys(config, slice.getBaseFile().get());
        for (HoodieLogFile logFile : slice.getLogFiles().collect(Collectors.toList())) {
          try (HoodieLogFormat.Reader reader = HoodieLogFormat.newReader(metaClient, logFile, schema)) {
            while (reader.hasNext()) {
              HoodieLogBlock block = reader.next();
              List<String> keys = new ArrayList<>();
              if (block instanceof HoodieDeleteBlock) {
                Arrays.stream(((HoodieDeleteBlock) block).getRecordsToDelete()).forEach(delete -> keys.add(delete.getRecordKey()));
                deletesAndDataRecords[0] += keys.size();
              } else {
                assertTrue(block instanceof HoodieDataBlock, "unexpected block " + block.getBlockType() + " in " + logFile.getFileName());
                try (ClosableIterator<HoodieRecord<IndexedRecord>> records = ((HoodieDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
                  records.forEachRemaining(record -> keys.add(((GenericRecord) record.getData()).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString()));
                }
                deletesAndDataRecords[1] += keys.size();
              }
              List<Long> positions = block.getRecordPositionList();
              assertEquals(keys.size(), positions.size(),
                  "every record of a " + block.getBlockType() + " must carry a position: " + logFile.getFileName());
              assertEquals(slice.getBaseFile().get().getCommitTime(), block.getBaseFileInstantTimeOfPositions(),
                  "positions must reference the file slice's base file");
              assertEquals(new HashSet<>(keys), positions.stream().map(position -> baseFileKeys.get(position.intValue())).collect(Collectors.toSet()),
                  "the base file rows at the positions of a block must hold its record keys");
            }
          }
        }
      }
    } finally {
      fsView.close();
    }
    return deletesAndDataRecords;
  }

  /**
   * Returns the record keys of a base file in row order, so that a key's index is its row position.
   */
  private List<String> readRecordKeys(HoodieWriteConfig config, HoodieBaseFile baseFile) throws IOException {
    List<String> keys = new ArrayList<>();
    try (HoodieFileReader reader = HoodieIOFactory.getIOFactory(hoodieStorage())
        .getReaderFactory(HoodieRecord.HoodieRecordType.AVRO)
        .getFileReader(config, new StoragePath(baseFile.getPath()));
         ClosableIterator<String> keyIterator = reader.getRecordKeyIterator()) {
      keyIterator.forEachRemaining(keys::add);
    }
    return keys;
  }
}
