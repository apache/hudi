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

package org.apache.hudi.common.table.read;

import org.apache.hudi.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.expression.Literal;
import org.apache.hudi.common.expression.Predicate;
import org.apache.hudi.common.expression.Predicates;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.common.table.HoodieTableConfig.ORDERING_FIELDS;
import static org.apache.hudi.common.table.log.block.HoodieLogBlock.HoodieLogBlockType.PARQUET_DATA_BLOCK;
import static org.apache.hudi.common.testutils.FileCreateUtilsLegacy.logFileName;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.HoodieTestUtils.DEFAULT_WRITE_TOKEN;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieFileGroupReaderNativeLogs extends HoodieFileGroupReaderTestHarness {

  @Override
  protected Properties getMetaProps() {
    Properties metaProps = super.getMetaProps();
    metaProps.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    metaProps.setProperty(ORDERING_FIELDS.key(), "timestamp");
    return metaProps;
  }

  @BeforeAll
  public static void setUp() throws IOException {
    properties.setProperty("hoodie.write.record.merge.mode", RecordMergeMode.EVENT_TIME_ORDERING.name());
    instantTimes = Arrays.asList("001", "002", "003", "004");
  }

  @BeforeEach
  public void initialize() throws Exception {
    setTableName(TestHoodieFileGroupReaderNativeLogs.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[] {PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    readerContext = new HoodieAvroReaderContext(
        storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty());
    setUpMockCommits();
  }

  @Test
  public void testPureNativeLogFiles() throws IOException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(basePath, storageConf);
    preparePartitionPath();

    HoodieLogFile nativeDataLog1 = createNativeDataLogFile("001", 1, records(Arrays.asList("1", "2", "3"), 2L, false));
    HoodieLogFile nativeDataLog2 = createNativeDataLogFile("002", 2, records(Arrays.asList("1", "4"), 4L, false));
    HoodieLogFile nativeDeleteLog = createNativeDeleteLogFile(hoodieStorage, "003", 3, records(Arrays.asList("2"), 3L, true));
    FileSlice fileSlice = createFileSlice(null, nativeDataLog1, nativeDataLog2, nativeDeleteLog);

    assertFileGroupRecords(fileSlice,
        Arrays.asList("1", "3", "4"),
        Arrays.asList(4L, 2L, 4L));
  }

  @Test
  public void testNativeLogFilesWithKeyFilters() throws IOException {
    preparePartitionPath();

    HoodieLogFile nativeDataLog1 = createNativeDataLogFile("001", 1, records(Arrays.asList("1", "2", "30"), 2L, false));
    HoodieLogFile nativeDataLog2 = createNativeDataLogFile("002", 2, records(Arrays.asList("1", "4", "31"), 4L, false));
    FileSlice fileSlice = createFileSlice(null, nativeDataLog1, nativeDataLog2);

    Predicate inPredicate = Predicates.in(
        Literal.from(ROW_KEY),
        Arrays.asList(Literal.from("1"), Literal.from("31")));
    assertFileGroupRecords(fileSlice, inPredicate,
        Arrays.asList("1", "31"),
        Arrays.asList(4L, 4L));

    Predicate prefixPredicate = Predicates.startsWithAny(
        Literal.from(ROW_KEY),
        Arrays.asList(Literal.from("3")));
    assertFileGroupRecords(fileSlice, prefixPredicate,
        Arrays.asList("30", "31"),
        Arrays.asList(2L, 4L));
  }

  @Test
  public void testBaseFileWithNativeLogFiles() throws IOException {
    preparePartitionPath();

    HoodieBaseFile baseFile = createBaseFile("001", records(Arrays.asList("1", "2", "3"), 2L, false));
    HoodieLogFile nativeDataLog1 = createNativeDataLogFile("002", 1, records(Arrays.asList("2"), 4L, false));
    HoodieLogFile nativeDataLog2 = createNativeDataLogFile("003", 2, records(Arrays.asList("4"), 3L, false));
    FileSlice fileSlice = createFileSlice(baseFile, nativeDataLog1, nativeDataLog2);

    assertFileGroupRecords(fileSlice,
        Arrays.asList("1", "2", "3", "4"),
        Arrays.asList(2L, 4L, 2L, 3L));
  }

  @Test
  public void testBaseFileWithNativeDeleteFiles() throws IOException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(basePath, storageConf);
    preparePartitionPath();

    HoodieBaseFile baseFile = createBaseFile("001", records(Arrays.asList("1", "2", "3", "4", "5"), 2L, false));
    HoodieLogFile nativeDeleteLog1 = createNativeDeleteLogFile(hoodieStorage, "002", 1, records(Arrays.asList("1", "2"), Arrays.asList(3L, 1L), true));
    HoodieLogFile nativeDeleteLog2 = createNativeDeleteLogFile(hoodieStorage, "003", 2, records(Arrays.asList("4"), 3L, true));
    FileSlice fileSlice = createFileSlice(baseFile, nativeDeleteLog1, nativeDeleteLog2);

    assertFileGroupRecords(fileSlice,
        Arrays.asList("2", "3", "5"),
        Arrays.asList(2L, 2L, 2L));
  }

  @Test
  public void testMixedNativeAndLegacyLogFiles() throws IOException, InterruptedException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(basePath, storageConf);
    preparePartitionPath();

    HoodieBaseFile baseFile = createBaseFile("001", records(Arrays.asList("1", "2", "3", "4"), 2L, false));
    Map<String, Long> keyToPositionMap = keyToPositionMap("1", "2", "3", "4", "5");
    HoodieLogFile legacyDataLog = createLegacyLogFile(
        hoodieStorage, "002", 1, records(Arrays.asList("1", "5"), 3L, false),
        PARQUET_DATA_BLOCK, keyToPositionMap);
    HoodieLogFile nativeDataLog = createNativeDataLogFile("003", 2, records(Arrays.asList("2", "6"), 4L, false));
    FileSlice fileSlice = createFileSlice(baseFile, legacyDataLog, nativeDataLog);

    assertFileGroupRecords(fileSlice,
        Arrays.asList("1", "2", "3", "4", "5", "6"),
        Arrays.asList(3L, 4L, 2L, 2L, 3L, 4L));
  }

  @Test
  public void testMixedLegacyLogFileAndNativeDeleteFile() throws IOException, InterruptedException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(basePath, storageConf);
    preparePartitionPath();

    HoodieBaseFile baseFile = createBaseFile("001", records(Arrays.asList("1", "2", "3", "4"), 2L, false));
    Map<String, Long> keyToPositionMap = keyToPositionMap("1", "2", "3", "4", "5");
    HoodieLogFile legacyDataLog = createLegacyLogFile(
        hoodieStorage, "002", 1, records(Arrays.asList("1", "5"), 4L, false),
        PARQUET_DATA_BLOCK, keyToPositionMap);
    HoodieLogFile nativeDeleteLog = createNativeDeleteLogFile(
        hoodieStorage, "003", 2, records(Arrays.asList("1", "3", "5"), Arrays.asList(3L, 3L, 5L), true));
    FileSlice fileSlice = createFileSlice(baseFile, legacyDataLog, nativeDeleteLog);

    assertFileGroupRecords(fileSlice,
        Arrays.asList("1", "2", "4"),
        Arrays.asList(4L, 2L, 2L));
  }

  @Test
  public void testMixedLegacyAndNativeLogsPreserveEventTimeDeleteOrdering() throws IOException, InterruptedException {
    HoodieStorage hoodieStorage = HoodieStorageUtils.getStorage(basePath, storageConf);
    FileSlice fileSlice = HoodieFileSliceTestUtils.getMixedLegacyAndNativeFileSlice(
        hoodieStorage, basePath, PARTITION_PATH, FILE_ID)
        .orElseThrow(() -> new IllegalArgumentException("FileSlice is not present"));

    assertFileGroupRecords(fileSlice,
        Arrays.asList("1", "2", "6", "7", "8", "9", "10"),
        Arrays.asList(4L, 4L, 2L, 2L, 2L, 2L, 2L));
  }

  private void assertFileGroupRecords(FileSlice fileSlice, List<String> expectedKeys, List<Long> expectedTimestamps)
      throws IOException {
    try (ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(fileSlice, false, false)) {
      assertRecords(iterator, expectedKeys, expectedTimestamps);
    }
  }

  private void assertFileGroupRecords(FileSlice fileSlice, Predicate predicate, List<String> expectedKeys, List<Long> expectedTimestamps)
      throws IOException {
    try (ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(fileSlice, predicate)) {
      assertRecords(iterator, expectedKeys, expectedTimestamps);
    }
  }

  private static void assertRecords(ClosableIterator<IndexedRecord> iterator, List<String> expectedKeys, List<Long> expectedTimestamps) {
    List<String> actualKeys = new ArrayList<>();
    List<Long> actualTimestamps = new ArrayList<>();
    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      actualKeys.add(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      actualTimestamps.add((Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    assertEquals(expectedKeys, actualKeys);
    assertEquals(expectedTimestamps, actualTimestamps);
  }

  private ClosableIterator<IndexedRecord> getFileGroupIterator(FileSlice fileSlice, Predicate predicate)
      throws IOException {
    HoodieReaderContext<IndexedRecord> originalReaderContext = readerContext;
    readerContext = new HoodieAvroReaderContext(
        storageConf, metaClient.getTableConfig(), Option.empty(), Option.of(predicate));
    try {
      return getFileGroupIterator(fileSlice, false, false);
    } finally {
      readerContext = originalReaderContext;
    }
  }

  private static FileSlice createFileSlice(HoodieBaseFile baseFile, HoodieLogFile... logFiles) {
    return new FileSlice(new HoodieFileGroupId(PARTITION_PATH, FILE_ID),
        baseFile == null ? null : baseFile.getCommitTime(),
        baseFile,
        Arrays.asList(logFiles));
  }

  private HoodieBaseFile createBaseFile(String instantTime, List<IndexedRecord> records) throws IOException {
    return HoodieFileSliceTestUtils.createBaseFile(
        partitionBasePath() + "/" + FSUtils.makeBaseFileName(instantTime, DEFAULT_WRITE_TOKEN, FILE_ID, ".parquet"),
        records,
        HOODIE_SCHEMA,
        instantTime);
  }

  private HoodieLogFile createNativeDataLogFile(String instantTime, int version, List<IndexedRecord> records)
      throws IOException {
    return HoodieFileSliceTestUtils.createNativeDataLogFile(
        partitionBasePath() + "/" + nativeLogFileName(instantTime, version, ".log.parquet"),
        records,
        HOODIE_SCHEMA,
        instantTime);
  }

  private HoodieLogFile createNativeDeleteLogFile(
      HoodieStorage hoodieStorage, String instantTime, int version, List<IndexedRecord> records) throws IOException {
    return HoodieFileSliceTestUtils.createNativeDeleteLogFile(
        hoodieStorage,
        partitionBasePath() + "/" + nativeLogFileName(instantTime, version, ".deletes.parquet"),
        records,
        HOODIE_SCHEMA,
        instantTime);
  }

  private HoodieLogFile createLegacyLogFile(
      HoodieStorage hoodieStorage,
      String instantTime,
      int version,
      List<IndexedRecord> records,
      HoodieLogBlockType blockType,
      Map<String, Long> keyToPositionMap
  ) throws IOException, InterruptedException {
    return HoodieFileSliceTestUtils.createLogFile(
        hoodieStorage,
        partitionBasePath() + "/" + logFileName(instantTime, FILE_ID, version),
        records,
        HOODIE_SCHEMA,
        FILE_ID,
        "001",
        instantTime,
        version,
        blockType,
        false,
        keyToPositionMap);
  }

  private static Map<String, Long> keyToPositionMap(String... keys) {
    Map<String, Long> keyToPositionMap = new HashMap<>();
    for (int i = 0; i < keys.length; i++) {
      keyToPositionMap.put(keys[i], (long) i);
    }
    return keyToPositionMap;
  }

  private void preparePartitionPath() {
    new File(partitionBasePath()).mkdirs();
  }

  private String partitionBasePath() {
    return basePath + "/" + PARTITION_PATH;
  }

  private static String nativeLogFileName(String instantTime, int version, String extension) {
    return String.format("%s_%s_%s_%d%s", FILE_ID, DEFAULT_WRITE_TOKEN, instantTime, version, extension);
  }

  private static List<IndexedRecord> records(List<String> keys, long timestamp, boolean isDelete) {
    List<Long> timestamps = new ArrayList<>();
    keys.forEach(key -> timestamps.add(timestamp));
    return records(keys, timestamps, isDelete);
  }

  private static List<IndexedRecord> records(List<String> keys, List<Long> timestamps, boolean isDelete) {
    List<IndexedRecord> records = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      records.add(HoodieFileSliceTestUtils.DATA_GEN.generateGenericRecord(
          keys.get(i), PARTITION_PATH, "rider." + keys.get(i), "driver." + keys.get(i), timestamps.get(i), isDelete, false));
    }
    return records;
  }
}
