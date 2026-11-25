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

package org.apache.hudi.common.table.log;

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMemoryConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieAvroDataBlock;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieDataBlock;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.io.SeekableDataInputStream;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.HOODIE_SCHEMA;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.DATA_GEN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reads multi-file, multi-block log files through {@link LogReadAheadStorage} with windows of different sizes and
 * checks the merged records match a read through the file system.
 */
class TestLogReadAheadStorage extends HoodieFileGroupReaderTestHarness {

  private static final String BASE_INSTANT = "001";
  private static final String FAILED_INSTANT = "004";
  private static final int BASE_RECORDS = 200;

  private final Map<String, Long> keyToPosition = new HashMap<>();
  private HoodieStorage storage;

  @Override
  protected Properties getMetaProps() {
    Properties metaProps = super.getMetaProps();
    metaProps.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.COMMIT_TIME_ORDERING.name());
    return metaProps;
  }

  @BeforeAll
  static void setUpInstants() {
    // The failed instant 004 has no completed commit; its blocks are rolled back by a rollback block of 005
    instantTimes = Arrays.asList(BASE_INSTANT, "002", "003", "005", "006");
  }

  @BeforeEach
  void initialize() throws Exception {
    setTableName(TestLogReadAheadStorage.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[] {PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    setUpMockCommits();
    storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    for (int i = 1; i <= BASE_RECORDS; i++) {
      keyToPosition.put(String.valueOf(i), (long) i - 1);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMergedReadMatchesSerialRead(boolean useRecordPositions) throws Exception {
    FileSlice fileSlice = writeFileSlice(useRecordPositions);
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    long total = logFiles.stream().mapToLong(HoodieLogFile::getFileSize).sum();
    long smallest = logFiles.stream().mapToLong(HoodieLogFile::getFileSize).min().getAsLong();
    long largest = logFiles.stream().mapToLong(HoodieLogFile::getFileSize).max().getAsLong();
    assertTrue(largest < total / 2, "The window between the largest file and the total needs room");

    List<String> expected = readMerged(fileSlice, storage, useRecordPositions, Long.MAX_VALUE);
    // Keys 1-15 and 190-195 are deleted
    assertEquals(BASE_RECORDS - 21, expected.size());

    // Smaller than every file: no file is fetched whole, and the block contents are fetched ahead of the merge
    LogReadAheadStorage.Stats small = assertReadAheadMatches(fileSlice, useRecordPositions, smallest / 2, expected);
    assertEquals(0, small.filesFetched);
    assertTrue(small.inlineReads > 0);
    // Larger than any file but smaller than all of them: files are fetched ahead of the header pass, and the merge
    // fetches the blocks of the files that could not be kept
    LogReadAheadStorage.Stats medium = assertReadAheadMatches(fileSlice, useRecordPositions, total / 2, expected);
    assertTrue(medium.filesFetched > 0);
    assertTrue(medium.blocksFetched > 0);
    // Larger than all of them: each file is fetched once and kept for the merge
    LogReadAheadStorage.Stats large = assertReadAheadMatches(fileSlice, useRecordPositions, total * 2, expected);
    assertEquals(logFiles.size(), large.filesFetched);
    assertEquals(0, large.blocksFetched);
    assertEquals(0, large.inlineReads);
  }

  @Test
  void testExhaustedExecutorCapReadsInLine() throws Exception {
    FileSlice fileSlice = writeFileSlice(false);
    List<String> expected = readMerged(fileSlice, storage, false, Long.MAX_VALUE);
    long cap = LogReadAheadStorage.getExecutorBudget();
    LogReadAheadStorage.setExecutorBudget(0);
    try {
      LogReadAheadStorage.Stats stats = assertReadAheadMatches(fileSlice, false, Long.MAX_VALUE / 4, expected);
      assertEquals(0, stats.filesFetched);
      assertEquals(0, stats.blocksFetched);
      assertTrue(stats.executorCapDenials > 0);
      assertTrue(stats.inlineReads > 0);
    } finally {
      LogReadAheadStorage.setExecutorBudget(cap);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testMergedRecordsSpillPastMergeBudget(boolean useRecordPositions) throws Exception {
    FileSlice fileSlice = writeFileSlice(useRecordPositions);
    List<String> expected = readMerged(fileSlice, storage, useRecordPositions, Long.MAX_VALUE);
    long total = fileSlice.getLogFiles().mapToLong(HoodieLogFile::getFileSize).sum();

    long mergeBudget = 4 * 1024;
    try (LogReadAheadStorage readAhead = LogReadAheadStorage.create(storage, logFilesOf(fileSlice), total / 2)) {
      assertEquals(expected, readMerged(fileSlice, readAhead, useRecordPositions, mergeBudget));
    }
    assertEquals(0, LogReadAheadStorage.executorBytesInUse());
  }

  @Test
  void testStreamsBehaveLikeFileSystemStreamsAtEndOfFile() throws Exception {
    FileSlice fileSlice = writeFileSlice(false);
    HoodieLogFile logFile = logFilesOf(fileSlice).get(0);
    long size = logFile.getFileSize();
    for (long window : new long[] {Long.MAX_VALUE / 4, 0}) {
      try (LogReadAheadStorage readAhead = LogReadAheadStorage.create(storage, Collections.singletonList(logFile), window);
           SeekableDataInputStream in = readAhead.openSeekable(logFile.getPath(), 4096, true)) {
        byte[] whole = new byte[(int) size];
        in.readFully(whole);
        assertEquals(size, in.getPos());
        assertEquals(-1, in.read());
        in.seek(size);
        assertEquals(-1, in.read(new byte[8], 0, 8));
        assertThrows(EOFException.class, () -> in.seek(size + 1));
        in.seek(0);
        byte[] again = new byte[(int) size];
        in.readFully(again);
        assertTrue(Arrays.equals(whole, again));
      }
    }
  }

  @Test
  void testFailedFetchIsReleasedAndReadInLine() throws Exception {
    FileSlice fileSlice = writeFileSlice(false);
    HoodieLogFile logFile = logFilesOf(fileSlice).get(0);
    List<String> expected = readBlocks(storage, logFile, null);
    // A recorded size past the end of the file makes the whole-file fetch fail
    HoodieLogFile oversized = new HoodieLogFile(logFile.getPath(), logFile.getFileSize() + 100);
    try (LogReadAheadStorage readAhead = LogReadAheadStorage.create(storage, Collections.singletonList(oversized), Long.MAX_VALUE / 4)) {
      assertEquals(expected, readBlocks(readAhead, oversized, null));
      assertEquals(1, readAhead.getStats().filesFetched);
      assertTrue(readAhead.getStats().inlineReads > 0);
      assertEquals(0, LogReadAheadStorage.executorBytesInUse());
    }
  }

  @Test
  void testBlockContentsReadAheadOfTheMerge() throws Exception {
    FileSlice fileSlice = writeFileSlice(false);
    List<HoodieLogFile> logFiles = logFilesOf(fileSlice);
    List<String> expected = new ArrayList<>();
    for (HoodieLogFile logFile : logFiles) {
      expected.addAll(readBlocks(storage, logFile, null));
    }
    long smallest = logFiles.stream().mapToLong(HoodieLogFile::getFileSize).min().getAsLong();
    try (LogReadAheadStorage readAhead = LogReadAheadStorage.create(storage, logFiles, smallest - 1)) {
      List<String> actual = new ArrayList<>();
      List<HoodieLogBlock> blocks = new ArrayList<>();
      for (HoodieLogFile logFile : logFiles) {
        try (HoodieLogFileReader reader = new HoodieLogFileReader(readAhead, logFile, HOODIE_SCHEMA, 4096)) {
          while (reader.hasNext()) {
            blocks.add(reader.next());
          }
        }
      }
      readAhead.planBlockReads(blocks);
      for (HoodieLogBlock block : blocks) {
        actual.add(describe(block));
      }
      assertEquals(expected, actual);
      LogReadAheadStorage.Stats stats = readAhead.getStats();
      assertEquals(0, stats.filesFetched);
      assertTrue(stats.blocksFetched > 0);
      assertTrue(stats.peakHeldBytes <= smallest - 1);
    }
    assertEquals(0, LogReadAheadStorage.executorBytesInUse());
  }

  private LogReadAheadStorage.Stats assertReadAheadMatches(FileSlice fileSlice, boolean useRecordPositions, long window,
                                                           List<String> expected) throws Exception {
    LogReadAheadStorage.Stats stats;
    try (LogReadAheadStorage readAhead = LogReadAheadStorage.create(storage, logFilesOf(fileSlice), window)) {
      assertEquals(expected, readMerged(fileSlice, readAhead, useRecordPositions, Long.MAX_VALUE), "window " + window);
      stats = readAhead.getStats();
      assertTrue(stats.peakHeldBytes <= window, "Held " + stats.peakHeldBytes + " bytes in a window of " + window);
    }
    assertEquals(0, LogReadAheadStorage.executorBytesInUse());
    return stats;
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Reads
  // ---------------------------------------------------------------------------------------------------------------

  private HoodieFileGroupReader<IndexedRecord> newReader(FileSlice fileSlice, HoodieStorage readStorage,
                                                         boolean useRecordPositions, long mergeBudget) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieMemoryConfig.MAX_MEMORY_FOR_MERGE.key(), String.valueOf(Math.min(mergeBudget, 1024L * 1024 * 1000)));
    props.setProperty(HoodieMemoryConfig.SPILLABLE_MAP_BASE_PATH.key(), basePath + "/" + HoodieTableMetaClient.TEMPFOLDER_NAME);
    props.setProperty(HoodieCommonConfig.SPILLABLE_DISK_MAP_TYPE.key(), ExternalSpillableMap.DiskMapType.BITCASK.name());
    return HoodieFileGroupReader.<IndexedRecord>builder()
        .withReaderContext(new HoodieAvroReaderContext(storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty()))
        .withHoodieTableMetaClient(metaClient)
        .withStorage(readStorage)
        .withLatestCommitTime("1000")
        .withBaseFileOption(fileSlice.getBaseFile())
        .withLogFiles(fileSlice.getLogFiles())
        .withPartitionPath(fileSlice.getPartitionPath())
        .withDataSchema(HOODIE_SCHEMA)
        .withRequestedSchema(HOODIE_SCHEMA)
        .withProps(props)
        .withShouldUseRecordPosition(useRecordPositions)
        .build();
  }

  private List<String> readMerged(FileSlice fileSlice, HoodieStorage readStorage, boolean useRecordPositions,
                                  long mergeBudget) throws IOException {
    List<String> rows = new ArrayList<>();
    try (HoodieFileGroupReader<IndexedRecord> reader = newReader(fileSlice, readStorage, useRecordPositions, mergeBudget);
         ClosableIterator<IndexedRecord> iterator = reader.getClosableIterator()) {
      while (iterator.hasNext()) {
        rows.add(iterator.next().toString());
      }
    }
    Collections.sort(rows);
    return rows;
  }

  private static List<String> readBlocks(HoodieStorage readStorage, HoodieLogFile logFile, List<HoodieLogBlock> out)
      throws IOException {
    List<String> blocks = new ArrayList<>();
    try (HoodieLogFileReader reader = new HoodieLogFileReader(readStorage, logFile, HOODIE_SCHEMA, 4096)) {
      while (reader.hasNext()) {
        HoodieLogBlock block = reader.next();
        if (out != null) {
          out.add(block);
        }
        blocks.add(describe(block));
      }
    }
    return blocks;
  }

  private static String describe(HoodieLogBlock block) throws IOException {
    StringBuilder description = new StringBuilder(block.getBlockType().name());
    if (block instanceof HoodieDeleteBlock) {
      for (DeleteRecord delete : ((HoodieDeleteBlock) block).getRecordsToDelete()) {
        description.append(',').append(delete.getRecordKey());
      }
    } else if (block instanceof HoodieDataBlock) {
      try (ClosableIterator<HoodieRecord<IndexedRecord>> records =
               ((HoodieDataBlock) block).getRecordIterator(HoodieRecord.HoodieRecordType.AVRO)) {
        while (records.hasNext()) {
          description.append(',').append(records.next().getData());
        }
      }
    }
    return description.toString();
  }

  // ---------------------------------------------------------------------------------------------------------------
  // Writes
  // ---------------------------------------------------------------------------------------------------------------

  /**
   * A base file of keys 1 to 200 and five log files:
   * <ul>
   *   <li>002: updates of keys 1-60, then of keys 61-120, in two data blocks</li>
   *   <li>003: deletes of keys 1-15, then updates of keys 150-200</li>
   *   <li>004: updates of keys 1-100 from a failed write</li>
   *   <li>005: a rollback of 004, then updates of keys 20-70</li>
   *   <li>006: updates of keys 180-200, then deletes of keys 190-195</li>
   * </ul>
   */
  private FileSlice writeFileSlice(boolean writePositions) throws Exception {
    String baseFilePath = basePath + "/" + FILE_ID + "_1-0-1_" + BASE_INSTANT + ".parquet";
    HoodieBaseFile baseFile = HoodieFileSliceTestUtils.createBaseFile(
        baseFilePath, records(1, BASE_RECORDS, BASE_INSTANT, 1L), HOODIE_SCHEMA, BASE_INSTANT);
    List<HoodieLogFile> logFiles = new ArrayList<>();
    logFiles.add(writeLogFile("002", 1,
        dataBlock("002", 1, 60, 2L, writePositions), dataBlock("002", 61, 120, 2L, writePositions)));
    logFiles.add(writeLogFile("003", 2,
        deleteBlock("003", 1, 15, writePositions), dataBlock("003", 150, 200, 3L, writePositions)));
    logFiles.add(writeLogFile(FAILED_INSTANT, 3, dataBlock(FAILED_INSTANT, 1, 100, 4L, writePositions)));
    logFiles.add(writeLogFile("005", 4, rollbackBlock("005", FAILED_INSTANT), dataBlock("005", 20, 70, 5L, writePositions)));
    logFiles.add(writeLogFile("006", 5,
        dataBlock("006", 180, 200, 6L, writePositions), deleteBlock("006", 190, 195, writePositions)));
    return new FileSlice(new HoodieFileGroupId(PARTITION_PATH, FILE_ID), BASE_INSTANT, baseFile, logFiles);
  }

  private static List<HoodieLogFile> logFilesOf(FileSlice fileSlice) {
    return fileSlice.getLogFiles().collect(Collectors.toList());
  }

  private HoodieLogFile writeLogFile(String instantTime, int version, HoodieLogBlock... blocks) throws Exception {
    StoragePath path;
    try (HoodieLogFormat.Writer writer = HoodieLogFormatWriter.builder()
        .withParentPath(new StoragePath(basePath))
        .withFileExtension(HoodieLogFile.DELTA_EXTENSION)
        .withLogFileId(FILE_ID)
        .withInstantTime(instantTime)
        .withLogVersion(version)
        .withStorage(storage)
        .build()) {
      for (HoodieLogBlock block : blocks) {
        writer.appendBlock(block);
      }
      path = writer.getLogFile().getPath();
    }
    return new HoodieLogFile(path, storage.getPathInfo(path).getLength());
  }

  private Map<HeaderMetadataType, String> header(String instantTime, boolean writePositions) {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    header.put(HeaderMetadataType.SCHEMA, HOODIE_SCHEMA.toString());
    if (writePositions) {
      header.put(HeaderMetadataType.BASE_FILE_INSTANT_TIME_OF_RECORD_POSITIONS, BASE_INSTANT);
    }
    return header;
  }

  private static List<IndexedRecord> records(int from, int to, String instantTime, long timestamp) {
    return IntStream.rangeClosed(from, to)
        .mapToObj(key -> (IndexedRecord) DATA_GEN.generateGenericRecord(
            String.valueOf(key), PARTITION_PATH, "rider-" + instantTime + "-" + key, "driver-" + instantTime, timestamp))
        .collect(Collectors.toList());
  }

  private HoodieDataBlock dataBlock(String instantTime, int from, int to, long timestamp, boolean writePositions) {
    List<HoodieRecord> records = records(from, to, instantTime, timestamp).stream()
        .map(record -> (HoodieRecord) new HoodieAvroIndexedRecord(record,
            new HoodieRecordLocation("", "", keyToPosition.get(record.get(HoodieFileSliceTestUtils.RECORD_KEY_INDEX).toString()))))
        .collect(Collectors.toList());
    return new HoodieAvroDataBlock(records, header(instantTime, writePositions), HoodieRecord.RECORD_KEY_METADATA_FIELD);
  }

  private HoodieDeleteBlock deleteBlock(String instantTime, int from, int to, boolean writePositions) {
    List<Pair<DeleteRecord, Long>> deletes = IntStream.rangeClosed(from, to)
        .mapToObj(key -> Pair.of(DeleteRecord.create(new HoodieKey(String.valueOf(key), PARTITION_PATH)),
            keyToPosition.get(String.valueOf(key))))
        .collect(Collectors.toList());
    return new HoodieDeleteBlock(deletes, header(instantTime, writePositions));
  }

  private static HoodieCommandBlock rollbackBlock(String instantTime, String targetInstantTime) {
    Map<HeaderMetadataType, String> header = new HashMap<>();
    header.put(HeaderMetadataType.INSTANT_TIME, instantTime);
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, targetInstantTime);
    header.put(HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlock.HoodieCommandBlockTypeEnum.ROLLBACK_BLOCK.ordinal()));
    return new HoodieCommandBlock(header);
  }
}
