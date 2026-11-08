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

import org.apache.hudi.common.avro.HoodieAvroReaderContext;
import org.apache.hudi.common.config.HoodieReaderConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import static org.apache.hudi.common.table.HoodieTableConfig.ORDERING_FIELDS;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.UPDATE;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests {@link HoodieRecordReader#getLogRecordsMap()} as implemented by {@link HoodieFileGroupReader},
 * which indexes the merged log records of a file slice without draining the base-plus-log iterator.
 */
class TestHoodieFileGroupReaderLogRecords extends HoodieFileGroupReaderTestHarness {

  // Number of record sets (base file plus log files) making up the file slice under test.
  private static final int NUM_FILES = 3;

  @Override
  protected Properties getMetaProps() {
    Properties metaProps = super.getMetaProps();
    metaProps.setProperty(HoodieTableConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.EVENT_TIME_ORDERING.name());
    metaProps.setProperty(ORDERING_FIELDS.key(), "timestamp");
    return metaProps;
  }

  @BeforeAll
  static void setUp() {
    properties.setProperty("hoodie.write.record.merge.mode", RecordMergeMode.EVENT_TIME_ORDERING.name());

    // -------------------------------------------------------------
    // 1. The base file holds keys 1 to 5 at ordering value 2.
    // 2. The first log file updates keys 1 to 3 at ordering value 3.
    // 3. The second log file updates keys 2 to 4 at ordering value 4,
    //    which wins over the first log file for keys 2 and 3.
    // So the merged log records are {1 -> 3, 2 -> 4, 3 -> 4, 4 -> 4},
    // and key 5, which only ever appears in the base file, is absent.
    // -------------------------------------------------------------
    keyRanges = Arrays.asList(
        new HoodieFileSliceTestUtils.KeyRange(1, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 3),
        new HoodieFileSliceTestUtils.KeyRange(2, 4));
    timestamps = Arrays.asList(2L, 3L, 4L);
    operationTypes = Arrays.asList(INSERT, UPDATE, UPDATE);
    instantTimes = Arrays.asList("001", "002", "003");
  }

  @BeforeEach
  void initialize() throws Exception {
    setTableName(TestHoodieFileGroupReaderLogRecords.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[] {PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    readerContext = new HoodieAvroReaderContext(
        storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty());
    setUpMockCommits();
  }

  @Test
  void testLogRecordsMapIsKeyedByRecordKey() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(false, false, false);
    try (HoodieFileGroupReader<IndexedRecord> reader = getFileGroupReader(NUM_FILES, false, false)) {
      Map<Serializable, BufferedRecord<IndexedRecord>> logRecords = reader.getLogRecordsMap();

      Map<Serializable, Long> expected = new HashMap<>();
      expected.put("1", 3L);
      expected.put("2", 4L);
      expected.put("3", 4L);
      expected.put("4", 4L);
      assertEquals(expected, orderingValuesOf(logRecords));

      // Key 5 only exists in the base file, so it is not a log record.
      assertFalse(logRecords.containsKey("5"));
      // The map is the reader's own index, so callers get it read-only.
      assertThrows(UnsupportedOperationException.class, () -> logRecords.remove("1"));
      logRecords.forEach((key, record) -> {
        assertEquals(key, record.getRecordKey());
        assertFalse(record.isDelete());
      });
    }
  }

  @Test
  void testLogRecordsMapIsKeyedByPositionWhenMergingOnPositions() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(true, true, true);
    try (HoodieFileGroupReader<IndexedRecord> reader = getFileGroupReader(NUM_FILES, true, false)) {
      Map<Serializable, BufferedRecord<IndexedRecord>> logRecords = reader.getLogRecordsMap();

      // Positions are zero-based indexes into the base file, so keys 1 to 4 map to positions 0 to 3.
      Map<Serializable, Long> expected = new HashMap<>();
      expected.put(0L, 3L);
      expected.put(1L, 4L);
      expected.put(2L, 4L);
      expected.put(3L, 4L);
      assertEquals(expected, orderingValuesOf(logRecords));

      // Position 4 belongs to key 5, which only exists in the base file.
      assertFalse(logRecords.containsKey(4L));
      Map<Serializable, String> expectedRecordKeys = new HashMap<>();
      expectedRecordKeys.put(0L, "1");
      expectedRecordKeys.put(1L, "2");
      expectedRecordKeys.put(2L, "3");
      expectedRecordKeys.put(3L, "4");
      Map<Serializable, String> actualRecordKeys = new HashMap<>();
      logRecords.forEach((key, record) -> actualRecordKeys.put(key, record.getRecordKey()));
      assertEquals(expectedRecordKeys, actualRecordKeys);
    }
  }

  @Test
  void testLogRecordsMapIsEmptyWithoutLogFiles() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(false, false, false);
    // A slice of just the base file has nothing to merge, so there are no log records to index.
    try (HoodieRecordReader<IndexedRecord> reader = getFileGroupReader(1, false, false)) {
      assertTrue(reader.getLogRecordsMap().isEmpty());
      try (ClosableIterator<BufferedRecord<IndexedRecord>> iterator = reader.getLogRecordsOnly()) {
        assertFalse(iterator.hasNext());
      }
    }
  }

  @Test
  void testSkipMergeReadRejectsBothLogRecordAccessors() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(false, false, false);
    properties.setProperty(HoodieReaderConfig.MERGE_TYPE.key(), HoodieReaderConfig.REALTIME_SKIP_MERGE);
    try (HoodieRecordReader<IndexedRecord> reader = getFileGroupReader(NUM_FILES, false, false)) {
      // Reading with merging skipped leaves the log records unindexed, so an empty map would be a
      // wrong answer. Both accessors have to refuse rather than under-report.
      assertThrows(UnsupportedOperationException.class, reader::getLogRecordsMap);
      assertThrows(UnsupportedOperationException.class, reader::getLogRecordsOnly);
    } finally {
      properties.remove(HoodieReaderConfig.MERGE_TYPE.key());
    }
  }

  @Test
  void testMergedReadIsUnaffectedByReadingTheLogRecordsMap() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(false, false, false);
    try (HoodieFileGroupReader<IndexedRecord> reader = getFileGroupReader(NUM_FILES, false, false)) {
      assertEquals(4, reader.getLogRecordsMap().size());

      // Every key survives the merge whether or not the log records were lost, so assert the
      // ordering values: losing them would leave the base file's 2 behind on keys 1 to 4.
      Map<String, Long> expected = new TreeMap<>();
      expected.put("1", 3L);
      expected.put("2", 4L);
      expected.put("3", 4L);
      expected.put("4", 4L);
      expected.put("5", 2L);
      Map<String, Long> merged = new TreeMap<>();
      try (ClosableIterator<IndexedRecord> iterator = reader.getClosableIterator()) {
        while (iterator.hasNext()) {
          IndexedRecord record = iterator.next();
          merged.put(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString(),
              (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
        }
      }
      assertEquals(expected, merged);
    }
  }

  private static Map<Serializable, Long> orderingValuesOf(Map<Serializable, BufferedRecord<IndexedRecord>> logRecords) {
    Map<Serializable, Long> orderingValues = new HashMap<>();
    logRecords.forEach((key, record) -> orderingValues.put(key, (Long) record.getOrderingValue()));
    return orderingValues;
  }
}
