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

import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieAvroRecordTestMerger;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.testutils.reader.HoodieTestReaderContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.DELETE;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.SKIP;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.UPDATE;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestFileSliceWithOnlyLogFiles extends HoodieFileGroupReaderTestHarness {
  @BeforeAll
  public static void setUp() throws IOException {
    // Use dedicated merger to avoid current delete logic holes.
    HoodieAvroRecordMerger merger = new HoodieAvroRecordTestMerger();
    readerContext = new HoodieTestReaderContext(Option.of(merger));

    // -------------------------------------------------------------
    // The test logic is as follows:
    // 1. No base file is created.
    // 2. After adding the first log file,
    //    we add the records with keys from 1 to 5
    //    with ordering value 3.
    //    Current existing keys: [1, 2, 3, 4, 5]
    // 3. After adding the second log file,
    //    we delete the records with keys from 1 to 3
    //    with ordering value 1. We still get the
    //    same result as before, since their ordering
    //    value is 1 < 3.
    //    Current existing keys: [1, 2, 3, 4, 5]
    // -------------------------------------------------------------

    keyRanges = Arrays.asList(
        new HoodieFileSliceTestUtils.KeyRange(1, 10),
        new HoodieFileSliceTestUtils.KeyRange(1, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 3),
        new HoodieFileSliceTestUtils.KeyRange(4, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 2),
        new HoodieFileSliceTestUtils.KeyRange(2, 4));
    // Specify the value of `timestamp` column for each file.
    timestamps = Arrays.asList(
        2L, 3L, 1L, 1L, 4L, 5L);
    // Specify the operation type for each file.
    operationTypes = Arrays.asList(
        SKIP, INSERT, UPDATE, DELETE, UPDATE, DELETE);
    // Specify the instant time for each file.
    instantTimes = Arrays.asList(
        "001", "002", "003", "004", "005", "006");
  }

  @BeforeEach
  public void initialize() throws Exception {
    setTableName(TestEventTimeMerging.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[]{PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    setUpMockCommits();
  }

  @Test
  public void testWithOneLogFile() throws IOException, InterruptedException {
    // The FileSlice contains only a log file, no base file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(2);
    List<String> leftKeysExpected = Arrays.asList("1", "2", "3", "4", "5");
    List<Long> leftTimestampsExpected = Arrays.asList(3L, 3L, 3L, 3L, 3L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();

    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(
          record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add(
          (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    iterator.close();
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithTwoLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains two log files only, no base file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(3);
    List<String> leftKeysExpected = Arrays.asList("1", "2", "3", "4", "5");
    List<Long> leftTimestampsExpected = Arrays.asList(3L, 3L, 3L, 3L, 3L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();

    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(
          record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add(
          (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    iterator.close();
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithThreeLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains three log files only, no base file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(4);
    List<String> leftKeysExpected = Arrays.asList("1", "2", "3", "4", "5");
    List<Long> leftTimestampsExpected = Arrays.asList(3L, 3L, 3L, 3L, 3L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();

    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(
          record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add(
          (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    iterator.close();
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithFourLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains four log files only, no base file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(5);
    List<String> leftKeysExpected = Arrays.asList("1", "2", "3", "4", "5");
    List<Long> leftTimestampsExpected = Arrays.asList(4L, 4L, 3L, 3L, 3L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();

    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(
          record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add(
          (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    iterator.close();
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithFiveLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains five log files only, no base file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(6);
    List<String> leftKeysExpected = Arrays.asList("1", "5");
    List<Long> leftTimestampsExpected = Arrays.asList(4L, 3L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();

    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(
          record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add(
          (Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    iterator.close();
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }
}
