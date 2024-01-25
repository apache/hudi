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
import org.apache.hudi.common.testutils.reader.HoodieRecordTestPayload;
import org.apache.hudi.common.testutils.reader.HoodieTestReaderContext;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.Option;

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
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.UPDATE;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestEventTimeMerging extends HoodieFileGroupReaderTestHarness {
  @BeforeAll
  public static void setUp() throws IOException {
    // Create dedicated merger to avoid current delete logic holes.
    // TODO: Unify delete logic (HUDI-7240).
    HoodieAvroRecordMerger merger = new HoodieAvroRecordTestMerger();
    readerContext = new HoodieTestReaderContext(
        Option.of(merger),
        Option.of(HoodieRecordTestPayload.class.getName()));

    // -------------------------------------------------------------
    // The test logic is as follows:
    // 1. Base file contains 10 records,
    //    whose key values are from 1 to 10,
    //    whose instant time is "001" and ordering value is 2.
    // 2. After adding the first log file,
    //    we delete the records with keys from 1 to 5
    //    with ordering value 3.
    //    Current existing keys: [6, 7, 8, 9, 10]
    // 3. After adding the second log file,
    //    we tried to add the records with keys from 1 to 3 back,
    //    but we cannot since their ordering value is 1 < 3.
    //    Current existing keys: [6, 7, 8, 9, 10]
    // 4. After adding the third log file,
    //    we tried to delete records with keys from 6 to 8,
    //    but we cannot since their ordering value is 1 < 2.
    //    Current existing keys: [6, 7, 8, 9, 10]
    // 5. After adding the fourth log file,
    //    we tried to add the records with keys from 1 to 2 back,
    //    and it worked since their ordering value is 4 > 3.
    //    Current existing keys: [1, 2, 6, 7, 8, 9, 10]
    // -------------------------------------------------------------

    // Specify the key column values for each file.
    keyRanges = Arrays.asList(
        new HoodieFileSliceTestUtils.KeyRange(1, 10),
        new HoodieFileSliceTestUtils.KeyRange(1, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 3),
        new HoodieFileSliceTestUtils.KeyRange(6, 8),
        new HoodieFileSliceTestUtils.KeyRange(1, 2));
    // Specify the value of `timestamp` column for each file.
    timestamps = Arrays.asList(
        2L, 3L, 1L, 1L, 4L);
    // Specify the operation type for each file.
    operationTypes = Arrays.asList(
        INSERT, DELETE, UPDATE, DELETE, UPDATE);
    // Specify the instant time for each file.
    instantTimes = Arrays.asList(
        "001", "002", "003", "004", "005");
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
    // The FileSlice contains a base file and a log file.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(2);
    List<String> leftKeysExpected = Arrays.asList("6", "7", "8", "9", "10");
    List<Long> leftTimestampsExpected = Arrays.asList(2L, 2L, 2L, 2L, 2L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();
    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add((Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithTwoLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains a base file and two log files.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(3);
    List<String> leftKeysExpected = Arrays.asList("6", "7", "8", "9", "10");
    List<Long> leftTimestampsExpected = Arrays.asList(2L, 2L, 2L, 2L, 2L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();
    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add((Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithThreeLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains a base file and three log files.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(4);
    List<String> leftKeysExpected = Arrays.asList("6", "7", "8", "9", "10");
    List<Long> leftTimestampsExpected = Arrays.asList(2L, 2L, 2L, 2L, 2L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();
    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add((Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }

  @Test
  public void testWithFourLogFiles() throws IOException, InterruptedException {
    // The FileSlice contains a base file and three log files.
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(5);
    List<String> leftKeysExpected = Arrays.asList("1", "2", "6", "7", "8", "9", "10");
    List<Long> leftTimestampsExpected = Arrays.asList(4L, 4L, 2L, 2L, 2L, 2L, 2L);
    List<String> leftKeysActual = new ArrayList<>();
    List<Long> leftTimestampsActual = new ArrayList<>();
    while (iterator.hasNext()) {
      IndexedRecord record = iterator.next();
      leftKeysActual.add(record.get(AVRO_SCHEMA.getField(ROW_KEY).pos()).toString());
      leftTimestampsActual.add((Long) record.get(AVRO_SCHEMA.getField("timestamp").pos()));
    }
    assertEquals(leftKeysExpected, leftKeysActual);
    assertEquals(leftTimestampsExpected, leftTimestampsActual);
  }
}
