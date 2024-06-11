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

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
import org.apache.hudi.common.testutils.reader.HoodieRecordTestPayload;
import org.apache.hudi.common.testutils.reader.HoodieTestReaderContext;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.hudi.common.model.HoodieRecord.HoodieRecordType.AVRO;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.AVRO_SCHEMA;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.DELETE;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.UPDATE;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestCustomMerger extends HoodieFileGroupReaderTestHarness {
  @BeforeAll
  public static void setUp() throws IOException {
    // Enable our custom merger.
    readerContext = new HoodieTestReaderContext(
        Option.of(new CustomAvroMerger()),
        Option.of(HoodieRecordTestPayload.class.getName()));
    properties.setProperty(
        HoodieCommonConfig.RECORD_MERGE_MODE.key(), RecordMergeMode.CUSTOM.name());

    // -------------------------------------------------------------
    // The test logic is as follows:
    // 1. Base file contains 10 records,
    //    whose key values are from 1 to 10,
    //    whose instant time is "001" and ordering value is 2.
    // 2. After adding the first log file,
    //    we delete the records with keys from 1 to 5
    //    with ordering value 3. Since the rest of records are
    //    not through merger, they are kept as it is.
    //    Current existing keys: [6, 7, 8, 9, 10]
    // 3. After adding the second log file,
    //    we tried to add the records with keys from 1 to 3 back,
    //    and we did it since their ordering value is 4 > 3, with
    //    the merge and flush function, only 1, 3 stay. Records with
    //    keys from 6 to 10 are as it is.
    //    Current existing keys: [1, 3, 6, 7, 8, 9, 10]
    // 4. After adding the third log file,
    //    we tried to delete records with keys from 6 to 8,
    //    but we cannot since their ordering value is 1 < 2.
    //    This step brings records from 6 to 8 into the merger and flush,
    //    and only record with key 7 left.
    //    Current existing keys: [1, 3, 7, 9, 10]
    // 5. After adding the fourth log file,
    //    we tried to add the records with keys from 1 to 2 back,
    //    and it worked since their ordering value is 9.
    //    Current existing keys: [1, 3, 7, 9]
    // -------------------------------------------------------------

    keyRanges = Arrays.asList(
        new HoodieFileSliceTestUtils.KeyRange(1, 10),
        new HoodieFileSliceTestUtils.KeyRange(1, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 3),
        new HoodieFileSliceTestUtils.KeyRange(6, 8),
        new HoodieFileSliceTestUtils.KeyRange(1, 10));
    timestamps = Arrays.asList(
        2L, 3L, 4L, 1L, 9L);
    operationTypes = Arrays.asList(
        INSERT, DELETE, UPDATE, DELETE, UPDATE);
    instantTimes = Arrays.asList(
        "001", "002", "003", "004", "005");
    shouldWritePositions = Arrays.asList(false, false, false, false, false);
  }

  @BeforeEach
  public void initialize() throws Exception {
    setTableName(TestCustomMerger.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[]{PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    setUpMockCommits();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWithOneLogFile(boolean useRecordPositions) throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(useRecordPositions, useRecordPositions);
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(2, useRecordPositions);
    List<String> leftKeysExpected =
        Arrays.asList("6", "7", "8", "9", "10");
    List<String> leftKeysActual = new ArrayList<>();
    while (iterator.hasNext()) {
      leftKeysActual.add(iterator.next()
          .get(AVRO_SCHEMA.getField(ROW_KEY).pos())
          .toString());
    }
    assertEquals(leftKeysExpected, leftKeysActual);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWithTwoLogFiles(boolean useRecordPositions) throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(useRecordPositions, useRecordPositions, useRecordPositions);
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(3, useRecordPositions);
    List<String> leftKeysExpected =
        Arrays.asList("1", "3", "6", "7", "8", "9", "10");
    List<String> leftKeysActual = new ArrayList<>();
    while (iterator.hasNext()) {
      leftKeysActual.add(iterator.next()
          .get(AVRO_SCHEMA.getField(ROW_KEY).pos())
          .toString());
    }
    assertEquals(leftKeysExpected, leftKeysActual);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testWithThreeLogFiles(boolean useRecordPositions) throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(useRecordPositions, useRecordPositions, useRecordPositions, useRecordPositions);
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(4, useRecordPositions);
    List<String> leftKeysExpected =
        Arrays.asList("1", "3", "7", "9", "10");
    List<String> leftKeysActual = new ArrayList<>();
    while (iterator.hasNext()) {
      leftKeysActual.add(iterator.next()
          .get(AVRO_SCHEMA.getField(ROW_KEY).pos())
          .toString());
    }
    assertEquals(leftKeysExpected, leftKeysActual);
  }

  @Test
  public void testWithFourLogFiles() throws IOException, InterruptedException {
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(5);
    List<String> leftKeysExpected =
        Arrays.asList("1", "3", "5", "7", "9");
    List<String> leftKeysActual = new ArrayList<>();
    while (iterator.hasNext()) {
      leftKeysActual.add(iterator.next()
          .get(AVRO_SCHEMA.getField(ROW_KEY).pos())
          .toString());
    }
    assertEquals(leftKeysExpected, leftKeysActual);
  }

  @ParameterizedTest
  @MethodSource("testArgs")
  public void testPositionMergeFallback(boolean log1haspositions, boolean log2haspositions,
                                        boolean log3haspositions, boolean log4haspositions) throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(true, log1haspositions, log2haspositions, log3haspositions, log4haspositions);
    ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(5, true);
    List<String> leftKeysExpected =
        Arrays.asList("1", "3", "5", "7", "9");
    List<String> leftKeysActual = new ArrayList<>();
    while (iterator.hasNext()) {
      leftKeysActual.add(iterator.next()
          .get(AVRO_SCHEMA.getField(ROW_KEY).pos())
          .toString());
    }
    assertEquals(leftKeysExpected, leftKeysActual);
  }

  //generate all possible combos of 4 booleans
  private static Stream<Arguments> testArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    for (int i = 0; i < 16; i++) {
      b.add(Arguments.of(i % 2 == 0, (i / 2) % 2 == 0,  (i / 4) % 2 == 0, (i / 8) % 2 == 0));
    }
    return b.build();
  }

  /**
   * This merger is designed to save records whose record key is odd.
   * That means, if the record is not a delete record, and its record
   * key is odd, then it will be output. Before the flush stage, we only
   * flush records whose timestamp is multiple of 3; but since the write
   * is insert operation, it will not go through the flush stage. Therefore,
   * in this test, we solely test the merge function.
   */
  public static class CustomAvroMerger implements HoodieRecordMerger {
    public static final String KEEP_CERTAIN_TIMESTAMP_VALUE_ONLY =
        "KEEP_CERTAIN_TIMESTAMP_VALUE_ONLY";
    public static final String TIMESTAMP = "timestamp";

    @Override
    public Option<Pair<HoodieRecord, Schema>> merge(
        HoodieRecord older,
        Schema oldSchema,
        HoodieRecord newer,
        Schema newSchema,
        TypedProperties props
    ) throws IOException {
      if (newer.getOrderingValue(newSchema, props).compareTo(
          older.getOrderingValue(oldSchema, props)) >= 0) {
        if (newer.isDelete(newSchema, props)) {
          return Option.empty();
        }
        int id = Integer.parseInt(((HoodieAvroIndexedRecord) newer)
            .getData().get(newSchema.getField(ROW_KEY).pos()).toString());
        if (id % 2 == 1L) {
          return Option.of(Pair.of(newer, newSchema));
        }
      } else {
        if (older.isDelete(oldSchema, props)) {
          return Option.empty();
        }
        int id = Integer.parseInt(((HoodieAvroIndexedRecord) older)
            .getData().get(oldSchema.getField(ROW_KEY).pos()).toString());
        if (id % 2 == 1L) {
          return Option.of(Pair.of(older, oldSchema));
        }
      }
      return Option.empty();
    }

    @Override
    public boolean shouldFlush(
        HoodieRecord record,
        Schema schema,
        TypedProperties props
    ) {
      long timestamp = (long) ((HoodieAvroIndexedRecord) record)
          .getData()
          .get(schema.getField(TIMESTAMP).pos());
      return timestamp % 3 == 0L;
    }

    @Override
    public HoodieRecord.HoodieRecordType getRecordType() {
      return AVRO;
    }

    @Override
    public String getMergingStrategy() {
      return KEEP_CERTAIN_TIMESTAMP_VALUE_ONLY;
    }
  }
}
