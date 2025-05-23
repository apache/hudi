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
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestTable;
import org.apache.hudi.common.testutils.reader.HoodieAvroRecordTestMerger;
import org.apache.hudi.common.testutils.reader.HoodieFileGroupReaderTestHarness;
import org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils;
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
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.INSERT;
import static org.apache.hudi.common.testutils.reader.DataGenerationPlan.OperationType.UPDATE;
import static org.apache.hudi.common.testutils.reader.HoodieFileSliceTestUtils.ROW_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestHoodieFileGroupReaderInflightCommit extends HoodieFileGroupReaderTestHarness {

  @BeforeAll
  public static void setUp() throws IOException {
    // -------------------------------------------------------------
    // The test logic is as follows:
    // 1. Base file contains 10 records,
    //    whose key values are from 1 to 5,
    //    whose instant time is "001" and ordering value is 2.
    // 2. After adding the first base file,
    //    we update the records with keys from 1 to 3
    //    with ordering value 3.

    // Specify the key column values for each file.
    keyRanges = Arrays.asList(
        new HoodieFileSliceTestUtils.KeyRange(1, 5),
        new HoodieFileSliceTestUtils.KeyRange(1, 3));
    // Specify the value of `timestamp` column for each file.
    timestamps = Arrays.asList(
        2L, 3L);
    // Specify the operation type for each file.
    operationTypes = Arrays.asList(
        INSERT, UPDATE);
    // Specify the instant time for each file.
    instantTimes = Arrays.asList(
        "001", "002");
  }

  @BeforeEach
  public void initialize() throws Exception {
    setTableName(TestEventTimeMerging.class.getName());
    initPath(tableName);
    initMetaClient();
    initTestDataGenerator(new String[]{PARTITION_PATH});
    testTable = HoodieTestTable.of(metaClient);
    readerContext = new HoodieAvroReaderContext(
        storageConf, metaClient.getTableConfig(), Option.empty(), Option.empty());
    readerContext.setRecordMerger(Option.of(new HoodieAvroRecordTestMerger()));
    setUpMockCommits();
  }

  @Test
  public void testInflightDataRead() throws IOException, InterruptedException {
    shouldWritePositions = Arrays.asList(false, false);
    // delete the completed instant to convert last update commit to inflight commit
    testTable.moveCompleteCommitToInflight(instantTimes.get(1));
    metaClient = HoodieTableMetaClient.reload(metaClient);
    // The FileSlice contains a base file and a log file.
    try (ClosableIterator<IndexedRecord> iterator = getFileGroupIterator(2, false, true)) {
      List<String> leftKeysExpected = Arrays.asList("1", "2", "3", "4", "5");
      List<Long> leftTimestampsExpected = Arrays.asList(3L, 3L, 3L, 2L, 2L);
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
}
