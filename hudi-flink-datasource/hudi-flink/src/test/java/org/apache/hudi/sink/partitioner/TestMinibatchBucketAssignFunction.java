/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.partitioner;

import org.apache.hudi.client.model.HoodieFlinkInternalRow;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.List;

import static org.apache.hudi.utils.TestData.insertRow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link MinibatchBucketAssignFunction}.
 */
public class TestMinibatchBucketAssignFunction {
  private OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> testHarness;
  private static Configuration conf;

  @TempDir
  static File tempFile;

  @BeforeAll
  public static void beforeAll() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    // Create the MinibatchBucketAssignFunction
    MinibatchBucketAssignFunction function = new MinibatchBucketAssignFunction(conf);
    // Set up test harness
    testHarness = new OneInputStreamOperatorTestHarness<>(new MiniBatchBucketAssignOperator(function, new OperatorID()), 1, 1, 0);
    testHarness.open();
  }

  @Test
  public void testMinibatchSize() {
    Configuration config = Configuration.fromMap(conf.toMap());
    config.set(FlinkOptions.INDEX_RLI_LOOKUP_MINIBATCH_SIZE, 200);
    MinibatchBucketAssignFunction function = new MinibatchBucketAssignFunction(config);
    // although the minibatch size is set to 200, but the final value is 1000 since the minimum allowed minibatch size is 1000.
    assertEquals(FlinkOptions.INDEX_RLI_LOOKUP_MINIBATCH_SIZE.defaultValue(), function.getMiniBatchSize());
  }

  @Test
  public void testProcessElementWithBufferData() throws Exception {
    // Create test records
    HoodieFlinkInternalRow record1 = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23, TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    HoodieFlinkInternalRow record2 = new HoodieFlinkInternalRow("id2", "par1", "I",
        insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33, TimestampData.fromEpochMillis(2), StringData.fromString("par1")));

    // Process first two records - they should be buffered
    testHarness.processElement(new StreamRecord<>(record1));
    testHarness.processElement(new StreamRecord<>(record2));

    // At this point, no output should be collected because records are buffered
    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Records should be buffered until batch size is reached");

    for (int i = 0; i < 1000; i++) {
      // Process third record - this should trigger processing of the first two buffered records
      String recordKey = "new_key_" + i;
      HoodieFlinkInternalRow record = new HoodieFlinkInternalRow(recordKey, "par5", "I",
          insertRow(StringData.fromString(recordKey), StringData.fromString("Julian"), 53, TimestampData.fromEpochMillis(3), StringData.fromString("par5")));
      testHarness.processElement(new StreamRecord<>(record));
    }

    // Now we should have processed records
    output = testHarness.extractOutputValues();
    assertEquals(1000, output.size(), "All three records should be processed");

    // Verify that the records have proper bucket assignments
    for (HoodieFlinkInternalRow row : output) {
      // Check that file ID and instant time are assigned
      assertTrue(row.getFileId() != null && !row.getFileId().isEmpty(), "File ID should be assigned");
      if (row.getRecordKey().startsWith("new_key")) {
        assertEquals("I", row.getInstantTime(), "the record is an insert record");
      } else {
        assertEquals("U", row.getInstantTime(), "the record is an update record");
      }
    }
  }

  @Test
  public void testProcessElementWithIndexRecords() throws Exception {
    // insert 500 index records
    for (int i = 0; i < 500; i++) {
      // Process third record - this should trigger processing of the first two buffered records
      String recordKey = "index_new_key_" + i;
      // Create an index record (one that has isIndexRecord() returning true)
      HoodieFlinkInternalRow indexRecord = new HoodieFlinkInternalRow(recordKey, "par1", "file_id1", "000");
      testHarness.processElement(new StreamRecord<>(indexRecord));
    }

    // insert 500 regular records
    for (int i = 0; i < 500; i++) {
      // Process third record - this should trigger processing of the first two buffered records
      String recordKey = "new_key_" + i;
      HoodieFlinkInternalRow record = new HoodieFlinkInternalRow(recordKey, "par5", "I",
          insertRow(StringData.fromString(recordKey), StringData.fromString("Julian"), 53, TimestampData.fromEpochMillis(3), StringData.fromString("par5")));
      testHarness.processElement(new StreamRecord<>(record));
    }

    // index records will not be buffered, so buffer will not be flushing
    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Record should be buffered since batch size is 2");

    // insert another 500 regular records
    for (int i = 0; i < 500; i++) {
      // Process third record - this should trigger processing of the first two buffered records
      String recordKey = "new_key_" + i;
      HoodieFlinkInternalRow record = new HoodieFlinkInternalRow(recordKey, "par5", "I",
          insertRow(StringData.fromString(recordKey), StringData.fromString("Julian"), 53, TimestampData.fromEpochMillis(3), StringData.fromString("par5")));
      testHarness.processElement(new StreamRecord<>(record));
    }

    // the expected size is 3, without index record
    output = testHarness.extractOutputValues();
    assertEquals(1000, output.size(), "Both records should be processed");
  }

  @Test
  public void testEndInputProcessesRemainingRecords() throws Exception {
    // Create and process one record
    HoodieFlinkInternalRow record = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23, TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    testHarness.processElement(new StreamRecord<>(record));
    
    // At this point, record should be buffered (since batch size is 2)
    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Record should be buffered");
    
    // Call endInput to process remaining buffered records
    testHarness.endInput();
    
    output = testHarness.extractOutputValues();
    assertEquals(1, output.size(), "Remaining record should be processed by endInput");
    
    HoodieFlinkInternalRow processedRecord = output.get(0);
    assertTrue(processedRecord.getFileId() != null && !processedRecord.getFileId().isEmpty(), "File ID should be assigned");
    assertEquals("U", processedRecord.getInstantTime(), "the record is an update record");
  }

  @Test
  public void testPrepareSnapshotPreBarrier() throws Exception {
    // Create and process one record
    HoodieFlinkInternalRow record = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23, TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    testHarness.processElement(new StreamRecord<>(record));
    
    // At this point, record should be buffered
    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Record should be buffered");
    
    // Simulate checkpoint preparation which should process buffered records
    testHarness.prepareSnapshotPreBarrier(1L);
    
    output = testHarness.extractOutputValues();
    // After snapshot, the buffered record should be processed
    assertEquals(1, output.size(), "Buffered record should be processed during snapshot");

    HoodieFlinkInternalRow processedRecord = output.get(0);
    assertTrue(processedRecord.getFileId() != null && !processedRecord.getFileId().isEmpty(), "File ID should be assigned");
    assertEquals("U", processedRecord.getInstantTime(), "the record is an update record");
  }

  @Test
  public void testGlobalIndexUpdate() throws Exception {
    // Create test records change the partition of record key 'id1'.
    HoodieFlinkInternalRow record1 = new HoodieFlinkInternalRow("id1", "par2", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23, TimestampData.fromEpochMillis(1), StringData.fromString("par2")));

    // Process first two records - they should be buffered
    testHarness.processElement(new StreamRecord<>(record1));

    // At this point, no output should be collected because records are buffered
    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Records should be buffered until batch size is reached");


    // Simulate checkpoint preparation which should process buffered records
    testHarness.prepareSnapshotPreBarrier(1L);

    // Now we should have processed records
    output = testHarness.extractOutputValues();
    assertEquals(2, output.size(), "All three records should be processed");

    // Verify that the records have proper bucket assignments
    for (int i = 0; i < output.size(); i++) {
      HoodieFlinkInternalRow row = output.get(i);
      // Check that file ID and instant time are assigned
      assertTrue(row.getFileId() != null && !row.getFileId().isEmpty(), "File ID should be assigned");
      assertEquals("U", row.getInstantTime(), "the record is an update record");
      assertEquals(i == 0 ? "-U" : "I", row.getOperationType());
    }
  }

  @Test
  public void testCloseFunction() throws Exception {
    // Test that close doesn't throw exceptions
    HoodieFlinkInternalRow record = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23, TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    testHarness.processElement(new StreamRecord<>(record));
    
    // Close should not throw any exceptions
    testHarness.close();
  }
}