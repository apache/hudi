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
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metrics.FlinkBucketAssignMetrics;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test for {@link MinibatchBucketAssignFunction}.
 */
public class TestMinibatchBucketAssignFunction {
  private OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> testHarness;
  private MinibatchBucketAssignFunction function;
  private static Configuration conf;

  @TempDir
  static File tempFile;

  @BeforeAll
  public static void beforeAll() throws Exception {
    final String basePath = tempFile.getAbsolutePath();
    conf = TestConfigurations.getDefaultConf(basePath);
    conf.setString(HoodieMetadataConfig.GLOBAL_RECORD_LEVEL_INDEX_ENABLE_PROP.key(), "true");
    TestData.writeData(TestData.DATA_SET_INSERT, conf);
    conf.set(FlinkOptions.INDEX_TYPE, HoodieIndex.IndexType.GLOBAL_RECORD_LEVEL_INDEX.name());
  }

  @BeforeEach
  public void beforeEach() throws Exception {
    // Create the MinibatchBucketAssignFunction
    function = new MinibatchBucketAssignFunction(conf);
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
  public void testDuplicateKeyCrossPartitionUpdate() throws Exception {
    HoodieFlinkInternalRow record1 = insertRecord("id1", "par2", 1);
    HoodieFlinkInternalRow record2 = insertRecord("id1", "par3", 2);

    testHarness.processElement(new StreamRecord<>(record1));
    testHarness.processElement(new StreamRecord<>(record2));
    testHarness.prepareSnapshotPreBarrier(1L);

    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(4, output.size(), "Two cross-partition updates should each emit delete and insert records");

    HoodieFlinkInternalRow deleteForOriginalPartition = output.get(0);
    HoodieFlinkInternalRow insertForFirstUpdate = output.get(1);
    HoodieFlinkInternalRow deleteForFirstUpdate = output.get(2);
    HoodieFlinkInternalRow insertForSecondUpdate = output.get(3);

    assertEquals("par1", deleteForOriginalPartition.getPartitionPath(),
        "First update should delete the location prefetched from the metadata table");
    assertEquals("-U", deleteForOriginalPartition.getOperationType());
    assertEquals("U", deleteForOriginalPartition.getInstantTime());

    assertEquals("par2", insertForFirstUpdate.getPartitionPath());
    assertEquals("I", insertForFirstUpdate.getOperationType());
    assertEquals("U", insertForFirstUpdate.getInstantTime());
    assertTrue(insertForFirstUpdate.getFileId() != null && !insertForFirstUpdate.getFileId().isEmpty(),
        "First update should be assigned to a data bucket");

    assertEquals("par2", deleteForFirstUpdate.getPartitionPath(),
        "Duplicate key should re-read the location updated by the preceding record in the same minibatch");
    assertEquals(insertForFirstUpdate.getFileId(), deleteForFirstUpdate.getFileId(),
        "The delete for the second update should target the bucket assigned to the first update");
    assertEquals("-U", deleteForFirstUpdate.getOperationType());
    assertEquals("U", deleteForFirstUpdate.getInstantTime());

    assertEquals("par3", insertForSecondUpdate.getPartitionPath());
    assertEquals("I", insertForSecondUpdate.getOperationType());
    assertEquals("U", insertForSecondUpdate.getInstantTime());
    assertTrue(insertForSecondUpdate.getFileId() != null && !insertForSecondUpdate.getFileId().isEmpty(),
        "Second update should be assigned to a data bucket");
  }

  @Test
  public void testDuplicateKeyInSameMinibatch() throws Exception {
    HoodieFlinkInternalRow record1 = insertRecord("new_duplicate_key", "par_insert", 1);
    HoodieFlinkInternalRow record2 = insertRecord("new_duplicate_key", "par_insert", 2);

    testHarness.processElement(new StreamRecord<>(record1));
    testHarness.processElement(new StreamRecord<>(record2));
    testHarness.prepareSnapshotPreBarrier(1L);

    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(2, output.size(), "Duplicate insert-miss records should both be emitted");

    HoodieFlinkInternalRow insertRecord = output.get(0);
    HoodieFlinkInternalRow duplicateRecord = output.get(1);

    assertEquals("new_duplicate_key", insertRecord.getRecordKey());
    assertEquals("par_insert", insertRecord.getPartitionPath());
    assertEquals("I", insertRecord.getInstantTime(),
        "The first record should be assigned as an insert because the prefetched location is missing");
    assertEquals("I", insertRecord.getOperationType());
    assertTrue(insertRecord.getFileId() != null && !insertRecord.getFileId().isEmpty(),
        "First insert should be assigned to a data bucket");

    assertEquals("new_duplicate_key", duplicateRecord.getRecordKey());
    assertEquals("par_insert", duplicateRecord.getPartitionPath());
    assertEquals(insertRecord.getFileId(), duplicateRecord.getFileId(),
        "Duplicate key should re-read the location written by the first insert in the same minibatch");
    assertEquals("U", duplicateRecord.getInstantTime(),
        "The duplicate record should be assigned as an update to the first record's bucket");
    assertEquals("I", duplicateRecord.getOperationType());
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

  @Test
  public void testInsertOperationFlushedByEndInput() throws Exception {
    // With OPERATION=INSERT, endInput() should flush the partial buffer.
    Configuration insertConf = Configuration.fromMap(conf.toMap());
    insertConf.set(FlinkOptions.OPERATION, WriteOperationType.INSERT.value());

    MinibatchBucketAssignFunction insertFunction = new MinibatchBucketAssignFunction(insertConf);
    OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> insertHarness =
        new OneInputStreamOperatorTestHarness<>(new MiniBatchBucketAssignOperator(insertFunction, new OperatorID()), 1, 1, 0);
    insertHarness.open();
    try {
      for (int i = 0; i < 3; i++) {
        String key = "ins_key_" + i;
        HoodieFlinkInternalRow record = new HoodieFlinkInternalRow(key, "par9", "I",
            insertRow(StringData.fromString(key), StringData.fromString("Name"), 30,
                TimestampData.fromEpochMillis(1), StringData.fromString("par9")));
        insertHarness.processElement(new StreamRecord<>(record));
      }

      // Buffer holds 3 records, no flush yet.
      assertEquals(0, insertHarness.extractOutputValues().size(), "Records should still be buffered");

      insertHarness.endInput();

      List<HoodieFlinkInternalRow> output = insertHarness.extractOutputValues();
      assertEquals(3, output.size(), "endInput should flush all buffered records");
      for (HoodieFlinkInternalRow row : output) {
        assertTrue(row.getFileId() != null && !row.getFileId().isEmpty(), "File ID should be assigned");
        assertEquals("I", row.getInstantTime(), "All records should be INSERT bucket assignments");
      }
    } finally {
      insertHarness.close();
    }
  }

  @Test
  public void testEmptyBufferPrepareSnapshotPreBarrierEmitsNothing() throws Exception {
    // prepareSnapshotPreBarrier on an empty buffer should not throw and should emit no records.
    testHarness.prepareSnapshotPreBarrier(1L);

    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(0, output.size(), "Empty buffer should produce no output during snapshot");
  }

  @Test
  public void testMultipleCheckpointFlushes() throws Exception {
    // Each prepareSnapshotPreBarrier call should flush only the records buffered since the last flush.
    HoodieFlinkInternalRow record1 = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    HoodieFlinkInternalRow record2 = new HoodieFlinkInternalRow("id2", "par1", "I",
        insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
            TimestampData.fromEpochMillis(2), StringData.fromString("par1")));

    // First checkpoint cycle.
    testHarness.processElement(new StreamRecord<>(record1));
    testHarness.prepareSnapshotPreBarrier(1L);
    assertEquals(1, testHarness.extractOutputValues().size(), "First checkpoint should flush 1 record");

    // Second checkpoint cycle.
    testHarness.processElement(new StreamRecord<>(record2));
    testHarness.prepareSnapshotPreBarrier(2L);

    List<HoodieFlinkInternalRow> output = testHarness.extractOutputValues();
    assertEquals(2, output.size(), "Second checkpoint should have flushed 1 more record (2 cumulative)");
    for (HoodieFlinkInternalRow row : output) {
      assertTrue(row.getFileId() != null && !row.getFileId().isEmpty(), "File ID should be assigned");
      assertEquals("U", row.getInstantTime(), "Previously written records should be updates");
    }
  }

  @Test
  public void testCustomMinibatchSizeAboveDefault() {
    // When the configured size exceeds the default minimum, it should be used as-is.
    Configuration customConf = Configuration.fromMap(conf.toMap());
    customConf.set(FlinkOptions.INDEX_RLI_LOOKUP_MINIBATCH_SIZE, 2000);
    MinibatchBucketAssignFunction customFunction = new MinibatchBucketAssignFunction(customConf);
    assertEquals(2000, customFunction.getMiniBatchSize(),
        "Should use the configured size when it is above the default minimum");
  }

  @Test
  public void testEndInputOnEmptyBufferDoesNotThrow() throws Exception {
    // endInput with an empty buffer must not throw and should emit no records.
    testHarness.endInput();
    assertEquals(0, testHarness.extractOutputValues().size(), "No output expected when buffer was already empty");
  }

  @Test
  public void testDelegateMetricsNonNullAfterOpen() {
    FlinkBucketAssignMetrics delegateMetrics = function.getDelegateMetrics();
    assertNotNull(delegateMetrics, "Delegate metrics should be initialized after open");
  }

  @Test
  public void testBufferingMetricIncrementedAfterFullBatchFlush() throws Exception {
    // Processing exactly miniBatchSize records triggers one automatic flush, recording one buffering sample.
    for (int i = 0; i < FlinkOptions.INDEX_RLI_LOOKUP_MINIBATCH_SIZE.defaultValue(); i++) {
      String key = "batch_key_" + i;
      HoodieFlinkInternalRow record = new HoodieFlinkInternalRow(key, "par5", "I",
          insertRow(StringData.fromString(key), StringData.fromString("Name"), 25,
              TimestampData.fromEpochMillis(1), StringData.fromString("par5")));
      testHarness.processElement(new StreamRecord<>(record));
    }
    assertEquals(1, function.getDelegateMetrics().getRecordBufferingCount(),
        "One buffering cycle should be recorded after a full batch flush");
  }

  @Test
  public void testBufferingMetricIncrementedAfterCheckpointFlush() throws Exception {
    // A partial buffer flushed by a checkpoint barrier records one buffering sample.
    HoodieFlinkInternalRow record = new HoodieFlinkInternalRow("id1", "par1", "I",
        insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(1), StringData.fromString("par1")));
    testHarness.processElement(new StreamRecord<>(record));
    testHarness.prepareSnapshotPreBarrier(1L);
    assertEquals(1, function.getDelegateMetrics().getRecordBufferingCount(),
        "One buffering cycle should be recorded after a checkpoint flush");
  }

  @Test
  public void testNumShardsAssignedMetricIsSet() throws Exception {
    // With global RLI enabled the numShardsAssigned gauge must be a non-negative value after open().
    // The test harness runs with parallelism 1, so this single task owns all shards.
    FlinkBucketAssignMetrics metrics = function.getDelegateMetrics();
    assertTrue(metrics.getNumShardsAssigned() >= 0,
            "numShardsAssigned must be set when global RLI is active");
  }

  @Test
  public void testNumShardsAssignedIsNegativeOneWhenBootstrapEnabled() throws Exception {
    // During index bootstrap the metadata file-group count may be unavailable;
    // numShardsAssigned must stay at its sentinel -1.
    Configuration bootstrapConf = Configuration.fromMap(conf.toMap());
    bootstrapConf.set(FlinkOptions.INDEX_BOOTSTRAP_ENABLED, true);

    MinibatchBucketAssignFunction bootstrapFunction = new MinibatchBucketAssignFunction(bootstrapConf);
    OneInputStreamOperatorTestHarness<HoodieFlinkInternalRow, HoodieFlinkInternalRow> bootstrapHarness =
        new OneInputStreamOperatorTestHarness<>(new MiniBatchBucketAssignOperator(bootstrapFunction, new OperatorID()), 1, 1, 0);
    bootstrapHarness.open();
    try {
      assertEquals(-1, bootstrapFunction.getDelegateMetrics().getNumShardsAssigned(),
          "numShardsAssigned must remain -1 when INDEX_BOOTSTRAP_ENABLED is true");
    } finally {
      bootstrapHarness.close();
    }
  }

  private static HoodieFlinkInternalRow insertRecord(String recordKey, String partitionPath, long ts) {
    return new HoodieFlinkInternalRow(recordKey, partitionPath, "I",
        insertRow(StringData.fromString(recordKey), StringData.fromString("Danny"), 23,
            TimestampData.fromEpochMillis(ts), StringData.fromString(partitionPath)));
  }
}
