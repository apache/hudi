/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.append;

import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.utils.TestData;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test cases for append write functions with buffer sorting.
 * Tests both {@link AppendWriteFunctionWithDisruptorBufferSort} (DISRUPTOR) and
 * {@link AppendWriteFunctionWithBIMBufferSort} (BOUNDED_IN_MEMORY) buffer types.
 */
public class ITTestAppendWriteFunctionWithBufferSort extends TestWriteBase {

  private RowType rowType;

  @Override
  protected void setUp(Configuration conf) {
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 100L);

    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("uuid", VarCharType.STRING_TYPE));
    fields.add(new RowType.RowField("name", VarCharType.STRING_TYPE));
    fields.add(new RowType.RowField("age", new IntType()));
    fields.add(new RowType.RowField("ts", new TimestampType()));
    fields.add(new RowType.RowField("partition", VarCharType.STRING_TYPE));
    this.rowType = new RowType(fields);
  }

  private void configureBufferType(BufferType bufferType) {
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    if (bufferType == BufferType.DISRUPTOR) {
      conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 1024);
    }
  }

  // ==================== Common tests for both buffer types ====================

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testBufferFlushOnRecordNumberLimit(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create test data that exceeds buffer size (150 records > 100 buffer size)
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(150, actualData.size());
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testBufferFlushOnCheckpoint(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data and flush on checkpoint
    preparePipeline(conf)
        .consume(inputData)
        .checkpoint(1)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testSortedResult(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create test data with unsorted records (sort keys: name, age)
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
        createRowData("uuid3", "Bob", 21, "1970-01-01 00:00:31.124", "p1")
    );

    // Expected order after sorting by name, then age
    List<String> expected = Arrays.asList(
        "uuid2,Alice,25,1970-01-01 00:00:01.124,p1",
        "uuid3,Bob,21,1970-01-01 00:00:31.124,p1",
        "uuid1,Bob,30,1970-01-01 00:00:01.123,p1");

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .checkpoint(1)
        .endInput();

    // Verify data is sorted correctly
    List<GenericRecord> result = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(3, result.size());

    List<String> filteredResult = result.stream()
        .map(TestData::filterOutVariablesWithoutHudiMetadata)
        .collect(Collectors.toList());

    assertArrayEquals(expected.toArray(), filteredResult.toArray());
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testMultipleCheckpoints(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create first batch of test data
    List<RowData> batch1 = Arrays.asList(
        createRowData("uuid1", "Charlie", 35, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Create second batch of test data
    List<RowData> batch2 = Arrays.asList(
        createRowData("uuid3", "Bob", 30, "1970-01-01 00:00:01.125", "p1"),
        createRowData("uuid4", "Diana", 28, "1970-01-01 00:00:01.126", "p1")
    );

    // Write batches with checkpoints between them
    TestHarness testHarness = preparePipeline(conf);

    testHarness.consume(batch1).checkpoint(1);
    testHarness.consume(batch2).checkpoint(2);
    testHarness.endInput();

    // Verify all data from both batches was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(4, actualData.size());
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testLargeDatasetWithMultipleFlushes(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);
    // Configure small buffer to force multiple flushes
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 50L);

    // Create large dataset across multiple partitions
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + (i % 10), i % 100, "1970-01-01 00:00:01.123", "p" + (i % 3)));
    }

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written across all partitions
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 3);
    assertEquals(500, actualData.size());
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testSortStabilityWithDuplicateKeys(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create test data with duplicate sort keys (same name and age for first 3 records)
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
        createRowData("uuid3", "Alice", 25, "1970-01-01 00:00:01.125", "p1"),
        createRowData("uuid4", "Bob", 30, "1970-01-01 00:00:01.126", "p1")
    );

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written and sorted (Alice records before Bob)
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(4, actualData.size());

    List<String> filteredResult = actualData.stream()
        .map(TestData::filterOutVariablesWithoutHudiMetadata)
        .collect(Collectors.toList());

    assertTrue(filteredResult.get(0).contains("Alice"));
    assertTrue(filteredResult.get(1).contains("Alice"));
    assertTrue(filteredResult.get(2).contains("Alice"));
    assertTrue(filteredResult.get(3).contains("Bob"));
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testDifferentPartitions(BufferType bufferType) throws Exception {
    configureBufferType(bufferType);

    // Create test data across different partitions
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Bob", 30, "1970-01-01 00:00:01.124", "p2"),
        createRowData("uuid3", "Charlie", 35, "1970-01-01 00:00:01.125", "p3"),
        createRowData("uuid4", "Diana", 28, "1970-01-01 00:00:01.126", "p1")
    );

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput()
        .endInputComplete();

    // Verify all data was written across all partitions
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 3);
    assertEquals(4, actualData.size());
  }

  // ==================== DISRUPTOR-specific tests ====================

  @Test
  public void testBackwardCompatibilityWithSortEnabled() throws Exception {
    // Use deprecated write.buffer.sort.enabled=true (should resolve to DISRUPTOR)
    conf.removeConfig(FlinkOptions.WRITE_BUFFER_TYPE);
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);

    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput()
        .endInputComplete();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  // ==================== BOUNDED_IN_MEMORY-specific tests ====================

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBIMBufferFlushOnCheckpointOrEndInput(boolean flushOnCheckpoint) throws Exception {
    configureBufferType(BufferType.BOUNDED_IN_MEMORY);

    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data and flush either on checkpoint or endInput
    TestHarness testHarness =
        preparePipeline(conf)
            .consume(inputData);
    if (flushOnCheckpoint) {
      testHarness.checkpoint(1);
    } else {
      testHarness.endInput();
    }

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  @Test
  public void testBIMBufferFlushOnBufferSizeLimit() throws Exception {
    configureBufferType(BufferType.BOUNDED_IN_MEMORY);
    // Configure large record count limit but small memory limit to trigger memory-based flush
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 10000L);
    conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 400.1D);

    // Create large dataset
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data
    preparePipeline(conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2000, actualData.size());
  }

  @Test
  public void testBIMConcurrentWriteScenario() throws Exception {
    configureBufferType(BufferType.BOUNDED_IN_MEMORY);
    // Configure small buffer to trigger frequent async writes
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 20L);

    // Create test data
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + (i % 5), i % 50, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write data in small batches with periodic checkpoints
    TestHarness testHarness = preparePipeline(conf);

    for (int i = 0; i < inputData.size(); i += 10) {
      List<RowData> batch = inputData.subList(i, Math.min(i + 10, inputData.size()));
      testHarness.consume(batch);
      if (i % 50 == 0) {
        testHarness.checkpoint(i / 50 + 1);
      }
    }
    testHarness.endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(200, actualData.size());
  }

  // ==================== Helper methods ====================

  private GenericRowData createRowData(String uuid, String name, int age, String timestamp, String partition) {
    return GenericRowData.of(StringData.fromString(uuid), StringData.fromString(name),
        age, TimestampData.fromTimestamp(Timestamp.valueOf(timestamp)), StringData.fromString(partition));
  }
}
