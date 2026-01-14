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

import org.apache.hudi.common.util.queue.BufferType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.utils.TestConfigurations;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
 * Test cases for {@link AppendWriteFunctionWithDisruptorSort} (DISRUPTOR buffer type).
 */
public class ITTestAppendWriteFunctionWithDisruptorSort extends TestWriteBase {

  private Configuration conf;
  private RowType rowType;

  @BeforeEach
  public void before(@TempDir File tempDir) throws Exception {
    super.before();
    this.conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    this.conf.set(FlinkOptions.WRITE_BUFFER_TYPE, BufferType.DISRUPTOR.name());
    this.conf.set(FlinkOptions.OPERATION, "insert");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 100L);

    List<RowType.RowField> fields = new ArrayList<>();
    fields.add(new RowType.RowField("uuid", VarCharType.STRING_TYPE));
    fields.add(new RowType.RowField("name", VarCharType.STRING_TYPE));
    fields.add(new RowType.RowField("age", new IntType()));
    fields.add(new RowType.RowField("ts", new TimestampType()));
    fields.add(new RowType.RowField("partition", VarCharType.STRING_TYPE));
    this.rowType = new RowType(fields);
  }

  @Test
  public void testBufferFlushOnRecordNumberLimit() throws Exception {
    conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 1024);

    // Create test data that exceeds buffer size (150 records > 100 buffer size)
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 150; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(150, actualData.size());
  }

  @Test
  public void testBufferFlushOnCheckpoint() throws Exception {
    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data and flush on checkpoint
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .checkpoint(1)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  @Test
  public void testSortedResult() throws Exception {
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
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
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

  @Test
  public void testMultipleCheckpoints() throws Exception {
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
    TestHarness testHarness = TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf);

    testHarness.consume(batch1).checkpoint(1);
    testHarness.consume(batch2).checkpoint(2);
    testHarness.endInput();

    // Verify all data from both batches was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(4, actualData.size());
  }

  @Test
  public void testLargeDatasetWithMultipleFlushes() throws Exception {
    // Configure small buffer to force multiple flushes
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 50L);

    // Create large dataset across multiple partitions
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 500; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + (i % 10), i % 100, "1970-01-01 00:00:01.123", "p" + (i % 3)));
    }

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written across all partitions
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 3);
    assertEquals(500, actualData.size());
  }

  @Test
  public void testSortStabilityWithDuplicateKeys() throws Exception {
    // Create test data with duplicate sort keys (same name and age for first 3 records)
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
        createRowData("uuid3", "Alice", 25, "1970-01-01 00:00:01.125", "p1"),
        createRowData("uuid4", "Bob", 30, "1970-01-01 00:00:01.126", "p1")
    );

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
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

  @Test
  public void testDifferentPartitions() throws Exception {
    // Create test data across different partitions
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Bob", 30, "1970-01-01 00:00:01.124", "p2"),
        createRowData("uuid3", "Charlie", 35, "1970-01-01 00:00:01.125", "p3"),
        createRowData("uuid4", "Diana", 28, "1970-01-01 00:00:01.126", "p1")
    );

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written across all partitions
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 3);
    assertEquals(4, actualData.size());
  }

  @Test
  public void testBackwardCompatibilityWithSortEnabled() throws Exception {
    // Use deprecated write.buffer.sort.enabled=true (should resolve to DISRUPTOR)
    conf.removeConfig(FlinkOptions.WRITE_BUFFER_TYPE);
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);

    // Verify deprecated config resolves to DISRUPTOR
    assertEquals(BufferType.DISRUPTOR.name(), AppendWriteFunctions.resolveBufferType(conf));

    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  @Test
  public void testExplicitBufferTypeTakesPrecedence() throws Exception {
    // Set both new and deprecated config
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, BufferType.BOUNDED_IN_MEMORY.name());
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);

    // Verify explicit write.buffer.type takes precedence over deprecated write.buffer.sort.enabled
    assertEquals(BufferType.BOUNDED_IN_MEMORY.name(), AppendWriteFunctions.resolveBufferType(conf));
  }

  private GenericRowData createRowData(String uuid, String name, int age, String timestamp, String partition) {
    return GenericRowData.of(StringData.fromString(uuid), StringData.fromString(name),
        age, TimestampData.fromTimestamp(Timestamp.valueOf(timestamp)), StringData.fromString(partition));
  }
}
