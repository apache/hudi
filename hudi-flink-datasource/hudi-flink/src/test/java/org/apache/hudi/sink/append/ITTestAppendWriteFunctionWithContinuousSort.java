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

import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Test cases for {@link AppendWriteFunctionWithContinuousSort}.
 */
public class ITTestAppendWriteFunctionWithContinuousSort extends TestWriteBase {
  private Configuration conf;
  private RowType rowType;

  @TempDir
  protected File tempFile;

  @BeforeEach
  public void before(@TempDir File tempDir) throws Exception {
    this.conf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_ENABLED, true);
    this.conf.set(FlinkOptions.OPERATION, "insert");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 100L);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 1);

    // Define the row type with fields: name (STRING), age (INT), partition (STRING)
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
    // Create test data that exceeds buffer size
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testBufferFlush(boolean flushOnCheckpoint) throws Exception {
    // Create test data
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
    );

    // Write the data and wait for timer
    TestHarness testHarness =
        TestWriteBase.TestHarness.instance()
            .preparePipeline(tempFile, conf)
            .consume(inputData);
    if (flushOnCheckpoint) {
      testHarness.checkpoint(1);
    } else {
      testHarness.endInput();
    }

    // Verify data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2, actualData.size());
  }

  @Test
  public void testBufferFlushOnBufferSizeLimit() throws Exception {
    // enlarge the write buffer record size
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 10000L);
    // use a very small buffer memory size here
    this.conf.set(FlinkOptions.WRITE_TASK_MAX_SIZE, 200.1D);

    // Create test data that exceeds buffer size
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 2000; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(2000, actualData.size());
  }

  @Test
  public void testSortedResult() throws Exception {
    // Create test data in unsorted order
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
        createRowData("uuid3", "Bob", 21, "1970-01-01 00:00:31.124", "p1")
    );

    // Expected result after sorting by name, then age
    List<String> expected = Arrays.asList(
        "uuid2,Alice,25,1970-01-01 00:00:01.124,p1",
        "uuid3,Bob,21,1970-01-01 00:00:31.124,p1",
        "uuid1,Bob,30,1970-01-01 00:00:01.123,p1");

    // Write the data and wait for timer
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .checkpoint(1)
        .endInput();

    // Verify data was written
    List<GenericRecord> result = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(3, result.size());

    List<String> filteredResult =
        result.stream().map(TestData::filterOutVariablesWithoutHudiMetadata).collect(Collectors.toList());

    assertArrayEquals(expected.toArray(), filteredResult.toArray());
  }

  @Test
  public void testContinuousDrainBehavior() throws Exception {
    // Set buffer size to 10 records
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 10L);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 2);

    // Create test data that will trigger drain
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 12; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data - should trigger continuous draining
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written despite buffer size limit
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(12, actualData.size());
  }

  @Test
  public void testDrainSizeConfiguration() throws Exception {
    // Set buffer size to 10 and drain size to 5
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 10L);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 5);

    // Create test data that will trigger multiple drains
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data - should trigger draining in batches of 5
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(20, actualData.size());
  }

  @Test
  public void testSortedResultWithContinuousDrain() throws Exception {
    // Set smaller buffer to force continuous draining
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 5L);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 1);

    // Create test data with various names and ages
    List<RowData> inputData = Arrays.asList(
        createRowData("uuid1", "Charlie", 35, "1970-01-01 00:00:01.123", "p1"),
        createRowData("uuid2", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
        createRowData("uuid3", "Bob", 30, "1970-01-01 00:00:01.125", "p1"),
        createRowData("uuid4", "Alice", 20, "1970-01-01 00:00:01.126", "p1"),
        createRowData("uuid5", "Bob", 28, "1970-01-01 00:00:01.127", "p1"),
        createRowData("uuid6", "Charlie", 40, "1970-01-01 00:00:01.128", "p1")
    );

    // Expected result after sorting by name, then age
    List<String> expected = Arrays.asList(
        "uuid4,Alice,20,1970-01-01 00:00:01.126,p1",
        "uuid2,Alice,25,1970-01-01 00:00:01.124,p1",
        "uuid5,Bob,28,1970-01-01 00:00:01.127,p1",
        "uuid3,Bob,30,1970-01-01 00:00:01.125,p1",
        "uuid1,Charlie,35,1970-01-01 00:00:01.123,p1",
        "uuid6,Charlie,40,1970-01-01 00:00:01.128,p1"
    );

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .checkpoint(1)
        .endInput();

    // Verify data was written in sorted order
    List<GenericRecord> result = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(6, result.size());

    List<String> filteredResult =
        result.stream().map(TestData::filterOutVariablesWithoutHudiMetadata).collect(Collectors.toList());

    assertArrayEquals(expected.toArray(), filteredResult.toArray());
  }

  @Test
  public void testLargeDrainSize() throws Exception {
    // Set larger drain size to test batch draining
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 20L);
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 5);

    // Create test data
    List<RowData> inputData = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      inputData.add(createRowData("uuid" + i, "Name" + i, i, "1970-01-01 00:00:01.123", "p1"));
    }

    // Write the data
    TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, conf)
        .consume(inputData)
        .endInput();

    // Verify all data was written
    List<GenericRecord> actualData = TestData.readAllData(new File(conf.get(FlinkOptions.PATH)), rowType, 1);
    assertEquals(30, actualData.size());
  }

  @Test
  public void testInvalidDrainSizeZero() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 0);

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  @Test
  public void testInvalidDrainSizeNegative() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, -5);

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  @Test
  public void testInvalidBufferSizeZero() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 0L);

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  @Test
  public void testInvalidBufferSizeNegative() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, -100L);

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  @Test
  public void testInvalidSortKeysOnlyCommas() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, " , , ");

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  @Test
  public void testInvalidSortKeysOnlyWhitespace() {
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "   ");

    AppendWriteFunctionWithContinuousSort<RowData> function =
        new AppendWriteFunctionWithContinuousSort<>(conf, rowType);

    assertThrows(IllegalArgumentException.class, () -> {
      function.open(conf);
    });
  }

  private GenericRowData createRowData(String uuid, String name, int age, String timestamp, String partition) {
    return GenericRowData.of(StringData.fromString(uuid), StringData.fromString(name),
        age, TimestampData.fromTimestamp(Timestamp.valueOf(timestamp)), StringData.fromString(partition));
  }
}
