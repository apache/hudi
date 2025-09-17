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

/**
 * Test cases for {@link AppendWriteFunctionWithBufferSort}.
 */
public class ITTestAppendWriteFunctionWithBufferSort extends TestWriteBase {

  private Configuration conf;
  private RowType rowType;

  @BeforeEach
  public void before(@TempDir File tempDir) throws Exception {
    super.before();
    this.conf = TestConfigurations.getDefaultConf(tempDir.getAbsolutePath());
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);
    this.conf.set(FlinkOptions.OPERATION, "insert");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    this.conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 100L);

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
        createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.124", "p1")
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
    // enlarge the wirte buffer record size
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
    // Create test data
    List<RowData> inputData = Arrays.asList(
            createRowData("uuid1", "Bob", 30, "1970-01-01 00:00:01.123", "p1"),
            createRowData("uuid1", "Alice", 25, "1970-01-01 00:00:01.124", "p1"),
            createRowData("uuid1", "Bob", 21, "1970-01-01 00:00:31.124", "p1")
    );

    List<String> expected = Arrays.asList(
            "uuid1,Alice,25,1970-01-01 00:00:01.124,p1",
            "uuid1,Bob,21,1970-01-01 00:00:31.124,p1",
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

  private GenericRowData createRowData(String uuid, String name, int age, String timestamp, String partition) {
    return GenericRowData.of(StringData.fromString(uuid), StringData.fromString(name),
        age, TimestampData.fromTimestamp(Timestamp.valueOf(timestamp)), StringData.fromString(partition));
  }
} 