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

import org.apache.hudi.client.transaction.lock.InProcessLockProvider;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.config.HoodieLockConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsInference;
import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.util.HoodieSchemaConverter;
import org.apache.hudi.util.JsonDeserializationFunction;
import org.apache.hudi.util.StreamerUtil;
import org.apache.hudi.utils.FlinkMiniCluster;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;
import org.apache.hudi.utils.source.ContinuousFileSource;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Integration tests for append write functions with buffer sorting using Flink MiniCluster.
 *
 * <p>Tests all buffer types (DISRUPTOR, BOUNDED_IN_MEMORY, NONE) with real Flink runtime.
 *
 * @see AppendWriteFunctionWithDisruptorBufferSort
 * @see AppendWriteFunctionWithBIMBufferSort
 * @see AppendWriteFunction
 */
@ExtendWith(FlinkMiniCluster.class)
public class ITTestAppendWriteFunctionWithBufferSort extends TestLogger {

  private static final Map<String, List<String>> EXPECTED = new HashMap<>();

  static {
    EXPECTED.put("par1", Arrays.asList("id1,par1,id1,Danny,23,1000,par1", "id2,par1,id2,Stephen,33,2000,par1"));
    EXPECTED.put("par2", Arrays.asList("id3,par2,id3,Julian,53,3000,par2", "id4,par2,id4,Fabian,31,4000,par2"));
    EXPECTED.put("par3", Arrays.asList("id5,par3,id5,Sophia,18,5000,par3", "id6,par3,id6,Emma,20,6000,par3"));
    EXPECTED.put("par4", Arrays.asList("id7,par4,id7,Bob,44,7000,par4", "id8,par4,id8,Han,56,8000,par4"));
  }

  @TempDir
  File tempFile;

  /**
   * Tests basic write with sorting for all buffer types.
   * Verifies core functionality: records are written and sorted correctly.
   */
  @ParameterizedTest
  @EnumSource(BufferType.class)
  void testBasicWriteWithSorting(BufferType bufferType) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");

    if (bufferType == BufferType.DISRUPTOR) {
      conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 1024);
    }

    executeAndVerify(conf, "basic_write_" + bufferType.name().toLowerCase(), 1, EXPECTED);
  }

  /**
   * Tests small buffer with 100 records for DISRUPTOR and BOUNDED_IN_MEMORY.
   * With buffer size 32 and 100 records, triggers multiple flushes to test buffer management.
   */
  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"DISRUPTOR", "BOUNDED_IN_MEMORY"})
  void testSmallBufferFlush(BufferType bufferType) throws Exception {
    // Generate 100 records across 4 partitions
    int recordCount = 100;
    // Create source file in a separate directory from the Hudi table
    File sourceDir = new File(tempFile, "source");
    sourceDir.mkdirs();
    File sourceFile = new File(sourceDir, "test_100_records.data");

    // Create Hudi table in a separate directory
    File tableDir = new File(tempFile, "table");
    tableDir.mkdirs();

    generateTestDataFile(sourceFile, recordCount);

    Configuration conf = TestConfigurations.getDefaultConf(tableDir.toURI().toString());
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    conf.set(FlinkOptions.WRITE_BUFFER_SIZE, 32L);

    if (bufferType == BufferType.DISRUPTOR) {
      conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 64);
    }

    executeAndVerifyRecordCount(conf, "small_buffer_" + bufferType.name().toLowerCase(),
        sourceFile.toURI().toString(), 1, recordCount, tableDir);
  }

  /**
   * Tests concurrent write with multiple parallel tasks for all buffer types.
   * Verifies that buffer management works correctly under concurrent access.
   */
  @ParameterizedTest
  @EnumSource(BufferType.class)
  void testConcurrentWrite(BufferType bufferType) throws Exception {
    // Generate 200 records across 4 partitions (50 per partition)
    int recordCount = 200;
    File sourceDir = new File(tempFile, "source");
    sourceDir.mkdirs();
    File sourceFile = new File(sourceDir, "test_200_records.data");

    File tableDir = new File(tempFile, "table");
    tableDir.mkdirs();

    generateTestDataFile(sourceFile, recordCount);

    Configuration conf = TestConfigurations.getDefaultConf(tableDir.toURI().toString());
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");

    // Enable OCC with InProcessLockProvider for concurrent write testing
    conf.setString(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(),
        WriteConcurrencyMode.OPTIMISTIC_CONCURRENCY_CONTROL.name());
    conf.setString(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key(),
        InProcessLockProvider.class.getName());

    if (bufferType == BufferType.DISRUPTOR) {
      conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 1024);
    }

    executeAndVerifyRecordCount(conf, "concurrent_write_" + bufferType.name().toLowerCase(),
        sourceFile.toURI().toString(), 1, recordCount, tableDir);
  }

  /**
   * Generates test data file with specified record count.
   */
  private void generateTestDataFile(File outputFile, int recordCount) throws Exception {
    String[] partitions = {"par1", "par2", "par3", "par4"};
    String[] names = {"Alice", "Bob", "Charlie", "Diana", "Emma", "Frank", "Grace", "Han"};

    try (PrintWriter writer = new PrintWriter(new FileWriter(outputFile))) {
      for (int i = 0; i < recordCount; i++) {
        String partition = partitions[i % partitions.length];
        String name = names[i % names.length];
        int age = 20 + (i % 50);
        int seconds = (i % 60) + 1;
        String uuid = "id" + i;

        // Write JSON line (ts format matches original test_source.data)
        writer.printf("{\"uuid\": \"%s\", \"name\": \"%s\", \"age\": %d, \"ts\": \"1970-01-01T00:00:%02d\", \"partition\": \"%s\"}%n",
            uuid, name, age, seconds, partition);
      }
    }
  }

  /**
   * Tests different Disruptor wait strategies.
   * Each strategy has different CPU/latency tradeoffs.
   */
  @ParameterizedTest
  @ValueSource(strings = {"BLOCKING_WAIT", "SLEEPING_WAIT", "YIELDING_WAIT"})
  void testDisruptorWaitStrategies(String waitStrategy) throws Exception {
    Configuration conf = TestConfigurations.getDefaultConf(tempFile.toURI().toString());
    conf.set(FlinkOptions.OPERATION, "insert");
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, BufferType.DISRUPTOR.name());
    conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 1024);
    conf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_WAIT_STRATEGY, waitStrategy);
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");

    executeAndVerify(conf, "disruptor_wait_" + waitStrategy.toLowerCase(), 1, EXPECTED);
  }

  private void executeAndVerify(
      Configuration conf,
      String jobName,
      int checkpoints,
      Map<String, List<String>> expected) throws Exception {
    String sourcePath = Objects.requireNonNull(Thread.currentThread()
        .getContextClassLoader().getResource("test_source.data")).toString();
    executeAndVerifyWithSource(conf, jobName, sourcePath, checkpoints, expected, tempFile);
  }

  private void executeAndVerifyWithSource(
      Configuration conf,
      String jobName,
      String sourcePath,
      int checkpoints,
      Map<String, List<String>> expected,
      File tableDir) throws Exception {

    runPipeline(conf, jobName, sourcePath, checkpoints);
    TestData.checkWrittenDataCOW(tableDir, expected);
  }

  private void executeAndVerifyRecordCount(
      Configuration conf,
      String jobName,
      String sourcePath,
      int checkpoints,
      int expectedCount,
      File tableDir) throws Exception {

    runPipeline(conf, jobName, sourcePath, checkpoints);

    // Count records in all partitions
    RowType rowType =
        (RowType) HoodieSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();
    List<org.apache.avro.generic.GenericRecord> records = TestData.readAllData(tableDir, rowType, 4);
    org.junit.jupiter.api.Assertions.assertEquals(expectedCount, records.size(),
        "Expected " + expectedCount + " records but found " + records.size());
  }

  private void runPipeline(
      Configuration conf,
      String jobName,
      String sourcePath,
      int checkpoints) throws Exception {

    Configuration envConf = new Configuration();
    envConf.setString("execution.checkpointing.interval", "4s");
    envConf.setString("execution.checkpointing.mode", "EXACTLY_ONCE");
    envConf.setString("execution.checkpointing.max-concurrent-checkpoints", "1");

    StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment(envConf);
    execEnv.getConfig().disableObjectReuse();
    execEnv.setParallelism(FlinkMiniCluster.DEFAULT_PARALLELISM);

    RowType rowType =
        (RowType) HoodieSchemaConverter.convertToDataType(StreamerUtil.getSourceSchema(conf))
            .getLogicalType();

    DataStream<RowData> dataStream = execEnv
        .addSource(new ContinuousFileSource.BoundedSourceFunction(new Path(sourcePath), checkpoints))
        .name("continuous_file_source")
        .setParallelism(1)
        .map(JsonDeserializationFunction.getInstance(rowType))
        .setParallelism(FlinkMiniCluster.DEFAULT_PARALLELISM);

    OptionsInference.setupSinkTasks(conf, execEnv.getParallelism());
    DataStream<RowData> pipeline = Pipelines.append(conf, rowType, dataStream);
    execEnv.addOperator(pipeline.getTransformation());

    execEnv.execute(jobName);
  }
}
