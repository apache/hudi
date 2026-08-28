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

package org.apache.hudi.sink.append;

import org.apache.hudi.common.util.queue.DisruptorMessageQueue;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.buffer.BufferType;
import org.apache.hudi.sink.utils.TestWriteBase;
import org.apache.hudi.utils.TestConfigurations;
import org.apache.hudi.utils.TestData;

import org.apache.avro.generic.GenericRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for append write functions with buffer sorting.
 */
public class TestAppendWriteFunctionWithBufferSort {

  private Configuration conf;

  @TempDir
  File tempFile;

  @BeforeEach
  void setUp() {
    conf = new Configuration();
  }

  @Test
  public void testExplicitBufferTypeTakesPrecedence() {
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, BufferType.BOUNDED_IN_MEMORY.name());
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);

    assertEquals(BufferType.BOUNDED_IN_MEMORY.name(), AppendWriteFunctions.resolveBufferType(conf));
  }

  @Test
  public void testDeprecatedSortEnabledResolvesToDisruptor() {
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_ENABLED, true);

    assertEquals(BufferType.DISRUPTOR.name(), AppendWriteFunctions.resolveBufferType(conf));
  }

  @Test
  public void testSortKeysDefaultToRecordKey() {
    conf.set(FlinkOptions.RECORD_KEY_FIELD, "uuid");

    List<String> resolvedSortKeys = AppendWriteFunctions.resolveSortKeys(conf);
    assertEquals(Arrays.asList("uuid"), resolvedSortKeys);
  }

  @Test
  public void testCustomSortKeysOverrideDefault() {
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "age,name");

    List<String> resolvedSortKeys = AppendWriteFunctions.resolveSortKeys(conf);
    assertEquals(Arrays.asList("age", "name"), resolvedSortKeys);
  }

  @Test
  public void testEmptySortKeysAreRejected() {
    conf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, " , , ");

    assertThrows(IllegalArgumentException.class, () -> AppendWriteFunctions.resolveSortKeys(conf));
  }

  @ParameterizedTest
  @EnumSource(BufferType.class)
  public void testFactorySelectsConfiguredBuffer(BufferType bufferType) {
    conf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    RowType rowType = TestConfigurations.ROW_TYPE;

    AppendWriteFunction<RowData> function = AppendWriteFunctions.create(conf, rowType);

    switch (bufferType) {
      case CONTINUOUS_SORT:
        assertInstanceOf(AppendWriteFunctionWithContinuousSort.class, function);
        break;
      case DISRUPTOR:
        assertInstanceOf(AppendWriteFunctionWithDisruptorBufferSort.class, function);
        break;
      case BOUNDED_IN_MEMORY:
        assertInstanceOf(AppendWriteFunctionWithBIMBufferSort.class, function);
        break;
      case NONE:
        assertEquals(AppendWriteFunction.class, function.getClass());
        break;
      default:
        throw new AssertionError("Unexpected buffer type " + bufferType);
    }
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"CONTINUOUS_SORT", "DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testBufferedAppendFunctionsWriteAndFlushOnCheckpoint(BufferType bufferType) throws Exception {
    Configuration writeConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    writeConf.set(FlinkOptions.OPERATION, "insert");
    writeConf.set(FlinkOptions.METADATA_ENABLED, false);
    writeConf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    writeConf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    writeConf.set(FlinkOptions.WRITE_BUFFER_SIZE, 3L);
    if (bufferType == BufferType.CONTINUOUS_SORT) {
      writeConf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 1);
    } else if (bufferType == BufferType.DISRUPTOR) {
      writeConf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 16);
    }

    TestWriteBase.TestHarness harness = TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, writeConf);
    try {
      harness
          .consume(TestData.DATA_SET_INSERT)
          .checkpoint(1)
          .assertNextEvent(4, "par1,par2,par3,par4")
          .checkpointComplete(1);
    } finally {
      harness.end();
    }
  }

  @ParameterizedTest
  @EnumSource(value = BufferType.class, names = {"CONTINUOUS_SORT", "DISRUPTOR", "BOUNDED_IN_MEMORY"})
  public void testBufferedAppendFunctionsPersistRowsInSortOrder(BufferType bufferType) throws Exception {
    Configuration writeConf = TestConfigurations.getDefaultConf(tempFile.getAbsolutePath());
    writeConf.set(FlinkOptions.OPERATION, "insert");
    writeConf.set(FlinkOptions.METADATA_ENABLED, false);
    writeConf.set(FlinkOptions.WRITE_BUFFER_TYPE, bufferType.name());
    writeConf.set(FlinkOptions.WRITE_BUFFER_SORT_KEYS, "name,age");
    writeConf.set(FlinkOptions.WRITE_BUFFER_SIZE, 3L);
    if (bufferType == BufferType.CONTINUOUS_SORT) {
      writeConf.set(FlinkOptions.WRITE_BUFFER_SORT_CONTINUOUS_DRAIN_SIZE, 1);
    } else if (bufferType == BufferType.DISRUPTOR) {
      writeConf.set(FlinkOptions.WRITE_BUFFER_DISRUPTOR_RING_SIZE, 16);
    }

    List<RowData> inputData = Arrays.asList(
        TestData.insertRow(StringData.fromString("uuid1"), StringData.fromString("Bob"), 30,
            TimestampData.fromEpochMillis(1123), StringData.fromString("p1")),
        TestData.insertRow(StringData.fromString("uuid2"), StringData.fromString("Alice"), 25,
            TimestampData.fromEpochMillis(1124), StringData.fromString("p1")),
        TestData.insertRow(StringData.fromString("uuid3"), StringData.fromString("Bob"), 21,
            TimestampData.fromEpochMillis(31124), StringData.fromString("p1")));
    List<String> expected = Arrays.asList(
        "uuid2,Alice,25,1970-01-01 00:00:01.124,p1",
        "uuid3,Bob,21,1970-01-01 00:00:31.124,p1",
        "uuid1,Bob,30,1970-01-01 00:00:01.123,p1");

    TestWriteBase.TestHarness harness = TestWriteBase.TestHarness.instance()
        .preparePipeline(tempFile, writeConf);
    try {
      harness
          .consume(inputData)
          .checkpoint(1)
          .assertNextEvent(1, "p1")
          .checkpointComplete(1);

      List<GenericRecord> persistedRows =
          TestData.readAllData(new File(writeConf.get(FlinkOptions.PATH)), TestConfigurations.ROW_TYPE, 1);
      List<String> actual = persistedRows.stream()
          .map(TestData::filterOutVariablesWithoutHudiMetadata)
          .collect(Collectors.toList());
      assertEquals(expected, actual);
    } finally {
      harness.end();
    }
  }

  // -------------------------------------------------------------------------
  //  Tests for DisruptorMessageQueue.waitUntilDrained()
  // -------------------------------------------------------------------------

  @Test
  @Timeout(10)
  public void testWaitUntilDrainedOnEmptyQueue() {
    DisruptorMessageQueue<String, String> queue = new DisruptorMessageQueue<>(
        16, Function.identity(), "BLOCKING_WAIT", 1, null);
    AtomicInteger consumed = new AtomicInteger(0);
    queue.setHandlers(new HoodieConsumer<String, Void>() {
      @Override
      public void consume(String record) {
        consumed.incrementAndGet();
      }

      @Override
      public Void finish() {
        return null;
      }
    });
    queue.start();

    queue.waitUntilDrained();
    assertEquals(0, consumed.get());
    assertTrue(queue.isEmpty());
    queue.close();
  }

  @Test
  @Timeout(10)
  public void testWaitUntilDrainedConsumesAllRecords() throws Exception {
    DisruptorMessageQueue<String, String> queue = new DisruptorMessageQueue<>(
        16, Function.identity(), "BLOCKING_WAIT", 1, null);
    AtomicInteger consumed = new AtomicInteger(0);
    queue.setHandlers(new HoodieConsumer<String, Void>() {
      @Override
      public void consume(String record) {
        consumed.incrementAndGet();
      }

      @Override
      public Void finish() {
        return null;
      }
    });
    queue.start();

    for (int i = 0; i < 10; i++) {
      queue.insertRecord("record-" + i);
    }

    queue.waitUntilDrained();
    assertEquals(10, consumed.get());
    assertTrue(queue.isEmpty());
    queue.close();
  }

  @Test
  @Timeout(10)
  public void testWaitUntilDrainedKeepsThreadAlive() throws Exception {
    DisruptorMessageQueue<String, String> queue = new DisruptorMessageQueue<>(
        16, Function.identity(), "BLOCKING_WAIT", 1, null);
    AtomicReference<Thread> consumerThread = new AtomicReference<>();
    queue.setHandlers(new HoodieConsumer<String, Void>() {
      @Override
      public void consume(String record) {
        consumerThread.set(Thread.currentThread());
      }

      @Override
      public Void finish() {
        return null;
      }
    });
    queue.start();

    queue.insertRecord("record");
    queue.waitUntilDrained();

    assertTrue(consumerThread.get().isAlive(),
        "Disruptor thread must remain alive after waitUntilDrained()");

    // Queue should still accept new records
    queue.insertRecord("record-after-drain");
    queue.waitUntilDrained();

    queue.close();
  }

  @Test
  @Timeout(10)
  public void testWaitUntilDrainedReturnsOnInterrupt() throws Exception {
    DisruptorMessageQueue<String, String> queue = new DisruptorMessageQueue<>(
        16, Function.identity(), "BLOCKING_WAIT", 1, null);
    CountDownLatch blockConsumer = new CountDownLatch(1);
    queue.setHandlers(new HoodieConsumer<String, Void>() {
      @Override
      public void consume(String record) throws Exception {
        blockConsumer.await();
      }

      @Override
      public Void finish() {
        return null;
      }
    });
    queue.start();

    queue.insertRecord("blocked-record");

    Thread caller = Thread.currentThread();
    new Thread(() -> {
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        // ignored
      }
      caller.interrupt();
    }).start();

    queue.waitUntilDrained();
    assertTrue(Thread.interrupted(), "Interrupt flag should be set");
    assertTrue(queue.getThrowable() != null, "Interruption should be recorded as failure");

    blockConsumer.countDown();
    queue.close();
  }

  @Test
  @Timeout(10)
  public void testWaitUntilDrainedReturnsEarlyOnConsumerError() throws Exception {
    DisruptorMessageQueue<String, String> queue = new DisruptorMessageQueue<>(
        16, Function.identity(), "BLOCKING_WAIT", 1, null);
    queue.setHandlers(new HoodieConsumer<String, Void>() {
      @Override
      public void consume(String record) throws Exception {
        throw new RuntimeException("simulated consumer failure");
      }

      @Override
      public Void finish() {
        return null;
      }
    });
    queue.start();

    queue.insertRecord("will-fail");
    Thread.sleep(100);

    queue.waitUntilDrained();
    assertTrue(queue.getThrowable() != null, "Error should be recorded");
    queue.close();
  }
}
