/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.execution;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestHarness;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

import static org.apache.hudi.execution.HoodieLazyInsertIterable.getTransformFunction;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBoundedInMemoryExecutorInSpark extends HoodieClientTestHarness {

  private final String instantTime = HoodieActiveTimeline.createNewInstantTime();

  @BeforeEach
  public void setUp() throws Exception {
    initTestDataGenerator();
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  private Runnable getPreExecuteRunnable() {
    final TaskContext taskContext = TaskContext.get();
    return () -> TaskContext$.MODULE$.setTaskContext(taskContext);
  }

  @Test
  public void testExecutor() {

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE))).thenReturn(1024);
    BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          private int count = 0;

          @Override
          protected void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            count++;
          }

          @Override
          protected void finish() {
          }

          @Override
          protected Integer getResult() {
            return count;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor = null;
    try {
      executor = new BoundedInMemoryExecutor(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE)), hoodieRecords.iterator(), consumer,
          getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), getPreExecuteRunnable());
      int result = executor.execute();
      // It should buffer and write 100 records
      assertEquals(100, result);
      // There should be no remaining records in the buffer
      assertFalse(executor.isRemaining());
    } finally {
      if (executor != null) {
        executor.shutdownNow();
        executor.awaitTermination();
      }
    }
  }

  @Test
  public void testInterruptExecutor() {
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE))).thenReturn(1024);
    BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          @Override
          protected void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            try {
              while (true) {
                Thread.sleep(1000);
              }
            } catch (InterruptedException ie) {
              return;
            }
          }

          @Override
          protected void finish() {
          }

          @Override
          protected Integer getResult() {
            return 0;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor = null;
    try {
      executor = new BoundedInMemoryExecutor(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE)), hoodieRecords.iterator(), consumer,
          getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), getPreExecuteRunnable());
      BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> finalExecutor = executor;

      Thread.currentThread().interrupt();

      assertThrows(HoodieException.class, () -> finalExecutor.execute());
      assertTrue(Thread.interrupted());
    } finally {
      if (executor != null) {
        executor.shutdownNow();
        executor.awaitTermination();
      }
    }
  }

  @Test
  public void testExecutorTermination() {
    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE))).thenReturn(1024);
    Iterator<GenericRecord> unboundedRecordIter = new Iterator<GenericRecord>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public GenericRecord next() {
        return dataGen.generateGenericRecord();
      }
    };

    BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new BoundedInMemoryQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {
          @Override
          protected void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
          }

          @Override
          protected void finish() {
          }

          @Override
          protected Integer getResult() {
            return 0;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor =
        new BoundedInMemoryExecutor(Integer.parseInt(hoodieWriteConfig.getStringOrDefault(HoodieWriteConfig.WRITE_BUFFER_LIMIT_BYTES_VALUE)), unboundedRecordIter,
            consumer, getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA),
            getPreExecuteRunnable());
    executor.shutdownNow();
    boolean terminatedGracefully = executor.awaitTermination();
    assertTrue(terminatedGracefully);
  }
}
