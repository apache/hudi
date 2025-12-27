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
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;

import scala.Tuple2;

import static org.apache.hudi.execution.HoodieLazyInsertIterable.getTransformerInternal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestBoundedInMemoryExecutorInSpark extends HoodieSparkClientTestHarness {

  private final String instantTime = InProcessTimeGenerator.createNewInstantTime();

  private final HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
      .withExecutorType(ExecutorType.BOUNDED_IN_MEMORY.name())
      .withWriteBufferLimitBytes(1024)
      .build(false);

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

    final int recordNumber = 100;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, recordNumber);

    HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          private int count = 0;

          @Override
          public void consume(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor = null;
    try {
      executor = new BoundedInMemoryExecutor(writeConfig.getWriteBufferLimitBytes(), hoodieRecords.iterator(), consumer,
          getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig), getPreExecuteRunnable());
      int result = executor.execute();

      assertEquals(100, result);
      // There should be no remaining records in the buffer
      assertFalse(executor.isRunning());
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

    HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          @Override
          public void consume(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            try {
              while (true) {
                Thread.sleep(1000);
              }
            } catch (InterruptedException ie) {
              return;
            }
          }

          @Override
          public Integer finish() {
            return 0;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor =
        new BoundedInMemoryExecutor(writeConfig.getWriteBufferLimitBytes(), hoodieRecords.iterator(), consumer,
            getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig), getPreExecuteRunnable());

    // Interrupt the current thread (therefore triggering executor to throw as soon as it
    // invokes [[get]] on the [[CompletableFuture]])
    Thread.currentThread().interrupt();

    assertThrows(HoodieException.class, executor::execute);

    // Validate that interrupted flag is reset, after [[InterruptedException]] is thrown
    assertTrue(Thread.interrupted());

    executor.shutdownNow();
    executor.awaitTermination();
  }

  @Test
  public void testExecutorTermination() {
    Iterator<HoodieRecord> unboundedRecordIter = new Iterator<HoodieRecord>() {
      @Override
      public boolean hasNext() {
        return true;
      }

      @Override
      public HoodieRecord next() {
        return dataGen.generateInserts(instantTime, 1).get(0);
      }
    };

    HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {
          @Override
          public void consume(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
          }

          @Override
          public Integer finish() {
            return 0;
          }
        };

    BoundedInMemoryExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> executor =
        new BoundedInMemoryExecutor(writeConfig.getWriteBufferLimitBytes(), unboundedRecordIter,
            consumer, getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig),
            getPreExecuteRunnable());
    executor.shutdownNow();
    boolean terminatedGracefully = executor.awaitTermination();
    assertTrue(terminatedGracefully);
  }
}
