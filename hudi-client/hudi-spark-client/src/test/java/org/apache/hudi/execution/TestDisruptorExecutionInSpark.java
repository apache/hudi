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

import static org.apache.hudi.execution.HoodieLazyInsertIterable.getTransformFunction;

import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.IteratorBasedQueueConsumer;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDisruptorExecutionInSpark extends HoodieClientTestHarness {

  private final String instantTime = HoodieActiveTimeline.createNewInstantTime();

  @BeforeEach
  public void setUp() throws Exception {
    initTestDataGenerator();
    initExecutorServiceWithFixedThreadPool(2);
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

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 128);
    final List<HoodieRecord> consumedRecords = new ArrayList<>();

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(hoodieWriteConfig.getWriteBufferSize()).thenReturn(Option.of(8));
    IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          private int count = 0;

          @Override
          public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            consumedRecords.add(record.record);
            count++;
          }

          @Override
          protected Integer getResult() {
            return count;
          }
        };
    DisruptorExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> exec = null;

    try {
      exec = new DisruptorExecutor(hoodieWriteConfig.getWriteBufferSize(), hoodieRecords.iterator(), consumer,
          getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), Option.of(WaitStrategyFactory.DEFAULT_STRATEGY), getPreExecuteRunnable());
      int result = exec.execute();
      // It should buffer and write 100 records
      assertEquals(128, result);
      // There should be no remaining records in the buffer
      assertFalse(exec.isRemaining());

      // collect all records and assert that consumed records are identical to produced ones
      // assert there's no tampering, and that the ordering is preserved
      assertEquals(hoodieRecords.size(), consumedRecords.size());
      for (int i = 0; i < hoodieRecords.size(); i++) {
        assertEquals(hoodieRecords.get(i), consumedRecords.get(i));
      }

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  @Test
  public void testInterruptExecutor() {
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);
    final Lock lock = new ReentrantLock();
    Condition condition = lock.newCondition();

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(hoodieWriteConfig.getWriteBufferSize()).thenReturn(Option.of(1024));
    IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          @Override
          public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            try {
              condition.wait();
            } catch (InterruptedException ie) {
              // ignore here
            }
          }

          @Override
          protected Integer getResult() {
            return 0;
          }
        };

    DisruptorExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer>
        executor = new DisruptorExecutor(hoodieWriteConfig.getWriteBufferSize(), hoodieRecords.iterator(), consumer,
        getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA), Option.of(WaitStrategyFactory.DEFAULT_STRATEGY), getPreExecuteRunnable());

    try {
      Thread.currentThread().interrupt();
      assertThrows(HoodieException.class, executor::execute);
      assertTrue(Thread.interrupted());
    } catch (Exception e) {
      // ignore here
    } finally {
      executor.shutdownNow();
    }
  }
}
