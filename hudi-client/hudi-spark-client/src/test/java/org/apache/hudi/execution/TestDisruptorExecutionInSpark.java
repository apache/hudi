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
import org.apache.hudi.common.testutils.InProcessTimeGenerator;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDisruptorExecutionInSpark extends HoodieSparkClientTestHarness {

  private final String instantTime = InProcessTimeGenerator.createNewInstantTime();

  private final HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
      .withExecutorType(ExecutorType.DISRUPTOR.name())
      .withWriteExecutorDisruptorWriteBufferLimitBytes(8)
      .build(false);

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

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {

          private int count = 0;

          @Override
          public void consume(HoodieRecord record) {
            consumedRecords.add(record);
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };
    DisruptorExecutor<HoodieRecord, HoodieRecord, Integer> exec = null;

    try {
      exec = new DisruptorExecutor<>(writeConfig.getWriteExecutorDisruptorWriteBufferLimitBytes(), hoodieRecords.iterator(), consumer,
          Function.identity(), WaitStrategyFactory.DEFAULT_STRATEGY, getPreExecuteRunnable());
      int result = exec.execute();
      // It should buffer and write 100 records
      assertEquals(128, result);
      // There should be no remaining records in the buffer
      assertFalse(exec.isRunning());

      // collect all records and assert that consumed records are identical to produced ones
      // assert there's no tampering, and that the ordering is preserved
      assertEquals(hoodieRecords, consumedRecords);
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
  @Timeout(value = 60)
  public void testInterruptExecutor() {
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {

          @Override
          public void consume(HoodieRecord record) {
            try {
              synchronized (this) {
                wait();
              }
            } catch (InterruptedException ie) {
              // ignore here
            }
          }

          @Override
          public Integer finish() {
            return 0;
          }
        };

    DisruptorExecutor<HoodieRecord, HoodieRecord, Integer>
        executor = new DisruptorExecutor<>(1024, hoodieRecords.iterator(), consumer,
        Function.identity(), WaitStrategyFactory.DEFAULT_STRATEGY, getPreExecuteRunnable());

    try {
      Thread.currentThread().interrupt();
      assertThrows(HoodieException.class, executor::execute);
      assertTrue(Thread.interrupted());
    } catch (Exception e) {
      // ignore here
    }
  }
}
