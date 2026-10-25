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
import org.apache.hudi.common.util.queue.BaseHoodieQueueBasedExecutor;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Base test harness for write executor tests.
 * Provides common test logic to avoid duplication across different executor implementations.
 */
public abstract class BaseExecutorTestHarness extends HoodieSparkClientTestHarness {

  protected final String instantTime = InProcessTimeGenerator.createNewInstantTime();

  @BeforeEach
  public void setUp() throws Exception {
    initTestDataGenerator();
    if (requiresExecutorService()) {
      initExecutorServiceWithFixedThreadPool(2);
    }
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  /**
   * Create an executor instance for testing.
   *
   * @param records iterator of records to process
   * @param consumer consumer to process records
   * @return executor instance
   */
  protected abstract HoodieExecutor<Integer> createExecutor(
      Iterator<HoodieRecord> records, HoodieConsumer<HoodieRecord, Integer> consumer);

  /**
   * Whether the executor requires setting up an executor service.
   * @return true if executor service is needed
   */
  protected boolean requiresExecutorService() {
    return true;
  }

  /**
   * Whether the executor supports interrupt testing.
   * @return true if interrupt test should be run
   */
  protected boolean supportsInterruptTest() {
    return false;
  }

  /**
   * Whether the executor supports running status check.
   * @return true if running status check is supported
   */
  protected boolean supportsRunningStatusCheck() {
    return false;
  }

  /**
   * Get pre-execute runnable for Spark task context propagation.
   * @return runnable to execute before consuming records
   */
  protected Runnable getPreExecuteRunnable() {
    final TaskContext taskContext = TaskContext.get();
    return () -> TaskContext$.MODULE$.setTaskContext(taskContext);
  }

  /**
   * Create a simple counting consumer.
   *
   * @param consumedRecords list to store consumed records
   * @param <T> record type
   * @return consumer instance
   */
  protected <T> HoodieConsumer<T, Integer> createCountingConsumer(final List<T> consumedRecords) {
    return new HoodieConsumer<T, Integer>() {
      private int count = 0;

      @Override
      public void consume(T record) throws Exception {
        consumedRecords.add(record);
        count++;
      }

      @Override
      public Integer finish() {
        return count;
      }
    };
  }

  /**
   * Create a consumer that waits indefinitely (for interrupt testing).
   *
   * @param <T> record type
   * @return consumer instance
   */
  protected <T> HoodieConsumer<T, Integer> createWaitingConsumer() {
    return new HoodieConsumer<T, Integer>() {
      @Override
      public void consume(T record) {
        try {
          synchronized (this) {
            wait();
          }
        } catch (InterruptedException ie) {
          // Expected for interrupt tests
        }
      }

      @Override
      public Integer finish() {
        return 0;
      }
    };
  }

  @Test
  public void testExecutor() {
    final int numRecords = 128;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, numRecords);
    final List<HoodieRecord> consumedRecords = new ArrayList<>();

    HoodieConsumer<HoodieRecord, Integer> consumer = createCountingConsumer(consumedRecords);
    HoodieExecutor<Integer> exec = null;

    try {
      exec = createExecutor(hoodieRecords.iterator(), consumer);
      int result = exec.execute();

      // Verify all records were processed
      assertEquals(numRecords, result);

      // Verify consumed records are identical to produced ones and ordering is preserved
      assertEquals(hoodieRecords, consumedRecords);

      // Check running status if supported
      if (supportsRunningStatusCheck()) {
        BaseHoodieQueueBasedExecutor queueExec = (BaseHoodieQueueBasedExecutor) exec;
        assertFalse(queueExec.isRunning());
      }

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  @Test
  @Timeout(value = 60)
  public void testRecordReading() {
    final int numRecords = 100;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, numRecords);
    ArrayList<HoodieRecord> beforeRecord = new ArrayList<>();
    ArrayList<IndexedRecord> beforeIndexedRecord = new ArrayList<>();
    ArrayList<HoodieRecord> afterRecord = new ArrayList<>();
    ArrayList<IndexedRecord> afterIndexedRecord = new ArrayList<>();

    hoodieRecords.forEach(record -> {
      beforeRecord.add(record);
      beforeIndexedRecord.add((IndexedRecord) record.getData());
    });

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {
          private int count = 0;

          @Override
          public void consume(HoodieRecord record) {
            count++;
            afterRecord.add(record);
            afterIndexedRecord.add((IndexedRecord) record.getData());
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    HoodieExecutor<Integer> exec = null;

    try {
      exec = createExecutor(hoodieRecords.iterator(), consumer);
      int result = exec.execute();
      assertEquals(numRecords, result);

      assertEquals(beforeRecord, afterRecord);
      assertEquals(beforeIndexedRecord, afterIndexedRecord);

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  @Test
  @Timeout(value = 60)
  public void testException() {
    final int numRecords = 1000;
    final String errorMessage = "Exception when iterating records!!!";

    List<HoodieRecord> pRecs = dataGen.generateInserts(instantTime, numRecords);
    Iterator<HoodieRecord> iterator = new ThrowingIterator(pRecs.iterator(), errorMessage, numRecords / 10);

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {
          int count = 0;

          @Override
          public void consume(HoodieRecord payload) throws Exception {
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    HoodieExecutor<Integer> exec = createExecutor(iterator, consumer);

    final Throwable thrown = assertThrows(HoodieException.class, exec::execute,
        "exception is expected");
    assertTrue(thrown.getMessage().contains(errorMessage));
  }

  @Test
  @Timeout(value = 60)
  public void testInterruptExecutor() {
    if (!supportsInterruptTest()) {
      return;
    }

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);
    HoodieConsumer<HoodieRecord, Integer> consumer = createWaitingConsumer();

    HoodieExecutor<Integer> executor = createExecutor(hoodieRecords.iterator(), consumer);

    try {
      Thread.currentThread().interrupt();
      assertThrows(HoodieException.class, executor::execute);
      assertTrue(Thread.interrupted());
    } catch (Exception e) {
      // Expected
    }
  }

  /**
   * Iterator that throws an exception after a certain number of elements.
   */
  protected static class ThrowingIterator implements Iterator<HoodieRecord> {
    private final Iterator<HoodieRecord> iterator;
    private final AtomicInteger count = new AtomicInteger(0);
    private final String errorMessage;
    private final int errorMessageCount;

    public ThrowingIterator(Iterator<HoodieRecord> iterator, String errorMessage, int errorMessageCount) {
      this.iterator = iterator;
      this.errorMessage = errorMessage;
      this.errorMessageCount = errorMessageCount;
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public HoodieRecord next() {
      if (count.get() == errorMessageCount) {
        throw new HoodieException(errorMessage);
      }
      count.incrementAndGet();
      return iterator.next();
    }
  }
}

