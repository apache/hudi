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
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.SimpleExecutor;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class  TestSimpleExecutionInSpark extends HoodieSparkClientTestHarness {

  private final String instantTime = InProcessTimeGenerator.createNewInstantTime();

  @BeforeEach
  public void setUp() throws Exception {
    initTestDataGenerator();
    initExecutorServiceWithFixedThreadPool(2);
  }

  @AfterEach
  public void tearDown() throws Exception {
    cleanupResources();
  }

  @Test
  public void testExecutor() {

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 128);
    final List<HoodieRecord> consumedRecords = new ArrayList<>();

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {
          private int count = 0;

          @Override
          public void consume(HoodieRecord record) throws Exception {
            consumedRecords.add(record);
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };
    SimpleExecutor<HoodieRecord, HoodieRecord, Integer> exec = null;

    try {
      exec = new SimpleExecutor<>(hoodieRecords.iterator(), consumer, Function.identity());

      int result = exec.execute();
      // It should buffer and write 128 records
      assertEquals(128, result);

      // collect all records and assert that consumed records are identical to produced ones
      // assert there's no tampering, and that the ordering is preserved
      assertEquals(hoodieRecords, consumedRecords);

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  // Test to ensure that we are reading all records from queue iterator in the same order
  // without any exceptions.
  @SuppressWarnings("unchecked")
  @Test
  @Timeout(value = 60)
  public void testRecordReading() {

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);
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

    SimpleExecutor<HoodieRecord, HoodieRecord, Integer> exec = null;

    try {
      exec = new SimpleExecutor<>(hoodieRecords.iterator(), consumer, Function.identity());
      int result = exec.execute();
      assertEquals(100, result);

      assertEquals(beforeRecord, afterRecord);
      assertEquals(beforeIndexedRecord, afterIndexedRecord);

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  /**
   * Test to ensure exception happen in iterator then we need to stop the simple ingestion.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Timeout(value = 60)
  public void testException() {
    final int numRecords = 1000;
    final String errorMessage = "Exception when iterating records!!!";

    List<HoodieRecord> pRecs = dataGen.generateInserts(instantTime, numRecords);
    InnerIterator iterator = new InnerIterator(pRecs.iterator(), errorMessage, numRecords / 10);

    HoodieConsumer<HoodieRecord, Integer> consumer =
        new HoodieConsumer<HoodieRecord, Integer>() {
          int count = 0;

          @Override
          public void consume(HoodieRecord payload) throws Exception {
            // Read recs and ensure we have covered all producer recs.
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    SimpleExecutor<HoodieRecord, HoodieRecord, Integer> exec =
        new SimpleExecutor<>(iterator, consumer, Function.identity());

    final Throwable thrown = assertThrows(HoodieException.class, exec::execute,
        "exception is expected");
    assertTrue(thrown.getMessage().contains(errorMessage));
  }

  class InnerIterator implements Iterator<HoodieRecord> {

    private Iterator<HoodieRecord> iterator;
    private AtomicInteger count = new AtomicInteger(0);
    private String errorMessage;
    private int errorMessageCount;

    public InnerIterator(Iterator<HoodieRecord> iterator, String errorMessage, int errorMessageCount) {
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
