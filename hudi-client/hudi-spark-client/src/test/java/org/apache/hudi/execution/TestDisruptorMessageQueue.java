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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.DisruptorMessageQueue;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.HoodieProducer;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.execution.HoodieLazyInsertIterable.HoodieInsertValueGenResult;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;

import org.apache.avro.generic.IndexedRecord;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.apache.hudi.common.testutils.HoodieTestUtils.getRootCause;
import static org.apache.hudi.execution.HoodieLazyInsertIterable.getTransformerInternal;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestDisruptorMessageQueue extends HoodieSparkClientTestHarness {

  private final String instantTime = InProcessTimeGenerator.createNewInstantTime();

  private final HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
      .withExecutorType(ExecutorType.DISRUPTOR.name())
      .withWriteExecutorDisruptorWriteBufferLimitBytes(16)
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

    HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          private int count = 0;

          @Override
          public void consume(HoodieInsertValueGenResult<HoodieRecord> record) {
            count++;
            afterRecord.add(record.getResult());
            afterIndexedRecord.add((IndexedRecord) record.getResult().getData());
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    DisruptorExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> exec = null;

    try {
      exec = new DisruptorExecutor(writeConfig.getWriteExecutorDisruptorWriteBufferLimitBytes(), hoodieRecords.iterator(), consumer,
          getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig), WaitStrategyFactory.DEFAULT_STRATEGY, getPreExecuteRunnable());
      int result = exec.execute();
      // It should buffer and write 100 records
      assertEquals(100, result);
      // There should be no remaining records in the buffer
      assertFalse(exec.isRunning());

      assertEquals(beforeRecord, afterRecord);
      assertEquals(beforeIndexedRecord, afterIndexedRecord);

    } finally {
      if (exec != null) {
        exec.shutdownNow();
      }
    }
  }

  /**
   * Test to ensure that we are reading all records from queue iterator when we have multiple producers.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Timeout(value = 60)
  public void testCompositeProducerRecordReading() throws Exception {
    final int numRecords = 1000;
    final int numProducers = 40;
    final List<List<HoodieRecord>> recs = new ArrayList<>();

    final DisruptorMessageQueue<HoodieRecord, HoodieInsertValueGenResult> queue =
        new DisruptorMessageQueue(1024, getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig),
            "BLOCKING_WAIT", numProducers, new Runnable() {
              @Override
          public void run() {
                // do nothing.
              }
            });

    // Record Key to <Producer Index, Rec Index within a producer>
    Map<String, Pair<Integer, Integer>> keyToProducerAndIndexMap = new HashMap<>();

    for (int i = 0; i < numProducers; i++) {
      List<HoodieRecord> pRecs = dataGen.generateInserts(instantTime, numRecords);
      int j = 0;
      for (HoodieRecord r : pRecs) {
        assertFalse(keyToProducerAndIndexMap.containsKey(r.getRecordKey()));
        keyToProducerAndIndexMap.put(r.getRecordKey(), Pair.of(i, j));
        j++;
      }
      recs.add(pRecs);
    }

    List<HoodieProducer> producers = new ArrayList<>();
    for (int i = 0; i < recs.size(); i++) {
      final List<HoodieRecord> r = recs.get(i);
      // Alternate between pull and push based iterators
      if (i % 2 == 0) {
        HoodieProducer producer = new IteratorBasedQueueProducer<>(r.iterator());
        producers.add(producer);
      } else {
        HoodieProducer producer = new FunctionBasedQueueProducer<>((buf) -> {
          Iterator<HoodieRecord> itr = r.iterator();
          while (itr.hasNext()) {
            try {
              buf.insertRecord(itr.next());
            } catch (Exception e) {
              throw new HoodieException(e);
            }
          }
          return true;
        });
        producers.add(producer);
      }
    }

    // Used to ensure that consumer sees the records generated by a single producer in FIFO order
    Map<Integer, Integer> lastSeenMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> -1));
    Map<Integer, Integer> countMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> 0));

    // setup consumer and start disruptor
    HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          @Override
          public void consume(HoodieInsertValueGenResult<HoodieRecord> payload) {
            // Read recs and ensure we have covered all producer recs.
            final HoodieRecord rec = payload.getResult();
            Pair<Integer, Integer> producerPos = keyToProducerAndIndexMap.get(rec.getRecordKey());
            Integer lastSeenPos = lastSeenMap.get(producerPos.getLeft());
            countMap.put(producerPos.getLeft(), countMap.get(producerPos.getLeft()) + 1);
            lastSeenMap.put(producerPos.getLeft(), lastSeenPos + 1);
            // Ensure we are seeing the next record generated
            assertEquals(lastSeenPos + 1, producerPos.getRight().intValue());
          }

          @Override
          public Integer finish() {
            return 0;
          }
        };

    Method setHandlersFunc = queue.getClass().getDeclaredMethod("setHandlers", HoodieConsumer.class);
    setHandlersFunc.setAccessible(true);
    setHandlersFunc.invoke(queue, consumer);

    Method startFunc = queue.getClass().getDeclaredMethod("start");
    startFunc.setAccessible(true);
    startFunc.invoke(queue);

    // start to produce records
    CompletableFuture<Void> producerFuture = CompletableFuture.allOf(producers.stream().map(producer -> {
      return CompletableFuture.supplyAsync(() -> {
        try {
          producer.produce(queue);
        } catch (Throwable e) {
          throw new HoodieException("Error producing records in disruptor executor", e);
        }
        return true;
      }, executorService);
    }).toArray(CompletableFuture[]::new));

    producerFuture.get();

    // wait for all the records consumed.
    queue.close();

    for (int i = 0; i < numProducers; i++) {
      // Ensure we have seen all the records for each producers
      assertEquals(Integer.valueOf(numRecords), countMap.get(i));
    }
  }

  /**
   * Test to ensure that one of the producers exception will stop current ingestion.
   */
  @SuppressWarnings("unchecked")
  @Test
  @Timeout(value = 60)
  public void testException() throws Exception {
    final int numRecords = 1000;

    List<HoodieRecord> pRecs = dataGen.generateInserts(instantTime, numRecords);

    // create 2 producers
    // producer1 : common producer
    // producer2 : exception producer
    List<HoodieProducer> producers = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      if (i % 2 == 0) {
        producers.add(new IteratorBasedQueueProducer<>(pRecs.iterator()));
      } else {
        producers.add(new FunctionBasedQueueProducer<>((buf) -> {
          throw new HoodieException("Exception when produce records!!!");
        }));
      }
    }

    HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new HoodieConsumer<HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          int count = 0;

          @Override
          public void consume(HoodieInsertValueGenResult<HoodieRecord> payload) {
            // Read recs and ensure we have covered all producer recs.
            final HoodieRecord rec = payload.getResult();
            count++;
          }

          @Override
          public Integer finish() {
            return count;
          }
        };

    DisruptorExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> exec = new DisruptorExecutor(1024,
        producers, consumer, getTransformerInternal(HoodieTestDataGenerator.HOODIE_SCHEMA, writeConfig),
        WaitStrategyFactory.DEFAULT_STRATEGY, getPreExecuteRunnable());

    final Throwable thrown = assertThrows(HoodieException.class, exec::execute,
        "exception is expected");

    assertEquals("Exception when produce records!!!", getRootCause(thrown).getMessage());
  }
}
