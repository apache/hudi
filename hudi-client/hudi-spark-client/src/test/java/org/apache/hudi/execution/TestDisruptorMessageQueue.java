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
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.DisruptorMessageHandler;
import org.apache.hudi.common.util.queue.DisruptorMessageQueue;
import org.apache.hudi.common.util.queue.DisruptorPublisher;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.common.util.queue.HoodieProducer;
import org.apache.hudi.common.util.queue.IteratorBasedQueueConsumer;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieClientTestHarness;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDisruptorMessageQueue extends HoodieClientTestHarness {

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

  // Test to ensure that we are reading all records from queue iterator in the same order
  // without any exceptions.
  @SuppressWarnings("unchecked")
  @Test
  @Timeout(value = 60)
  public void testRecordReading() throws Exception {

    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(instantTime, 100);
    ArrayList<HoodieRecord> beforeRecord = new ArrayList<>();
    ArrayList<IndexedRecord> beforeIndexedRecord = new ArrayList<>();
    ArrayList<HoodieAvroRecord> afterRecord = new ArrayList<>();
    ArrayList<IndexedRecord> afterIndexedRecord = new ArrayList<>();

    hoodieRecords.forEach(record -> {
      final HoodieAvroRecord originalRecord = (HoodieAvroRecord) record;
      beforeRecord.add(originalRecord);
      try {
        final Option<IndexedRecord> originalInsertValue =
            originalRecord.getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA);
        beforeIndexedRecord.add(originalInsertValue.get());
      } catch (IOException e) {
        // ignore exception here.
      }
    });

    HoodieWriteConfig hoodieWriteConfig = mock(HoodieWriteConfig.class);
    when(hoodieWriteConfig.getWriteBufferSize()).thenReturn(Option.of(16));
    IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          private int count = 0;

          @Override
          public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> record) {
            count++;
            afterRecord.add((HoodieAvroRecord) record.record);
            try {
              IndexedRecord indexedRecord = (IndexedRecord)((HoodieAvroRecord) record.record)
                  .getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA).get();
              afterIndexedRecord.add(indexedRecord);
            } catch (IOException e) {
             //ignore exception here.
            }
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
      assertEquals(100, result);
      // There should be no remaining records in the buffer
      assertFalse(exec.isRemaining());

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

    final DisruptorMessageQueue<HoodieRecord, HoodieLazyInsertIterable.HoodieInsertValueGenResult> queue =
        new DisruptorMessageQueue(Option.of(1024), getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA),
            Option.of("BLOCKING_WAIT"), numProducers, new Runnable() {
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

    List<DisruptorPublisher> disruptorPublishers = new ArrayList<>();
    for (int i = 0; i < recs.size(); i++) {
      final List<HoodieRecord> r = recs.get(i);
      // Alternate between pull and push based iterators
      if (i % 2 == 0) {
        DisruptorPublisher publisher = new DisruptorPublisher<>(new IteratorBasedQueueProducer<>(r.iterator()), queue);
        disruptorPublishers.add(publisher);
      } else {
        DisruptorPublisher publisher = new DisruptorPublisher<>(new FunctionBasedQueueProducer<>((buf) -> {
          Iterator<HoodieRecord> itr = r.iterator();
          while (itr.hasNext()) {
            try {
              buf.insertRecord(itr.next());
            } catch (Exception e) {
              throw new HoodieException(e);
            }
          }
          return true;
        }), queue);
        disruptorPublishers.add(publisher);
      }
    }

    // Used to ensure that consumer sees the records generated by a single producer in FIFO order
    Map<Integer, Integer> lastSeenMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> -1));
    Map<Integer, Integer> countMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> 0));


    // setup consumer and start disruptor
    DisruptorMessageHandler<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> handler =
        new DisruptorMessageHandler<>(new IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

          @Override
          public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> payload) {
            // Read recs and ensure we have covered all producer recs.
            final HoodieRecord rec = payload.record;
            Pair<Integer, Integer> producerPos = keyToProducerAndIndexMap.get(rec.getRecordKey());
            Integer lastSeenPos = lastSeenMap.get(producerPos.getLeft());
            countMap.put(producerPos.getLeft(), countMap.get(producerPos.getLeft()) + 1);
            lastSeenMap.put(producerPos.getLeft(), lastSeenPos + 1);
            // Ensure we are seeing the next record generated
            assertEquals(lastSeenPos + 1, producerPos.getRight().intValue());
          }

          @Override
          protected Integer getResult() {
            return 0;
          }
        });


    queue.setHandlers(handler);
    queue.start();


    // start to produce records
    final List<Future<Boolean>> futureList = disruptorPublishers.stream().map(disruptorPublisher -> {
      return executorService.submit(() -> {
        disruptorPublisher.startProduce();
        return true;
      });
    }).collect(Collectors.toList());

    // wait for all producers finished.
    futureList.forEach(future -> {
      try {
        future.get();
      } catch (Exception e) {
        // ignore here
      }
    });

    // wait for all the records consumed.
    queue.close();
    queue.waitForConsumingFinished();

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
    final int numProducers = 40;

    final DisruptorMessageQueue<HoodieRecord, HoodieLazyInsertIterable.HoodieInsertValueGenResult> queue =
        new DisruptorMessageQueue(Option.of(1024), getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA),
            Option.of("BLOCKING_WAIT"), numProducers, new Runnable() {
              @Override
          public void run() {
              // do nothing.
              }
            });

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


    IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer> consumer =
        new IteratorBasedQueueConsumer<HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord>, Integer>() {

      int count = 0;
      @Override
      public void consumeOneRecord(HoodieLazyInsertIterable.HoodieInsertValueGenResult<HoodieRecord> payload) {
        // Read recs and ensure we have covered all producer recs.
        final HoodieRecord rec = payload.record;
        count++;
      }

      @Override
      protected Integer getResult() {
        return count;
      }
    };

    DisruptorExecutor<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>, Integer> exec = new DisruptorExecutor(Option.of(1024),
        producers, Option.of(consumer), getTransformFunction(HoodieTestDataGenerator.AVRO_SCHEMA),
        Option.of(WaitStrategyFactory.DEFAULT_STRATEGY), getPreExecuteRunnable());

    final Throwable thrown = assertThrows(HoodieException.class, exec::execute,
        "exception is expected");
    assertEquals("java.util.concurrent.ExecutionException: org.apache.hudi.exception.HoodieException: Error producing records in disruptor executor",
        thrown.getMessage());
  }
}
