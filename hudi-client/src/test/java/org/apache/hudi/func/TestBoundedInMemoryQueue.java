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

package org.apache.hudi.func;

import org.apache.hudi.HoodieClientTestHarness;
import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueue;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueProducer;
import org.apache.hudi.common.util.queue.FunctionBasedQueueProducer;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.func.CopyOnWriteLazyInsertIterable.HoodieInsertValueGenResult;

import org.apache.avro.generic.IndexedRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Tuple2;

import static org.apache.hudi.func.CopyOnWriteLazyInsertIterable.getTransformFunction;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBoundedInMemoryQueue extends HoodieClientTestHarness {

  private final String commitTime = HoodieActiveTimeline.createNewInstantTime();

  @Before
  public void setUp() throws Exception {
    initTestDataGenerator();
    initExecutorServiceWithFixedThreadPool(2);
  }

  @After
  public void tearDown() throws Exception {
    cleanupTestDataGenerator();
    cleanupExecutorService();
  }

  // Test to ensure that we are reading all records from queue iterator in the same order
  // without any exceptions.
  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public void testRecordReading() throws Exception {
    final int numRecords = 128;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(commitTime, numRecords);
    final BoundedInMemoryQueue<HoodieRecord, HoodieInsertValueGenResult<HoodieRecord>> queue =
        new BoundedInMemoryQueue(FileIOUtils.KB, getTransformFunction(HoodieTestDataGenerator.avroSchema));
    // Produce
    Future<Boolean> resFuture = executorService.submit(() -> {
      new IteratorBasedQueueProducer<>(hoodieRecords.iterator()).produce(queue);
      queue.close();
      return true;
    });
    final Iterator<HoodieRecord> originalRecordIterator = hoodieRecords.iterator();
    int recordsRead = 0;
    while (queue.iterator().hasNext()) {
      final HoodieRecord originalRecord = originalRecordIterator.next();
      final Option<IndexedRecord> originalInsertValue =
          originalRecord.getData().getInsertValue(HoodieTestDataGenerator.avroSchema);
      final HoodieInsertValueGenResult<HoodieRecord> payload = queue.iterator().next();
      // Ensure that record ordering is guaranteed.
      Assert.assertEquals(originalRecord, payload.record);
      // cached insert value matches the expected insert value.
      Assert.assertEquals(originalInsertValue,
          payload.record.getData().getInsertValue(HoodieTestDataGenerator.avroSchema));
      recordsRead++;
    }
    Assert.assertFalse(queue.iterator().hasNext() || originalRecordIterator.hasNext());
    // all the records should be read successfully.
    Assert.assertEquals(numRecords, recordsRead);
    // should not throw any exceptions.
    resFuture.get();
  }

  /**
   * Test to ensure that we are reading all records from queue iterator when we have multiple producers.
   */
  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public void testCompositeProducerRecordReading() throws Exception {
    final int numRecords = 1000;
    final int numProducers = 40;
    final List<List<HoodieRecord>> recs = new ArrayList<>();

    final BoundedInMemoryQueue<HoodieRecord, HoodieInsertValueGenResult<HoodieRecord>> queue =
        new BoundedInMemoryQueue(FileIOUtils.KB, getTransformFunction(HoodieTestDataGenerator.avroSchema));

    // Record Key to <Producer Index, Rec Index within a producer>
    Map<String, Tuple2<Integer, Integer>> keyToProducerAndIndexMap = new HashMap<>();

    for (int i = 0; i < numProducers; i++) {
      List<HoodieRecord> pRecs = dataGen.generateInserts(commitTime, numRecords);
      int j = 0;
      for (HoodieRecord r : pRecs) {
        Assert.assertTrue(!keyToProducerAndIndexMap.containsKey(r.getRecordKey()));
        keyToProducerAndIndexMap.put(r.getRecordKey(), new Tuple2<>(i, j));
        j++;
      }
      recs.add(pRecs);
    }

    List<BoundedInMemoryQueueProducer<HoodieRecord>> producers = new ArrayList<>();
    for (int i = 0; i < recs.size(); i++) {
      final List<HoodieRecord> r = recs.get(i);
      // Alternate between pull and push based iterators
      if (i % 2 == 0) {
        producers.add(new IteratorBasedQueueProducer<>(r.iterator()));
      } else {
        producers.add(new FunctionBasedQueueProducer<>((buf) -> {
          Iterator<HoodieRecord> itr = r.iterator();
          while (itr.hasNext()) {
            try {
              buf.insertRecord(itr.next());
            } catch (Exception e) {
              throw new HoodieException(e);
            }
          }
          return true;
        }));
      }
    }

    final List<Future<Boolean>> futureList = producers.stream().map(producer -> {
      return executorService.submit(() -> {
        producer.produce(queue);
        return true;
      });
    }).collect(Collectors.toList());

    // Close queue
    Future<Boolean> closeFuture = executorService.submit(() -> {
      try {
        for (Future f : futureList) {
          f.get();
        }
        queue.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      return true;
    });

    // Used to ensure that consumer sees the records generated by a single producer in FIFO order
    Map<Integer, Integer> lastSeenMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> -1));
    Map<Integer, Integer> countMap =
        IntStream.range(0, numProducers).boxed().collect(Collectors.toMap(Function.identity(), x -> 0));

    // Read recs and ensure we have covered all producer recs.
    while (queue.iterator().hasNext()) {
      final HoodieInsertValueGenResult<HoodieRecord> payload = queue.iterator().next();
      final HoodieRecord rec = payload.record;
      Tuple2<Integer, Integer> producerPos = keyToProducerAndIndexMap.get(rec.getRecordKey());
      Integer lastSeenPos = lastSeenMap.get(producerPos._1());
      countMap.put(producerPos._1(), countMap.get(producerPos._1()) + 1);
      lastSeenMap.put(producerPos._1(), lastSeenPos + 1);
      // Ensure we are seeing the next record generated
      Assert.assertEquals(lastSeenPos + 1, producerPos._2().intValue());
    }

    for (int i = 0; i < numProducers; i++) {
      // Ensure we have seen all the records for each producers
      Assert.assertEquals(Integer.valueOf(numRecords), countMap.get(i));
    }

    // Ensure Close future is done
    closeFuture.get();
  }

  // Test to ensure that record queueing is throttled when we hit memory limit.
  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public void testMemoryLimitForBuffering() throws Exception {
    final int numRecords = 128;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(commitTime, numRecords);
    // maximum number of records to keep in memory.
    final int recordLimit = 5;
    final SizeEstimator<HoodieInsertValueGenResult<HoodieRecord>> sizeEstimator = new DefaultSizeEstimator<>();
    HoodieInsertValueGenResult<HoodieRecord> payload =
        getTransformFunction(HoodieTestDataGenerator.avroSchema).apply(hoodieRecords.get(0));
    final long objSize = sizeEstimator.sizeEstimate(payload);
    final long memoryLimitInBytes = recordLimit * objSize;
    final BoundedInMemoryQueue<HoodieRecord, HoodieInsertValueGenResult<HoodieRecord>> queue =
        new BoundedInMemoryQueue(memoryLimitInBytes, getTransformFunction(HoodieTestDataGenerator.avroSchema));

    // Produce
    executorService.submit(() -> {
      new IteratorBasedQueueProducer<>(hoodieRecords.iterator()).produce(queue);
      return true;
    });
    // waiting for permits to expire.
    while (!isQueueFull(queue.rateLimiter)) {
      Thread.sleep(10);
    }
    Assert.assertEquals(0, queue.rateLimiter.availablePermits());
    Assert.assertEquals(recordLimit, queue.currentRateLimit);
    Assert.assertEquals(recordLimit, queue.size());
    Assert.assertEquals(recordLimit - 1, queue.samplingRecordCounter.get());

    // try to read 2 records.
    Assert.assertEquals(hoodieRecords.get(0), queue.iterator().next().record);
    Assert.assertEquals(hoodieRecords.get(1), queue.iterator().next().record);

    // waiting for permits to expire.
    while (!isQueueFull(queue.rateLimiter)) {
      Thread.sleep(10);
    }
    // No change is expected in rate limit or number of queued records. We only expect
    // queueing thread to read
    // 2 more records into the queue.
    Assert.assertEquals(0, queue.rateLimiter.availablePermits());
    Assert.assertEquals(recordLimit, queue.currentRateLimit);
    Assert.assertEquals(recordLimit, queue.size());
    Assert.assertEquals(recordLimit - 1 + 2, queue.samplingRecordCounter.get());
  }

  // Test to ensure that exception in either queueing thread or BufferedIterator-reader thread
  // is propagated to
  // another thread.
  @SuppressWarnings("unchecked")
  @Test(timeout = 60000)
  public void testException() throws Exception {
    final int numRecords = 256;
    final List<HoodieRecord> hoodieRecords = dataGen.generateInserts(commitTime, numRecords);
    final SizeEstimator<Tuple2<HoodieRecord, Option<IndexedRecord>>> sizeEstimator = new DefaultSizeEstimator<>();
    // queue memory limit
    HoodieInsertValueGenResult<HoodieRecord> payload =
        getTransformFunction(HoodieTestDataGenerator.avroSchema).apply(hoodieRecords.get(0));
    final long objSize = sizeEstimator.sizeEstimate(new Tuple2<>(payload.record, payload.insertValue));
    final long memoryLimitInBytes = 4 * objSize;

    // first let us throw exception from queueIterator reader and test that queueing thread
    // stops and throws
    // correct exception back.
    BoundedInMemoryQueue<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>> queue1 =
        new BoundedInMemoryQueue(memoryLimitInBytes, getTransformFunction(HoodieTestDataGenerator.avroSchema));

    // Produce
    Future<Boolean> resFuture = executorService.submit(() -> {
      new IteratorBasedQueueProducer<>(hoodieRecords.iterator()).produce(queue1);
      return true;
    });

    // waiting for permits to expire.
    while (!isQueueFull(queue1.rateLimiter)) {
      Thread.sleep(10);
    }
    // notify queueing thread of an exception and ensure that it exits.
    final Exception e = new Exception("Failing it :)");
    queue1.markAsFailed(e);
    try {
      resFuture.get();
      Assert.fail("exception is expected");
    } catch (ExecutionException e1) {
      Assert.assertEquals(HoodieException.class, e1.getCause().getClass());
      Assert.assertEquals(e, e1.getCause().getCause());
    }

    // second let us raise an exception while doing record queueing. this exception should get
    // propagated to
    // queue iterator reader.
    final RuntimeException expectedException = new RuntimeException("failing record reading");
    final Iterator<HoodieRecord> mockHoodieRecordsIterator = mock(Iterator.class);
    when(mockHoodieRecordsIterator.hasNext()).thenReturn(true);
    when(mockHoodieRecordsIterator.next()).thenThrow(expectedException);
    BoundedInMemoryQueue<HoodieRecord, Tuple2<HoodieRecord, Option<IndexedRecord>>> queue2 =
        new BoundedInMemoryQueue(memoryLimitInBytes, getTransformFunction(HoodieTestDataGenerator.avroSchema));

    // Produce
    Future<Boolean> res = executorService.submit(() -> {
      try {
        new IteratorBasedQueueProducer<>(mockHoodieRecordsIterator).produce(queue2);
      } catch (Exception ex) {
        queue2.markAsFailed(ex);
        throw ex;
      }
      return true;
    });

    try {
      queue2.iterator().hasNext();
      Assert.fail("exception is expected");
    } catch (Exception e1) {
      Assert.assertEquals(expectedException, e1.getCause());
    }
    // queueing thread should also have exited. make sure that it is not running.
    try {
      res.get();
      Assert.fail("exception is expected");
    } catch (ExecutionException e2) {
      Assert.assertEquals(expectedException, e2.getCause());
    }
  }

  private boolean isQueueFull(Semaphore rateLimiter) {
    return (rateLimiter.availablePermits() == 0 && rateLimiter.hasQueuedThreads());
  }
}
