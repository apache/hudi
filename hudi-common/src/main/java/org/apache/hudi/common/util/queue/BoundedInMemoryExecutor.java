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

package org.apache.hudi.common.util.queue;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through 'BoundedInMemoryQueue'. This
 * class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> extends BaseHoodieQueueBasedExecutor<I, O, E> {

  private static final Logger LOG = LoggerFactory.getLogger(BoundedInMemoryExecutor.class);

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, final Iterator<I> inputItr,
                                 HoodieConsumer<O, E> consumer, Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, Collections.singletonList(new IteratorBasedQueueProducer<>(inputItr)),
        Option.of(consumer), transformFunction, new DefaultSizeEstimator<>(), preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, int recordSamplingRate, int recordCacheLimit, final Iterator<I> inputItr,
                                 HoodieConsumer<O, E> consumer, Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, recordSamplingRate, recordCacheLimit, Collections.singletonList(new IteratorBasedQueueProducer<>(inputItr)),
        Option.of(consumer), transformFunction, new DefaultSizeEstimator<>(), preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, List<HoodieProducer<I>> producers,
                                 Option<HoodieConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                                 final SizeEstimator<O> sizeEstimator, Runnable preExecuteRunnable) {
    super(producers, consumer, new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator), preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, int recordSamplingRate, int recordCacheLimit, List<HoodieProducer<I>> producers,
                                 Option<HoodieConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                                 final SizeEstimator<O> sizeEstimator, Runnable preExecuteRunnable) {
    super(producers, consumer,
        new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator, recordSamplingRate, recordCacheLimit),
        preExecuteRunnable);
  }

  @Override
  protected void doConsume(HoodieMessageQueue<I, O> queue, HoodieConsumer<O, E> consumer) {
    LOG.info("Starting consumer, consuming records from the queue");
    try {
      Iterator<O> it = ((BoundedInMemoryQueue<I, O>) queue).iterator();
      while (it.hasNext()) {
        consumer.consume(it.next());
      }
      LOG.info("All records from the queue have been consumed");
    } catch (Exception e) {
      LOG.error("Failed consuming records", e);
      queue.markAsFailed(e);
      throw new HoodieException(e);
    }
  }

  public Iterator<O> getRecordIterator() {
    return ((BoundedInMemoryQueue<I, O>) queue).iterator();
  }
}
