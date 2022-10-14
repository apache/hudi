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
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through 'BoundedInMemoryQueue'. This
 * class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> extends MessageQueueBasedHoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(BoundedInMemoryExecutor.class);

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, final Iterator<I> inputItr,
                                 IteratorBasedQueueConsumer<O, E> consumer, Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, HoodieProducer<I> producer,
                                 Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferLimitInBytes, producer, consumer, transformFunction, Functions.noop());
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, HoodieProducer<I> producer,
                                 Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, Collections.singletonList(producer), consumer, transformFunction, new DefaultSizeEstimator<>(), preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, List<HoodieProducer<I>> producers,
                                 Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                                 final SizeEstimator<O> sizeEstimator, Runnable preExecuteRunnable) {
    super(new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator),
        new HoodieExecutorBase<>(producers, consumer, preExecuteRunnable));
  }

  /**
   * Start all Producers.
   */
  public List<Future<Boolean>> startProducers() {
    // Latch to control when and which producer thread will close the queue
    final CountDownLatch latch = new CountDownLatch(hoodieExecutorBase.getProducers().size());
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(hoodieExecutorBase.getProducerExecutorService());
    List<Future<Boolean>> producerTasks = hoodieExecutorBase.getProducers().stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          hoodieExecutorBase.getPreExecuteRunnable().run();
          producer.produce(queue);
        } catch (Throwable e) {
          LOG.error("error producing records", e);
          queue.markAsFailed(e);
          throw new HoodieException("Error producing records in bounded in memory executor", e);
        } finally {
          synchronized (latch) {
            latch.countDown();
            if (latch.getCount() == 0) {
              // Mark production as done so that consumer will be able to exit
              queue.close();
            }
          }
        }
        return true;
      });
    }).collect(Collectors.toList());
    return producerTasks;
  }

  /**
   * Start only consumer.
   */
  @Override
  public Future<E> startConsumer() {
    return hoodieExecutorBase.getConsumer().map(consumer -> {
      return hoodieExecutorBase.getConsumerExecutorService().submit(() -> {
        LOG.info("starting consumer thread");
        hoodieExecutorBase.getPreExecuteRunnable().run();
        try {
          E result = consumer.consume(queue);
          LOG.info("Queue Consumption is done; notifying producer threads");
          return result;
        } catch (Exception e) {
          LOG.error("error consuming records", e);
          queue.markAsFailed(e);
          throw e;
        }
      });
    }).orElse(CompletableFuture.completedFuture(null));
  }

  public boolean isRemaining() {
    return ((BoundedInMemoryQueue)queue).iterator().hasNext();
  }

  @Override
  public void postAction() {
    hoodieExecutorBase.close();
  }

  public void shutdownNow() {
    hoodieExecutorBase.shutdownNow();
    // close queue to force producer stop
    try {
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("catch IOException while closing HoodieMessageQueue", e);
    }
  }

  public boolean awaitTermination() {
    return hoodieExecutorBase.awaitTermination();
  }

  public HoodieMessageQueue<I, O> getQueue() {
    return queue;
  }
}
