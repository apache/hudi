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

import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.SizeEstimator;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through a bounded in-memory queue. This
 * class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> extends HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(BoundedInMemoryExecutor.class);
  private static final long TERMINATE_WAITING_TIME_SECS = 60L;
  // Executor service used for launching write thread.
  private final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  private final ExecutorService consumerExecutorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final BoundedInMemoryQueue<I, O> queue;
  // Producers
  private final List<BoundedInMemoryQueueProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  private final Runnable preExecuteRunnable;

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, final Iterator<I> inputItr,
                                 BoundedInMemoryQueueConsumer<O, E> consumer, Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, BoundedInMemoryQueueProducer<I> producer,
                                 Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferLimitInBytes, producer, consumer, transformFunction, Functions.noop());
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, BoundedInMemoryQueueProducer<I> producer,
                                 Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    this(bufferLimitInBytes, Collections.singletonList(producer), consumer, transformFunction, new DefaultSizeEstimator<>(), preExecuteRunnable);
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, List<BoundedInMemoryQueueProducer<I>> producers,
                                 Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                                 final SizeEstimator<O> sizeEstimator, Runnable preExecuteRunnable) {
    this.producers = producers;
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new CustomizedThreadFactory("producer"));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("consumer"));
    this.queue = new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator);
  }

  /**
   * Start all Producers.
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    // Latch to control when and which producer thread will close the queue
    final CountDownLatch latch = new CountDownLatch(producers.size());
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(producerExecutorService);
    producers.stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          preExecuteRunnable.run();
          producer.produce(queue);
        } catch (Throwable e) {
          LOG.error("error producing records", e);
          queue.markAsFailed(e);
          throw e;
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
    return completionService;
  }

  /**
   * Start only consumer.
   */
  private Future<E> startConsumer() {
    return consumer.map(consumer -> {
      return consumerExecutorService.submit(() -> {
        LOG.info("starting consumer thread");
        preExecuteRunnable.run();
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

  /**
   * Main API to run both production and consumption.
   */
  public E execute() {
    try {
      startProducers();
      Future<E> future = startConsumer();
      // Wait for consumer to be done
      return future.get();
    } catch (InterruptedException ie) {
      shutdownNow();
      Thread.currentThread().interrupt();
      throw new HoodieException(ie);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return queue.iterator().hasNext();
  }

  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
    // close queue to force producer stop
    queue.close();
  }

  public boolean awaitTermination() {
    // if current thread has been interrupted before awaitTermination was called, we still give
    // executor a chance to proceeding. So clear the interrupt flag and reset it if needed before return.
    boolean interruptedBefore = Thread.interrupted();
    boolean producerTerminated = false;
    boolean consumerTerminated = false;
    try {
      producerTerminated = producerExecutorService.awaitTermination(TERMINATE_WAITING_TIME_SECS, TimeUnit.SECONDS);
      consumerTerminated = consumerExecutorService.awaitTermination(TERMINATE_WAITING_TIME_SECS, TimeUnit.SECONDS);
    } catch (InterruptedException ie) {
      // fail silently for any other interruption
    }
    // reset interrupt flag if needed
    if (interruptedBefore) {
      Thread.currentThread().interrupt();
    }
    return producerTerminated && consumerTerminated;
  }

  public BoundedInMemoryQueue<I, O> getQueue() {
    return queue;
  }
}
