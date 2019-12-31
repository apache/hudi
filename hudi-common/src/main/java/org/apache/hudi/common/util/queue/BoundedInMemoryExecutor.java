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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through a bounded in-memory queue. This
 * class takes as input the size limit, queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through a central bounded queue
 */
public class BoundedInMemoryExecutor<I, O, E> {

  private static final Logger LOG = LoggerFactory.getLogger(BoundedInMemoryExecutor.class);

  // Executor service used for launching writer thread.
  private final ExecutorService executorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final BoundedInMemoryQueue<I, O> queue;
  // Producers
  private final List<BoundedInMemoryQueueProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, BoundedInMemoryQueueProducer<I> producer,
      Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferLimitInBytes, Arrays.asList(producer), consumer, transformFunction, new DefaultSizeEstimator<>());
  }

  public BoundedInMemoryExecutor(final long bufferLimitInBytes, List<BoundedInMemoryQueueProducer<I>> producers,
      Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
      final SizeEstimator<O> sizeEstimator) {
    this.producers = producers;
    this.consumer = consumer;
    // Ensure single thread for each producer thread and one for consumer
    this.executorService = Executors.newFixedThreadPool(producers.size() + 1);
    this.queue = new BoundedInMemoryQueue<>(bufferLimitInBytes, transformFunction, sizeEstimator);
  }

  /**
   * Callback to implement environment specific behavior before executors (producers/consumer) run.
   */
  public void preExecute() {
    // Do Nothing in general context
  }

  /**
   * Start all Producers.
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    // Latch to control when and which producer thread will close the queue
    final CountDownLatch latch = new CountDownLatch(producers.size());
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(executorService);
    producers.stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          preExecute();
          producer.produce(queue);
        } catch (Exception e) {
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
      return executorService.submit(() -> {
        LOG.info("starting consumer thread");
        preExecute();
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
      ExecutorCompletionService<Boolean> producerService = startProducers();
      Future<E> future = startConsumer();
      // Wait for consumer to be done
      return future.get();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  public boolean isRemaining() {
    return queue.iterator().hasNext();
  }

  public void shutdownNow() {
    executorService.shutdownNow();
  }

  public BoundedInMemoryQueue<I, O> getQueue() {
    return queue;
  }
}
