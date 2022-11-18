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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FutureUtils.allOf;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieExecutorBase holds common elements producerExecutorService, consumerExecutorService, producers and a single consumer.
 * Also HoodieExecutorBase control the lifecycle of producerExecutorService and consumerExecutorService.
 */
public abstract class BaseHoodieQueueBasedExecutor<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final long TERMINATE_WAITING_TIME_SECS = 60L;

  private final Logger logger = LogManager.getLogger(getClass());

  // Executor service used for launching write thread.
  private final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  private final ExecutorService consumerExecutorService;
  // Queue
  protected final HoodieMessageQueue<I, O> queue;
  // Producers
  private final List<HoodieProducer<I>> producers;
  // Consumer
  protected final Option<HoodieConsumer<O, E>> consumer;

  public BaseHoodieQueueBasedExecutor(List<HoodieProducer<I>> producers,
                                      Option<HoodieConsumer<O, E>> consumer,
                                      HoodieMessageQueue<I, O> queue,
                                      Runnable preExecuteRunnable) {
    this.queue = queue;
    this.producers = producers;
    this.consumer = consumer;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new CustomizedThreadFactory("executor-queue-producer", preExecuteRunnable));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("executor-queue-consumer", preExecuteRunnable));
  }

  protected void doProduce(HoodieMessageQueue<I, O> queue, HoodieProducer<I> producer) {
    logger.info("Starting producer, populating records into the queue");
    try {
      producer.produce(queue);
      logger.info("Finished producing records into the queue");
    } catch (Exception e) {
      logger.error("Failed to produce records", e);
      queue.markAsFailed(e);
      throw new HoodieException("Failed to produce records", e);
    }
  }

  protected abstract E doConsume(HoodieMessageQueue<I, O> queue, HoodieConsumer<O, E> consumer);

  /**
   * Start producing
   */
  public final CompletableFuture<Void> startProducingAsync() {
    return allOf(producers.stream()
        .map(producer -> CompletableFuture.supplyAsync(() -> {
          doProduce(queue, producer);
          return (Void) null;
        }, producerExecutorService))
        .collect(Collectors.toList())
    )
        .thenApply(ignored -> (Void) null)
        .whenComplete((result, throwable) -> {
          // Regardless of how producing has completed, we have to close producers
          // to make sure resources are properly cleaned up
          producers.forEach(HoodieProducer::close);
          // Mark production as done so that consumer will be able to exit
          queue.seal();
        });
  }

  /**
   * Start consumer
   *
   * NOTE: This is a sync operation that _will_ block, until all records
   *       are fully consumed
   */
  private CompletableFuture<E> startConsumingAsync(CompletableFuture<Void> producingFuture) {
    return consumer.map(consumer ->
            CompletableFuture.supplyAsync(() -> {
              return doConsume(queue, consumer);
            }, consumerExecutorService)
        )
        // NOTE: To properly support mode when there's no consumer, we have to fall back
        //       to producing future as the trigger for us to clean up the queue
        .orElse(producingFuture.thenApply(ignored -> null))
        .whenComplete((result, throwable) -> {
          // Close the queue, after finishing the consuming
          queue.close();
        });
  }

  @Override
  public final boolean awaitTermination() {
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

  @Override
  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
  }

  public boolean isRunning() {
    return !queue.isEmpty();
  }

  /**
   * Main API to run both production and consumption.
   */
  @Override
  public E execute() {
    try {
      checkState(this.consumer.isPresent());
      // Start producing/consuming asynchronously
      CompletableFuture<Void> producing = startProducingAsync();
      CompletableFuture<E> consuming = startConsumingAsync(producing);
      // Block until producing and consuming both finish
      producing.join();
      return consuming.join();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }
}
