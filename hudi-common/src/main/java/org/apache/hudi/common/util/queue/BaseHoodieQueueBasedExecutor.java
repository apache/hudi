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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hudi.common.util.FutureUtils.allOf;
import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * Base class for multi-threaded queue-based executor which
 *
 * <ul>
 *   <li>Can be ingesting instances of type {@link I} from multiple {@link HoodieProducer}s
 *   into the {@link HoodieMessageQueue}</li>
 *   <li>Can be ingesting instances of type {@link I} into an (optional) {@link HoodieConsumer}
 *   from the internal {@link HoodieMessageQueue} (when no consumer is provided records are
 *   simply accumulated into internal queue)</li>
 * </ul>
 *
 * Such executors are allowing to setup an ingestion pipeline w/ N:1 configuration, where data
 * is ingested from multiple sources (ie producers) into a singular sink (ie consumer), using
 * an internal queue to stage the records ingested from producers before these are consumed
 */
public abstract class BaseHoodieQueueBasedExecutor<I, O, E> implements HoodieExecutor<E> {

  private static final long TERMINATE_WAITING_TIME_SECS = 60L;

  private final Logger logger = LoggerFactory.getLogger(getClass());

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
  // Futures corresponding to producing/consuming processes
  private CompletableFuture<Void> consumingFuture;
  private CompletableFuture<Void> producingFuture;

  public BaseHoodieQueueBasedExecutor(List<HoodieProducer<I>> producers,
                                      Option<HoodieConsumer<O, E>> consumer,
                                      HoodieMessageQueue<I, O> queue,
                                      Runnable preExecuteRunnable) {
    this.queue = queue;
    this.producers = producers;
    this.consumer = consumer;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(Math.max(1, producers.size()), new CustomizedThreadFactory("executor-queue-producer", preExecuteRunnable));
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

  protected abstract void doConsume(HoodieMessageQueue<I, O> queue, HoodieConsumer<O, E> consumer);

  protected void setUp() {}

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
   */
  private CompletableFuture<Void> startConsumingAsync() {
    return consumer.map(consumer ->
            CompletableFuture.supplyAsync(() -> {
              doConsume(queue, consumer);
              return (Void) null;
            }, consumerExecutorService)
        )
        .orElseGet(() -> CompletableFuture.completedFuture(null));
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
  public final void shutdownNow() {
    // NOTE: PLEASE READ CAREFULLY
    //       Graceful shutdown sequence have been a source of multiple issues in the
    //       past (for ex HUDI-2875, HUDI-5238). To handle it appropriately in a graceful
    //       fashion we're consolidating shutdown sequence w/in the Executor itself (in
    //       this method) shutting down in following order
    //
    //          1. We shut down producing/consuming pipeline (by interrupting
    //             corresponding futures), then
    //          2. We shut down producer and consumer (if present), and after that
    //          3. We shut down the executors
    //
    if (producingFuture != null) {
      producingFuture.cancel(true);
    }
    if (consumingFuture != null) {
      consumingFuture.cancel(true);
    }
    // Clean up resources associated w/ producers/consumers
    producers.forEach(HoodieProducer::close);
    consumer.ifPresent(HoodieConsumer::finish);
    // Shutdown executor-services
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
      setUp();
      // Start consuming/producing asynchronously
      this.consumingFuture = startConsumingAsync();
      this.producingFuture = startProducingAsync();

      // NOTE: To properly support mode when there's no consumer, we have to fall back
      //       to producing future as the trigger for us to shut down the queue
      return allOf(Arrays.asList(producingFuture, consumingFuture))
          .whenComplete((ignored, throwable) -> {
            // Close the queue to release the resources
            queue.close();
          })
          .thenApply(ignored -> consumer.get().finish())
          // Block until producing and consuming both finish
          .get();
    } catch (Exception e) {
      if (e instanceof InterruptedException) {
        // In case {@code InterruptedException} was thrown, resetting the interrupted flag
        // of the thread, we reset it (to true) again to permit subsequent handlers
        // to be interrupted as well
        Thread.currentThread().interrupt();
      }
      // throw if we have any other exception seen already. There is a chance that cancellation/closing of producers with CompletableFuture wins before the actual exception
      // is thrown.
      if (this.queue.getThrowable() != null) {
        throw new HoodieException(queue.getThrowable());
      }

      throw new HoodieException(e);
    }
  }
}
