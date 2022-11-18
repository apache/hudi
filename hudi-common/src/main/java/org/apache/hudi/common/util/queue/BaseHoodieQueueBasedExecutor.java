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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieExecutorBase holds common elements producerExecutorService, consumerExecutorService, producers and a single consumer.
 * Also HoodieExecutorBase control the lifecycle of producerExecutorService and consumerExecutorService.
 */
public abstract class BaseHoodieQueueBasedExecutor<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final long TERMINATE_WAITING_TIME_SECS = 60L;

  // Executor service used for launching write thread.
  protected final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  protected final ExecutorService consumerExecutorService;
  // Queue
  protected final HoodieMessageQueue<I, O> queue;
  // Producers
  protected final List<HoodieProducer<I>> producers;
  // Consumer
  protected final Option<HoodieConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  protected final Runnable preExecuteRunnable;

  public BaseHoodieQueueBasedExecutor(List<HoodieProducer<I>> producers,
                                      Option<HoodieConsumer<O, E>> consumer,
                                      HoodieMessageQueue<I, O> queue,
                                      Runnable preExecuteRunnable) {
    this.queue = queue;
    this.producers = producers;
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new CustomizedThreadFactory("executor-queue-producer", preExecuteRunnable));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("executor-queue-consumer", preExecuteRunnable));
  }

  protected abstract CompletableFuture<Void> doStartProducing();

  protected abstract CompletableFuture<E> doStartConsuming();

  /**
   * Start producers
   */
  public void startProducing() {
    doStartProducing().whenComplete((result, throwable) -> {
      producers.forEach(HoodieProducer::close);
      // Mark production as done so that consumer will be able to exit
      queue.close();
    });
  }

  /**
   * Start consumer
   *
   * NOTE: This is a sync operation that _will_ block, until all records
   *       are fully consumed
   */
  private E startConsuming() {
    //
    return doStartConsuming().join();
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

      // Start producing/consuming
      startProducing();
      E result = startConsuming();

      // Shut down the queue
      queue.close();

      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }
}
