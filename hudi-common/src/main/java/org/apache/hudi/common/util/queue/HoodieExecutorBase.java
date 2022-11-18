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

import static org.apache.hudi.common.util.ValidationUtils.checkState;

/**
 * HoodieExecutorBase holds common elements producerExecutorService, consumerExecutorService, producers and a single consumer.
 * Also HoodieExecutorBase control the lifecycle of producerExecutorService and consumerExecutorService.
 */
public abstract class HoodieExecutorBase<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(HoodieExecutorBase.class);

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
  protected final Option<IteratorBasedQueueConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  protected final Runnable preExecuteRunnable;

  public HoodieExecutorBase(List<HoodieProducer<I>> producers,
                            Option<IteratorBasedQueueConsumer<O, E>> consumer,
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

  /**
   * Start all Producers.
   */
  public abstract void startProducing();

  /**
   * Start consumer.
   */
  protected abstract CompletableFuture<E> startConsuming();

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
    queue.close();
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
  }

  @Override
  public void close() {
    queue.close();
    producerExecutorService.shutdown();
    consumerExecutorService.shutdown();
  }

  /**
   * Main API to run both production and consumption.
   */
  @Override
  public E execute() {
    try {
      checkState(this.consumer.isPresent());
      startProducing();
      CompletableFuture<E> future = startConsuming();
      return future.join();
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }
}
