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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * HoodieExecutorBase holds common elements producerExecutorService, consumerExecutorService, producers and a single consumer.
 * Also HoodieExecutorBase control the lifecycle of producerExecutorService and consumerExecutorService.
 */
public abstract class HoodieExecutorBase<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(HoodieExecutorBase.class);

  private static final long TERMINATE_WAITING_TIME_SECS = 60L;
  // Executor service used for launching write thread.
  public final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  public final ExecutorService consumerExecutorService;
  // Producers
  public final List<HoodieProducer<I>> producers;
  // Consumer
  public final Option<IteratorBasedQueueConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  public final Runnable preExecuteRunnable;

  public List<Future<Boolean>> producerTasks;

  public HoodieExecutorBase(List<HoodieProducer<I>> producers, Option<IteratorBasedQueueConsumer<O, E>> consumer,
                            Runnable preExecuteRunnable) {
    this.producers = producers;
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new HoodieDaemonThreadFactory("producer", preExecuteRunnable));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("consumer"));
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

  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
  }

  public void close() {
    producerExecutorService.shutdown();
    consumerExecutorService.shutdown();
  }

  /**
   * Main API to run both production and consumption.
   */
  @Override
  public E execute() {
    try {
      ValidationUtils.checkState(this.consumer.isPresent());
      setup();
      producerTasks = startProducers();
      Future<E> future = startConsumer();
      return future.get();
    } catch (InterruptedException ie) {
      shutdownNow();
      Thread.currentThread().interrupt();
      throw new HoodieException(ie);
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      postAction();
    }
  }

  public void setup(){}
}
