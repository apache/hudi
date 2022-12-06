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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through 'DisruptorMessageQueue'. This
 * class takes as queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through disruptor
 */
public class DisruptorExecutor<I, O, E> extends HoodieExecutorBase<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(DisruptorExecutor.class);
  private final HoodieMessageQueue<I, O> queue;

  public DisruptorExecutor(final Option<Integer> bufferSize, final Iterator<I> inputItr,
                           IteratorBasedQueueConsumer<O, E> consumer, Function<I, O> transformFunction, Option<String> waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final Option<Integer> bufferSize, HoodieProducer<I> producer,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, Option<String> waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(producer), consumer, transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final Option<Integer> bufferSize, List<HoodieProducer<I>> producers,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                           final Option<String> waitStrategy, Runnable preExecuteRunnable) {
    super(producers, consumer, preExecuteRunnable);
    this.queue = new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategy, producers.size(), preExecuteRunnable);
  }

  /**
   * Start all Producers.
   */
  @Override
  public CompletableFuture<Void> startProducers() {
    return CompletableFuture.allOf(producers.stream().map(producer -> {
      return CompletableFuture.supplyAsync(() -> {
        try {
          producer.produce(queue);
        } catch (Throwable e) {
          LOG.error("error producing records", e);
          throw new HoodieException("Error producing records in disruptor executor", e);
        }
        return true;
      }, producerExecutorService);
    }).toArray(CompletableFuture[]::new));
  }

  @Override
  protected void setup() {
    ((DisruptorMessageQueue)queue).setHandlers(consumer.get());
    ((DisruptorMessageQueue)queue).start();
  }

  @Override
  protected void postAction() {
    try {
      super.close();
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("Catch IOException while closing DisruptorMessageQueue", e);
    }
  }

  @Override
  protected CompletableFuture<E> startConsumer() {
    return producerFuture.thenApplyAsync(res -> {
      try {
        queue.close();
        consumer.get().finish();
        return consumer.get().getResult();
      } catch (IOException e) {
        throw new HoodieIOException("Catch Exception when closing", e);
      }
    }, consumerExecutorService);
  }

  @Override
  public boolean isRemaining() {
    return !queue.isEmpty();
  }

  @Override
  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
    try {
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("Catch IOException while closing DisruptorMessageQueue");
    }
  }

  @Override
  public DisruptorMessageQueue<I, O> getQueue() {
    return (DisruptorMessageQueue<I, O>)queue;
  }
}
