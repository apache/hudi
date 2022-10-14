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

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through 'DisruptorMessageQueue'. This
 * class takes as queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through disruptor
 */
public class DisruptorExecutor<I, O, E> extends MessageQueueBasedHoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(DisruptorExecutor.class);

  public DisruptorExecutor(final Option<Integer> bufferSize, final Iterator<I> inputItr,
                           IteratorBasedQueueConsumer<O, E> consumer, Function<I, O> transformFunction, Option<String> waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final Option<Integer> bufferSize, HoodieProducer<I> producer,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferSize, producer, consumer, transformFunction, Option.of(WaitStrategyFactory.DEFAULT_STRATEGY), Functions.noop());
  }

  public DisruptorExecutor(final Option<Integer> bufferSize, HoodieProducer<I> producer,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, Option<String> waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(producer), consumer, transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final Option<Integer> bufferSize, List<HoodieProducer<I>> producers,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                           final Option<String> waitStrategy, Runnable preExecuteRunnable) {
    super(new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategy, producers.size(), preExecuteRunnable),
        new HoodieExecutorBase<>(producers, consumer, preExecuteRunnable));
  }

  /**
   * Start all Producers.
   */
  public List<Future<Boolean>> startProducers() {
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(hoodieExecutorBase.getProducerExecutorService());
    return hoodieExecutorBase.getProducers().stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          producer.produce(queue);
        } catch (Throwable e) {
          LOG.error("error producing records", e);
          throw new HoodieException("Error producing records in disruptor executor", e);
        }
        return true;
      });
    }).collect(Collectors.toList());
  }

  @Override
  public void setup() {
    DisruptorMessageHandler<O, E> handler = new DisruptorMessageHandler<>(hoodieExecutorBase.getConsumer().get());
    ((DisruptorMessageQueue)queue).setHandlers(handler);
    ((DisruptorMessageQueue)queue).start();
  }

  @Override
  public void postAction() {
    try {
      queue.close();
      hoodieExecutorBase.close();
    } catch (IOException e) {
      throw new HoodieIOException("Catch IOException while closing DisruptorMessageQueue", e);
    }
  }

  @Override
  public Future<E> startConsumer() {
    // consumer is already started. Here just wait for all record consumed and return the result.
    return hoodieExecutorBase.getConsumerExecutorService().submit(() -> {

      // Waits for all producers finished.
      for (Future f : producerTasks) {
        f.get();
      }

      // Call disruptor queue close function which will wait until all events currently in the disruptor have been processed by all event processors
      queue.close();
      hoodieExecutorBase.getConsumer().get().finish();
      return hoodieExecutorBase.getConsumer().get().getResult();
    });
  }

  @Override
  public boolean isRemaining() {
    return !queue.isEmpty();
  }

  @Override
  public void shutdownNow() {
    hoodieExecutorBase.shutdownNow();
    try {
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("Catch IOException while closing DisruptorMessageQueue");
    }
  }

  @Override
  public DisruptorMessageQueue<I, O> getQueue() {
    return (DisruptorMessageQueue)queue;
  }

  @Override
  public boolean awaitTermination() {
    return hoodieExecutorBase.awaitTermination();
  }
}
