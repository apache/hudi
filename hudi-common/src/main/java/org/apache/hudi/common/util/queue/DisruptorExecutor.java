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

import com.lmax.disruptor.dsl.Disruptor;
import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DisruptorExecutor<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(DisruptorExecutor.class);

  // Executor service used for launching write thread.
  private final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  private final ExecutorService consumerExecutorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final DisruptorMessageQueue<I, O> queue;
  // Producers
  private final List<HoodieProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  private final Runnable preExecuteRunnable;

  public DisruptorExecutor(final int bufferSize, final Iterator<I> inputItr,
                           BoundedInMemoryQueueConsumer<O, E> consumer, Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, HoodieProducer<I> producer,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferSize, producer, consumer, transformFunction, WaitStrategyFactory.DEFAULT_STRATEGY, Functions.noop());
  }

  public DisruptorExecutor(final int bufferSize, HoodieProducer<I> producer,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(producer), consumer, transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, List<HoodieProducer<I>> producers,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                           final String waitStrategy, Runnable preExecuteRunnable) {
    this.producers = producers;
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new CustomizedThreadFactory("producer"));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("consumer"));
    this.queue = new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategy, producers.size(), preExecuteRunnable);
  }

  /**
   * Start all Producers.
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(producerExecutorService);
    producers.stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          preExecuteRunnable.run();

          DisruptorPublisher publisher = new DisruptorPublisher<>(producer, queue);
          publisher.startProduce();

        } catch (Throwable e) {
          LOG.error("error producing records", e);
          throw e;
        }
        return true;
      });
    }).collect(Collectors.toList());
    return completionService;
  }

  @Override
  public E execute() {
    try {
      assert consumer.isPresent();
      startConsumer();
      ExecutorCompletionService<Boolean> pool = startProducers();
      return finishConsuming(pool);
    } catch (InterruptedException ie) {
      shutdownNow();
      Thread.currentThread().interrupt();
      throw new HoodieException(ie);
    } catch (Exception e) {
      throw new HoodieException(e);
    }
  }

  @Override
  public E finishConsuming(Object o) throws ExecutionException, InterruptedException {
    ExecutorCompletionService<Boolean> pool = (ExecutorCompletionService) o;
    waitForProducersFinished(pool);
    queue.getInnerQueue().shutdown();
    consumer.get().finish();

    return consumer.get().getResult();
  }

  private void waitForProducersFinished(ExecutorCompletionService<Boolean> pool) throws InterruptedException, ExecutionException {
    for (int i = 0; i < producers.size(); i++) {
      pool.take().get();
    }
  }

  /**
   * Start only consumer.
   */
  @Override
  public Future<E> startConsumer() {
    DisruptorMessageHandler<O, E> handler = new DisruptorMessageHandler<>(consumer.get());

    Disruptor<HoodieDisruptorEvent<O>> innerQueue = queue.getInnerQueue();
    innerQueue.handleEventsWith(handler);
    innerQueue.start();
    return null;
  }

  @Override
  public boolean isRemaining() {
    return !queue.isEmpty();
  }

  @Override
  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
    queue.close();
  }

  @Override
  public DisruptorMessageQueue<I, O> getQueue() {
    return queue;
  }

  @Override
  public boolean awaitTermination() {
    return true;
  }
}
