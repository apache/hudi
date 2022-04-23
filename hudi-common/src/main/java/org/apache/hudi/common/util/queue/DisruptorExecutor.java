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
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DisruptorExecutor<I, O, E> extends HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(DisruptorExecutor.class);

  // Executor service used for launching write thread.
  private final ExecutorService producerExecutorService;
  // Executor service used for launching read thread.
  private final ExecutorService consumerExecutorService;
  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final DisruptorMessageQueue<I, O> queue;
  // Producers
  private final List<DisruptorBasedProducer<I>> producers;
  // Consumer
  private final Option<BoundedInMemoryQueueConsumer<O, E>> consumer;
  // pre-execute function to implement environment specific behavior before executors (producers/consumer) run
  private final Runnable preExecuteRunnable;

  public DisruptorExecutor(final int bufferSize, final Iterator<I> inputItr,
                           BoundedInMemoryQueueConsumer<O, E> consumer, Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, new IteratorBasedDisruptorProducer<>(inputItr), Option.of(consumer), transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, DisruptorBasedProducer<I> producer,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferSize, producer, consumer, transformFunction, WaitStrategyFactory.DEFAULT_STRATEGY, Functions.noop());
  }

  public DisruptorExecutor(final int bufferSize, DisruptorBasedProducer<I> producer,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(producer), consumer, transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, List<DisruptorBasedProducer<I>> producers,
                           Option<BoundedInMemoryQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                           final String waitStrategy, Runnable preExecuteRunnable) {
    this.producers = producers;
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
    // Ensure fixed thread for each producer thread
    this.producerExecutorService = Executors.newFixedThreadPool(producers.size(), new CustomizedThreadFactory("producer"));
    // Ensure single thread for consumer
    this.consumerExecutorService = Executors.newSingleThreadExecutor(new CustomizedThreadFactory("consumer"));
    this.queue = new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategy, producers.size());
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
      Future<E> future = startConsumer(queue.getInnerQueue());
      startProducers();
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

  /**
   * Start only consumer.
   */
  private Future<E> startConsumer(Disruptor<HoodieDisruptorEvent<O>> disruptor) {
    AtomicBoolean isRegister = new AtomicBoolean(false);
    Future<E> future = consumer.map(consumer -> {
      return consumerExecutorService.submit(() -> {
        LOG.info("starting consumer thread");
        preExecuteRunnable.run();
        try {
          DisruptorMessageHandler<O, E> handler = new DisruptorMessageHandler<>(consumer, preExecuteRunnable);
          disruptor.handleEventsWith(handler);

          // start disruptor
          queue.getInnerQueue().start();
          isRegister.set(true);
          while (!handler.isFinished()) {
            Thread.sleep(1 * 1000);
          }

          LOG.info("Queue Consumption is done; notifying producer threads");
          consumer.finish();
          return consumer.getResult();
        } catch (Exception e) {
          LOG.error("error consuming records", e);
          throw e;
        }
      });
    }).orElse(CompletableFuture.completedFuture(null));

    // waiting until consumer registered.
    while (!isRegister.get()) {
      try {
        Thread.sleep(1 * 1000);
      } catch (InterruptedException e) {
        // ignore here
      }
    }

    return future;
  }

  @Override
  public boolean isRemaining() {
    return false;
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
}
