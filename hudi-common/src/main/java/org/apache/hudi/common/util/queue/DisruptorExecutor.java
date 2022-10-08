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
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DisruptorExecutor<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(DisruptorExecutor.class);

  // Used for buffering records which is controlled by HoodieWriteConfig#WRITE_BUFFER_LIMIT_BYTES.
  private final DisruptorMessageQueue<I, O> queue;
  private final HoodieExecutorBase<I, O, E> hoodieExecutorBase;

  public DisruptorExecutor(final int bufferSize, final Iterator<I> inputItr,
                           IteratorBasedQueueConsumer<O, E> consumer, Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, new IteratorBasedQueueProducer<>(inputItr), Option.of(consumer), transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, HoodieProducer<I> producer,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction) {
    this(bufferSize, producer, consumer, transformFunction, WaitStrategyFactory.DEFAULT_STRATEGY, Functions.noop());
  }

  public DisruptorExecutor(final int bufferSize, HoodieProducer<I> producer,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction, String waitStrategy, Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(producer), consumer, transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(final int bufferSize, List<HoodieProducer<I>> producers,
                           Option<IteratorBasedQueueConsumer<O, E>> consumer, final Function<I, O> transformFunction,
                           final String waitStrategy, Runnable preExecuteRunnable) {
    this.queue = new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategy, producers.size(), preExecuteRunnable);
    this.hoodieExecutorBase = new HoodieExecutorBase<>(producers, consumer, preExecuteRunnable);
  }

  /**
   * Start all Producers.
   */
  public ExecutorCompletionService<Boolean> startProducers() {
    final ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<Boolean>(hoodieExecutorBase.getProducerExecutorService());
    hoodieExecutorBase.getProducers().stream().map(producer -> {
      return completionService.submit(() -> {
        try {
          hoodieExecutorBase.getPreExecuteRunnable().run();

          DisruptorPublisher publisher = new DisruptorPublisher<>(producer, queue);
          publisher.startProduce();

        } catch (Throwable e) {
          LOG.error("error producing records", e);
          throw new HoodieException("Error producing records in disruptor executor", e);
        }
        return true;
      });
    }).collect(Collectors.toList());
    return completionService;
  }

  @Override
  public E execute() {
    try {
      ValidationUtils.checkState(hoodieExecutorBase.getConsumer().isPresent());
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
    queue.close();
    queue.waitForConsumingFinished();
    hoodieExecutorBase.getConsumer().get().finish();

    return hoodieExecutorBase.getConsumer().get().getResult();
  }

  private void waitForProducersFinished(ExecutorCompletionService<Boolean> pool) throws InterruptedException, ExecutionException {
    for (int i = 0; i < hoodieExecutorBase.getProducers().size(); i++) {
      pool.take().get();
    }
  }

  /**
   * Start only consumer.
   */
  @Override
  public Future<E> startConsumer() {
    DisruptorMessageHandler<O, E> handler = new DisruptorMessageHandler<>(hoodieExecutorBase.getConsumer().get());
    queue.setHandlers(handler);
    queue.start();
    return null;
  }

  @Override
  public boolean isRemaining() {
    return !queue.isEmpty();
  }

  @Override
  public void shutdownNow() {
    hoodieExecutorBase.shutdownNow();
    queue.close();
  }

  @Override
  public DisruptorMessageQueue<I, O> getQueue() {
    return queue;
  }

  @Override
  public boolean awaitTermination() {
    return hoodieExecutorBase.awaitTermination();
  }
}
