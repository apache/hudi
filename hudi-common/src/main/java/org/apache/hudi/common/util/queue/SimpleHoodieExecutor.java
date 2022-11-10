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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Single Writer and Single Reader mode. Also this SimpleHoodieExecutor has no inner message queue and no inner lock.
 * Consuming and writing records from iterator directly.
 *
 * Compared with queue based Executor
 * Advantages: there is no need for additional memory and cpu resources due to lock or multithreading.
 * Disadvantages: lost some benefits such as speed limit. And maybe lower throughput.
 */
public class SimpleHoodieExecutor<I, O, E> extends HoodieExecutorBase<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(SimpleHoodieExecutor.class);
  private final HoodieMessageQueue<I, O> queue;

  public SimpleHoodieExecutor(final Iterator<I> inputItr, IteratorBasedQueueConsumer<O, E> consumer,
                              Function<I, O> transformFunction, Runnable preExecuteRunnable) {
    super(new ArrayList<>(), Option.of(consumer), preExecuteRunnable);
    this.queue = new SimpleHoodieQueueIterable<>(inputItr, transformFunction);
  }

  /**
   * No producer is needed here.
   */
  @Override
  public CompletableFuture<Void> startProducers() {
    return CompletableFuture.completedFuture(null);
  }

  /**
   * Start only consumer. And consume from SimpleHoodieQueueIterable which is a wrapper of input records iterator directly
   */
  @Override
  protected CompletableFuture<E> startConsumer() {
    return consumer.map(consumer -> {
      return CompletableFuture.supplyAsync(() -> {
        LOG.info("starting consumer thread");
        try {
          E result = consumer.consume(queue);
          LOG.info("Queue Consumption is done; notifying producer threads");
          return result;
        } catch (Exception e) {
          LOG.error("error consuming records", e);
          throw new HoodieException(e);
        }
      }, consumerExecutorService);
    }).orElse(CompletableFuture.completedFuture(null));
  }

  @Override
  public boolean isRemaining() {
    return getQueue().iterator().hasNext();
  }

  @Override
  protected void postAction() {
    super.close();
    try {
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("Catch Exception when closing queue", e);
    }
  }

  @Override
  public void shutdownNow() {
    producerExecutorService.shutdownNow();
    consumerExecutorService.shutdownNow();
    // close queue to force producer stop
    try {
      queue.close();
    } catch (IOException e) {
      throw new HoodieIOException("catch IOException while closing HoodieMessageQueue", e);
    }
  }

  @Override
  public SimpleHoodieQueueIterable<I, O> getQueue() {
    return (SimpleHoodieQueueIterable<I, O>) queue;
  }

  @Override
  protected void setup() {
    // do nothing.
  }
}
