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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

/**
 * Executor which orchestrates concurrent producers and consumers communicating through 'DisruptorMessageQueue'. This
 * class takes as queue producer(s), consumer and transformer and exposes API to orchestrate
 * concurrent execution of these actors communicating through disruptor
 */
public class DisruptorExecutor<I, O, E> extends BaseHoodieQueueBasedExecutor<I, O, E> {

  public DisruptorExecutor(Integer bufferSize,
                           Iterator<I> inputItr,
                           HoodieConsumer<O, E> consumer,
                           Function<I, O> transformFunction,
                           String waitStrategy,
                           Runnable preExecuteRunnable) {
    this(bufferSize, Collections.singletonList(new IteratorBasedQueueProducer<>(inputItr)), consumer,
        transformFunction, waitStrategy, preExecuteRunnable);
  }

  public DisruptorExecutor(int bufferSize,
                           List<HoodieProducer<I>> producers,
                           HoodieConsumer<O, E> consumer,
                           Function<I, O> transformFunction,
                           String waitStrategyId,
                           Runnable preExecuteRunnable) {
    super(producers, Option.of(consumer), new DisruptorMessageQueue<>(bufferSize, transformFunction, waitStrategyId, producers.size(), preExecuteRunnable), preExecuteRunnable);
  }

  @Override
  protected void setUp() {
    DisruptorMessageQueue<I, O> disruptorQueue = (DisruptorMessageQueue<I, O>) queue;
    // Before we start producing, we need to set up Disruptor's queue
    disruptorQueue.setHandlers(consumer.get());
    disruptorQueue.start();
  }

  @Override
  protected void doConsume(HoodieMessageQueue<I, O> queue, HoodieConsumer<O, E> consumer) {
  }
}
