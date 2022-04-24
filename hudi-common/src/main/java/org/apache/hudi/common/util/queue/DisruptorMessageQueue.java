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

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.function.Function;

public class DisruptorMessageQueue<I, O> extends HoodieMessageQueue<I, O> {

  private final Disruptor<HoodieDisruptorEvent<O>> queue;
  private final Function<I, O> transformFunction;
  private RingBuffer<HoodieDisruptorEvent<O>> ringBuffer;

  public DisruptorMessageQueue(int bufferSize, Function<I, O> transformFunction, String waitStrategyName, int producerNumber, Runnable preExecuteRunnable) {
    WaitStrategy waitStrategy = WaitStrategyFactory.build(waitStrategyName);
    HoodieDaemonThreadFactory threadFactory = new HoodieDaemonThreadFactory(preExecuteRunnable);

    if (producerNumber > 1) {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, threadFactory, ProducerType.MULTI, waitStrategy);
    } else {
      this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, threadFactory, ProducerType.SINGLE, waitStrategy);
    }

    this.ringBuffer = queue.getRingBuffer();
    this.transformFunction = transformFunction;
  }

  @Override
  public long size() {
    return queue.getBufferSize();
  }

  @Override
  public void insertRecord(I value) throws Exception {
    O applied = transformFunction.apply(value);

    EventTranslator<HoodieDisruptorEvent<O>> translator = new EventTranslator<HoodieDisruptorEvent<O>>() {
      @Override
      public void translateTo(HoodieDisruptorEvent<O> event, long sequence) {
        event.set(applied);
      }
    };

    queue.getRingBuffer().publishEvent(translator);
  }

  @Override
  public void close() {
    queue.shutdown();
  }

  public Disruptor<HoodieDisruptorEvent<O>> getInnerQueue() {
    return this.queue;
  }

  public boolean isEmpty() {
    return ringBuffer.getBufferSize() == ringBuffer.remainingCapacity();
  }
}
