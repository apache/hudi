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

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.function.Function;

public class DisruptorMessageQueue<I, O> implements HoodieMessageQueue<I, O> {

  private static final Logger LOG = LogManager.getLogger(DisruptorMessageQueue.class);

  private final Disruptor<HoodieDisruptorEvent> queue;
  private final Function<I, O> transformFunction;
  private final RingBuffer<HoodieDisruptorEvent> ringBuffer;

  public DisruptorMessageQueue(Option<Integer> bufferSize, Function<I, O> transformFunction, Option<String> waitStrategyName, int totalProducers, Runnable preExecuteRunnable) {
    WaitStrategy waitStrategy = WaitStrategyFactory.build(waitStrategyName);
    HoodieDaemonThreadFactory threadFactory = new HoodieDaemonThreadFactory(preExecuteRunnable);

    this.queue = new Disruptor<>(new HoodieDisruptorEventFactory(), bufferSize.get(), threadFactory, totalProducers > 1 ? ProducerType.MULTI : ProducerType.SINGLE, waitStrategy);
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
    EventTranslator<HoodieDisruptorEvent> translator = (event, sequence) -> event.set(applied);
    queue.getRingBuffer().publishEvent(translator);
  }

  @Override
  public Option<O> readNextRecord() {
    throw new UnsupportedOperationException("Should not call readNextRecord here. And let DisruptorMessageHandler to handle consuming logic");
  }

  @Override
  public void markAsFailed(Throwable e) {
    // do nothing.
  }

  @Override
  public void close() {
    // Waits until all events currently in the disruptor have been processed by all event processors
    queue.shutdown();
  }

  public boolean isEmpty() {
    return ringBuffer.getBufferSize() == ringBuffer.remainingCapacity();
  }

  public void setHandlers(IteratorBasedQueueConsumer consumer) {
    queue.handleEventsWith(new EventHandler<HoodieDisruptorEvent>() {

      @Override
      public void onEvent(HoodieDisruptorEvent event, long sequence, boolean endOfBatch) throws Exception {
        consumer.consumeOneRecord(event.get());
      }
    });
  }

  public void start() {
    queue.start();
  }

  /**
   * HoodieDisruptorEventFactory is used to create/preallocate HoodieDisruptorEvent.
   *
   */
  class HoodieDisruptorEventFactory implements EventFactory<HoodieDisruptorEvent> {

    @Override
    public HoodieDisruptorEvent newInstance() {
      return new HoodieDisruptorEvent();
    }
  }

  /**
   * The unit of data passed from producer to consumer in disruptor world.
   */
  class HoodieDisruptorEvent {

    private O value;

    public void set(O value) {
      this.value = value;
    }

    public O get() {
      return this.value;
    }
  }
}

