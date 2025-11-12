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
import org.apache.hudi.exception.HoodieException;

import com.lmax.disruptor.EventTranslator;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Implementation of {@link HoodieMessageQueue} based on Disruptor Queue.
 *
 * @param <I> Input type.
 * @param <O> Transformed output type.
 */
public class DisruptorMessageQueue<I, O> implements HoodieMessageQueue<I, O> {

  private static final Logger LOG = LoggerFactory.getLogger(DisruptorMessageQueue.class);

  private final Disruptor<HoodieDisruptorEvent> queue;
  private final Function<I, O> transformFunction;
  private final RingBuffer<HoodieDisruptorEvent> ringBuffer;
  private final AtomicReference<Throwable> throwable = new AtomicReference<>(null);

  private boolean isShutdown = false;
  private boolean isStarted = false;

  private static final long TIMEOUT_WAITING_SECS = 10L;

  public DisruptorMessageQueue(int bufferSize, Function<I, O> transformFunction, String waitStrategyId, int totalProducers, Runnable preExecuteRunnable) {
    WaitStrategy waitStrategy = WaitStrategyFactory.build(waitStrategyId);
    CustomizedThreadFactory threadFactory = new CustomizedThreadFactory("disruptor", true, preExecuteRunnable);

    this.queue = new Disruptor<>(HoodieDisruptorEvent::new, bufferSize, threadFactory, totalProducers > 1 ? ProducerType.MULTI : ProducerType.SINGLE, waitStrategy);
    this.ringBuffer = queue.getRingBuffer();
    this.transformFunction = transformFunction;
  }

  @Override
  public long size() {
    return ringBuffer.getBufferSize() - ringBuffer.remainingCapacity();
  }

  @Override
  public void insertRecord(I value) throws Exception {
    if (!isStarted) {
      throw new HoodieException("Can't insert into the queue since the queue is not started yet");
    }

    if (isShutdown) {
      throw new HoodieException("Can't insert into the queue after it had already been closed");
    }

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
    this.throwable.compareAndSet(null, e);
    // no-op
  }

  @Override
  public Throwable getThrowable() {
    return this.throwable.get();
  }

  @Override
  public boolean isEmpty() {
    return ringBuffer.getBufferSize() == ringBuffer.remainingCapacity();
  }

  @Override
  public void seal() {
  }

  @Override
  public void close() {
    synchronized (this) {
      if (!isShutdown) {
        isShutdown = true;
        isStarted = false;
        if (Thread.currentThread().isInterrupted()) {
          // if current thread has been interrupted, we still give executor a chance to proceeding.
          LOG.error("Disruptor Queue has been interrupted! Shutdown now.");
          try {
            queue.shutdown(TIMEOUT_WAITING_SECS, TimeUnit.SECONDS);
          } catch (TimeoutException e) {
            LOG.error("Disruptor queue shutdown timeout: " + e);
            throw new HoodieException(e);
          }
          throw new HoodieException("Disruptor Queue has been interrupted! Shutdown now.");
        } else {
          queue.shutdown();
        }
      }
    }
  }

  protected void setHandlers(HoodieConsumer<O, ?> consumer) {
    queue.handleEventsWith((event, sequence, endOfBatch) -> {
      try {
        consumer.consume(event.get());
      } catch (Exception e) {
        LOG.error("Failed consuming records", e);
      }
    });
  }

  protected void start() {
    synchronized (this) {
      if (!isStarted) {
        queue.start();
        isStarted = true;
      }
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

