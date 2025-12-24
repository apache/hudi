/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.disruptor;

import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.queue.WaitStrategyFactory;
import org.apache.hudi.exception.HoodieException;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

public class DisruptorQueue<T> implements Closeable {

  private final Disruptor<DisruptorEvent<T>> disruptor;
  private final RingBuffer<DisruptorEvent<T>> ringBuffer;
  private DisruptorEventHandler<T> handler;

  public DisruptorQueue(int bufferSize, String waitStrategyId) {
    WaitStrategy waitStrategy = WaitStrategyFactory.build(waitStrategyId);
    disruptor = new Disruptor<>(new DisruptorEventFactory<>(), bufferSize, new CustomizedThreadFactory("disruptor"), ProducerType.SINGLE, waitStrategy);
    ringBuffer = disruptor.getRingBuffer();
  }

  public void start() {
    disruptor.start();
  }

  public void produce(T data) {
    long sequence = ringBuffer.next();
    try {
      DisruptorEvent<T> event = ringBuffer.get(sequence);
      event.setData(data);
    } finally {
      ringBuffer.publish(sequence);
    }
  }

  public void addConsumer(DisruptorEventHandler<T> handler) {
    this.handler = handler;
    disruptor.handleEventsWith(handler);
  }

  public boolean waitFor() {
    while (!handler.isStoped()) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new HoodieException(e.getMessage(), e);
      }
    }
    return true;
  }

  @Override
  public void close() {
    disruptor.shutdown();
  }

  public void closeNow() {
    try {
      disruptor.shutdown(1, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      throw new HoodieException(e.getMessage(), e);
    }
  }
}
