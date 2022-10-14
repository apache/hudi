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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.Future;

/**
 * MessageQueueBasedHoodieExecutor holds a inner message queue, producers and consumer.
 * Producers produce message into message queue and consumer can consume message from this inner message queue.
 */
public abstract class MessageQueueBasedHoodieExecutor<I, O, E> implements HoodieExecutor<I, O, E> {

  private static final Logger LOG = LogManager.getLogger(MessageQueueBasedHoodieExecutor.class);

  public HoodieMessageQueue<I, O> queue;
  public HoodieExecutorBase<I, O, E> hoodieExecutorBase;
  public List<Future<Boolean>> producerTasks;

  public MessageQueueBasedHoodieExecutor(HoodieMessageQueue<I, O> queue, HoodieExecutorBase<I, O, E> hoodieExecutorBase) {
    this.hoodieExecutorBase = hoodieExecutorBase;
    this.queue = queue;
  }

  /**
   * Main API to run both production and consumption.
   */
  @Override
  public E execute() {
    try {
      ValidationUtils.checkState(hoodieExecutorBase.getConsumer().isPresent());
      setup();
      producerTasks = startProducers();
      Future<E> future = startConsumer();
      return future.get();
    } catch (InterruptedException ie) {
      shutdownNow();
      Thread.currentThread().interrupt();
      throw new HoodieException(ie);
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      postAction();
    }
  }

  public void setup(){}
}
