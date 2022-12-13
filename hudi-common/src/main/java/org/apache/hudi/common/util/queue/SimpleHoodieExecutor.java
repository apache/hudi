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

import static org.apache.hudi.common.util.ValidationUtils.checkState;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Single Writer and Single Reader mode. Also this SimpleHoodieExecutor has no inner message queue and no inner lock.
 * Consuming and writing records from iterator directly.
 *
 * Compared with queue based Executor
 * Advantages: there is no need for additional memory and cpu resources due to lock or multithreading.
 * Disadvantages: lost some benefits such as speed limit. And maybe lower throughput.
 */
public class SimpleHoodieExecutor<I, O, E> implements HoodieExecutor<E> {

  private static final Logger LOG = LogManager.getLogger(SimpleHoodieExecutor.class);

  // Consumer
  protected final Option<HoodieConsumer<O, E>> consumer;
  // records iterator
  protected final Iterator<I> it;
  private final Function<I, O> transformFunction;

  public SimpleHoodieExecutor(final Iterator<I> inputItr, HoodieConsumer<O, E> consumer,
                              Function<I, O> transformFunction) {
    this(inputItr, Option.of(consumer), transformFunction);
  }

  public SimpleHoodieExecutor(final Iterator<I> inputItr, Option<HoodieConsumer<O, E>> consumer,
                              Function<I, O> transformFunction) {
    this.it = inputItr;
    this.consumer = consumer;
    this.transformFunction = transformFunction;
  }

  /**
   * Consuming records from input iterator directly without any producers and inner message queue.
   */
  @Override
  public E execute() {
    checkState(this.consumer.isPresent());

    try {
      LOG.info("Starting consumer, consuming records from the records iterator directly");
      while (it.hasNext()) {
        O payload = transformFunction.apply(it.next());
        consumer.get().consume(payload);
      }

      return consumer.get().finish();
    } catch (Exception e) {
      LOG.error("Error consuming records in SimpleHoodieExecutor", e);
      throw new HoodieException(e);
    }
  }

  @Override
  public void shutdownNow() {
    // no-op
  }

  @Override
  public boolean awaitTermination() {
    // no-op
    return true;
  }
}
