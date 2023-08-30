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

import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.exception.HoodieException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.function.Function;

/**
 * Simple implementation of the {@link HoodieExecutor} interface assuming single-writer/single-reader
 * mode allowing it to consume from the input {@link Iterator} directly avoiding the need for
 * any internal materialization (ie queueing).
 *
 * <p>
 * Such executor is aimed primarily at allowing
 * the production-consumption chain to run w/ as little overhead as possible, at the expense of
 * limited parallelism and therefore throughput, which is not an issue for execution environments
 * such as Spark, where it's used primarily in a parallelism constraint environment (on executors)
 */
public class SimpleExecutor<I, O, E> implements HoodieExecutor<E> {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleExecutor.class);

  // Record iterator (producer)
  private final Iterator<I> itr;
  // Consumer
  private final HoodieConsumer<O, E> consumer;

  private final Function<I, O> transformFunction;

  public SimpleExecutor(Iterator<I> inputItr,
                        HoodieConsumer<O, E> consumer,
                        Function<I, O> transformFunction) {
    this.itr = inputItr;
    this.consumer = consumer;
    this.transformFunction = transformFunction;
  }

  /**
   * Consuming records from input iterator directly without any producers and inner message queue.
   */
  @Override
  public E execute() {
    try {
      LOG.info("Starting consumer, consuming records from the records iterator directly");
      while (itr.hasNext()) {
        O payload = transformFunction.apply(itr.next());
        consumer.consume(payload);
      }

      return consumer.finish();
    } catch (Exception e) {
      LOG.error("Failed consuming records", e);
      throw new HoodieException(e);
    }
  }

  @Override
  public void shutdownNow() {
    // Consumer is already closed when the execution completes
    if (itr instanceof ClosableIterator) {
      ((ClosableIterator<I>) itr).close();
    }
  }

  @Override
  public boolean awaitTermination() {
    // no-op
    return true;
  }
}
