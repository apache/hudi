/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.common.util.queue;

import java.util.function.Function;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Buffer producer which allows custom functions to insert entries to queue.
 *
 * @param <I> Type of entry produced for queue
 */
public class FunctionBasedQueueProducer<I> implements BoundedInMemoryQueueProducer<I> {

  private static final Logger logger = LogManager.getLogger(FunctionBasedQueueProducer.class);

  private final Function<BoundedInMemoryQueue<I, ?>, Boolean> producerFunction;

  public FunctionBasedQueueProducer(Function<BoundedInMemoryQueue<I, ?>, Boolean> producerFunction) {
    this.producerFunction = producerFunction;
  }

  @Override
  public void produce(BoundedInMemoryQueue<I, ?> queue) {
    logger.info("starting function which will enqueue records");
    producerFunction.apply(queue);
    logger.info("finished function which will enqueue records");
  }
}
