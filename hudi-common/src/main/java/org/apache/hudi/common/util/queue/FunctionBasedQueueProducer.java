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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/**
 * Buffer producer which allows custom functions to insert entries to queue.
 *
 * @param <I> Type of entry produced for queue
 */
public class FunctionBasedQueueProducer<I> implements HoodieProducer<I> {

  private static final Logger LOG = LoggerFactory.getLogger(FunctionBasedQueueProducer.class);

  private final Function<HoodieMessageQueue<I, ?>, Boolean> producerFunction;

  public FunctionBasedQueueProducer(Function<HoodieMessageQueue<I, ?>, Boolean> producerFunction) {
    this.producerFunction = producerFunction;
  }

  @Override
  public void produce(HoodieMessageQueue<I, ?> queue) {
    LOG.info("Starting to produce records into the queue");
    producerFunction.apply(queue);
    LOG.info("Finished producing records into the queue");
  }

  @Override
  public void close() {
    /* no-op */
  }
}
