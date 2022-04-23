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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.function.Function;

public class FunctionBasedDisruptorProducer<I> extends DisruptorBasedProducer<I> {

  private static final Logger LOG = LogManager.getLogger(FunctionBasedDisruptorProducer.class);

  private final Function<DisruptorMessageQueue<I, ?>, Boolean> producerFunction;

  public FunctionBasedDisruptorProducer(Function<DisruptorMessageQueue<I, ?>, Boolean> producerFunction) {
    this.producerFunction = producerFunction;
  }

  @Override
  public void produce(DisruptorMessageQueue<I, ?> queue) throws Exception {
    LOG.info("starting function which will enqueue records");
    producerFunction.apply(queue);
    LOG.info("finished function which will enqueue records");
  }
}
