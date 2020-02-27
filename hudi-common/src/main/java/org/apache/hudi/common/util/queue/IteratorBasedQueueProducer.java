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

import java.util.Iterator;

/**
 * Iterator based producer which pulls entry from iterator and produces items for the queue.
 *
 * @param <I> Item type produced for the buffer.
 */
public class IteratorBasedQueueProducer<I> implements BoundedInMemoryQueueProducer<I> {

  private static final Logger LOG = LogManager.getLogger(IteratorBasedQueueProducer.class);

  // input iterator for producing items in the buffer.
  private final Iterator<I> inputIterator;

  public IteratorBasedQueueProducer(Iterator<I> inputIterator) {
    this.inputIterator = inputIterator;
  }

  @Override
  public void produce(BoundedInMemoryQueue<I, ?> queue) throws Exception {
    LOG.info("starting to buffer records");
    while (inputIterator.hasNext()) {
      queue.insertRecord(inputIterator.next());
    }
    LOG.info("finished buffering records");
  }
}
