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

import java.util.Iterator;

/**
 * Consume entries from queue and execute callback function.
 */
public abstract class IteratorBasedQueueConsumer<I, O> implements HoodieConsumer<I, O> {

  /**
   * API to de-queue entries to memory bounded queue.
   *
   * @param queue In Memory bounded queue
   */
  @Override
  public O consume(HoodieMessageQueue<?, I> queue) throws Exception {

    Iterator<I> iterator = ((HoodieIterableMessageQueue) queue).iterator();

    while (iterator.hasNext()) {
      consumeOneRecord(iterator.next());
    }

    // Notifies done
    finish();

    return getResult();
  }

  /**
   * Consumer One record.
   */
  public abstract void consumeOneRecord(I record);

  /**
   * Notifies implementation that we have exhausted consuming records from queue.
   */
  public void finish(){}

  /**
   * Return result of consuming records so far.
   */
  protected abstract O getResult();

}
