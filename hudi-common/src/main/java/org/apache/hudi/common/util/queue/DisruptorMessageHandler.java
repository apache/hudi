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

import com.lmax.disruptor.EventHandler;

public class DisruptorMessageHandler<O, E> implements EventHandler<HoodieDisruptorEvent<O>> {

  private BoundedInMemoryQueueConsumer<O, E> consumer;
  private boolean finished = false;
  private Runnable preExecuteRunnable;

  public DisruptorMessageHandler(BoundedInMemoryQueueConsumer<O, E> consumer, Runnable preExecuteRunnable) {
    this.consumer = consumer;
    this.preExecuteRunnable = preExecuteRunnable;
  }

  @Override
  public void onEvent(HoodieDisruptorEvent<O> event, long sequence, boolean endOfBatch) {
    preExecuteRunnable.run();
    if (event == null || event.get() == null) {
      // end of ingestion
      finished = true;
    } else {
      consumer.consumeOneRecord(event.get());
      event.clear();
    }
  }

  public boolean isFinished() {
    return finished;
  }
}
