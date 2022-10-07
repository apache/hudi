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

package org.apache.hudi.util;

import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.common.util.queue.IteratorBasedQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import java.util.Iterator;
import java.util.function.Function;

public class MessageQueueFactory {

  /**
   * Create a new hoodie executor instance on demand.
   */
  public static HoodieExecutor create(HoodieWriteConfig hoodieConfig, Iterator inputItr, IteratorBasedQueueConsumer consumer,
                                      Function transformFunction, Runnable preExecuteRunnable) {
    ExecutorType executorType = hoodieConfig.getExecutorType();

    switch (executorType) {
      case BOUNDED_IN_MEMORY:
        return new BoundedInMemoryExecutor<>(hoodieConfig.getWriteBufferLimitBytes(), inputItr, consumer,
            transformFunction, preExecuteRunnable);
      case DISRUPTOR:
        return new DisruptorExecutor<>(hoodieConfig.getWriteBufferSize(), inputItr, consumer,
            transformFunction, hoodieConfig.getWriteWaitStrategy(), preExecuteRunnable);
      default:
        throw new HoodieException("Unsupported Executor Type " + executorType);
    }
  }
}
