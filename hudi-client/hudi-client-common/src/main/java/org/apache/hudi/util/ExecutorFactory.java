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

import org.apache.hudi.common.util.Functions;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.DisruptorExecutor;
import org.apache.hudi.common.util.queue.ExecutorType;
import org.apache.hudi.common.util.queue.HoodieConsumer;
import org.apache.hudi.common.util.queue.HoodieExecutor;
import org.apache.hudi.common.util.queue.SimpleExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

import java.util.Iterator;
import java.util.function.Function;

public class ExecutorFactory {

  public static <I, O, E> HoodieExecutor<E> create(HoodieWriteConfig config,
                                                   Iterator<I> inputItr,
                                                   HoodieConsumer<O, E> consumer,
                                                   Function<I, O> transformFunction) {
    return create(config, inputItr, consumer, transformFunction, Functions.noop());
  }

  public static <I, O, E> HoodieExecutor<E> create(HoodieWriteConfig config,
                                                   Iterator<I> inputItr,
                                                   HoodieConsumer<O, E> consumer,
                                                   Function<I, O> transformFunction,
                                                   Runnable preExecuteRunnable) {
    ExecutorType executorType = config.getExecutorType();
    switch (executorType) {
      case BOUNDED_IN_MEMORY:
        return new BoundedInMemoryExecutor<>(config.getWriteBufferLimitBytes(), config.getWriteBufferRecordSamplingRate(), config.getWriteBufferRecordCacheLimit(),
            inputItr, consumer, transformFunction, preExecuteRunnable);
      case DISRUPTOR:
        return new DisruptorExecutor<>(config.getWriteExecutorDisruptorWriteBufferLimitBytes(), inputItr, consumer,
            transformFunction, config.getWriteExecutorDisruptorWaitStrategy(), preExecuteRunnable);
      case SIMPLE:
        return new SimpleExecutor<>(inputItr, consumer, transformFunction);
      default:
        throw new HoodieException("Unsupported Executor Type " + executorType);
    }
  }

  /**
   * Checks whether configured {@link HoodieExecutor} buffer records (for ex, by holding them
   * in the queue)
   */
  public static boolean isBufferingRecords(HoodieWriteConfig config) {
    ExecutorType executorType = config.getExecutorType();
    switch (executorType) {
      case BOUNDED_IN_MEMORY:
      case DISRUPTOR:
        return true;
      case SIMPLE:
        return false;
      default:
        throw new HoodieException("Unsupported Executor Type " + executorType);
    }
  }
}
