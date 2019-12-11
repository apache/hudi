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

package org.apache.hudi.func;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueProducer;
import org.apache.hudi.common.util.queue.IteratorBasedQueueProducer;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

import java.util.Iterator;
import java.util.function.Function;

public class SparkBoundedInMemoryExecutor<I, O, E> extends BoundedInMemoryExecutor<I, O, E> {

  // Need to set current spark thread's TaskContext into newly launched thread so that new thread can access
  // TaskContext properties.
  final TaskContext sparkThreadTaskContext;

  public SparkBoundedInMemoryExecutor(final HoodieWriteConfig hoodieConfig, final Iterator<I> inputItr,
      BoundedInMemoryQueueConsumer<O, E> consumer, Function<I, O> bufferedIteratorTransform) {
    this(hoodieConfig, new IteratorBasedQueueProducer<>(inputItr), consumer, bufferedIteratorTransform);
  }

  public SparkBoundedInMemoryExecutor(final HoodieWriteConfig hoodieConfig, BoundedInMemoryQueueProducer<I> producer,
      BoundedInMemoryQueueConsumer<O, E> consumer, Function<I, O> bufferedIteratorTransform) {
    super(hoodieConfig.getWriteBufferLimitBytes(), producer, Option.of(consumer), bufferedIteratorTransform);
    this.sparkThreadTaskContext = TaskContext.get();
  }

  public void preExecute() {
    // Passing parent thread's TaskContext to newly launched thread for it to access original TaskContext properties.
    TaskContext$.MODULE$.setTaskContext(sparkThreadTaskContext);
  }
}
