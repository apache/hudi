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

package org.apache.hudi.utilities.unstructured.memory;

import org.apache.hudi.common.util.collection.ClosableIterator;

import org.apache.spark.TaskContext;

import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * How many video decodes of this JVM run at once, for the video memory cells: a counting preprocessor counts a decode from its
 * {@code open} until its chunk iterator is closed, and records the Spark partition of each decode. Printed by
 * {@link VideoRoundMemoryChild} as {@code VIDEO_DECODES max_in_flight=<n> tasks=<partition>=<decodes>,...}.
 */
final class VideoDecodeConcurrency {

  private static final AtomicInteger IN_FLIGHT = new AtomicInteger();
  private static final AtomicInteger MAX_IN_FLIGHT = new AtomicInteger();
  private static final Map<Integer, Integer> DECODES_PER_TASK = new TreeMap<>();

  private VideoDecodeConcurrency() {
  }

  /** Counts a decode that starts now in this task, and returns {@code chunks} counting it as finished when closed. */
  static <T> ClosableIterator<T> track(ClosableIterator<T> chunks) {
    TaskContext task = TaskContext.get();
    synchronized (DECODES_PER_TASK) {
      DECODES_PER_TASK.merge(task == null ? -1 : task.partitionId(), 1, Integer::sum);
    }
    MAX_IN_FLIGHT.accumulateAndGet(IN_FLIGHT.incrementAndGet(), Math::max);
    return new ClosableIterator<T>() {
      private boolean closed;

      @Override
      public boolean hasNext() {
        return chunks.hasNext();
      }

      @Override
      public T next() {
        return chunks.next();
      }

      @Override
      public void close() {
        try {
          chunks.close();
        } finally {
          if (!closed) {
            closed = true;
            IN_FLIGHT.decrementAndGet();
          }
        }
      }
    };
  }

  /** The line {@code VIDEO_DECODES max_in_flight=<n> tasks=<partition>=<decodes>,...}. */
  static String report() {
    StringBuilder tasks = new StringBuilder();
    synchronized (DECODES_PER_TASK) {
      DECODES_PER_TASK.forEach((partition, decodes) ->
          tasks.append(tasks.length() == 0 ? "" : ",").append(partition).append('=').append(decodes));
    }
    return "VIDEO_DECODES max_in_flight=" + MAX_IN_FLIGHT.get() + " tasks=" + tasks;
  }
}
