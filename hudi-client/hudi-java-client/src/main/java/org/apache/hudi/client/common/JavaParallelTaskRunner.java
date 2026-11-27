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

package org.apache.hudi.client.common;

import org.apache.hudi.common.util.CustomizedThreadFactory;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Applies a task to a list of items on a bounded pool of threads created for one call, and returns the results in
 * the order of the items.
 *
 * <p>The pool has at most {@code parallelism} threads, each taking the next item until none is left. Taking an item
 * and stopping are atomic: once a task failure or an interrupt of the calling thread has been recorded, no thread
 * takes another item, while the items already taken run to their end. The call returns or throws only after the pool
 * has terminated, so no task is running any more when it does. The first task failure recorded is thrown, with the
 * other distinct failures added to it as suppressed. If a pool thread cannot be created, that failure is thrown
 * instead, with the task failures added to it as suppressed.
 */
public final class JavaParallelTaskRunner {

  private JavaParallelTaskRunner() {
  }

  public static <I, O> List<O> mapInOrder(List<I> items, Function<I, O> task, int parallelism, String threadNamePrefix) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    return mapInOrder(items, task, parallelism, threadNamePrefix,
        new CustomizedThreadFactory(threadNamePrefix, true, () -> Thread.currentThread().setContextClassLoader(classLoader)),
        () -> { }, () -> { });
  }

  /**
   * As {@link #mapInOrder(List, Function, int, String)}, with the pool threads created by {@code threadFactory},
   * {@code beforeTake} run on a pool thread each time before it takes an item, and {@code afterStopRequest} run on the
   * recording thread after each task failure or interrupt has been recorded.
   */
  @VisibleForTesting
  static <I, O> List<O> mapInOrder(List<I> items, Function<I, O> task, int parallelism, String threadNamePrefix,
                                   ThreadFactory threadFactory, Runnable beforeTake, Runnable afterStopRequest) {
    if (parallelism <= 1 || items.size() <= 1) {
      List<O> results = new ArrayList<>(items.size());
      for (I item : items) {
        results.add(task.apply(item));
      }
      return results;
    }

    int threads = Math.min(parallelism, items.size());
    Object[] results = new Object[items.size()];
    WorkState state = new WorkState(items.size(), beforeTake, afterStopRequest);
    ExecutorService pool = Executors.newFixedThreadPool(threads, threadFactory);
    Throwable submitFailure = null;
    try {
      for (int i = 0; i < threads; i++) {
        pool.execute(() -> {
          int index;
          while ((index = state.take()) >= 0) {
            try {
              results[index] = task.apply(items.get(index));
            } catch (Throwable t) {
              state.stop(t);
            }
          }
        });
      }
    } catch (Throwable t) {
      submitFailure = t;
      state.stop(null);
    }
    pool.shutdown();
    boolean interrupted = awaitTermination(pool, state);
    if (interrupted) {
      Thread.currentThread().interrupt();
    }

    List<Throwable> failures = state.failures();
    if (submitFailure != null) {
      failures.add(0, submitFailure);
    }
    if (!failures.isEmpty()) {
      Throwable first = withOthersSuppressed(failures);
      if (first instanceof RuntimeException) {
        throw (RuntimeException) first;
      }
      if (first instanceof Error) {
        throw (Error) first;
      }
      throw new HoodieException(first);
    }
    if (interrupted) {
      throw new HoodieException("Interrupted while running tasks on " + threadNamePrefix + " threads");
    }

    List<O> ordered = new ArrayList<>(results.length);
    for (Object result : results) {
      @SuppressWarnings("unchecked")
      O value = (O) result;
      ordered.add(value);
    }
    return ordered;
  }

  private static Throwable withOthersSuppressed(List<Throwable> failures) {
    Throwable first = failures.get(0);
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    seen.add(first);
    for (Throwable other : failures.subList(1, failures.size())) {
      if (seen.add(other)) {
        first.addSuppressed(other);
      }
    }
    return first;
  }

  private static boolean awaitTermination(ExecutorService pool, WorkState state) {
    boolean interrupted = false;
    while (true) {
      try {
        if (pool.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
          return interrupted;
        }
      } catch (InterruptedException e) {
        interrupted = true;
        state.stop(null);
      }
    }
  }

  /**
   * Hands out item indexes until every item is taken or a stop is recorded. Both hold the same lock, so no index is
   * handed out once a stop has been recorded.
   */
  private static final class WorkState {
    private final int size;
    private final Runnable beforeTake;
    private final Runnable afterStopRequest;
    private final List<Throwable> failures = new ArrayList<>();
    private int next;
    private boolean stopped;

    WorkState(int size, Runnable beforeTake, Runnable afterStopRequest) {
      this.size = size;
      this.beforeTake = beforeTake;
      this.afterStopRequest = afterStopRequest;
    }

    int take() {
      beforeTake.run();
      synchronized (this) {
        return stopped || next >= size ? -1 : next++;
      }
    }

    void stop(Throwable failure) {
      synchronized (this) {
        if (failure != null) {
          failures.add(failure);
        }
        stopped = true;
      }
      afterStopRequest.run();
    }

    synchronized List<Throwable> failures() {
      return new ArrayList<>(failures);
    }
  }
}
