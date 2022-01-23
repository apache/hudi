/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hbase;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * ScheduledThreadPoolExecutor that will add some jitter to the RunnableScheduledFuture.getDelay.
 *
 * This will spread out things on a distributed cluster.
 */
@InterfaceAudience.Private
public class JitterScheduledThreadPoolExecutorImpl extends ScheduledThreadPoolExecutor {
  private final double spread;

  /**
   * Main constructor.
   * @param spread The percent up and down that RunnableScheduledFuture.getDelay should be jittered.
   */
  public JitterScheduledThreadPoolExecutorImpl(int corePoolSize,
                                               ThreadFactory threadFactory,
                                               double spread) {
    super(corePoolSize, threadFactory);
    this.spread = spread;
  }

  @Override
  protected <V> java.util.concurrent.RunnableScheduledFuture<V> decorateTask(
      Runnable runnable, java.util.concurrent.RunnableScheduledFuture<V> task) {
    return new JitteredRunnableScheduledFuture<>(task);
  }

  @Override
  protected <V> java.util.concurrent.RunnableScheduledFuture<V> decorateTask(
      Callable<V> callable, java.util.concurrent.RunnableScheduledFuture<V> task) {
    return new JitteredRunnableScheduledFuture<>(task);
  }

  /**
   * Class that basically just defers to the wrapped future.
   * The only exception is getDelay
   */
  protected class JitteredRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
    private final RunnableScheduledFuture<V> wrapped;
    JitteredRunnableScheduledFuture(RunnableScheduledFuture<V> wrapped) {
      this.wrapped = wrapped;
    }

    @Override
    public boolean isPeriodic() {
      return wrapped.isPeriodic();
    }

    @Override
    public long getDelay(TimeUnit unit) {
      long baseDelay = wrapped.getDelay(unit);
      long spreadTime = (long) (baseDelay * spread);
      long delay = spreadTime <= 0 ? baseDelay
          : baseDelay + ThreadLocalRandom.current().nextLong(-spreadTime, spreadTime);
      // Ensure that we don't roll over for nanoseconds.
      return (delay < 0) ? baseDelay : delay;
    }

    @Override
    public int compareTo(Delayed o) {
      return wrapped.compareTo(o);
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      return obj instanceof Delayed? compareTo((Delayed)obj) == 0: false;
    }

    @Override
    public int hashCode() {
      return this.wrapped.hashCode();
    }

    @Override
    public void run() {
      wrapped.run();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
      return wrapped.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return wrapped.get();
    }

    @Override
    public V get(long timeout,
                 TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return wrapped.get(timeout, unit);
    }
  }
}
