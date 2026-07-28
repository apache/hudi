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

package org.apache.hudi.hive.util;

import org.apache.hudi.common.util.VisibleForTesting;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handle to one fan-out batch of partition work: the submitted futures plus the shared
 * abort flag the tasks consult before running.
 *
 * <p>The abort flag exists because waiting on futures in <i>submission</i> order is not
 * enough to stop queued work. If a later task fails quickly while an earlier one is slow,
 * the awaiting thread is still parked on the earlier {@code Future.get()}, and the
 * executor happily keeps starting every queued task in the meantime. By the time the
 * failure is observed, most of the "not-yet-started" work has already run.
 *
 * <p>Two mechanisms fix that, and both are needed:
 * <ul>
 *   <li>a {@link CountDownLatch} tripped by the <i>first</i> abort, so the awaiting
 *       thread wakes on failure rather than on its turn in submission order;</li>
 *   <li>a task-side {@link #aborted()} check on entry, because cancelling from the
 *       awaiting thread is inherently late — a worker can pull its next task off the
 *       queue at any moment.</li>
 * </ul>
 *
 * <p>Shared by {@link HiveDriverPool} (Hive {@code Driver} statements) and
 * {@link IMetaStoreClientPool} (Thrift {@code dropPartition} batches), which fan out over
 * different execution models but need identical abort-on-first-error semantics.
 */
public final class ParallelDispatch {

  private final List<Future<?>> futures;
  private final int total;
  private final AtomicInteger settled = new AtomicInteger(0);
  private final AtomicBoolean aborted = new AtomicBoolean(false);
  private final CountDownLatch done = new CountDownLatch(1);
  private volatile boolean sealed;

  ParallelDispatch(int total) {
    this.total = total;
    this.futures = new ArrayList<>(total);
  }

  void add(Future<?> future) {
    futures.add(future);
  }

  // Called once submission finishes. A task that settles before the last submit
  // would otherwise see settled < total and never trip the latch, so re-check here.
  void sealed() {
    sealed = true;
    signalIfComplete();
  }

  boolean aborted() {
    return aborted.get();
  }

  void abort() {
    aborted.set(true);
    done.countDown();
  }

  void taskSettled() {
    settled.incrementAndGet();
    signalIfComplete();
  }

  private void signalIfComplete() {
    if (sealed && settled.get() >= total) {
      done.countDown();
    }
  }

  void awaitSettledOrAborted() {
    if (total == 0) {
      return;
    }
    try {
      done.await();
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      aborted.set(true);
    }
  }

  // mayInterruptIfRunning=false: a worker may be mid-statement against a Hive Driver or
  // a Thrift client, and we don't want to tear that down partway. Cancel only tasks that
  // haven't started; in-flight work runs to completion.
  int cancelPending() {
    int cancelled = 0;
    for (Future<?> f : futures) {
      if (f.cancel(false)) {
        cancelled++;
      }
    }
    return cancelled;
  }

  List<Future<?>> futures() {
    return futures;
  }

  /**
   * Wraps {@code body} so it observes this batch's abort flag: it skips itself if a
   * sibling has already failed, trips the flag if it fails, and always records that it
   * settled so the awaiting thread can be released.
   */
  Callable<Void> guard(Callable<Void> body, String skipMessage) {
    return () -> {
      if (aborted()) {
        throw new CancellationException(skipMessage);
      }
      try {
        return body.call();
      } catch (Throwable t) {
        abort();
        throw t;
      } finally {
        taskSettled();
      }
    };
  }

  /**
   * Waits for the batch to settle (or abort), cancels whatever had not started, and
   * returns the outcome. Errors are observed in <i>completion</i> order, not submission
   * order, so a failure on a fast worker stops the other queues even while a slow worker
   * is still mid-statement.
   */
  Outcome awaitOutcome() {
    awaitSettledOrAborted();
    int cancelled = cancelPending();

    Exception firstError = null;
    int completed = 0;
    List<Exception> suppressed = new ArrayList<>();
    for (Future<?> f : futures) {
      try {
        f.get();
        completed++;
      } catch (CancellationException ce) {
        // Either we cancelled it before it started, or the task itself observed the
        // abort flag and bailed. Not a new failure; just note it for the summary.
        cancelled++;
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        if (firstError == null) {
          firstError = ie;
        }
      } catch (ExecutionException ee) {
        Exception cause = unwrap(ee);
        if (cause instanceof CancellationException) {
          cancelled++;
        } else if (firstError == null) {
          firstError = cause;
        } else {
          suppressed.add(cause);
        }
      }
    }
    return new Outcome(firstError, completed, cancelled, suppressed);
  }

  private static Exception unwrap(ExecutionException ee) {
    Throwable cause = ee.getCause();
    return (cause instanceof Exception) ? (Exception) cause : ee;
  }

  @VisibleForTesting
  public int size() {
    return futures.size();
  }

  @VisibleForTesting
  public Future<?> futureAt(int index) {
    return futures.get(index);
  }

  /** Result of awaiting a batch: the first real failure, if any, plus counts for logging. */
  static final class Outcome {
    private final Exception firstError;
    private final int completed;
    private final int cancelled;
    private final List<Exception> suppressed;

    private Outcome(Exception firstError, int completed, int cancelled, List<Exception> suppressed) {
      this.firstError = firstError;
      this.completed = completed;
      this.cancelled = cancelled;
      this.suppressed = suppressed;
    }

    Exception firstError() {
      return firstError;
    }

    boolean failed() {
      return firstError != null;
    }

    int completed() {
      return completed;
    }

    int cancelled() {
      return cancelled;
    }

    List<Exception> suppressed() {
      return suppressed;
    }
  }
}
