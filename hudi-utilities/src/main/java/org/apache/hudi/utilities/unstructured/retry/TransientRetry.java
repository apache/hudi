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

package org.apache.hudi.utilities.unstructured.retry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.Serializable;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.function.Predicate;

/**
 * The in-commit retry of a transient failure: a step that fails with a failure its stage calls
 * transient is tried again, up to {@code maxAttempts} tries in all, waiting {@code backoffMs} before the second try and doubling
 * each later wait up to {@link #MAX_BACKOFF_MS}. When the last try fails too, its failure propagates, the first try's failure
 * attached as suppressed: the stage fails the round, nothing is committed, and the next run resumes from the same checkpoint. A failure that
 * is not transient (bad input, a platform failure such as a 403) propagates at once, and so does an interruption (the thread's
 * interrupt flag set, or an InterruptedIOException that is not a SocketTimeoutException / a ClosedByInterruptException in the
 * failure's cause chain: the task is being killed, a timed-out request is retried). Nothing is recorded anywhere but the log.
 */
public final class TransientRetry implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(TransientRetry.class);

  /** Cap of one wait between tries: 30 s. */
  public static final long MAX_BACKOFF_MS = 30_000L;
  /** One try, no retry. */
  public static final TransientRetry NONE = new TransientRetry(1, 0L);

  private final int maxAttempts;
  private final long backoffMs;

  /**
   * @param maxAttempts tries in all, at least 1
   * @param backoffMs   the wait before the second try, at least 0
   * @throws IllegalArgumentException for a value out of range
   */
  public TransientRetry(int maxAttempts, long backoffMs) {
    if (maxAttempts < 1) {
      throw new IllegalArgumentException("A retry needs at least one attempt, got " + maxAttempts);
    }
    if (backoffMs < 0) {
      throw new IllegalArgumentException("A retry backoff must not be negative, got " + backoffMs);
    }
    this.maxAttempts = maxAttempts;
    this.backoffMs = backoffMs;
  }

  /** A step that may fail with an {@link IOException}. */
  @FunctionalInterface
  public interface IoStep<T> {
    T run() throws IOException;
  }

  /**
   * Runs {@code step}, trying again while it fails with a failure {@code isTransient} accepts and tries are left.
   *
   * @param what names the step in the log
   * @throws IOException      the last try's failure, or a failure that is not transient
   * @throws InterruptedIOException when the thread is interrupted while it waits; the interrupt flag is set again
   */
  public <T> T call(String what, IoStep<T> step, Predicate<Throwable> isTransient) throws IOException {
    Throwable first = null;
    for (int attempt = 1; ; attempt++) {
      try {
        return step.run();
      } catch (IOException | RuntimeException e) {
        if (!isTransient.test(e) || attempt >= maxAttempts || isInterruption(e)) {
          if (first != null && first != e) {
            e.addSuppressed(first);
          }
          throw e;
        }
        if (first == null) {
          first = e;
        }
        long wait = waitBefore(attempt + 1);
        LOG.warn("UNSTRUCTURED-TRANSIENT-RETRY {}: try {} of {} failed, retrying in {} ms", what, attempt, maxAttempts, wait, e);
        sleep(wait, what);
      }
    }
  }

  /** Whether {@code failure} says the thread or its task is being stopped: never retried. */
  static boolean isInterruption(Throwable failure) {
    if (Thread.currentThread().isInterrupted()) {
      return true;
    }
    for (Throwable cause = failure; cause != null; cause = cause.getCause() == cause ? null : cause.getCause()) {
      // a SocketTimeoutException (and Hadoop's ConnectTimeoutException, S3A's AWSApiCallTimeoutException) is an
      // InterruptedIOException by type but a timed-out request, so transient and retried
      boolean interruptedIo = cause instanceof InterruptedIOException && !(cause instanceof SocketTimeoutException);
      if (interruptedIo || cause instanceof ClosedByInterruptException || cause instanceof InterruptedException) {
        return true;
      }
    }
    return false;
  }

  /** The wait before try {@code attempt} (2 or more): {@link #backoffMs} doubled per earlier retry, capped. */
  long waitBefore(int attempt) {
    double wait = backoffMs * Math.pow(2, attempt - 2);
    return (long) Math.min(MAX_BACKOFF_MS, wait);
  }

  private static void sleep(long millis, String what) throws InterruptedIOException {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      InterruptedIOException interrupted = new InterruptedIOException("Interrupted waiting to retry " + what);
      interrupted.initCause(e);
      throw interrupted;
    }
  }

  @Override
  public String toString() {
    return "TransientRetry{maxAttempts=" + maxAttempts + ", backoffMs=" + backoffMs + "}";
  }
}
