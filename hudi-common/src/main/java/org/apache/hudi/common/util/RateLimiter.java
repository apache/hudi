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

package org.apache.hudi.common.util;

import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Thread-safe rate limiter implementation.
 */
@Slf4j
@ThreadSafe
public class RateLimiter {

  private final Semaphore semaphore;
  private final int maxPermits;
  private final long releasePermitsPeriod;
  private final TimeUnit timePeriod;
  private final AtomicBoolean stopped = new AtomicBoolean(false);
  private volatile ScheduledExecutorService scheduler;
  private static final long RELEASE_PERMITS_PERIOD_IN_SECONDS = 1L;
  private static final long WAIT_BEFORE_NEXT_ACQUIRE_PERMIT_IN_MS = 5;
  private static final int SCHEDULER_CORE_THREAD_POOL_SIZE = 1;

  public static RateLimiter create(int permits, TimeUnit timePeriod) {
    return create(permits, RELEASE_PERMITS_PERIOD_IN_SECONDS, timePeriod);
  }

  public static RateLimiter create(int permits, long releasePermitsPeriod, TimeUnit timePeriod) {
    validateConfiguration(permits, releasePermitsPeriod, timePeriod);
    final RateLimiter limiter = new RateLimiter(permits, releasePermitsPeriod, timePeriod);
    limiter.releasePermitsPeriodically();
    return limiter;
  }

  @VisibleForTesting
  public static RateLimiter create(int permits, long releasePermitsPeriod, TimeUnit timePeriod,
                                   ScheduledExecutorService scheduler) {
    validateConfiguration(permits, releasePermitsPeriod, timePeriod);
    ValidationUtils.checkArgument(scheduler != null, "Scheduler must not be null");
    final RateLimiter limiter = new RateLimiter(permits, releasePermitsPeriod, timePeriod);
    limiter.releasePermitsPeriodically(scheduler);
    return limiter;
  }

  private static void validateConfiguration(int permits, long releasePermitsPeriod, TimeUnit timePeriod) {
    ValidationUtils.checkArgument(permits > 0, "Permits must be greater than zero");
    ValidationUtils.checkArgument(releasePermitsPeriod > 0, "Release permits period must be greater than zero");
    ValidationUtils.checkArgument(timePeriod != null, "Time period must not be null");
  }

  private RateLimiter(int permits, long releasePermitsPeriod, TimeUnit timePeriod) {
    this.semaphore = new Semaphore(permits);
    this.maxPermits = permits;
    this.releasePermitsPeriod = releasePermitsPeriod;
    this.timePeriod = timePeriod;
  }

  public boolean tryAcquire(int numPermits) {
    ValidationUtils.checkArgument(numPermits > 0, "Number of permits must be greater than zero");
    int remainingPermits = numPermits;
    while (remainingPermits > 0) {
      if (remainingPermits > maxPermits) {
        acquire(maxPermits);
        remainingPermits -= maxPermits;
      } else {
        return acquire(remainingPermits);
      }
    }
    return true;
  }

  public boolean acquire(int numOps) {
    ValidationUtils.checkArgument(numOps > 0 && numOps <= maxPermits,
        "Number of permits must be between one and the configured maximum");
    return acquireInternal(numOps);
  }

  public boolean acquire() {
    return acquireInternal(1);
  }

  private boolean acquireInternal(int numOps) {
    try {
      while (!stopped.get() && !semaphore.tryAcquire(numOps)) {
        Thread.sleep(WAIT_BEFORE_NEXT_ACQUIRE_PERMIT_IN_MS);
      }
      ValidationUtils.checkState(!stopped.get(), "Rate limiter is stopped");
      log.debug("acquire permits: {}, maxPermits: {}", numOps, maxPermits);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Unable to acquire permits", e);
    }
    return true;
  }

  public synchronized void stop() {
    if (stopped.compareAndSet(false, true)) {
      ScheduledExecutorService currentScheduler = scheduler;
      if (currentScheduler != null) {
        currentScheduler.shutdownNow();
      }
    }
  }

  public boolean isStopped() {
    return stopped.get();
  }

  public synchronized void releasePermitsPeriodically() {
    ValidationUtils.checkState(!stopped.get(), "Cannot start a stopped rate limiter");
    if (scheduler != null) {
      return;
    }
    releasePermitsPeriodically(Executors.newScheduledThreadPool(SCHEDULER_CORE_THREAD_POOL_SIZE,
        new CustomizedThreadFactory("rate-limiter", true)));
  }

  private synchronized void releasePermitsPeriodically(ScheduledExecutorService scheduler) {
    ValidationUtils.checkState(!stopped.get(), "Cannot start a stopped rate limiter");
    if (this.scheduler != null) {
      return;
    }
    this.scheduler = scheduler;
    try {
      scheduler.scheduleAtFixedRate(() -> {
        log.debug("Release permits: maxPermits: {}, available: {}", maxPermits, semaphore.availablePermits());
        semaphore.release(maxPermits - semaphore.availablePermits());
      }, releasePermitsPeriod, releasePermitsPeriod, timePeriod);
    } catch (RuntimeException e) {
      this.scheduler = null;
      scheduler.shutdownNow();
      throw e;
    }

  }

}
