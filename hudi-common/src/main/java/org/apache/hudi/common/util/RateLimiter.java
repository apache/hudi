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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class RateLimiter {

  private final Semaphore semaphore;
  private final int maxPermits;
  private final TimeUnit timePeriod;
  private ScheduledExecutorService scheduler;
  private static final long RELEASE_PERMITS_PERIOD_IN_SECONDS = 1L;
  private static final long WAIT_BEFORE_NEXT_ACQUIRE_PERMIT_IN_MS = 5;
  private static final int SCHEDULER_CORE_THREAD_POOL_SIZE = 1;

  private static final Logger LOG = LogManager.getLogger(RateLimiter.class);

  public static RateLimiter create(int permits, TimeUnit timePeriod) {
    final RateLimiter limiter = new RateLimiter(permits, timePeriod);
    limiter.releasePermitsPeriodically();
    return limiter;
  }

  private RateLimiter(int permits, TimeUnit timePeriod) {
    this.semaphore = new Semaphore(permits);
    this.maxPermits = permits;
    this.timePeriod = timePeriod;
  }

  public boolean tryAcquire(int numPermits) {
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
    try {
      while (!semaphore.tryAcquire(numOps)) {
        Thread.sleep(WAIT_BEFORE_NEXT_ACQUIRE_PERMIT_IN_MS);
      }
      LOG.debug(String.format("acquire permits: %s, maxPremits: %s", numOps, maxPermits));
    } catch (InterruptedException e) {
      throw new RuntimeException("Unable to acquire permits", e);
    }
    return true;
  }

  public void stop() {
    scheduler.shutdownNow();
  }

  public void releasePermitsPeriodically() {
    scheduler = Executors.newScheduledThreadPool(SCHEDULER_CORE_THREAD_POOL_SIZE);
    scheduler.scheduleAtFixedRate(() -> {
      LOG.debug(String.format("Release permits: maxPremits: %s, available: %s", maxPermits,
          semaphore.availablePermits()));
      semaphore.release(maxPermits - semaphore.availablePermits());
    }, RELEASE_PERMITS_PERIOD_IN_SECONDS, RELEASE_PERMITS_PERIOD_IN_SECONDS, timePeriod);

  }

}
