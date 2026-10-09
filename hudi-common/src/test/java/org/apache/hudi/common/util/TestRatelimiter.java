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

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests {@link RateLimiter}.
 */
public class TestRatelimiter {

  @Test
  public void testRateLimiterWithNoThrottling() throws InterruptedException {
    RateLimiter limiter =  RateLimiter.create(1000, TimeUnit.SECONDS);
    try {
      long start = System.currentTimeMillis();
      assertEquals(true, limiter.tryAcquire(1000));
      // Sleep to represent some operation
      Thread.sleep(500);
      long end = System.currentTimeMillis();
      // With a large permit limit, there shouldn't be any throttling of operations
      assertTrue((end - start) < TimeUnit.SECONDS.toMillis(2));
    } finally {
      limiter.stop();
    }
  }

  @Test
  public void testRateLimiterWithThrottling() throws InterruptedException {
    RateLimiter limiter =  RateLimiter.create(100, TimeUnit.SECONDS);
    try {
      long start = System.currentTimeMillis();
      assertEquals(true, limiter.tryAcquire(400));
      // Sleep to represent some operation
      Thread.sleep(500);
      long end = System.currentTimeMillis();
      // As size of operations is more than the maximum permits per second,
      // whole execution should be greater than 1 second
      assertTrue((end - start) >= TimeUnit.SECONDS.toMillis(2));
    } finally {
      limiter.stop();
    }
  }

  @Test
  public void testCustomReleasePeriod() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);

    RateLimiter limiter = RateLimiter.create(1, 100, TimeUnit.MILLISECONDS, scheduler);
    ArgumentCaptor<Runnable> refillTask = ArgumentCaptor.forClass(Runnable.class);
    try {
      verify(scheduler).scheduleAtFixedRate(
          refillTask.capture(), eq(100L), eq(100L), eq(TimeUnit.MILLISECONDS));
      assertTrue(limiter.acquire());
      refillTask.getValue().run();
      assertTrue(limiter.acquire());
    } finally {
      limiter.stop();
    }
    verify(scheduler).shutdownNow();
  }

  @Test
  public void testInvalidConfigurationIsRejected() {
    assertThrows(IllegalArgumentException.class, () -> RateLimiter.create(0, TimeUnit.SECONDS));
    assertThrows(IllegalArgumentException.class, () -> RateLimiter.create(1, 0, TimeUnit.SECONDS));
    assertThrows(IllegalArgumentException.class, () -> RateLimiter.create(1, 1, null));
  }

  @Test
  public void testInvalidAcquireRequestIsRejected() {
    RateLimiter limiter = RateLimiter.create(1, 1, TimeUnit.DAYS);
    try {
      assertThrows(IllegalArgumentException.class, () -> limiter.acquire(0));
      assertThrows(IllegalArgumentException.class, () -> limiter.acquire(2));
    } finally {
      limiter.stop();
    }
  }

  @Test
  public void testStopIsIdempotentAndUnblocksAcquire() throws Exception {
    CountDownLatch waitingForPermit = new CountDownLatch(1);
    RateLimiter limiter = RateLimiter.create(1, 1, TimeUnit.DAYS, waitingForPermit::countDown);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      assertTrue(limiter.acquire());
      Future<Boolean> blockedAcquire = executor.submit(() -> limiter.acquire());
      assertTrue(waitingForPermit.await(1, TimeUnit.SECONDS));

      limiter.stop();
      limiter.stop();

      ExecutionException exception = assertThrows(
          ExecutionException.class, () -> blockedAcquire.get(1, TimeUnit.SECONDS));
      assertInstanceOf(IllegalStateException.class, exception.getCause());
      assertTrue(limiter.isStopped());
    } finally {
      limiter.stop();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(1, TimeUnit.SECONDS));
    }
  }
}
