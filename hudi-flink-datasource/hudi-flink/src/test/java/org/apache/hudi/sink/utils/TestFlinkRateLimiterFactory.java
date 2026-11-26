/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.utils;

import org.apache.hudi.common.util.RateLimiter;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class TestFlinkRateLimiterFactory {

  @Test
  void testRateLimitLowerThanParallelismUsesLongerReleasePeriod() {
    ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
    long expectedPeriodNanos = TimeUnit.SECONDS.toNanos(4);

    RateLimiter limiter = FlinkRateLimiterFactory.create(1, 4, scheduler);
    ArgumentCaptor<Runnable> refillTask = ArgumentCaptor.forClass(Runnable.class);
    try {
      verify(scheduler).scheduleAtFixedRate(
          refillTask.capture(), eq(expectedPeriodNanos), eq(expectedPeriodNanos), eq(TimeUnit.NANOSECONDS));
      assertTrue(limiter.acquire());
      refillTask.getValue().run();
      assertTrue(limiter.acquire());
    } finally {
      limiter.stop();
    }
    verify(scheduler).shutdownNow();
  }

  @Test
  void testRateLimitAtLeastParallelismUsesPermitsPerSecond() {
    FlinkRateLimiterFactory.RateLimitConfig config = FlinkRateLimiterFactory.resolveRateLimit(8, 4);

    assertEquals(2, config.getPermits());
    assertEquals(1, config.getReleasePeriod());
    assertEquals(TimeUnit.SECONDS, config.getTimeUnit());
  }

  @Test
  void testFractionalRateLimitRoundsReleasePeriodUp() {
    FlinkRateLimiterFactory.RateLimitConfig config = FlinkRateLimiterFactory.resolveRateLimit(3, 4);

    assertEquals(1, config.getPermits());
    assertEquals(1_333_333_334L, config.getReleasePeriod());
    assertEquals(TimeUnit.NANOSECONDS, config.getTimeUnit());
  }

  @Test
  void testInvalidRateLimitInputsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> FlinkRateLimiterFactory.create(0, 4));
    assertThrows(IllegalArgumentException.class, () -> FlinkRateLimiterFactory.create(1, 0));
  }
}
