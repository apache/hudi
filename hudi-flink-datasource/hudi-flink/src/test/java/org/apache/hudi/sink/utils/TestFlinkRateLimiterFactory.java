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

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestFlinkRateLimiterFactory {

  @Test
  void testRateLimitLowerThanParallelismDoesNotCreateZeroPermitLimiter() {
    RateLimiter limiter = FlinkRateLimiterFactory.create(1, 4);
    try {
      assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(limiter.acquire(1)));
    } finally {
      limiter.stop();
    }
  }

  @Test
  void testRateLimitAtLeastParallelismUsesPermitsPerSecond() {
    RateLimiter limiter = FlinkRateLimiterFactory.create(8, 4);
    try {
      assertTrue(limiter.acquire(2));
    } finally {
      limiter.stop();
    }
  }

  @Test
  void testInvalidRateLimitInputsAreRejected() {
    assertThrows(IllegalArgumentException.class, () -> FlinkRateLimiterFactory.create(0, 4));
    assertThrows(IllegalArgumentException.class, () -> FlinkRateLimiterFactory.create(1, 0));
  }
}
