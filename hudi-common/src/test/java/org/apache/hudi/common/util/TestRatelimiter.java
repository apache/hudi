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

import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestRatelimiter {

  @Test
  public void testRateLimiterWithNoThrottling() throws InterruptedException {
    RateLimiter limiter =  RateLimiter.create(1000, TimeUnit.SECONDS);
    long start = System.currentTimeMillis();
    assertEquals(true, limiter.tryAcquire(1000));
    // Sleep to represent some operation
    Thread.sleep(500);
    long end = System.currentTimeMillis();
    // With a large permit limit, there shouldn't be any throttling of operations
    assertTrue((end - start) < TimeUnit.SECONDS.toMillis(2));
  }

  @Test
  public void testRateLimiterWithThrottling() throws InterruptedException {
    RateLimiter limiter =  RateLimiter.create(100, TimeUnit.SECONDS);
    long start = System.currentTimeMillis();
    assertEquals(true, limiter.tryAcquire(400));
    // Sleep to represent some operation
    Thread.sleep(500);
    long end = System.currentTimeMillis();
    // As size of operations is more than the maximum permits per second,
    // whole execution should be greater than 1 second
    assertTrue((end - start) >= TimeUnit.SECONDS.toMillis(2));
  }
}
