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
import org.apache.hudi.common.util.ValidationUtils;

import java.util.concurrent.TimeUnit;

/**
 * Factory for distributing a job-wide write rate limit across Flink subtasks.
 */
public final class FlinkRateLimiterFactory {

  private FlinkRateLimiterFactory() {
  }

  public static RateLimiter create(long totalRateLimit, int parallelism) {
    ValidationUtils.checkArgument(totalRateLimit > 0, "Total rate limit must be greater than zero");
    ValidationUtils.checkArgument(parallelism > 0, "Parallelism must be greater than zero");

    if (totalRateLimit >= parallelism) {
      long rateLimitPerSubtask = totalRateLimit / parallelism;
      ValidationUtils.checkArgument(rateLimitPerSubtask <= Integer.MAX_VALUE,
          "Rate limit per subtask exceeds the supported maximum");
      return RateLimiter.create((int) rateLimitPerSubtask, TimeUnit.SECONDS);
    }

    long periodNanos = divideRoundingUp(
        Math.multiplyExact(TimeUnit.SECONDS.toNanos(1), parallelism), totalRateLimit);
    return RateLimiter.create(1, periodNanos, TimeUnit.NANOSECONDS);
  }

  private static long divideRoundingUp(long dividend, long divisor) {
    return 1 + (dividend - 1) / divisor;
  }
}
