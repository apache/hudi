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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import java.math.BigDecimal;
import java.util.function.LongSupplier;

/**
 * A token bucket: {@link #acquire} takes one token, waiting for the refill. The bucket holds at most
 * {@link #RATE_LIMIT_BURST_SUFFIX} tokens, starts full, and refills at {@link #RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX} tokens per
 * second. Without a rate the limiter is {@link #isLimited() unlimited}: {@link #acquire} returns at once and nothing changes.
 * At most about 1000 tokens are taken per second whatever the rate (the clock and the shortest wait are in milliseconds).
 *
 * <p>Each compute task owns its client and limiter. Rates and bursts are per task: 10 requests/s with 64 active compute
 * tasks permits up to 640 requests/s across those tasks.
 *
 * <p>Not thread safe.
 */
public final class RequestRateLimiter {

  /**
   * {@code hoodie.unstructured.embedding.model.<name>.rate.limit.requests.per.second}: the refill rate, a positive decimal
   * ({@code 0.5} = one token every 2 s), per compute task; unset or blank = no limit.
   */
  public static final String RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX = ".rate.limit.requests.per.second";

  /**
   * {@code hoodie.unstructured.embedding.model.<name>.rate.limit.burst}: the bucket capacity, a positive integer; default
   * {@value #DEFAULT_BURST}. Requires {@link #RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX}.
   */
  public static final String RATE_LIMIT_BURST_SUFFIX = ".rate.limit.burst";

  public static final int DEFAULT_BURST = 1;
  private static final double MILLIS_PER_SECOND = 1000d;
  /** Rounding slack of the token arithmetic, so 250 ms at 4/s refills exactly one token. */
  private static final double EPSILON = 1e-9;

  /** Waits; injectable so tests advance a fake clock instead of sleeping. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final double requestsPerSecond;
  private final int burst;
  private final LongSupplier clockMillis;
  private final Sleeper sleeper;
  private double tokens;
  private long refilledAt;

  private RequestRateLimiter(double requestsPerSecond, int burst, LongSupplier clockMillis, Sleeper sleeper) {
    this.requestsPerSecond = requestsPerSecond;
    this.burst = burst;
    this.clockMillis = clockMillis;
    this.sleeper = sleeper;
    this.tokens = burst;
    this.refilledAt = clockMillis.getAsLong();
  }

  /** A limiter that never waits. */
  public static RequestRateLimiter unlimited() {
    return new RequestRateLimiter(0d, 0, () -> 0L, millis -> { });
  }

  /**
   * The limiter of {@code spec}'s binding: unlimited when its rate key is unset.
   *
   * @throws EmbeddingClientConfigException naming the key: a rate that is not a finite number above 0, a burst that is not an
   *     integer of 1 or more, or a burst set without a rate
   */
  public static RequestRateLimiter from(EmbeddingModelSpec spec, TypedProperties props, LongSupplier clockMillis,
                                        Sleeper sleeper) {
    Settings settings = settings(spec, props);
    return settings == null ? unlimited() : new RequestRateLimiter(settings.requestsPerSecond, settings.burst, clockMillis, sleeper);
  }

  /**
   * Checks the binding's rate-limit keys, so a bad value fails the run at start, before any provider call.
   *
   * @throws EmbeddingClientConfigException naming the key: a rate that is not a finite number above 0, a burst that is not an
   *     integer of 1 or more, or a burst set without a rate
   */
  public static void validate(EmbeddingModelSpec spec, TypedProperties props) {
    settings(spec, props);
  }

  /** Whether a rate is set; an unlimited limiter never waits. */
  public boolean isLimited() {
    return burst > 0;
  }

  /** The configured steady rate in requests per second, 0 when unlimited. */
  public double requestsPerSecond() {
    return requestsPerSecond;
  }

  /** The bucket capacity, 0 when unlimited. */
  public int burst() {
    return burst;
  }

  /**
   * Takes one token, waiting until the bucket holds one.
   *
   * @return the milliseconds waited for the token, 0 when one was available (always 0 when unlimited)
   * @throws InterruptedException when interrupted while waiting; no token is taken then
   */
  public long acquire() throws InterruptedException {
    return acquire(Long.MAX_VALUE);
  }

  /**
   * Takes one token, waiting at most {@code maxWaitMillis}. If the budget expires, returns without taking a token;
   * the caller must check its call deadline before sending, including after a delayed wakeup.
   *
   * @return the milliseconds waited, including a wait that exhausted the budget
   * @throws InterruptedException when interrupted while waiting; no token is taken then
   */
  public long acquire(long maxWaitMillis) throws InterruptedException {
    if (!isLimited()) {
      return 0L;
    }
    long start = clockMillis.getAsLong();
    while (true) {
      long waited = Math.max(0L, clockMillis.getAsLong() - start);
      if (waited >= maxWaitMillis) {
        return waited;
      }
      refill();
      if (tokens >= 1d - EPSILON) {
        tokens = Math.max(0d, tokens - 1d);
        return Math.max(0L, clockMillis.getAsLong() - start);
      }
      long tokenWait = Math.max(1L, (long) Math.ceil((1d - tokens) * MILLIS_PER_SECOND / requestsPerSecond - EPSILON));
      sleeper.sleep(Math.min(tokenWait, maxWaitMillis - waited));
    }
  }

  private void refill() {
    long now = clockMillis.getAsLong();
    if (now > refilledAt) {
      tokens = Math.min(burst, tokens + (now - refilledAt) * requestsPerSecond / MILLIS_PER_SECOND);
    }
    // a clock stepped back restarts the refill from now instead of freezing it until the old time comes round again
    refilledAt = now;
  }

  @Override
  public String toString() {
    return isLimited() ? "RequestRateLimiter{requestsPerSecond=" + requestsPerSecond() + ", burst=" + burst + "}"
        : "RequestRateLimiter{unlimited}";
  }

  /** The validated settings, or null when no rate is set. */
  private static Settings settings(EmbeddingModelSpec spec, TypedProperties props) {
    String rateKey = EmbeddingTableConfig.MODEL_PREFIX + spec.getName() + RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX;
    String burstKey = EmbeddingTableConfig.MODEL_PREFIX + spec.getName() + RATE_LIMIT_BURST_SUFFIX;
    String rawRate = trimmed(props, rateKey);
    String rawBurst = trimmed(props, burstKey);
    if (rawRate == null) {
      if (rawBurst != null) {
        throw new EmbeddingClientConfigException("Config " + burstKey + " is set but " + rateKey
            + " is not: a burst needs a rate");
      }
      return null;
    }
    double rate;
    try {
      // a plain decimal only: no NaN, Infinity, hex or type suffix, which Double.parseDouble would take
      rate = new BigDecimal(rawRate).doubleValue();
    } catch (NumberFormatException e) {
      throw new EmbeddingClientConfigException("Config " + rateKey + "=" + rawRate + " is not a number");
    }
    if (Double.isNaN(rate) || rate <= 0d || Double.isInfinite(rate)) {
      throw new EmbeddingClientConfigException("Config " + rateKey + "=" + rawRate + " must be a number > 0");
    }
    int burst = DEFAULT_BURST;
    if (rawBurst != null) {
      try {
        burst = Integer.parseInt(rawBurst);
      } catch (NumberFormatException e) {
        throw new EmbeddingClientConfigException("Config " + burstKey + "=" + rawBurst + " is not an integer");
      }
      if (burst < 1) {
        throw new EmbeddingClientConfigException("Config " + burstKey + "=" + rawBurst + " must be >= 1");
      }
    }
    return new Settings(rate, burst);
  }

  private static String trimmed(TypedProperties props, String key) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), null);
    return value == null || value.trim().isEmpty() ? null : value.trim();
  }

  private static final class Settings {
    private final double requestsPerSecond;
    private final int burst;

    private Settings(double requestsPerSecond, int burst) {
      this.requestsPerSecond = requestsPerSecond;
      this.burst = burst;
    }
  }
}
