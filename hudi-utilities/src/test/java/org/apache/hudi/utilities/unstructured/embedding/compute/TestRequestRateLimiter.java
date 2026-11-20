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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RequestRateLimiter} on a fake clock: a sleep only advances it, so every wait is exact and nothing sleeps.
 */
public class TestRequestRateLimiter {

  private static final String ALIAS = "m";
  private static final String RATE_KEY = EmbeddingTableConfig.MODEL_PREFIX + ALIAS
      + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX;
  private static final String BURST_KEY = EmbeddingTableConfig.MODEL_PREFIX + ALIAS + RequestRateLimiter.RATE_LIMIT_BURST_SUFFIX;

  private final AtomicLong now = new AtomicLong(5_000L);
  private final List<Long> sleeps = new ArrayList<>();

  /**
   * After being idle a task sends up to B requests at once, then one every 1/N s; with no burst set B is 1, so the second
   * request already waits a full interval.
   */
  @Test
  public void testBurstAfterIdleThenSteadyRate() throws InterruptedException {
    RequestRateLimiter limiter = limiter(props("4", "3"));
    // idle start: the bucket is full, three tokens go at once
    assertEquals(Arrays.asList(0L, 0L, 0L), acquire(limiter, 3));
    assertEquals(5_000L, now.get(), "no wait for the burst");
    // then 4/s: one token every 250 ms
    assertEquals(Arrays.asList(250L, 250L, 250L), acquire(limiter, 3));
    // idle for 10 s: the bucket refills to B, never above
    now.addAndGet(10_000L);
    assertEquals(Arrays.asList(0L, 0L, 0L, 250L), acquire(limiter, 4));

    sleeps.clear();
    RequestRateLimiter noBurst = limiter(props("4", null));
    assertEquals(RequestRateLimiter.DEFAULT_BURST, noBurst.burst());
    assertEquals(Arrays.asList(0L, 250L, 250L), acquire(noBurst, 3), "B defaults to 1: no burst above the steady rate");
  }

  /** A fractional rate: 0.5/s is one request every 2 s. */
  @Test
  public void testFractionalRate() throws InterruptedException {
    RequestRateLimiter limiter = limiter(props("0.5", null));
    assertEquals(Arrays.asList(0L, 2_000L, 2_000L), acquire(limiter, 3));
    assertEquals(0.5d, limiter.requestsPerSecond(), 0d);
  }

  /** A partial refill counts: a token half refilled waits only the other half. */
  @Test
  public void testPartialRefillWaitsTheRemainder() throws InterruptedException {
    RequestRateLimiter limiter = limiter(props("10", null));
    assertEquals(0L, limiter.acquire());
    now.addAndGet(40L);
    assertEquals(60L, limiter.acquire(), "100 ms per token, 40 ms already passed");
  }

  /** Without a rate the limiter never waits, never reads the clock's progress, never sleeps. */
  @Test
  public void testNoRateIsUnlimited() throws InterruptedException {
    RequestRateLimiter limiter = limiter(new TypedProperties());
    assertFalse(limiter.isLimited());
    for (int i = 0; i < 1000; i++) {
      assertEquals(0L, limiter.acquire());
    }
    assertTrue(sleeps.isEmpty());
    assertEquals(5_000L, now.get());
  }

  /** A bad rate or burst is refused with an {@link EmbeddingClientConfigException} naming the key. */
  @ParameterizedTest
  @CsvSource(value = {
      "0, NULL, rate",
      "-1, NULL, rate",
      "0.0, NULL, rate",
      "abc, NULL, rate",
      "NaN, NULL, rate",
      "Infinity, NULL, rate",
      "5d, NULL, rate",
      "5, 0, burst",
      "5, -2, burst",
      "5, 1.5, burst",
      "5, x, burst",
      "NULL, 3, burst"}, nullValues = "NULL")
  public void testBadConfigRefusedNamingTheKey(String rate, String burst, String badKey) {
    TypedProperties props = props(rate, burst);
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class,
        () -> RequestRateLimiter.validate(spec(), props));
    String key = "rate".equals(badKey) ? RATE_KEY : BURST_KEY;
    assertTrue(thrown.getMessage().contains(key), thrown.getMessage());
    assertThrows(EmbeddingClientConfigException.class, () -> limiter(props));
  }

  @Test
  public void testValidConfigAccepted() {
    RequestRateLimiter.validate(spec(), props("2.5", "10"));
    RequestRateLimiter.validate(spec(), props(" 7 ", null));
    RequestRateLimiter.validate(spec(), new TypedProperties());
    RequestRateLimiter limiter = limiter(props("2.5", "10"));
    assertEquals(2.5d, limiter.requestsPerSecond(), 0d);
    assertEquals(10, limiter.burst());
  }

  /** An interrupt during the wait takes no token: the next acquire still finds the bucket where it was. */
  @Test
  public void testInterruptedWaitTakesNoToken() throws InterruptedException {
    boolean[] interrupt = {false};
    RequestRateLimiter limiter = RequestRateLimiter.from(spec(), props("1", null), now::get, millis -> {
      if (interrupt[0]) {
        throw new InterruptedException("test");
      }
      now.addAndGet(millis);
    });
    assertEquals(0L, limiter.acquire());
    interrupt[0] = true;
    assertThrows(InterruptedException.class, limiter::acquire);
    interrupt[0] = false;
    assertEquals(1_000L, limiter.acquire());
  }

  /** A clock stepped back does not freeze the refill: the next token comes one interval after the step, not after the old time. */
  @Test
  public void testClockSteppedBackKeepsRefilling() throws InterruptedException {
    RequestRateLimiter limiter = limiter(props("2", null));
    assertEquals(0L, limiter.acquire());
    now.addAndGet(-60_000L);
    assertEquals(500L, limiter.acquire(), "one 500 ms interval, not 60 s");
  }

  private List<Long> acquire(RequestRateLimiter limiter, int count) throws InterruptedException {
    List<Long> waits = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      waits.add(limiter.acquire());
    }
    return waits;
  }

  private RequestRateLimiter limiter(TypedProperties props) {
    return RequestRateLimiter.from(spec(), props, now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    });
  }

  private static TypedProperties props(String rate, String burst) {
    TypedProperties props = new TypedProperties();
    if (rate != null) {
      props.setProperty(RATE_KEY, rate);
    }
    if (burst != null) {
      props.setProperty(BURST_KEY, burst);
    }
    return props;
  }

  private static EmbeddingModelSpec spec() {
    return new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, 4, null, "openai", "text-embedding-3-small");
  }
}
