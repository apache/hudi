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
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop.Answer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link ProviderRetryLoop} on a fake clock: retries, hints, deadline, interrupt and outage. */
public class TestProviderRetryLoop {

  private static final String ALIAS = "m";
  private final AtomicLong now = new AtomicLong(10_000L);
  private final List<Long> sleeps = new ArrayList<>();

  @Test
  public void transientAnswersAreRetriedThenTheLastErrorIsReported() {
    ProviderRetryLoop loop = loop(props("3", "100000", "60000", "5"));
    loop.startCall();
    AtomicInteger attempts = new AtomicInteger();
    Answer<String> answer = loop.send(() -> {
      attempts.incrementAndGet();
      return Answer.failed(ProviderResponseKind.RETRYABLE, "busy " + attempts.get(), -1);
    });
    assertEquals(ProviderResponseKind.RETRYABLE, answer.kind());
    assertEquals("busy 3 (after 3 attempts)", answer.error());
    assertEquals(3, attempts.get());
    assertEquals(2, sleeps.size(), "a backoff before each retry");
    assertEquals(3, loop.stats().getRequests());
    assertEquals(2, loop.stats().getRetries());
  }

  @Test
  public void providerHintIsWaitedAndAHintOverTheMaxBackoffEndsTheRequest() {
    ProviderRetryLoop loop = loop(props("5", "100000", "4000", "5"));
    loop.startCall();
    List<Answer<String>> script = new ArrayList<>(Arrays.asList(
        Answer.failed(ProviderResponseKind.RETRYABLE, "slow down", 3000), Answer.ok("v")));
    assertEquals("v", loop.send(() -> script.remove(0)).value());
    assertEquals(Arrays.asList(3000L), sleeps);

    AtomicInteger attempts = new AtomicInteger();
    Answer<String> tooLong = loop.send(() -> {
      attempts.incrementAndGet();
      return Answer.failed(ProviderResponseKind.RETRYABLE, "come back later", 5000);
    });
    assertEquals(1, attempts.get());
    assertTrue(tooLong.error().contains("provider asks to wait 5000 ms, over the 4000 ms max backoff"), tooLong.error());
  }

  @Test
  public void retryThatWouldPassTheDeadlineIsNotWaitedFor() {
    ProviderRetryLoop loop = loop(props("5", "2000", "60000", "5"));
    loop.startCall();
    Answer<String> answer = loop.send(() -> Answer.failed(ProviderResponseKind.RETRYABLE, "busy", 2500));
    assertTrue(answer.error().contains(ProviderRetryLoop.DEADLINE_EXCEEDED + " before the next attempt"), answer.error());
    assertTrue(sleeps.isEmpty());
    now.addAndGet(2000);
    assertTrue(loop.notSentReason().startsWith(ProviderRetryLoop.DEADLINE_EXCEEDED), loop.notSentReason());
  }

  @Test
  public void nonTransientAnswersAreNotRetried() {
    ProviderRetryLoop loop = loop(props("5", "100000", "60000", "5"));
    loop.startCall();
    for (ProviderResponseKind kind : new ProviderResponseKind[] {ProviderResponseKind.INPUT, ProviderResponseKind.CONFIG}) {
      AtomicInteger attempts = new AtomicInteger();
      assertEquals(kind, loop.send(() -> {
        attempts.incrementAndGet();
        return Answer.<String>failed(kind, "no", -1);
      }).kind());
      assertEquals(1, attempts.get());
    }
    assertEquals(0, loop.consecutiveFailures(), "input and configuration answers are not provider outages");
  }

  @Test
  public void interruptStopsTheCallWithoutCountingAnOutage() {
    ProviderRetryLoop loop = loop(props("5", "100000", "60000", "1"));
    loop.startCall();
    Answer<String> answer;
    try {
      answer = loop.send(() -> {
        throw new InterruptedException("killed");
      });
    } finally {
      assertTrue(Thread.interrupted(), "the interrupt is kept for the caller");
    }
    assertTrue(answer.isInterrupted());
    assertEquals(ProviderResponseKind.RETRYABLE, answer.kind());
    assertFalse(loop.outageOpen());
    assertTrue(loop.notSentReason().startsWith(ProviderRetryLoop.INTERRUPTED), loop.notSentReason());
  }

  @Test
  public void consecutiveFailedRequestsOpenTheOutageAndASuccessResetsTheCount() {
    ProviderRetryLoop loop = loop(props("1", "100000", "60000", "2"));
    loop.startCall();
    loop.send(() -> Answer.failed(ProviderResponseKind.RETRYABLE, "down", -1));
    loop.send(() -> Answer.ok("v"));
    assertEquals(0, loop.consecutiveFailures());
    loop.send(() -> Answer.failed(ProviderResponseKind.RETRYABLE, "down", -1));
    assertFalse(loop.outageOpen());
    loop.send(() -> Answer.failed(ProviderResponseKind.RETRYABLE, "down", -1));
    assertTrue(loop.outageOpen());
    assertTrue(loop.notSentReason().startsWith(ProviderRetryLoop.OUTAGE), loop.notSentReason());
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.1", "1"})
  public void rateWaitStopsAtDeadlineWithoutSending(String rate) {
    TypedProperties properties = props("3", "1000", "60000", "1");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX, rate);
    ProviderRetryLoop loop = loop(properties);
    loop.startCall();
    AtomicInteger sent = new AtomicInteger();
    assertEquals("first", loop.send(() -> {
      sent.incrementAndGet();
      return Answer.ok("first");
    }).value());
    Answer<String> answer = loop.send(() -> {
      sent.incrementAndGet();
      return Answer.ok("late");
    });
    assertEquals(ProviderResponseKind.RETRYABLE, answer.kind());
    assertTrue(answer.error().contains(ProviderRetryLoop.DEADLINE_EXCEEDED), answer.error());
    assertEquals(1, sent.get());
    assertEquals(11_000L, now.get(), "the token wait consumes only the remaining call budget");
    assertEquals(Arrays.asList(1000L), sleeps);
    assertEquals(1, loop.stats().getRequests());
    assertEquals(0, loop.stats().getRetries());
    assertEquals(1000L, loop.stats().getRateLimitWaitMillis());
    assertEquals(0, loop.consecutiveFailures(), "an unsent request is not a provider outage");
    assertFalse(loop.outageOpen());
    now.addAndGet(9000L);
    loop.startCall();
    assertEquals("next call", loop.send(() -> Answer.ok("next call")).value());
  }

  @Test
  public void retryRateWaitDoesNotSendAfterDeadline() {
    TypedProperties properties = props("3", "1000", "60000", "5");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX, "0.1");
    ProviderRetryLoop loop = loop(properties);
    loop.startCall();
    AtomicInteger sent = new AtomicInteger();
    Answer<String> answer = loop.send(() -> {
      sent.incrementAndGet();
      return Answer.failed(ProviderResponseKind.RETRYABLE, "busy", 0);
    });
    assertTrue(answer.error().contains(ProviderRetryLoop.DEADLINE_EXCEEDED), answer.error());
    assertEquals(1, sent.get());
    assertEquals(1000L, loop.stats().getRateLimitWaitMillis());
    assertEquals(0, loop.stats().getRetries());
  }

  @Test
  public void alreadyExpiredCallDoesNotSend() {
    ProviderRetryLoop loop = loop(props("3", "1000", "60000", "5"));
    loop.startCall();
    now.addAndGet(1000L);
    AtomicInteger sent = new AtomicInteger();
    Answer<String> answer = loop.send(() -> {
      sent.incrementAndGet();
      return Answer.ok("late");
    });
    assertEquals(ProviderResponseKind.RETRYABLE, answer.kind());
    assertEquals(0, sent.get());
    assertEquals(0, loop.stats().getRequests());
  }

  @Test
  public void delayedWakeupDoesNotSendAtDeadline() {
    TypedProperties properties = props("3", "1000", "60000", "5");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX, "2");
    EmbeddingModelSpec spec = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, 8, null, "jina", "x");
    ProviderRetryLoop loop = new ProviderRetryLoop("test", ProviderClientSettings.from(spec, properties),
        RequestRateLimiter.from(spec, properties, now::get, millis -> now.addAndGet(millis + 500L)),
        now::get, now::addAndGet, new Random(3));
    loop.startCall();
    assertEquals("first", loop.send(() -> Answer.ok("first")).value());
    AtomicInteger lateSends = new AtomicInteger();
    Answer<String> answer = loop.send(() -> {
      lateSends.incrementAndGet();
      return Answer.ok("late");
    });
    assertEquals(ProviderResponseKind.RETRYABLE, answer.kind());
    assertEquals(0, lateSends.get());
    assertEquals(1, loop.stats().getRequests());
  }

  private ProviderRetryLoop loop(TypedProperties props) {
    EmbeddingModelSpec spec = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, 8, null, "jina", "x");
    ProviderRetryLoop.Sleeper sleeper = millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    };
    return new ProviderRetryLoop("test", ProviderClientSettings.from(spec, props),
        RequestRateLimiter.from(spec, props, now::get, sleeper::sleep), now::get, sleeper, new Random(3));
  }

  private static TypedProperties props(String attempts, String deadlineMs, String maxBackoffMs, String outage) {
    TypedProperties props = new TypedProperties();
    String prefix = EmbeddingTableConfig.MODEL_PREFIX + ALIAS;
    props.setProperty(prefix + ProviderClientSettings.RETRY_MAX_ATTEMPTS_SUFFIX, attempts);
    props.setProperty(prefix + ProviderClientSettings.REQUEST_DEADLINE_MS_SUFFIX, deadlineMs);
    props.setProperty(prefix + ProviderClientSettings.RETRY_MAX_BACKOFF_MS_SUFFIX, maxBackoffMs);
    props.setProperty(prefix + ProviderClientSettings.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX, outage);
    return props;
  }
}
