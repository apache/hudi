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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.embeddings;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.recorded;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.vectorFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The request rate limit ({@link RequestRateLimiter}) at the send point of {@link OpenAiCompatibleEmbeddingClient}, against a
 * counting {@link StubEmbeddingServer} that records each request's arrival on the test's fake clock. A sleep only advances that
 * clock, so pacing is exact and nothing sleeps; no assertion reads the wall clock. One client = one compute task.
 */
class TestOpenAiClientRateLimits {

  private static final String ALIAS = "oai";
  private static final String MODEL = "text-embedding-3-small";
  private static final int DIM = 4;
  private static final String KEY_ENV = "TEST_OPENAI_KEY";
  private static final String PATH = "/v1/embeddings";
  private static final long WINDOW_MS = 1_000L;

  private StubEmbeddingServer server;
  private final AtomicLong now = new AtomicLong(1_000_000L);
  private final List<Long> sleeps = new ArrayList<>();
  /** Arrival of every embeddings request on the fake clock, in order. */
  private final List<Long> arrivals = Collections.synchronizedList(new ArrayList<>());
  /** The stats of the last {@link #throttledRun}. */
  private EmbeddingCallStats lastStats;

  @BeforeEach
  void setUp() throws IOException {
    server = new StubEmbeddingServer();
  }

  @AfterEach
  void tearDown() {
    server.close();
  }

  /**
   * With N requests/s (burst B), a task embedding more than one second's worth of requests sends no more than N + B − 1 in
   * any half-open one-second window (for B = 1: N), and it does reach that bound, so the pacing is real, not incidental.
   */
  @ParameterizedTest
  @CsvSource({"5, 1, 20", "2, 3, 12", "0.5, 1, 4", "20, 1, 50"})
  void testConfiguredRateIsRespected(String rate, int burst, int requests) {
    server.otherwise(PATH, counting(embeddings(DIM)));
    TypedProperties props = oneInputPerRequest();
    limit(props, rate, burst);
    OpenAiCompatibleEmbeddingClient client = client(props);
    List<String> texts = texts(requests);
    assertOk(client.embed(inputs(texts)), texts);

    assertEquals(requests, arrivals.size(), "one request per input");
    int bound = maxPerWindow(Double.parseDouble(rate), burst);
    assertEquals(bound, maxInAnyWindow(arrivals), "the window bound is reached and never passed: " + arrivals);
    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(requests, stats.getRequests());
    long expectedWait = arrivals.get(arrivals.size() - 1) - arrivals.get(0);
    assertEquals(expectedWait, stats.getRateLimitWaitMillis(), "the only time that passed is the token wait");
    assertTrue(stats.getRateLimitWaitMillis() > 0, stats.toString());
  }

  /**
   * A request retried after a 429 (the provider asks for no wait, {@code retry-after-ms: 0}), after a 5xx, and the
   * requests of a bisection each take a token: at 1 request/s every request, first try or not, arrives at least 1 s after the
   * one before. Without a token per retry, a retry would go out in the same millisecond as the 429.
   */
  @Test
  void testRetriesAndBisectionConsumeTokens() {
    AtomicInteger seen = new AtomicInteger();
    server.otherwise(PATH, counting(request -> {
      int n = seen.incrementAndGet();
      if (n <= 3) {
        return recorded(429, "error-429-rate.json").withHeader("retry-after-ms", "0");
      }
      if (n == 4) {
        return recorded(503, "error-503.json").withHeader("retry-after-ms", "0");
      }
      return request.inputs().stream().anyMatch(text -> text.contains("BAD"))
          ? recorded(400, "error-400-input.json") : embeddings(DIM).apply(request);
    }));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "4");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "6");
    limit(props, "1", 1);
    OpenAiCompatibleEmbeddingClient client = client(props);
    List<EmbeddingResult> results = client.embed(inputs(listOf("t0", "t1", "BAD", "t3")));

    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(2).getStatus());
    List<String> texts = listOf("t0", "t1", "BAD", "t3");
    for (int i : new int[] {0, 1, 3}) {
      assertEquals(EmbeddingResult.Status.OK, results.get(i).getStatus(), results.get(i).toString());
      assertArrayEquals(vectorFor(texts.get(i), DIM), results.get(i).getVector(), texts.get(i));
    }
    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(4, stats.getRetries(), "three 429s and one 503 retried: " + stats);
    assertTrue(stats.getBisectRequests() > 0, stats.toString());
    assertEquals(stats.getRequests(), arrivals.size());
    assertEquals(1, maxInAnyWindow(arrivals), "every request took a token: " + arrivals);
    for (int i = 1; i < arrivals.size(); i++) {
      assertTrue(arrivals.get(i) - arrivals.get(i - 1) >= WINDOW_MS, "request " + i + " paced: " + arrivals);
    }
    assertEquals((stats.getRequests() - 1) * WINDOW_MS, stats.getRateLimitWaitMillis(),
        "with zero provider waits, the call's whole wait is the limiter's");
    assertTrue(sleeps.stream().allMatch(sleep -> sleep == 0L || sleep == WINDOW_MS),
        "the provider asked for no wait (0 ms backoff sleeps); every other sleep is a 1 s token wait: " + sleeps);
  }

  /**
   * With no rate-limit key the client does not pace: requests, results and stats match a client without a limiter, no sleep,
   * no wait, and every request goes out at the same fake instant.
   */
  @Test
  void testNoSettingDoesNotPace() {
    server.otherwise(PATH, counting(embeddings(DIM)));
    OpenAiCompatibleEmbeddingClient client = client(oneInputPerRequest());
    List<String> texts = texts(25);
    assertOk(client.embed(inputs(texts)), texts);
    assertEquals(25, arrivals.size());
    assertEquals(Collections.singleton(1_000_000L), new HashSet<>(arrivals), "no request waited");
    assertEquals(Collections.emptyList(), sleeps);
    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(25, stats.getRequests());
    assertEquals(0, stats.getRetries());
    assertEquals(0L, stats.getRateLimitWaitMillis());
    assertEquals(0L, stats.getWallMillis());
    assertTrue(client.toString().contains("unlimited"), client.toString());
  }

  /**
   * A limit set above the provider's quota changes nothing in how a 429 is handled: the {@code Retry-After} hint is slept
   * exactly as without a limit, the classification and retry count are the same, and the limiter adds no wait of its own
   * because its bucket still holds tokens.
   */
  @Test
  void testProviderThrottlingHandledUnderALimit() {
    List<Long> unlimitedSleeps = throttledRun(new TypedProperties());
    EmbeddingCallStats unlimited = lastStats;
    sleeps.clear();
    arrivals.clear();
    TypedProperties props = new TypedProperties();
    limit(props, "100", 10);
    List<Long> limitedSleeps = throttledRun(props);
    EmbeddingCallStats limited = lastStats;

    assertEquals(Collections.singletonList(2000L), unlimitedSleeps, "the Retry-After hint");
    assertEquals(unlimitedSleeps, limitedSleeps, "the same backoff with the limit set");
    assertEquals(unlimited.getRequests(), limited.getRequests());
    assertEquals(unlimited.getRetries(), limited.getRetries());
    assertEquals(0L, limited.getRateLimitWaitMillis(), "a bucket with tokens never waits");

    // a binding limit (1/s, burst 1): the 2 s Retry-After sleep already refilled the bucket, so no token wait is added to it
    sleeps.clear();
    arrivals.clear();
    TypedProperties binding = new TypedProperties();
    limit(binding, "1", 1);
    assertEquals(Collections.singletonList(2000L), throttledRun(binding), "only the provider's wait");
    assertEquals(0L, lastStats.getRateLimitWaitMillis());
    assertEquals(unlimited.getRetries(), lastStats.getRetries());
  }

  /** A bad rate or burst fails {@code init} with the key named, before any provider call (TEI's {@code GET /info} included). */
  @ParameterizedTest
  @CsvSource(value = {"0, NULL, rate", "-3, NULL, rate", "fast, NULL, rate", "5, 0, burst", "NULL, 2, burst"},
      nullValues = "NULL")
  void testBadConfigFailsInitBeforeAnyCall(String rate, String burst, String badKey) {
    server.otherwise("/info", request -> recorded(200, "tei-info.json"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), server.url() + "/v1");
    if (rate != null) {
      props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), rate);
    }
    if (burst != null) {
      props.setProperty(key(RequestRateLimiter.RATE_LIMIT_BURST_SUFFIX), burst);
    }
    OpenAiCompatibleEmbeddingClient client = newClient();
    EmbeddingModelSpec spec = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "hf-tei", "bge");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.init(spec, props));
    String badSuffix = "burst".equals(badKey)
        ? RequestRateLimiter.RATE_LIMIT_BURST_SUFFIX : RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX;
    assertTrue(thrown.getMessage().contains(key(badSuffix)), thrown.getMessage());
    assertEquals(0, server.requests().size(), "no provider call");
  }

  @ParameterizedTest
  @ValueSource(strings = {"0.1", "1"})
  void rateWaitStopsAtDeadlineWithoutSending(String rate) {
    server.otherwise(PATH, counting(embeddings(DIM)));
    TypedProperties props = oneInputPerRequest();
    props.setProperty(key(OpenAiClientConfig.REQUEST_DEADLINE_MS_SUFFIX), "1000");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "1");
    limit(props, rate, 1);
    OpenAiCompatibleEmbeddingClient client = client(props);
    List<EmbeddingResult> results = client.embed(inputs(listOf("first", "late", "unsent")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertArrayEquals(vectorFor("first", DIM), results.get(0).getVector());
    for (int i = 1; i < results.size(); i++) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(i).getStatus());
      assertTrue(results.get(i).getError().contains(OpenAiCompatibleEmbeddingClient.DEADLINE_EXCEEDED), results.get(i).getError());
    }
    assertEquals(Collections.singletonList(1_000_000L), arrivals);
    assertEquals(Collections.singletonList(1000L), sleeps);
    assertEquals(1000L, client.lastCallStats().getRateLimitWaitMillis());
    assertEquals(1000L, client.lastCallStats().getWallMillis());
    assertEquals(1, client.lastCallStats().getRequests());
    assertEquals(0, client.lastCallStats().getRetries());
    now.addAndGet(9000L);
    assertOk(client.embed(inputs(listOf("next call"))), listOf("next call"));
  }

  @Test
  void retryRateWaitDoesNotSendAfterDeadline() {
    server.otherwise(PATH, counting(request -> recorded(429, "error-429-rate.json").withHeader("retry-after-ms", "0")));
    TypedProperties props = oneInputPerRequest();
    props.setProperty(key(OpenAiClientConfig.REQUEST_DEADLINE_MS_SUFFIX), "1000");
    limit(props, "0.1", 1);
    OpenAiCompatibleEmbeddingClient client = client(props);
    EmbeddingResult result = client.embed(inputs(listOf("retry"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().contains(OpenAiCompatibleEmbeddingClient.DEADLINE_EXCEEDED), result.getError());
    assertEquals(Collections.singletonList(1_000_000L), arrivals);
    assertEquals(1000L, client.lastCallStats().getRateLimitWaitMillis());
    assertEquals(1, client.lastCallStats().getRequests());
    assertEquals(0, client.lastCallStats().getRetries());
  }

  // ---------------------------------------------------------------- helpers

  /** A 429 with {@code Retry-After: 2}, then success; returns the sleeps. */
  private List<Long> throttledRun(TypedProperties props) {
    try (StubEmbeddingServer throttling = new StubEmbeddingServer()) {
      throttling.then(PATH, counting(request -> recorded(429, "error-429-rate.json").withHeader("Retry-After", "2")))
          .otherwise(PATH, counting(embeddings(DIM)));
      props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), throttling.url() + "/v1");
      OpenAiCompatibleEmbeddingClient client = client(props);
      assertOk(client.embed(inputs(listOf("a", "b"))), listOf("a", "b"));
      lastStats = client.lastCallStats();
      return new ArrayList<>(sleeps);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The most requests of {@code times} in any half-open window [t, t + 1 s). */
  static int maxInAnyWindow(List<Long> times) {
    List<Long> sorted = new ArrayList<>(times);
    sorted.sort(null);
    int max = 0;
    int start = 0;
    for (int end = 0; end < sorted.size(); end++) {
      while (sorted.get(end) - sorted.get(start) >= WINDOW_MS) {
        start++;
      }
      max = Math.max(max, end - start + 1);
    }
    return max;
  }

  /**
   * The most requests a bucket of {@code burst} refilled at {@code rate}/s lets out in a half-open one-second window: the tokens
   * held at its start (at most B) plus those refilled before its end (fewer than N), so ceil(B + N) − 1.
   */
  static int maxPerWindow(double rate, int burst) {
    return (int) Math.ceil(burst + rate) - 1;
  }

  private StubEmbeddingServer.Responder counting(StubEmbeddingServer.Responder responder) {
    return request -> {
      arrivals.add(now.get());
      return responder.apply(request);
    };
  }

  private static void limit(TypedProperties props, String rate, int burst) {
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), rate);
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_BURST_SUFFIX), String.valueOf(burst));
  }

  private static TypedProperties oneInputPerRequest() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    return props;
  }

  private OpenAiCompatibleEmbeddingClient newClient() {
    return new OpenAiCompatibleEmbeddingClient(new JdkHttpTransport(), now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    }, new Random(7), name -> KEY_ENV.equals(name) ? "sk-test-RATELIMIT0123456789" : null);
  }

  private OpenAiCompatibleEmbeddingClient client(TypedProperties extra) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), server.url() + "/v1");
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), KEY_ENV);
    props.putAll(extra);
    OpenAiCompatibleEmbeddingClient client = newClient();
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "openai", MODEL), props);
    return client;
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }

  private static List<String> texts(int count) {
    List<String> texts = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      texts.add("text " + i);
    }
    return texts;
  }

  private static List<String> listOf(String... texts) {
    List<String> list = new ArrayList<>();
    Collections.addAll(list, texts);
    return list;
  }

  private static List<EmbeddingInput> inputs(List<String> texts) {
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < texts.size(); i++) {
      inputs.add(new EmbeddingInput("fid-" + i, i, EmbeddingTableSchema.MODALITY_TEXT, texts.get(i)));
    }
    return inputs;
  }

  private static void assertOk(List<EmbeddingResult> results, List<String> texts) {
    assertEquals(texts.size(), results.size());
    for (int i = 0; i < texts.size(); i++) {
      assertEquals(EmbeddingResult.Status.OK, results.get(i).getStatus(), texts.get(i) + ": " + results.get(i));
      assertArrayEquals(vectorFor(texts.get(i), DIM), results.get(i).getVector(), texts.get(i));
    }
  }
}
