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
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.Recorded;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.embeddings;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.json;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.recorded;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.vectorFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link OpenAiCompatibleEmbeddingClient} against {@link StubEmbeddingServer}, which replays the documented OpenAI,
 * Azure OpenAI and TEI response shapes (no live endpoint is called). The clock is fake and a sleep
 * only advances it, so backoff and deadlines are exact and instant.
 */
class TestOpenAiCompatibleEmbeddingClient {

  private static final String ALIAS = "oai";
  private static final String MODEL = "text-embedding-3-small";
  private static final int DIM = 4;
  private static final String KEY = "sk-test-ABCDEFGHIJKLMNOP1234";
  private static final String KEY_ENV = "TEST_OPENAI_KEY";
  private static final String PATH = "/v1/embeddings";
  /** Set (-Dopenai.client.log.dir=...) to write the captured client log there. */
  private static final String CLIENT_LOG_DIR = "openai.client.log.dir";

  private StubEmbeddingServer server;
  private final AtomicLong now = new AtomicLong(1_000_000L);
  private final List<Long> sleeps = new ArrayList<>();
  private final Map<String, String> environment = new HashMap<>();
  private CapturingAppender logs;
  private Logger clientLogger;
  private Level previousLevel;

  @BeforeEach
  void setUp() throws IOException {
    server = new StubEmbeddingServer();
    environment.put(KEY_ENV, KEY);
    logs = new CapturingAppender();
    clientLogger = (Logger) LogManager.getLogger(OpenAiCompatibleEmbeddingClient.class);
    previousLevel = clientLogger.getLevel();
    // the appender first: adding it rebuilds the logger from the active config, which would reset an earlier level
    clientLogger.addAppender(logs);
    clientLogger.setLevel(Level.ALL);
  }

  @AfterEach
  void tearDown() {
    clientLogger.removeAppender(logs);
    clientLogger.setLevel(previousLevel);
    logs.stop();
    server.close();
  }

  // ---------------------------------------------------------------- success, ordering, stats

  @Test
  void testSuccessfulBatch() {
    server.otherwise(PATH, embeddings(DIM));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingResult> results = client.embed(inputs("alpha", "beta", "gamma"));

    assertEquals(1, server.posts().size());
    Recorded request = server.posts().get(0);
    assertEquals("Bearer " + KEY, request.header("Authorization"));
    assertEquals(MODEL, request.json().get("model").asText());
    assertEquals(Arrays.asList("alpha", "beta", "gamma"), request.inputs());
    assertEquals("float", request.json().get("encoding_format").asText());
    assertEquals(DIM, request.json().get("dimensions").asInt(), "3-small at 4 dimensions sends the parameter");
    assertOk(results, "alpha", "beta", "gamma");

    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(1, stats.getRequests());
    assertEquals(0, stats.getRetries());
    assertEquals(0, stats.getBisectRequests());
    assertEquals(9, stats.getProviderPromptTokens());
  }

  @Test
  void testRecordedResponseShapeMapsByIndex() {
    server.otherwise(PATH, request -> recorded(200, "embeddings-response.json"));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("first", "second"));
    // the recording lists index 1 before index 0
    assertArrayEquals(new float[] {0.0023064255f, -0.009327292f, 0.015797347f, -0.0077780345f}, results.get(0).getVector());
    assertArrayEquals(new float[] {0.5f, -0.5f, 0.25f, -0.25f}, results.get(1).getVector());
  }

  @Test
  void testResultsMapByIndexNotPosition() {
    // embeddings() answers in reversed index order; every vector must still land on its own input
    server.otherwise(PATH, embeddings(DIM));
    assertOk(client(new TypedProperties()).embed(inputs("a1", "b2", "c3", "d4", "e5")), "a1", "b2", "c3", "d4", "e5");
  }

  @Test
  void duplicateResponseIndicesAreRetryable() {
    server.otherwise(PATH, request -> json(200, "{\"data\":[{\"index\":0,\"embedding\":[1,2,3,4]},"
        + "{\"index\":0,\"embedding\":[5,6,7,8]}]}"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    EmbeddingResult result = client(props).embed(inputs("one")).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertNull(result.getVector());
    assertEquals(1, server.posts().size());
  }

  @Test
  void nonIntegralResponseIndexIsRetryable() {
    server.otherwise(PATH, request -> json(200, "{\"data\":[{\"index\":0.5,\"embedding\":[1,2,3,4]}]}"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    EmbeddingResult result = client(props).embed(inputs("one")).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertNull(result.getVector());
    assertEquals(1, server.posts().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"1e100", "NaN"})
  void nonFiniteVectorElementsAreRetryable(String value) {
    server.otherwise(PATH, request -> json(200, "{\"data\":[{\"index\":0,\"embedding\":[" + value + ",2,3,4]}]}"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    EmbeddingResult result = client(props).embed(inputs("one")).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus(),
        "decoded vector=" + Arrays.toString(result.getVector()));
    assertNull(result.getVector());
    assertEquals(1, server.posts().size());
  }

  @Test
  void testMissingIndexIsRetryable() {
    server.otherwise(PATH, request -> json(200, "{\"object\":\"list\",\"model\":\"" + MODEL + "\",\"data\":[{\"object\":\"embedding\",\"index\":0,"
        + "\"embedding\":[0.1,0.2,0.3,0.4]}],\"usage\":{\"prompt_tokens\":2}}"));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("a", "b"));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus());
    assertTrue(results.get(1).getError().contains("malformed response"), results.get(1).getError());
  }

  @Test
  void testWrongModelDimensionAbortsTheRun() {
    // every vector 5 long for a column of 4: the configuration is wrong, so no row may be marked PERMANENT
    server.otherwise(PATH, embeddings(DIM + 1));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(thrown.getMessage().contains("returned 5-dimension vectors"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains(key(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX) + "=4"), thrown.getMessage());
  }

  @Test
  void testTeiWrongModelDimensionAbortsTheRun() {
    server.otherwise("/info", request -> recorded(200, "tei-info.json")).otherwise(PATH, embeddings(384));
    OpenAiCompatibleEmbeddingClient client = client("hf-tei", "BAAI/bge-small-en-v1.5", teiProps());
    List<EmbeddingInput> inputs = inputs("a", "b");
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
  }

  @Test
  void testOneVectorOfAnotherLengthIsRetryable() {
    server.otherwise(PATH, request -> json(200, "{\"object\":\"list\",\"model\":\"" + MODEL + "\",\"data\":["
        + "{\"object\":\"embedding\",\"index\":0,\"embedding\":[0.1,0.2,0.3,0.4]},"
        + "{\"object\":\"embedding\",\"index\":1,\"embedding\":[0.1,0.2,0.3]}]}"));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("a", "b"));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(1).getStatus());
    assertTrue(results.get(1).getError().contains("malformed response: expected 4 floats, got 3"), results.get(1).getError());
  }

  // ---------------------------------------------------------------- retries

  @Test
  void test429WithRetryAfterThenSuccess() {
    server.then(PATH, request -> recorded(429, "error-429-rate.json").withHeader("Retry-After", "2"))
        .otherwise(PATH, embeddings(DIM));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    assertOk(client.embed(inputs("a", "b")), "a", "b");
    assertEquals(Collections.singletonList(2000L), sleeps, "the Retry-After hint replaces the jittered backoff");
    assertEquals(2, client.lastCallStats().getRequests());
    assertEquals(1, client.lastCallStats().getRetries());
  }

  @Test
  void testAzureRetryAfterMsWins() {
    server.then(PATH, request -> recorded(429, "error-429-rate.json").withHeader("retry-after-ms", "750")
        .withHeader("Retry-After", "9")).otherwise(PATH, embeddings(DIM));
    assertOk(client(new TypedProperties()).embed(inputs("a")), "a");
    assertEquals(Collections.singletonList(750L), sleeps);
  }

  @Test
  void testRetryAfterAboveMaxBackoffEndsRetries() {
    server.otherwise(PATH, request -> recorded(429, "error-429-rate.json").withHeader("Retry-After", "5"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_BACKOFF_MS_SUFFIX), "1000");
    List<EmbeddingResult> results = client(props).embed(inputs("a", "b"));
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "over the 1000 ms max backoff");
    assertEquals(1, server.posts().size());
    assertEquals(Collections.emptyList(), sleeps);
  }

  @Test
  void test5xxThenSuccessWithinDeadline() {
    // Count response-driven retries independently of local socket scheduling; real transport failures have their own test.
    List<Recorded> requests = new ArrayList<>();
    HttpTransport transport = new HttpTransport() {
      @Override
      public HttpReply post(String url, Map<String, String> headers, byte[] body, int timeoutMs) {
        assertEquals(server.url() + PATH, url);
        Recorded request = new Recorded("POST", PATH, headers, new String(body, StandardCharsets.UTF_8));
        requests.add(request);
        StubEmbeddingServer.Reply reply;
        if (requests.size() == 1) {
          reply = recorded(503, "error-503.json");
        } else if (requests.size() == 2) {
          reply = json(500, "");
        } else {
          reply = embeddings(DIM).apply(request);
        }
        return new HttpReply(reply.status, Collections.emptyMap(), reply.body);
      }

      @Override
      public HttpReply get(String url, Map<String, String> headers, int timeoutMs) {
        throw new AssertionError("the embeddings call must use POST");
      }
    };
    OpenAiCompatibleEmbeddingClient client = client("openai", MODEL, new TypedProperties(), transport);
    assertOk(client.embed(inputs("a", "b")), "a", "b");
    assertEquals(3, requests.size());
    assertEquals(3, client.lastCallStats().getRequests());
    assertEquals(2, sleeps.size());
    assertTrue(sleeps.get(0) >= 0 && sleeps.get(0) <= 500, "full jitter within the initial backoff: " + sleeps);
    assertTrue(sleeps.get(1) >= 0 && sleeps.get(1) <= 1000, "full jitter within twice the initial backoff: " + sleeps);
    assertEquals(2, client.lastCallStats().getRetries());
  }

  @Test
  void testRetriesExhaustedAreRetryable() {
    server.otherwise(PATH, request -> recorded(503, "error-503.json"));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("a", "b"));
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "HTTP 503: The server is overloaded");
    assertTrue(results.get(0).getError().contains("after 5 attempts"), results.get(0).getError());
    assertEquals(5, server.posts().size());
  }

  @Test
  void testTransportFailureIsRetryable() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "http://127.0.0.1:1/v1");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "2");
    List<EmbeddingResult> results = client(props).embed(inputs("a"));
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "Exception");
    assertEquals(1, sleeps.size());
  }

  /** Out of quota is the account's state (class P): the call fails at once, without a retry. */
  @Test
  void testQuotaFailsTheCallWithoutRetry() {
    server.otherwise(PATH, request -> recorded(429, "error-429-quota.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b");
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("exceeded your current quota"), e.getMessage());
    assertEquals(1, server.posts().size());
  }

  /** A billing 429 (code credit_balance_exhausted, OpenAI error-codes guide) fails the call at once, like insufficient_quota. */
  @Test
  void testBillingRateLimitFailsTheCallWithoutRetry() {
    server.otherwise(PATH, request -> json(429, "{\"error\":{\"message\":\"Your organization has no prepaid credits remaining.\","
        + "\"type\":null,\"param\":null,\"code\":\"credit_balance_exhausted\"}}"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b");
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("for lack of quota") && e.getMessage().contains("no prepaid credits"), e.getMessage());
    assertEquals(1, server.posts().size());
    assertTrue(sleeps.isEmpty());
  }

  /** A 409 Conflict is retried, as the official SDK retries it; the retry's answer settles the inputs. */
  @Test
  void test409IsRetriedThenSucceeds() {
    server.then(PATH, request -> json(409, "{\"error\":{\"message\":\"conflict\",\"type\":null,\"param\":null,\"code\":null}}"))
        .otherwise(PATH, embeddings(DIM));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("a", "b"));
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.OK, result.getStatus(), String.valueOf(result));
      assertEquals(DIM, result.getVector().length);
    }
    assertEquals(2, server.posts().size());
    assertEquals(1, sleeps.size());
  }

  @Test
  void testRequestsAfterTheDeadlineAreNotSent() {
    // the first request takes 10 s of the 5 s deadline: the second is never sent
    server.then(PATH, request -> {
      now.addAndGet(10_000);
      return embeddings(DIM).apply(request);
    }).otherwise(PATH, embeddings(DIM));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.REQUEST_DEADLINE_MS_SUFFIX), "5000");
    List<EmbeddingResult> results = client(props).embed(inputs("a", "b", "c", "d"));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.OK, results.get(1).getStatus());
    assertAll(results.subList(2, 4), EmbeddingResult.Status.RETRYABLE_FAILURE, "deadline exceeded");
    assertEquals(1, server.posts().size());
  }

  @Test
  void testSleepThatWouldCrossTheDeadlineEndsRetries() {
    server.otherwise(PATH, request -> recorded(429, "error-429-rate.json").withHeader("Retry-After", "4"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_DEADLINE_MS_SUFFIX), "3000");
    List<EmbeddingResult> results = client(props).embed(inputs("a"));
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "deadline exceeded");
    assertEquals(Collections.emptyList(), sleeps);
  }

  // ---------------------------------------------------------------- classification + bisection

  @Test
  void test400BisectsToTheBadInput() {
    server.otherwise(PATH, rejecting("BAD"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<String> texts = Arrays.asList("t0", "t1", "t2", "BAD", "t4", "t5", "t6", "t7");
    List<EmbeddingResult> results = client.embed(inputs(texts.toArray(new String[0])));
    for (int i = 0; i < texts.size(); i++) {
      if (i == 3) {
        assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(i).getStatus());
        assertTrue(results.get(i).getError().contains("'$.input' is invalid"), results.get(i).getError());
      } else {
        assertEquals(EmbeddingResult.Status.OK, results.get(i).getStatus(), texts.get(i));
        assertArrayEquals(vectorFor(texts.get(i), DIM), results.get(i).getVector());
      }
    }
    // 8 -> [0,4) bad, [4,8) ok -> [0,2) ok, [2,4) bad -> [2] ok, [3] bad: 6 extra requests
    assertEquals(6, client.lastCallStats().getBisectRequests());
    assertEquals(7, server.posts().size());
  }

  @Test
  void testTwoBadInputsInDifferentHalvesAreBothIsolated() {
    server.otherwise(PATH, rejecting("BAD"));
    List<EmbeddingResult> results = client(new TypedProperties())
        .embed(inputs("t0", "BAD-1", "t2", "t3", "t4", "t5", "BAD-6", "t7"));
    for (int i = 0; i < 8; i++) {
      EmbeddingResult.Status expected = i == 1 || i == 6 ? EmbeddingResult.Status.PERMANENT_FAILURE : EmbeddingResult.Status.OK;
      assertEquals(expected, results.get(i).getStatus(), String.valueOf(i));
    }
  }

  @Test
  void testRequestLevelRejectionWithoutAParamMakesTheRequestRetryable() {
    // e.g. an Azure deployment of a non-embedding model: every request, of any size, gets the same param-less 400. That is
    // indistinguishable from a request whose probed inputs are all bad, so no row is PERMANENT and the run is not aborted:
    // the whole request is RETRYABLE and counts toward the outage short-circuit
    server.otherwise(PATH, request -> recorded(400, "error-400-input.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingResult> results = client.embed(inputs("t0", "t1", "t2", "t3", "t4", "t5", "t6", "t7"));
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, OpenAiCompatibleEmbeddingClient.REQUEST_REJECTED);
    // the 8-input request, halves [0,4) and [0,2), singletons [0] and [1], then the probe of the last input [7]
    assertEquals(6, server.posts().size());
    assertEquals(Collections.singletonList("t7"), server.posts().get(5).inputs());
    // every later batch is rejected alike: after outage.consecutive.failures (3) such requests the client stops calling
    client.embed(inputs("u0", "u1", "u2"));
    client.embed(inputs("v0", "v1", "v2"));
    int sent = server.posts().size();
    List<EmbeddingInput> inputs = inputs("w0", "w1", "w2");
    assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(inputs));
    assertEquals(sent, server.posts().size());
  }

  @Test
  void testBadInputsAtBothEndsDoNotAbortTheRun() {
    // bad data at positions 0, 1 and last, rejected with one generic message, must never abort
    server.otherwise(PATH, rejecting("BAD"));
    List<EmbeddingResult> results = client(new TypedProperties())
        .embed(inputs("BAD-0", "BAD-1", "t2", "t3", "t4", "t5", "t6", "BAD-7"));
    for (int i : new int[] {0, 1, 7}) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(i).getStatus(), String.valueOf(i));
    }
    for (int i = 2; i < 7; i++) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(i).getStatus(),
          "the whole request is retried later: " + i);
    }
  }

  @Test
  void testTwoBadInputsFirstInTheBatchAreStillIsolated() {
    // the neighbour of the request-level case: the first two singletons fail alike, but the probe of the last input succeeds
    server.otherwise(PATH, rejecting("BAD"));
    List<EmbeddingResult> results = client(new TypedProperties())
        .embed(inputs("BAD-0", "BAD-1", "t2", "t3", "t4", "t5", "t6", "t7"));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    for (int i = 2; i < 8; i++) {
      assertEquals(EmbeddingResult.Status.OK, results.get(i).getStatus(), String.valueOf(i));
      assertArrayEquals(vectorFor("t" + i, DIM), results.get(i).getVector());
    }
  }

  @Test
  void testBisectBudgetExhaustedLeavesInputsRetryable() {
    server.otherwise(PATH, rejecting("BAD"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.BISECT_MAX_REQUESTS_SUFFIX), "1");
    List<EmbeddingResult> results = client(props).embed(inputs("BAD", "t1", "t2", "t3"));
    // the first half [BAD, t1] is the one extra request allowed; it fails, and nothing can be isolated further
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "bisection budget exhausted");
    assertEquals(2, server.posts().size());
  }

  @Test
  void testDeadlineDuringBisectionLeavesInputsRetryable() {
    server.otherwise(PATH, request -> {
      now.addAndGet(4_000);
      return rejecting("BAD").apply(request);
    });
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_DEADLINE_MS_SUFFIX), "6000");
    List<EmbeddingResult> results = client(props).embed(inputs("BAD", "t1", "t2", "t3"));
    // request 1 (4 s) fails; half [BAD, t1] (8 s) fails; then the deadline has passed: nothing is PERMANENT
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
      assertTrue(result.getError().contains("deadline exceeded"), result.getError());
    }
    assertEquals(2, server.posts().size());
  }

  @Test
  void test400NamingDimensionsIsAConfigErrorWithoutBisection() {
    server.otherwise(PATH, request -> recorded(400, "error-400-dimensions.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b", "c", "d");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(thrown.getMessage().contains("does not support specifying dimensions"), thrown.getMessage());
    assertEquals(1, server.posts().size());
  }

  @Test
  void test401RaisesConfigException() {
    server.otherwise(PATH, request -> recorded(401, "error-401.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(thrown.getMessage().contains("HTTP 401"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("env " + KEY_ENV), "names the credential source: " + thrown.getMessage());
    assertEquals(1, server.posts().size(), "no retry and no bisection");
    // the recorded 401 echoes the key's 9th char and last 4 ("sk-test-A***1234"): an auth error keeps only status, code
    // and type, never the provider's message
    assertFalse(thrown.getMessage().contains(KEY.substring(KEY.length() - 4)), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("Incorrect API key"), thrown.getMessage());
    assertTrue(thrown.getMessage().contains("code invalid_api_key"), thrown.getMessage());
  }

  @Test
  void testProviderErrorTextIsRedacted() {
    // the 401 recording echoes the key's first chars ("sk-test-A***1234"); a 400 echoes the whole key
    server.then(PATH, request -> json(400, "{\"error\":{\"message\":\"bad input " + KEY + "\",\"type\":\"x\"}}"));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs("a"));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(0).getStatus());
    assertFalse(results.get(0).getError().contains(KEY), results.get(0).getError());
    assertTrue(results.get(0).getError().contains("bad input ***"), results.get(0).getError());

    server.otherwise(PATH, request -> recorded(401, "error-401.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertFalse(thrown.getMessage().contains(KEY.substring(0, OpenAiCompatibleEmbeddingClient.KEY_PREFIX_CHARS)),
        thrown.getMessage());
  }

  // ---------------------------------------------------------------- local validation + batching

  @Test
  void testOverLimitAndEmptyInputsFailLocallyWithoutARequest() {
    server.otherwise(PATH, embeddings(DIM));
    String overLimit = String.join(" ", Collections.nCopies(8200, "hello"));
    assertTrue(new OpenAiBpeTokenizer().countTokens(overLimit) > 8191);
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());

    List<EmbeddingResult> bad = client.embed(inputs(overLimit, "", "   "));
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, bad.get(0).getStatus());
    assertTrue(bad.get(0).getError().contains("over the per-input limit of 8191"), bad.get(0).getError());
    // PERMANENT is never reselected: the error names the key that set the limit, so a too-low setting can be found
    assertTrue(bad.get(0).getError().contains(key(OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX)), bad.get(0).getError());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, bad.get(1).getStatus());
    assertEquals(OpenAiCompatibleEmbeddingClient.EMPTY_INPUT, bad.get(1).getError());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, bad.get(2).getStatus());
    assertEquals(0, server.posts().size(), "nothing was sent");
    assertEquals(0, client.lastCallStats().getRequests());

    List<EmbeddingResult> mixed = client.embed(inputs("", "good", overLimit));
    assertEquals(EmbeddingResult.Status.OK, mixed.get(1).getStatus());
    assertEquals(Collections.singletonList("good"), server.posts().get(0).inputs(), "only the valid input is sent");
  }

  @Test
  void testNonTextAndNullTextArePermanent() {
    List<EmbeddingInput> inputs = Arrays.asList(
        new EmbeddingInput("img", 0, "unsupported", null),
        new EmbeddingInput("txt", 0, EmbeddingTableSchema.MODALITY_TEXT, null));
    List<EmbeddingResult> results = client(new TypedProperties()).embed(inputs);
    assertTrue(results.get(0).getError().contains("modality unsupported is not supported by openai"), results.get(0).getError());
    assertEquals(OpenAiCompatibleEmbeddingClient.EMPTY_INPUT, results.get(1).getError());
    assertEquals(0, server.posts().size());
  }

  @Test
  void testDuplicateIdsThrow() {
    List<EmbeddingInput> inputs = new ArrayList<>(inputs("a"));
    inputs.add(inputs.get(0));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> client.embed(inputs));
    assertTrue(failure.getMessage().contains("source_path_sha=fid-0"), failure.getMessage());
    assertTrue(failure.getMessage().contains("source_modification_time=0"), failure.getMessage());
    assertTrue(failure.getMessage().contains("source_file_size=0"), failure.getMessage());
    assertTrue(failure.getMessage().contains("modality=text"), failure.getMessage());
    assertTrue(failure.getMessage().contains("chunk_id=0"), failure.getMessage());
    assertEquals(0, server.posts().size());
  }

  @Test
  void testBatchingRespectsInputAndTokenLimits() {
    server.otherwise(PATH, embeddings(DIM));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "4");
    String[] texts = IntStream.range(0, 10).mapToObj(i -> "chunk number " + i + " of the document")
        .toArray(String[]::new);
    assertOk(client(props).embed(inputs(texts)), texts);
    // ceil(10 / 4) = 3 requests of 4, 4 and 2 inputs
    assertEquals(Arrays.asList(4, 4, 2), server.posts().stream().map(r -> r.inputs().size()).collect(Collectors.toList()));

    OpenAiBpeTokenizer tokenizer = new OpenAiBpeTokenizer();
    int perInput = tokenizer.countTokens(texts[0]);
    for (String text : texts) {
      assertEquals(perInput, tokenizer.countTokens(text), "the fixture's inputs have equal token counts");
    }
    long budget = 3L * perInput;
    TypedProperties tokenProps = new TypedProperties();
    tokenProps.setProperty(key(OpenAiClientConfig.REQUEST_MAX_TOKENS_SUFFIX), String.valueOf(budget));
    int before = server.posts().size();
    assertOk(client(tokenProps).embed(inputs(texts)), texts);
    List<Recorded> tokenRequests = server.posts().subList(before, server.posts().size());
    // ceil(10 / 3) = 4 requests, each within the summed-token budget
    assertEquals(4, tokenRequests.size());
    for (Recorded request : tokenRequests) {
      int tokens = request.inputs().stream().mapToInt(tokenizer::countTokens).sum();
      assertTrue(tokens <= budget, tokens + " > " + budget);
    }
  }

  // ---------------------------------------------------------------- outage short-circuit

  @Test
  void testOutageShortCircuit() {
    server.otherwise(PATH, request -> recorded(503, "error-503.json"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    OpenAiCompatibleEmbeddingClient client = client(props);
    List<EmbeddingResult> results = client.embed(inputs("a", "b", "c"));
    assertEquals(2, server.posts().size(), "the third request is not sent once two failed in a row");
    assertTrue(results.get(0).getError().contains("HTTP 503"), results.get(0).getError());
    assertTrue(results.get(2).getError().startsWith(OpenAiCompatibleEmbeddingClient.OUTAGE), results.get(2).getError());
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "");

    List<EmbeddingInput> inputs = inputs("d");
    assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(inputs));
    assertEquals(2, server.posts().size(), "no request after the short-circuit opened");
  }

  @Test
  void testSuccessResetsTheOutageCount() {
    server.then(PATH, request -> recorded(503, "error-503.json")).then(PATH, embeddings(DIM))
        .then(PATH, request -> recorded(503, "error-503.json")).otherwise(PATH, embeddings(DIM));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    OpenAiCompatibleEmbeddingClient client = client(props);
    List<EmbeddingResult> results = client.embed(inputs("a", "b", "c", "d"));
    assertEquals(4, server.posts().size());
    assertEquals(EmbeddingResult.Status.OK, results.get(3).getStatus());
    assertEquals(1, client.embed(inputs("e")).size(), "still calling: no two failures were consecutive");
  }

  // ---------------------------------------------------------------- Azure + TEI

  @ParameterizedTest
  @ValueSource(strings = {"openai", "hf-tei"})
  void testCompatibleProvidersRefuseWrongServedModel(String provider) {
    server.otherwise("/info", request -> recorded(200, "tei-info.json"));
    server.otherwise(PATH, request -> json(200, "{\"model\":\"other-model\","
        + "\"data\":[{\"index\":0,\"embedding\":[1,2,3,4]}]}"));
    String model = "hf-tei".equals(provider) ? "BAAI/bge-small-en-v1.5" : MODEL;
    OpenAiCompatibleEmbeddingClient client = client(provider, model, new TypedProperties());
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs("a")));
    assertEquals(1, server.posts().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"other/model\"", "null", "123", "\"\""})
  void testTeiRefusesUnverifiedServedModel(String servedModel) {
    server.otherwise("/info", request -> json(200, "{\"model_id\":" + servedModel
        + ",\"model_type\":{\"embedding\":{}},\"max_client_batch_size\":2,\"max_batch_tokens\":16384}"));
    EmbeddingClientConfigException failure = assertThrows(EmbeddingClientConfigException.class,
        () -> client("hf-tei", "BAAI/bge-small-en-v1.5", teiProps()));
    assertTrue(failure.getMessage().contains("model_id"), failure.getMessage());
    assertTrue(failure.getMessage().contains(key(EmbeddingTableConfig.MODEL_NAME_SUFFIX)), failure.getMessage());
    assertFalse(failure.getMessage().contains(key(OpenAiClientConfig.BASE_MODEL_SUFFIX)), failure.getMessage());
    assertTrue(server.posts().isEmpty());
  }

  @ParameterizedTest
  @ValueSource(strings = {"\"text-embedding-3-large\"", "null", "123", "\"\""})
  void testAzureRefusesUnverifiedServedModel(String servedModel) {
    server.otherwise(PATH, request -> json(200, "{\"model\":" + servedModel
        + ",\"data\":[{\"index\":0,\"embedding\":[1,2,3,4]}]}"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.BASE_MODEL_SUFFIX), MODEL);
    OpenAiCompatibleEmbeddingClient client = client("azure-openai", "my-dep", props);
    EmbeddingClientConfigException failure = assertThrows(EmbeddingClientConfigException.class,
        () -> client.embed(inputs("a")));
    assertTrue(failure.getMessage().contains("served model"), failure.getMessage());
    assertTrue(failure.getMessage().contains(key(OpenAiClientConfig.BASE_MODEL_SUFFIX)), failure.getMessage());
    assertEquals(1, server.posts().size(), "identity mismatch is never retried or bisected");
  }

  @Test
  void testAzureChecksEveryResponseForDeploymentChanges() {
    server.then(PATH, request -> json(200, "{\"model\":\"" + MODEL
        + "\",\"data\":[{\"index\":0,\"embedding\":[1,2,3,4]}]}"))
        .otherwise(PATH, request -> json(200, "{\"model\":\"text-embedding-3-large\","
            + "\"data\":[{\"index\":0,\"embedding\":[1,2,3,4]}]}"));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.BASE_MODEL_SUFFIX), MODEL);
    OpenAiCompatibleEmbeddingClient client = client("azure-openai", "my-dep", props);
    assertArrayEquals(new float[] {1, 2, 3, 4}, client.embed(inputs("a")).get(0).getVector());
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs("b")));
    assertEquals(2, server.posts().size());
  }

  @Test
  void testAzureUsesApiKeyHeaderAndDeploymentName() {
    server.otherwise("/openai/v1/embeddings", request -> {
      ObjectNode response = StubEmbeddingServer.MAPPER.createObjectNode().put("model", MODEL);
      ObjectNode entry = response.putArray("data").addObject().put("index", 0);
      for (float value : vectorFor(request.inputs().get(0), DIM)) {
        entry.withArray("embedding").add(value);
      }
      return json(200, response.toString());
    });
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), server.url() + "/openai/v1");
    props.setProperty(key(OpenAiClientConfig.BASE_MODEL_SUFFIX), MODEL);
    OpenAiCompatibleEmbeddingClient client = client("azure-openai", "my-dep", props);
    assertOk(client.embed(inputs("a")), "a");
    Recorded request = server.posts().get(0);
    assertEquals("/openai/v1/embeddings", request.path);
    assertEquals(KEY, request.header("api-key"));
    assertNull(request.header("Authorization"));
    assertEquals("my-dep", request.json().get("model").asText());
  }

  @Test
  void testTeiAuthenticationDiagnosticOmitsProviderMessage() {
    server.otherwise("/info", request -> recorded(401, "error-401.json"));
    IllegalStateException failure = assertThrows(IllegalStateException.class,
        () -> client("hf-tei", "BAAI/bge-small-en-v1.5", teiProps()));
    assertTrue(failure.getMessage().contains("HTTP 401"));
    assertTrue(failure.getMessage().contains("code invalid_api_key"));
    assertFalse(failure.getMessage().contains(KEY.substring(KEY.length() - 4)));
    assertFalse(failure.getMessage().contains("Incorrect API key"));
    assertTrue(server.posts().isEmpty());
  }

  @Test
  void testTei424IsRetryableAndInfoSetsLimits() {
    server.otherwise("/info", request -> recorded(200, "tei-info.json"))
        .then(PATH, request -> recorded(424, "tei-error-424.json")).otherwise(PATH, embeddings(DIM));
    OpenAiCompatibleEmbeddingClient client = client("hf-tei", "BAAI/bge-small-en-v1.5", teiProps());
    assertOk(client.embed(inputs("a", "b", "c", "d", "e")), "a", "b", "c", "d", "e");
    List<Recorded> posts = server.posts();
    // the 424 is retried; max_client_batch_size=2 from /info splits 5 inputs into 3 requests
    assertEquals(4, posts.size());
    assertEquals(Arrays.asList(2, 2, 2, 1), posts.stream().map(r -> r.inputs().size()).collect(Collectors.toList()));
    assertNull(posts.get(0).header("Authorization"), "TEI without a credential sends none");
    assertFalse(posts.get(0).json().has("dimensions"));
  }

  @Test
  void testTeiInitFailsWhenInfoSaysNotEmbedding() {
    server.otherwise("/info", request -> recorded(200, "tei-info-reranker.json"));
    TypedProperties props = teiProps();
    IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> client("hf-tei", "BAAI/bge-reranker-base", props));
    assertTrue(thrown.getMessage().contains("does not serve an embedding model"), thrown.getMessage());
  }

  @Test
  void testTeiInitFailsWhenInfoUnreachableOrFailing() {
    TypedProperties unreachable = teiProps();
    unreachable.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "http://127.0.0.1:1/v1");
    assertThrows(IllegalStateException.class, () -> client("hf-tei", "m", unreachable));
    server.otherwise("/info", request -> json(503, "{\"error\":\"Model is overloaded\",\"error_type\":\"overloaded\"}"));
    TypedProperties overloaded = teiProps();
    IllegalStateException failing = assertThrows(IllegalStateException.class, () -> client("hf-tei", "m", overloaded));
    assertTrue(failing.getMessage().contains("HTTP 503"), failing.getMessage());
  }

  // ---------------------------------------------------------------- secrets

  @Test
  void testKeyNeverInLogsOrToString() throws IOException {
    // call 1: 429 then OK; call 2: a 400 bisected to one rejected input; call 3: 503 five times; call 4: 401
    server.then(PATH, request -> recorded(429, "error-429-rate.json").withHeader("Retry-After", "1"))
        .then(PATH, embeddings(DIM))
        .then(PATH, rejecting("BAD"))
        .then(PATH, rejecting("BAD"))
        .then(PATH, rejecting("BAD"));
    for (int i = 0; i < 5; i++) {
      server.then(PATH, request -> recorded(503, "error-503.json"));
    }
    server.otherwise(PATH, request -> recorded(401, "error-401.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    client.embed(inputs("ok-after-retry"));
    client.embed(inputs("BAD", "fine"));
    client.embed(inputs("server-error"));
    List<EmbeddingInput> inputs = inputs("unauthorized");
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));

    String captured = logs.text();
    assertTrue(captured.contains("EMBED-OPENAI-REQUEST-FAILED"), "the capture saw the client's logging: " + captured);
    assertTrue(captured.contains("EMBED-OPENAI-INPUT-REJECTED"), captured);
    // the retry line is the one DEBUG line: the key checks below must see it under every logging profile
    assertTrue(captured.contains("EMBED-OPENAI-RETRY"), "the capture saw the client's DEBUG retry line: " + captured);
    String prefix = KEY.substring(0, OpenAiCompatibleEmbeddingClient.KEY_PREFIX_CHARS);
    assertFalse(captured.contains(KEY), "the key appears in the client log");
    assertFalse(captured.contains(prefix), "a key prefix appears in the client log");
    assertFalse(client.toString().contains(KEY), client.toString());
    assertTrue(client.toString().contains("credential=env " + KEY_ENV), client.toString());
    String dir = System.getProperty(CLIENT_LOG_DIR);
    if (dir != null) {
      Files.createDirectories(Paths.get(dir));
      Files.write(Paths.get(dir, "client-log-capture.txt"), captured.getBytes(StandardCharsets.UTF_8));
    }
  }

  @Test
  void testKeyWithControlCharactersIsRefusedAtInit() {
    // a two-line key file or env value: an HTTP client would quote it in an "invalid header value" error
    environment.put(KEY_ENV, "sk-test-AB\nCD");
    TypedProperties props = new TypedProperties();
    IllegalStateException thrown = assertThrows(IllegalStateException.class, () -> client(props));
    assertTrue(thrown.getMessage().contains("env " + KEY_ENV), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("sk-test"), thrown.getMessage());
    assertFalse(thrown.getMessage().contains("AB"), thrown.getMessage());
    assertEquals(0, server.requests().size());
  }

  @Test
  void testKeyCrossingTheMessageCutIsRedactedWhole() {
    // 1010 chars then the key: the 1024-char cut falls inside the key, after its first 14 chars
    String filler = String.join("", Collections.nCopies(1010, "x"));
    server.otherwise(PATH, request -> json(400, "{\"error\":{\"message\":\"" + filler + KEY + "\",\"type\":\"x\"}}"));
    String error = client(new TypedProperties()).embed(inputs("a")).get(0).getError();
    assertFalse(error.contains(KEY.substring(OpenAiCompatibleEmbeddingClient.KEY_PREFIX_CHARS, 14)), error);
    assertTrue(error.contains("***"), error);
  }

  @Test
  void testUnsendableRequestIsAConfigErrorWithoutTheHeader() {
    // an HTTP client that refuses to build the request quotes the header value, as the JDK's does
    HttpTransport refusing = new HttpTransport() {
      @Override
      public HttpReply post(String url, Map<String, String> headers, byte[] body, int timeoutMs) {
        throw new IllegalArgumentException("invalid header value: \"" + headers.get("Authorization") + "\"");
      }

      @Override
      public HttpReply get(String url, Map<String, String> headers, int timeoutMs) {
        throw new IllegalArgumentException("invalid header value");
      }
    };
    OpenAiCompatibleEmbeddingClient client = new OpenAiCompatibleEmbeddingClient(refusing, now::get, sleeps::add,
        new Random(7), environment::get);
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), KEY_ENV);
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "openai", MODEL), props);
    List<EmbeddingInput> inputs = inputs("a");
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(thrown.getMessage().contains("cannot be sent: invalid header value: \"Bearer ***"), thrown.getMessage());
    assertFalse(thrown.getMessage().contains(KEY.substring(0, OpenAiCompatibleEmbeddingClient.KEY_PREFIX_CHARS)),
        thrown.getMessage());
    assertEquals(1, client.lastCallStats().getRequests(), "stats describe the call that threw");
  }

  @Test
  void testInterruptStopsTheCallWithoutCountingAnOutage() {
    server.otherwise(PATH, request -> recorded(503, "error-503.json"));
    OpenAiCompatibleEmbeddingClient client = new OpenAiCompatibleEmbeddingClient(new JdkHttpTransport(), now::get,
        millis -> {
          throw new InterruptedException("task killed");
        }, new Random(7), environment::get);
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), server.url() + "/v1");
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), KEY_ENV);
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "1");
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "openai", MODEL), props);
    List<EmbeddingResult> results;
    try {
      results = client.embed(inputs("a", "b", "c"));
    } finally {
      assertTrue(Thread.interrupted(), "the interrupt is restored for the caller");
    }
    assertEquals(1, server.posts().size(), "nothing is sent after the interrupt");
    assertAll(results, EmbeddingResult.Status.RETRYABLE_FAILURE, "interrupted");
    server.otherwise(PATH, embeddings(DIM));
    assertEquals(EmbeddingResult.Status.OK, client.embed(inputs("d")).get(0).getStatus(),
        "an interrupt is not a provider failure: the outage short-circuit stays closed");
  }

  @Test
  void testStatsAfterAConfigFailure() {
    server.otherwise(PATH, request -> recorded(401, "error-401.json"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    List<EmbeddingInput> inputs = inputs("a", "b");
    assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertEquals(1, client.lastCallStats().getRequests());
  }

  @Test
  void testHugeUsageKeepsTheVectors() {
    server.otherwise(PATH, request -> json(200, "{\"object\":\"list\",\"model\":\"" + MODEL + "\",\"data\":[{\"object\":\"embedding\",\"index\":0,"
        + "\"embedding\":[0.1,0.2,0.3,0.4]}],\"usage\":{\"prompt_tokens\":99999999999999999999999}}"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    assertEquals(EmbeddingResult.Status.OK, client.embed(inputs("a")).get(0).getStatus());
    assertEquals(-1, client.lastCallStats().getProviderPromptTokens());
  }

  @Test
  void testNonNumericUsageKeepsTheVectors() {
    server.otherwise(PATH, request -> json(200, "{\"object\":\"list\",\"model\":\"" + MODEL + "\",\"data\":[{\"object\":\"embedding\",\"index\":0,"
        + "\"embedding\":[0.1,0.2,0.3,0.4]}],\"usage\":{\"prompt_tokens\":\"n/a\"}}"));
    OpenAiCompatibleEmbeddingClient client = client(new TypedProperties());
    assertEquals(EmbeddingResult.Status.OK, client.embed(inputs("a")).get(0).getStatus());
    assertEquals(-1, client.lastCallStats().getProviderPromptTokens());
    assertEquals(1, server.posts().size());
  }

  // ---------------------------------------------------------------- factory

  @Test
  void testFactoryResolvesTheProvidersAndChecksConfigOnTheDriver() {
    for (String provider : new String[] {"openai", "azure-openai", "hf-tei"}) {
      EmbeddingModelSpec spec = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, provider, MODEL);
      assertEquals(OpenAiCompatibleEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(spec));
    }
    EmbeddingModelSpec ada = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "openai", "text-embedding-ada-002");
    TypedProperties noProps = new TypedProperties();
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(ada, noProps));
    EmbeddingClientFactory.checkConfig(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, "dummy", ALIAS),
        new TypedProperties());
    // an explicit client.class naming this client is checked on the driver too
    EmbeddingModelSpec explicitAda = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM,
        OpenAiCompatibleEmbeddingClient.class.getName(), "openai", "text-embedding-ada-002");
    TypedProperties explicitProps = new TypedProperties();
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(explicitAda, explicitProps));
  }

  // ---------------------------------------------------------------- helpers

  private OpenAiCompatibleEmbeddingClient client(TypedProperties props) {
    return client("openai", MODEL, props);
  }

  private OpenAiCompatibleEmbeddingClient client(String provider, String model, TypedProperties extra) {
    return client(provider, model, extra, new JdkHttpTransport());
  }

  private OpenAiCompatibleEmbeddingClient client(String provider, String model, TypedProperties extra, HttpTransport transport) {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), server.url() + "/v1");
    if (!"hf-tei".equals(provider)) {
      props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), KEY_ENV);
    }
    props.putAll(extra);
    OpenAiCompatibleEmbeddingClient client = new OpenAiCompatibleEmbeddingClient(transport, now::get,
        millis -> {
          sleeps.add(millis);
          now.addAndGet(millis);
        }, new Random(7), environment::get);
    client.init(new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, DIM, null, provider, model), props);
    return client;
  }

  private TypedProperties teiProps() {
    return new TypedProperties();
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }

  private static List<EmbeddingInput> inputs(String... texts) {
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < texts.length; i++) {
      inputs.add(new EmbeddingInput("fid-" + i, i, EmbeddingTableSchema.MODALITY_TEXT, texts[i]));
    }
    return inputs;
  }

  /** 400 with the documented generic input error for any request holding a text that contains {@code marker}. */
  private static StubEmbeddingServer.Responder rejecting(String marker) {
    return request -> request.inputs().stream().anyMatch(text -> text.contains(marker))
        ? recorded(400, "error-400-input.json") : embeddings(DIM).apply(request);
  }

  private static void assertOk(List<EmbeddingResult> results, String... texts) {
    assertEquals(texts.length, results.size());
    for (int i = 0; i < texts.length; i++) {
      assertEquals(EmbeddingResult.Status.OK, results.get(i).getStatus(), texts[i] + ": " + results.get(i));
      assertArrayEquals(vectorFor(texts[i], DIM), results.get(i).getVector(), texts[i]);
    }
  }

  private static void assertAll(List<EmbeddingResult> results, EmbeddingResult.Status status, String errorFragment) {
    for (EmbeddingResult result : results) {
      assertEquals(status, result.getStatus(), String.valueOf(result));
      assertTrue(result.getError().contains(errorFragment), result.getError());
    }
  }

  /** Collects every event of the client's logger: the formatted message and the full stack of any throwable. */
  private static final class CapturingAppender extends AbstractAppender {
    private final List<String> lines = Collections.synchronizedList(new ArrayList<>());

    private CapturingAppender() {
      super(UUID.randomUUID().toString(), null, null, false, null);
      start();
    }

    @Override
    public void append(LogEvent event) {
      StringBuilder line = new StringBuilder(event.getLevel() + " " + event.getMessage().getFormattedMessage());
      if (event.getThrown() != null) {
        StringWriter trace = new StringWriter();
        event.getThrown().printStackTrace(new PrintWriter(trace));
        line.append('\n').append(trace);
      }
      lines.add(line.toString());
    }

    String text() {
      return String.join("\n", lines);
    }
  }
}
