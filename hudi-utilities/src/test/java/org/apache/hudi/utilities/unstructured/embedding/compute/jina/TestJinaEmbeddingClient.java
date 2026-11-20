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

package org.apache.hudi.utilities.unstructured.embedding.compute.jina;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Recorded;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub.Reply;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiClientConfig;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Jina clients against a stub HTTP server ({@code .endpoint} → {@link ScriptedHttpStub}): request shape (object inputs,
 * {@code dimensions}, {@code embedding_type}, no {@code truncate}), the key from the credential file, and the outcome
 * classification: 429 and 409 retried, a rejected input isolated and quarantined, a rejected configuration failing the call,
 * the outage short-circuit, and one rate-limit token per attempt.
 */
public class TestJinaEmbeddingClient {

  private static final String ALIAS = "jn";
  private static final String KEY = "jina_stubkey_0123456789abcdef";

  @TempDir
  Path tempDir;
  private ScriptedHttpStub stub;
  private TypedProperties props;

  @BeforeEach
  public void setUp() throws IOException {
    stub = new ScriptedHttpStub();
    Path keyFile = tempDir.resolve("jina.key");
    Files.write(keyFile, (KEY + "\n").getBytes(StandardCharsets.UTF_8));
    props = new TypedProperties();
    props.setProperty(key(JinaClientConfig.ENDPOINT_SUFFIX), stub.url() + "/v1/");
    props.setProperty(key(JinaClientConfig.CREDENTIAL_FILE_SUFFIX), keyFile.toString());
    props.setProperty(key(OpenAiClientConfig.RETRY_INITIAL_BACKOFF_MS_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_BACKOFF_MS_SUFFIX), "5");
  }

  @AfterEach
  public void tearDown() {
    stub.close();
  }

  @Test
  public void textsArePackedAsObjectInputsWithTheKeyFromTheFile() throws Exception {
    stub.otherwise(JinaStubReplies.embeddings(256));
    JinaEmbeddingClient client = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "one"), text("b", "two")));
    assertEquals(1, stub.requests().size());
    Recorded request = stub.requests().get(0);
    assertEquals("/v1/embeddings", request.rawPath);
    assertEquals("Bearer " + KEY, request.header("Authorization"));
    JsonNode body = request.json();
    assertEquals("jina-embeddings-v3", body.get("model").asText());
    assertEquals("one", body.get("input").get(0).get("text").asText());
    assertEquals(256, body.get("dimensions").asInt());
    assertEquals("float", body.get("embedding_type").asText());
    assertFalse(body.has("encoding_format"));
    assertFalse(body.has("truncate"), "never truncated silently");
    assertArrayEquals(ScriptedHttpStub.vectorFor("one", 256), results.get(0).getVector());
    assertArrayEquals(ScriptedHttpStub.vectorFor("two", 256), results.get(1).getVector());
    assertEquals(10, client.lastCallStats().getProviderPromptTokens());
  }

  @Test
  public void maxInputsBoundsEachRequest() throws Exception {
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "2");
    stub.otherwise(JinaStubReplies.embeddings(64));
    List<EmbeddingResult> results = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 64)
        .embed(Arrays.asList(text("a", "1"), text("b", "2"), text("c", "3"), text("d", "4"), text("e", "5")));
    assertEquals(3, stub.requests().size());
    results.forEach(r -> assertEquals(EmbeddingResult.Status.OK, r.getStatus(), r.getError()));
  }

  @Test
  public void unsupportedModalityOnTheTextClientIsPermanentWithoutARequest() throws Exception {
    EmbeddingResult result = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256)
        .embed(Collections.singletonList(new EmbeddingInput("unsupported", 0, "unsupported", null))).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertEquals(0, stub.requests().size());
  }

  @Test
  public void rateLimitedAndBusyAnswersAreRetriedHonouringRetryAfter() throws Exception {
    stub.then(JinaStubReplies.error(429, "RATE_REQUEST_LIMIT_EXCEEDED", "slow down").withHeader("Retry-After", "3"))
        .then(JinaStubReplies.error(409, "CONFLICT_RESOURCE_BUSY", "busy"))
        .otherwise(JinaStubReplies.embeddings(256));
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_BACKOFF_MS_SUFFIX), "5000");
    List<Long> sleeps = new ArrayList<>();
    JinaEmbeddingClient client = client(seamed(new AtomicLong(5_000_000L), sleeps), "jina-embeddings-v3", 256);
    EmbeddingResult result = client.embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(3, stub.requests().size());
    assertEquals(3000L, sleeps.get(0).longValue(), "waited the provider's Retry-After");
    assertEquals(2, client.lastCallStats().getRetries());
  }

  @Test
  public void persistentServerErrorLeavesInputsRetryableAndOpensTheOutage() throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "1");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    stub.otherwise(request -> JinaStubReplies.error(503, "SERVICE_UNAVAILABLE", "down"));
    JinaEmbeddingClient client = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "x"), text("b", "y")));
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, results.get(0).getStatus());
    assertTrue(results.get(0).getError().contains("after 2 attempts"), results.get(0).getError());
    assertTrue(results.get(1).getError().contains("outage"), results.get(1).getError());
    assertEquals(2, stub.requests().size());
    assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(Collections.singletonList(text("c", "z"))));
  }

  @Test
  public void rejectedInputIsIsolatedAndPermanent() throws Exception {
    stub.otherwise(request -> {
      JsonNode input = request.json().get("input");
      boolean bad = false;
      for (JsonNode item : input) {
        bad |= item.path("text").asText().equals("too long");
      }
      return bad ? JinaStubReplies.error(400, "INPUT_TOKEN_LIMIT_EXCEEDED", "Input exceeds the model limit")
          : JinaStubReplies.embeddings(256).apply(request);
    });
    JinaEmbeddingClient client = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256);
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a", "ok"), text("b", "too long"), text("c", "fine")));
    assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, results.get(1).getStatus());
    assertTrue(results.get(1).getError().contains("INPUT_TOKEN_LIMIT_EXCEEDED"), results.get(1).getError());
    assertEquals(EmbeddingResult.Status.OK, results.get(2).getStatus());
    assertEquals(4, stub.requests().size());
    assertEquals(3, client.lastCallStats().getBisectRequests());
  }

  /**
   * A 422 is a schema rejection of the request (a dimension the model does not take, an image sent to a text model): the
   * configuration's, so the call fails instead of quarantining every row.
   */
  @Test
  public void schemaRejectionIsAConfigFailureNotAQuarantine() throws InterruptedException {
    stub.otherwise(request -> JinaStubReplies.error(422, "INPUT_VALIDATION", "body -> dimensions: must be one of 64, 96, 128"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-some-future-model", 256)
            .embed(Arrays.asList(text("a", "x"), new EmbeddingInput("unsupported", 0, "unsupported", null))));
    assertTrue(e.getMessage().contains("HTTP 422"), e.getMessage());
    assertEquals(1, stub.requests().size(), "the configuration is not retried or isolated");
  }

  @Test
  public void nonDenseAnswerIsAConfigFailure() {
    stub.otherwise(request -> ScriptedHttpStub.Reply.json(200,
        "{\"data\":[{\"object\":\"embeddings\",\"index\":0,\"embeddings\":[[0.1,0.2],[0.3,0.4]]}],"
            + "\"usage\":{\"total_tokens\":2}}"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-some-future-model", 2).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("multi-vector"), e.getMessage());
    assertEquals(1, stub.requests().size());
  }

  @Test
  public void documentedModelBoundsAreCheckedAtInit() throws InterruptedException {
    assertThrows(EmbeddingClientConfigException.class, () -> client(new JinaEmbeddingClient(), "jina-colbert-v2", 128));
    assertThrows(EmbeddingClientConfigException.class, () -> client(new JinaEmbeddingClient(), "jina-code-embeddings-0.5b", 1024));
    assertThrows(EmbeddingClientConfigException.class, () -> client(new JinaEmbeddingClient(), "jina-embeddings-v5-text-small", 2048));
    assertThrows(EmbeddingClientConfigException.class, () -> client(new JinaEmbeddingClient(), "jina-embeddings-v2-base-en", 512));
    stub.otherwise(JinaStubReplies.embeddings(768));
    client(new JinaEmbeddingClient(), "jina-embeddings-v2-base-en", 768).embed(Collections.singletonList(text("a", "x")));
    assertFalse(stub.requests().get(0).json().has("dimensions"), "a fixed-width model gets no dimensions");
  }

  @Test
  public void sparseAnswerIsAConfigFailure() {
    stub.otherwise(request -> ScriptedHttpStub.Reply.json(200, "{\"data\":[{\"hello\":0.4,\"world\":0.2}]}"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-some-future-model", 2).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("sparse"), e.getMessage());
  }

  @Test
  public void unknownModelIsAConfigFailure() {
    stub.otherwise(request -> JinaStubReplies.error(400, "INPUT_MODEL_NOT_FOUND", "Model not found"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v9", 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("INPUT_MODEL_NOT_FOUND"), e.getMessage());
  }

  @Test
  public void rejectedKeyIsAConfigFailureThatNeverEchoesIt() {
    stub.otherwise(request -> JinaStubReplies.error(401, "AUTH_INVALID_API_KEY", "Invalid API key " + KEY));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
    assertFalse(e.getMessage().contains(KEY), e.getMessage());
    assertTrue(e.getMessage().contains("file "), e.getMessage());
    assertEquals(1, stub.requests().size());
    stub.otherwise(request -> JinaStubReplies.error(403, "AUTHZ_INSUFFICIENT_BALANCE", "no balance"));
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x"))));
  }

  /** Providers echo part of the presented key in auth errors ("sk-ab***"): a 401/403 keeps only its code, never the detail. */
  @Test
  public void rejectedKeyFailureWithholdsTheProviderDetail() {
    String prefix = KEY.substring(0, 8);
    stub.otherwise(request -> JinaStubReplies.error(401, "AUTH_INVALID_API_KEY", "Invalid API key " + prefix + "***"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("HTTP 401 AUTH_INVALID_API_KEY"), e.getMessage());
    assertFalse(e.getMessage().contains(prefix), e.getMessage());
    assertFalse(e.getMessage().contains("Invalid API key"), e.getMessage());
    stub.otherwise(request -> JinaStubReplies.error(403, "AUTHZ_INSUFFICIENT_BALANCE", "no balance for " + prefix + "***"));
    e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x"))));
    assertTrue(e.getMessage().contains("HTTP 403 AUTHZ_INSUFFICIENT_BALANCE"), e.getMessage());
    assertFalse(e.getMessage().contains(prefix), e.getMessage());
  }

  @Test
  public void wrongDimensionAnswerIsAConfigFailure() {
    stub.otherwise(JinaStubReplies.embeddings(128));
    assertThrows(EmbeddingClientConfigException.class,
        () -> client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x"))));
  }

  @Test
  public void everyAttemptTakesARateLimitToken() throws Exception {
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "2");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "1");
    stub.then(JinaStubReplies.error(429, "RATE_REQUEST_LIMIT_EXCEEDED", "slow")).otherwise(JinaStubReplies.embeddings(256));
    JinaEmbeddingClient client = client(seamed(new AtomicLong(1_000L), new ArrayList<>()), "jina-embeddings-v3", 256);
    client.embed(Arrays.asList(text("a", "x"), text("b", "y")));
    assertEquals(3, stub.requests().size());
    // three attempts at 2 requests/s, burst 1: two token waits of 500 ms, less the backoff sleep (at most 1 ms) that refills one;
    // without a token per retry the wait would be one 500 ms
    assertTrue(client.lastCallStats().getRateLimitWaitMillis() >= 990, client.lastCallStats().toString());
  }

  @Test
  public void keyFromTheEnvironmentAndMissingKeyRefusedAtInit() throws Exception {
    props.remove(key(JinaClientConfig.CREDENTIAL_FILE_SUFFIX));
    stub.otherwise(JinaStubReplies.embeddings(256));
    JinaEmbeddingClient fromEnv = new JinaEmbeddingClient(System::currentTimeMillis, Thread::sleep, new Random(1),
        name -> JinaClientConfig.DEFAULT_CREDENTIAL_ENV.equals(name) ? KEY : null);
    client(fromEnv, "jina-embeddings-v3", 256).embed(Collections.singletonList(text("a", "x")));
    assertEquals("Bearer " + KEY, stub.requests().get(0).header("Authorization"));
    JinaEmbeddingClient noKey = new JinaEmbeddingClient(System::currentTimeMillis, Thread::sleep, new Random(1), name -> null);
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class,
        () -> client(noKey, "jina-embeddings-v3", 256));
    assertTrue(e.getMessage().contains("env " + JinaClientConfig.DEFAULT_CREDENTIAL_ENV), e.getMessage());
    props.setProperty(key(JinaClientConfig.CREDENTIAL_ENV_SUFFIX), KEY + "-pasted value");
    IllegalArgumentException pasted = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientFactory.checkConfig(spec("jina-embeddings-v3", 256), props));
    assertFalse(pasted.getMessage().contains(KEY));
  }

  @Test
  public void emptyTextIsPermanentWithoutARequest() throws Exception {
    EmbeddingResult result = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256)
        .embed(Collections.singletonList(text("a", ""))).get(0);
    assertEquals(EmbeddingResult.Status.PERMANENT_FAILURE, result.getStatus());
    assertTrue(result.isKnownInputFailure(), "local validation must not become a whole-call refusal");
    assertNull(result.getVector());
    assertEquals(0, stub.requests().size());
  }

  @Test
  public void interruptedCallStopsImmediatelyAndIsNotAnOutage() throws Exception {
    props.setProperty(key(OpenAiClientConfig.OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), "1");
    stub.otherwise(JinaStubReplies.embeddings(256));
    JinaEmbeddingClient client = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256);
    Thread.currentThread().interrupt();
    try {
      assertThrows(InterruptedException.class, () -> client.embed(Arrays.asList(text("a", "x"), text("b", "y"))));
    } finally {
      Thread.interrupted();
    }
    assertEquals(0, stub.requests().size());
    assertEquals(EmbeddingResult.Status.OK, client.embed(Collections.singletonList(text("c", "z"))).get(0).getStatus());
  }

  @Test
  public void incompleteEntryIsMalformedAndRetried() throws Exception {
    stub.then(request -> Reply.json(200, "{\"data\":[{\"object\":\"embedding\"}]}"));
    stub.otherwise(JinaStubReplies.embeddings(256));
    EmbeddingResult result = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus());
    assertEquals(2, stub.requests().size());
  }

  // ---------------------------------------------------------------- helpers

  @ParameterizedTest
  @ValueSource(strings = {"1e100", "NaN"})
  public void nonFiniteVectorElementsAreRetryable(String element) throws Exception {
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "1");
    String values = element + ",0.25".repeat(255);
    stub.otherwise(request -> Reply.json(200, "{\"data\":[{\"index\":0,\"embedding\":[" + values + "]}]}"));
    EmbeddingResult result = client(new JinaEmbeddingClient(), "jina-embeddings-v3", 256)
        .embed(Collections.singletonList(text("a", "x"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().startsWith("malformed answer"), result.getError());
    assertNull(result.getVector());
    assertEquals(1, stub.requests().size());
  }

  private JinaEmbeddingClient client(JinaEmbeddingClient client, String model, int dimension) {
    client.init(spec(model, dimension), props);
    return client;
  }

  private static JinaEmbeddingClient seamed(AtomicLong now, List<Long> sleeps) {
    return new JinaEmbeddingClient(now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    }, new Random(7), name -> null);
  }

  private static EmbeddingModelSpec spec(String model, int dimension) {
    return new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, dimension, null, JinaEmbeddingClient.PROVIDER, model);
  }

  private static EmbeddingInput text(String sha, String text) {
    return new EmbeddingInput(sha, 0, EmbeddingTableSchema.MODALITY_TEXT, text);
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
