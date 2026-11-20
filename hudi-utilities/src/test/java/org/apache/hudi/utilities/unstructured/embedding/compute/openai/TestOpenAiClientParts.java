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
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;
import org.apache.hudi.utilities.unstructured.embedding.compute.RetryAfter;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.ErrorClassifier.ErrorBody;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of the OpenAI-compatible client that need no endpoint: config keys and the known-model table, the
 * credential source, Retry-After parsing, error classification and request planning.
 */
class TestOpenAiClientParts {

  private static final String ALIAS = "oai";
  private static final String KEY = "sk-test-ABCDEFGHIJKLMNOP1234";

  @TempDir
  Path tempDir;

  // ---------------------------------------------------------------- request planner

  @Test
  void testPlannerSplitsByInputCount() {
    int[] ones = new int[2049];
    Arrays.fill(ones, 1);
    List<int[]> plan = RequestPlanner.plan(ones, 2048, 300_000L);
    assertEquals(2, plan.size());
    assertArrayEquals(new int[] {0, 2048}, plan.get(0));
    assertArrayEquals(new int[] {2048, 2049}, plan.get(1));
  }

  @Test
  void testPlannerSplitsByTokenBudget() {
    int[] hundreds = new int[10];
    Arrays.fill(hundreds, 100);
    List<int[]> plan = RequestPlanner.plan(hundreds, 2048, 350L);
    assertEquals(4, plan.size());
    assertArrayEquals(new int[] {0, 3}, plan.get(0));
    assertArrayEquals(new int[] {3, 6}, plan.get(1));
    assertArrayEquals(new int[] {6, 9}, plan.get(2));
    assertArrayEquals(new int[] {9, 10}, plan.get(3));
  }

  @Test
  void testPlannerGivesAnOversizedItemItsOwnRequest() {
    List<int[]> plan = RequestPlanner.plan(new int[] {10, 500, 10}, 2048, 350L);
    assertEquals(3, plan.size());
    assertArrayEquals(new int[] {1, 2}, plan.get(1));
    assertEquals(Collections.emptyList(), RequestPlanner.plan(new int[0], 2048, 350L));
  }

  // ---------------------------------------------------------------- Retry-After

  @Test
  void testRetryAfterHints() {
    long now = 1_760_000_000_000L;
    assertEquals(1500, RetryAfter.hintMillis("1500", "9", now), "retry-after-ms wins");
    assertEquals(2000, RetryAfter.hintMillis(null, "2", now));
    String inThreeSeconds = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(now + 3000), ZoneOffset.UTC));
    assertEquals(3000, RetryAfter.hintMillis(null, inThreeSeconds, now));
    String past = DateTimeFormatter.RFC_1123_DATE_TIME.format(
        ZonedDateTime.ofInstant(Instant.ofEpochMilli(now - 60_000), ZoneOffset.UTC));
    assertEquals(0, RetryAfter.hintMillis(null, past, now), "a date in the past means retry now");
    assertEquals(-1, RetryAfter.hintMillis(null, "soon", now));
    assertEquals(-1, RetryAfter.hintMillis("x", null, now));
    assertEquals(-1, RetryAfter.hintMillis(null, null, now));
    assertEquals(-1, RetryAfter.hintMillis(null, "-5", now));
    assertEquals(Long.MAX_VALUE, RetryAfter.hintMillis(null, String.valueOf(Long.MAX_VALUE / 10), now),
        "a huge delta-seconds saturates instead of overflowing");
  }

  // ---------------------------------------------------------------- error classification

  /**
   * OpenAI's billing 429s ("This is a billing error, not a rate limit." "Retrying billing, spend, or quota errors won't restore
   * access.", https://developers.openai.com/api/docs/guides/error-codes, read 2026-10-27) are out of quota, never a rate limit.
   */
  @Test
  void testBillingRateLimitCodesAreQuota() {
    for (String code : new String[] {"credit_balance_exhausted", "organization_spend_limit_exceeded", "project_spend_limit_exceeded",
        "organization_usage_limit_exceeded"}) {
      assertEquals(ProviderResponseKind.QUOTA, ErrorClassifier.classify(OpenAiProvider.OPENAI, 429,
          openAiError("billing", null, null, code)), code);
    }
    assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(OpenAiProvider.OPENAI, 429,
        openAiError("Your request rate increased too quickly.", "rate_limit_error", null, "slow_down")));
  }

  /** The official SDK retries 409 Conflict ("Retry on lock timeouts", openai-python _base_client._should_retry). */
  @Test
  void test409IsRetryable() {
    for (OpenAiProvider provider : OpenAiProvider.values()) {
      assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(provider, 409, ErrorBody.parse(new byte[0])),
          provider.id());
    }
  }

  @Test
  void testClassification() {
    OpenAiProvider oai = OpenAiProvider.OPENAI;
    assertEquals(ProviderResponseKind.OK, ErrorClassifier.classify(oai, 200, ErrorBody.parse(new byte[0])));
    assertEquals(ProviderResponseKind.QUOTA, ErrorClassifier.classify(oai, 429, openAiError("quota", "insufficient_quota", null, "insufficient_quota")));
    assertEquals(ProviderResponseKind.QUOTA, ErrorClassifier.classify(oai, 429, openAiError("quota", null, null, "insufficient_quota")));
    assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(oai, 429, openAiError("Rate limit reached", "requests", null, "rate_limit_exceeded")));
    for (int status : new int[] {408, 500, 502, 503, 504, 599}) {
      assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(oai, status, ErrorBody.parse(new byte[0])), String.valueOf(status));
    }
    for (int status : new int[] {401, 403, 404, 418}) {
      assertEquals(ProviderResponseKind.CONFIG, ErrorClassifier.classify(oai, status, ErrorBody.parse(new byte[0])), String.valueOf(status));
    }
    for (String param : new String[] {"model", "dimensions", "encoding_format", "user"}) {
      assertEquals(ProviderResponseKind.CONFIG, ErrorClassifier.classify(oai, 400, openAiError("bad " + param, "invalid_request_error", param, null)), param);
    }
    assertEquals(ProviderResponseKind.INPUT, ErrorClassifier.classify(oai, 400, openAiError("too long", "invalid_request_error", "input", null)));
    assertEquals(ProviderResponseKind.INPUT, ErrorClassifier.classify(oai, 400, ErrorBody.parse(new byte[0])));
    assertEquals(ProviderResponseKind.INPUT, ErrorClassifier.classify(oai, 413, ErrorBody.parse(new byte[0])));
    assertEquals(ProviderResponseKind.INPUT, ErrorClassifier.classify(oai, 422, ErrorBody.parse(new byte[0])));
    assertEquals(ProviderResponseKind.CONFIG, ErrorClassifier.classify(oai, 424, ErrorBody.parse(new byte[0])), "424 is TEI's");
    assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(OpenAiProvider.HF_TEI, 424,
        ErrorBody.parse("{\"error\":\"Inference failed\",\"error_type\":\"backend\"}".getBytes(StandardCharsets.UTF_8))));
    assertEquals(ProviderResponseKind.RETRYABLE, ErrorClassifier.classify(OpenAiProvider.HF_TEI, 424, ErrorBody.parse(new byte[0])),
        "any runtime 424 is retryable; a non-embedding model is caught by GET /info at init");
  }

  @Test
  void testErrorBodyParsing() {
    ErrorBody openAi = openAiError("Invalid value for 'dimensions'", "invalid_request_error", "dimensions", "invalid_value");
    assertEquals("Invalid value for 'dimensions'", openAi.message());
    assertEquals("dimensions", openAi.param());
    assertEquals("invalid_value", openAi.code());
    assertEquals("invalid_request_error", openAi.type());

    ErrorBody tei = ErrorBody.parse("{\"error\":\"Input validation error\",\"error_type\":\"validation\"}"
        .getBytes(StandardCharsets.UTF_8));
    assertEquals("Input validation error", tei.message());
    assertEquals("validation", tei.type());
    assertNull(tei.param());

    ErrorBody teiCompat = ErrorBody.parse("{\"message\":\"Batch size error\",\"code\":413,\"error_type\":\"validation\"}"
        .getBytes(StandardCharsets.UTF_8));
    assertEquals("Batch size error", teiCompat.message());
    assertEquals("validation", teiCompat.type());

    ErrorBody html = ErrorBody.parse("<html>Bad Gateway</html>".getBytes(StandardCharsets.UTF_8));
    assertEquals("<html>Bad Gateway</html>", html.message());
    assertNull(html.param());

    StringBuilder longBody = new StringBuilder();
    for (int i = 0; i < 2000; i++) {
      longBody.append('x');
    }
    assertEquals(ErrorBody.MAX_MESSAGE_CHARS, ErrorBody.parse(longBody.toString().getBytes(StandardCharsets.UTF_8))
        .message().length());
    assertNull(ErrorBody.parse(null).message());
  }

  private static ErrorBody openAiError(String message, String type, String param, String code) {
    String json = "{\"error\":{\"message\":\"" + message + "\",\"type\":" + quoted(type) + ",\"param\":" + quoted(param)
        + ",\"code\":" + quoted(code) + "}}";
    return ErrorBody.parse(json.getBytes(StandardCharsets.UTF_8));
  }

  private static String quoted(String value) {
    return value == null ? "null" : "\"" + value + "\"";
  }

  // ---------------------------------------------------------------- config

  @Test
  void testOpenAiDefaults() {
    OpenAiClientConfig config = config("openai", "text-embedding-3-small", 1536, new TypedProperties());
    assertEquals(OpenAiProvider.OPENAI, config.provider());
    assertEquals("https://api.openai.com/v1", config.endpoint());
    assertEquals("text-embedding-3-small", config.modelName());
    assertEquals("text-embedding-3-small", config.baseModel());
    assertTrue(config.knownModel().isPresent());
    assertNull(config.dimensionsParam(), "native dimension: not sent");
    assertEquals(2048, config.maxInputs());
    assertEquals(300_000L, config.maxTokens());
    assertEquals(8191, config.maxInputTokens());
    assertEquals("cl100k_base", config.encoding());
    assertEquals(30_000, config.requestTimeoutMs());
    assertEquals(300_000, config.deadlineMs());
    assertEquals(32, config.bisectMaxRequests());
    assertEquals(5, config.retryMaxAttempts());
    assertEquals(500, config.initialBackoffMs());
    assertEquals(60_000, config.maxBackoffMs());
    assertEquals(3, config.outageConsecutiveFailures());
    assertNull(config.credentialFile());
    assertEquals("OPENAI_API_KEY", config.credentialEnv());
  }

  @Test
  void testDimensionsRules() {
    assertEquals(Integer.valueOf(512), config("openai", "text-embedding-3-small", 512, new TypedProperties()).dimensionsParam());
    assertNull(config("openai", "text-embedding-3-large", 3072, new TypedProperties()).dimensionsParam());
    assertNull(config("openai", "text-embedding-ada-002", 1536, new TypedProperties()).dimensionsParam());
    TypedProperties adaProps = new TypedProperties();
    IllegalArgumentException ada = assertThrows(IllegalArgumentException.class,
        () -> config("openai", "text-embedding-ada-002", 512, adaProps));
    assertTrue(ada.getMessage().contains(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX), ada.getMessage());
    TypedProperties largeProps = new TypedProperties();
    assertThrows(IllegalArgumentException.class, () -> config("openai", "text-embedding-3-large", 4000, largeProps));
    // an unknown model needs its per-input limit and always gets the dimensions parameter
    TypedProperties unknown = new TypedProperties();
    unknown.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX), "4096");
    OpenAiClientConfig custom = config("openai", "my-future-model", 8, unknown);
    assertFalse(custom.knownModel().isPresent());
    assertEquals(Integer.valueOf(8), custom.dimensionsParam());
    assertEquals(4096, custom.maxInputTokens());
    TypedProperties noLimit = new TypedProperties();
    IllegalArgumentException missing = assertThrows(IllegalArgumentException.class,
        () -> config("openai", "my-future-model", 8, noLimit));
    assertTrue(missing.getMessage().contains(key(OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX)), missing.getMessage());
  }

  @Test
  void testAzureNeedsAnEndpointAndUsesTheBaseModelRow() {
    TypedProperties noEndpointProps = new TypedProperties();
    IllegalArgumentException noEndpoint = assertThrows(IllegalArgumentException.class,
        () -> config("azure-openai", "my-dep", 1536, noEndpointProps));
    assertTrue(noEndpoint.getMessage().contains(key(OpenAiClientConfig.ENDPOINT_SUFFIX)), noEndpoint.getMessage());

    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "https://res.openai.azure.com/openai/v1/");
    props.setProperty(key(OpenAiClientConfig.BASE_MODEL_SUFFIX), "text-embedding-ada-002");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX), "2046");
    OpenAiClientConfig azure = config("azure-openai", "my-dep", 1536, props);
    assertEquals(OpenAiProvider.AZURE_OPENAI, azure.provider());
    assertEquals("https://res.openai.azure.com/openai/v1", azure.endpoint(), "no trailing slash");
    assertEquals("my-dep", azure.modelName());
    assertEquals("text-embedding-ada-002", azure.baseModel());
    assertEquals(2046, azure.maxInputTokens(), "an ada-002 v1 deployment lowers the per-input limit");
    assertEquals("AZURE_OPENAI_API_KEY", azure.credentialEnv());

    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX), "9000");
    assertThrows(IllegalArgumentException.class, () -> config("azure-openai", "my-dep", 1536, props),
        "the per-input limit may only be lowered");
  }

  @Test
  void testEndpointMustBeAnAbsoluteHttpUrl() {
    for (String endpoint : new String[] {"http://exa mple/v1", "ftp://host/v1", "api.openai.com/v1",
        "https://user:" + KEY + "@api.example.com/v1", "https://api.example.com/v1?api-key=" + KEY,
        "https://api.example.com/v1#" + KEY}) {
      TypedProperties props = new TypedProperties();
      props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), endpoint);
      IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class,
          () -> config("hf-tei", "bge", 1024, props), endpoint);
      assertTrue(thrown.getMessage().contains(key(OpenAiClientConfig.ENDPOINT_SUFFIX)), thrown.getMessage());
      assertFalse(thrown.getMessage().contains(KEY), "the endpoint value is not echoed: " + thrown.getMessage());
    }
    // the neighbours that stay valid: a port, a path, a trailing slash
    TypedProperties valid = new TypedProperties();
    valid.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "http://tei.internal:8080/v1/");
    assertEquals("http://tei.internal:8080/v1", config("hf-tei", "bge", 1024, valid).endpoint());
  }

  @Test
  void testLimitsAreBoundedByTheProvider() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "4096");
    assertThrows(IllegalArgumentException.class, () -> config("openai", "text-embedding-3-small", 1536, props));
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_INPUTS_SUFFIX), "16");
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_TOKENS_SUFFIX), "400000");
    assertThrows(IllegalArgumentException.class, () -> config("openai", "text-embedding-3-small", 1536, props));
    props.setProperty(key(OpenAiClientConfig.REQUEST_MAX_TOKENS_SUFFIX), "1000");
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "0");
    IllegalArgumentException zero = assertThrows(IllegalArgumentException.class,
        () -> config("openai", "text-embedding-3-small", 1536, props));
    assertTrue(zero.getMessage().contains(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX)), zero.getMessage());
    props.setProperty(key(OpenAiClientConfig.RETRY_MAX_ATTEMPTS_SUFFIX), "2");
    OpenAiClientConfig config = config("openai", "text-embedding-3-small", 1536, props);
    assertEquals(16, config.maxInputs());
    assertEquals(1000L, config.maxTokens());
    assertEquals(2, config.retryMaxAttempts());
  }

  @Test
  void testTeiTakesLimitsFromInfoAndHasNoTokenizer() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "http://tei:8080/v1");
    OpenAiClientConfig tei = config("hf-tei", "BAAI/bge-large-en-v1.5", 1024, props);
    assertEquals(OpenAiProvider.HF_TEI, tei.provider());
    assertEquals(-1, tei.maxInputs());
    assertEquals(-1L, tei.maxTokens());
    assertEquals(0, tei.maxInputTokens(), "no local per-input check");
    assertNull(tei.encoding());
    assertNull(tei.credentialEnv(), "TEI auth is optional");
    assertEquals("http://tei:8080/info", OpenAiProvider.HF_TEI.infoUrl(tei.endpoint()));
  }

  @Test
  void testUnknownProviderIsRejected() {
    TypedProperties props = new TypedProperties();
    assertThrows(IllegalArgumentException.class, () -> config("vertex", "x", 8, props));
    assertFalse(OpenAiProvider.fromId("dummy").isPresent());
    assertEquals(OpenAiProvider.AZURE_OPENAI, OpenAiProvider.fromId("azure-openai").get());
  }

  @Test
  void testAuthorizationHeaders() {
    Map<String, String> headers = new HashMap<>();
    OpenAiProvider.OPENAI.authorize(headers, KEY);
    assertEquals(Collections.singletonMap("Authorization", "Bearer " + KEY), headers);
    headers.clear();
    OpenAiProvider.AZURE_OPENAI.authorize(headers, KEY);
    assertEquals(Collections.singletonMap("api-key", KEY), headers);
    headers.clear();
    OpenAiProvider.HF_TEI.authorize(headers, KEY);
    assertEquals(Collections.singletonMap("Authorization", "Bearer " + KEY), headers);
  }

  // ---------------------------------------------------------------- credentials

  @Test
  void testCredentialFileWinsOverEnv() throws IOException {
    Path keyFile = tempDir.resolve("openai.key");
    Files.write(keyFile, (KEY + "\n").getBytes(StandardCharsets.UTF_8));
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX), keyFile.toString());
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), "MY_KEY");
    OpenAiClientConfig config = config("openai", "text-embedding-3-small", 1536, props);
    CredentialSource source = CredentialSource.of(config);
    assertEquals("file " + keyFile, source.describe());
    assertEquals(KEY, source.read(name -> "from-env").get());
    assertFalse(config.toString().contains(KEY), config.toString());
    assertFalse(props.toString().contains(KEY), "only the path is a Hudi property");
  }

  @Test
  void testCredentialFromTheNamedEnvVar() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), "MY_KEY");
    CredentialSource source = CredentialSource.of(config("openai", "text-embedding-3-small", 1536, props));
    assertEquals("env MY_KEY", source.describe());
    assertEquals(KEY, source.read(name -> "MY_KEY".equals(name) ? KEY : null).get());
  }

  @Test
  void testMissingCredentialNamesItsSourceOnly() throws IOException {
    CredentialSource env = CredentialSource.of(config("openai", "text-embedding-3-small", 1536, new TypedProperties()));
    IllegalStateException unset = assertThrows(IllegalStateException.class, () -> env.read(name -> null));
    assertTrue(unset.getMessage().contains("env OPENAI_API_KEY"), unset.getMessage());
    assertThrows(IllegalStateException.class, () -> env.read(name -> "  "));

    Path empty = tempDir.resolve("empty.key");
    Files.write(empty, new byte[0]);
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX), empty.toString());
    CredentialSource file = CredentialSource.of(config("openai", "text-embedding-3-small", 1536, props));
    assertThrows(IllegalStateException.class, () -> file.read(name -> KEY));
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_FILE_SUFFIX), tempDir.resolve("absent.key").toString());
    CredentialSource absentFile = CredentialSource.of(config("openai", "text-embedding-3-small", 1536, props));
    IllegalStateException absent = assertThrows(IllegalStateException.class, () -> absentFile.read(name -> KEY));
    assertFalse(absent.getMessage().contains(KEY), absent.getMessage());
  }

  @Test
  void testCredentialEnvMustBeANameAndIsNeverEchoed() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX), KEY);
    IllegalArgumentException pasted = assertThrows(IllegalArgumentException.class,
        () -> config("openai", "text-embedding-3-small", 1536, props));
    assertTrue(pasted.getMessage().contains(key(OpenAiClientConfig.CREDENTIAL_ENV_SUFFIX)), pasted.getMessage());
    assertFalse(pasted.getMessage().contains(KEY), pasted.getMessage());
  }

  @Test
  void testTeiWithoutCredentialSendsNoKey() {
    TypedProperties props = new TypedProperties();
    props.setProperty(key(OpenAiClientConfig.ENDPOINT_SUFFIX), "http://tei:8080/v1");
    CredentialSource none = CredentialSource.of(config("hf-tei", "bge", 1024, props));
    assertEquals("none", none.describe());
    assertFalse(none.read(name -> KEY).isPresent());
  }

  // ---------------------------------------------------------------- helpers

  private static OpenAiClientConfig config(String provider, String model, int dimension, TypedProperties props) {
    EmbeddingModelSpec spec = new EmbeddingModelSpec(ALIAS, "emb_" + ALIAS, dimension, null, provider, model);
    return OpenAiClientConfig.from(spec, props);
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
