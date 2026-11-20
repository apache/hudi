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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop.Answer;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.RetryAfter;
import org.apache.hudi.utilities.unstructured.embedding.compute.SingleTokenReader;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * Text embeddings over Jina's HTTP endpoint, with bearer credentials from a file or named environment variable.
 * Text object inputs are packed up to the configured request limit, and response indices restore input order.
 * Transient and malformed responses follow {@link ProviderRetryLoop}; rejected packed inputs are isolated.
 * Configuration failures fail the call, while an input rejected alone fails permanently.
 */
public class JinaEmbeddingClient implements EmbeddingClient {

  private static final Logger LOG = LoggerFactory.getLogger(JinaEmbeddingClient.class);
  /** Provider id of the Jina clients ({@code hoodie.unstructured.embedding.model.<alias>.provider}). */
  public static final String PROVIDER = "jina";
  static final String EMPTY_INPUT = "empty input";
  static final String REDACTED = "***";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private static final int MAX_ERROR_CHARS = 1024;
  /** Jina: the model named in the request does not exist (400). */
  private static final String JINA_MODEL_NOT_FOUND = "INPUT_MODEL_NOT_FOUND";

  private final LongSupplier clock;
  private final ProviderRetryLoop.Sleeper sleeper;
  private final Random jitter;
  private final UnaryOperator<String> environment;
  private EmbeddingModelSpec spec;
  private JinaClientConfig config;
  private String key;
  private ProviderRetryLoop loop;
  private EmbeddingCallStats lastCallStats = EmbeddingCallStats.NONE;

  /** Production wiring: the system clock, real sleeps, the process environment. */
  public JinaEmbeddingClient() {
    this(System::currentTimeMillis, Thread::sleep, new SecureRandom(), System::getenv);
  }

  /** Test seam: the clock and sleeper of the retry loop and rate limiter, the backoff jitter, the environment. */
  JinaEmbeddingClient(LongSupplier clock, ProviderRetryLoop.Sleeper sleeper, Random jitter, UnaryOperator<String> environment) {
    this.clock = clock;
    this.sleeper = sleeper;
    this.jitter = jitter;
    this.environment = environment;
  }

  /** @throws EmbeddingClientConfigException naming the offending key, or when the API key cannot be read */
  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
    try {
      this.config = JinaClientConfig.from(spec, props);
    } catch (IllegalArgumentException e) {
      throw new EmbeddingClientConfigException(e.getMessage());
    }
    this.key = readKey(config, environment);
    RequestRateLimiter rateLimiter = RequestRateLimiter.from(spec, props, clock, sleeper::sleep);
    this.loop = new ProviderRetryLoop("model=" + spec.getName() + " provider=" + PROVIDER, config.settings(), rateLimiter, clock,
        sleeper, jitter);
  }

  @Override
  public EmbeddingModelSpec spec() {
    return spec;
  }

  @Override
  public EmbeddingCallStats lastCallStats() {
    return lastCallStats;
  }

  @Override
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws InterruptedException {
    lastCallStats = EmbeddingCallStats.NONE;
    if (loop.outageOpen()) {
      throw new EmbeddingProviderUnavailableException(ProviderRetryLoop.OUTAGE + " is open for model " + spec.getName() + " ("
          + PROVIDER + " " + config.endpoint() + ") after " + loop.consecutiveFailures()
          + " consecutive failed requests; the remaining rows of this task are left for a later run");
    }
    loop.startCall();
    Call call = new Call(inputs.size());
    try {
      List<Integer> texts = new ArrayList<>();
      for (int i = 0; i < inputs.size(); i++) {
        EmbeddingInput input = inputs.get(i);
        if (EmbeddingTableSchema.MODALITY_TEXT.equals(input.getModality())) {
          if (input.getText() == null || input.getText().trim().isEmpty()) {
            call.results[i] = EmbeddingResult.invalidInput(EMPTY_INPUT);
          } else {
            texts.add(i);
          }
        } else {
          call.results[i] = EmbeddingResult.invalidInput("modality " + input.getModality() + " is not supported by " + PROVIDER
              + " model " + config.modelName());
        }
      }
      int maxInputs = config.settings().maxInputs();
      for (int start = 0; start < texts.size(); start += maxInputs) {
        sendTexts(call, inputs, texts.subList(start, Math.min(texts.size(), start + maxInputs)));
      }
    } finally {
      // also when a config failure is thrown: the stats then describe the requests that led to it
      lastCallStats = loop.stats(call.isolationRequests, call.tokens);
    }
    return Arrays.asList(call.results);
  }

  private void sendTexts(Call call, List<EmbeddingInput> inputs, List<Integer> request) throws InterruptedException {
    String notSent = loop.notSentReason();
    if (notSent != null) {
      call.fill(request, EmbeddingResult.retryable(notSent));
      return;
    }
    ArrayNode items = MAPPER.createArrayNode();
    for (int index : request) {
      items.addObject().put("text", inputs.get(index).getText());
    }
    byte[] body = body(items);
    settle(call, request, loop.send(() -> attempt(call, body, request.size())), () -> {
      for (int index : request) {
        call.isolationRequests++;
        sendTexts(call, inputs, Collections.singletonList(index));
      }
    });
  }

  /** Applies a request's answer; {@code isolate} sends each input of a rejected multi-input request alone. */
  private void settle(Call call, List<Integer> request, Answer<float[][]> answer, ProviderRetryLoop.Preparation isolate)
      throws InterruptedException {
    if (answer.isInterrupted()) {
      Thread.interrupted();
      throw new InterruptedException(answer.error());
    }
    switch (answer.kind()) {
      case OK:
        float[][] vectors = answer.value();
        for (int i = 0; i < request.size(); i++) {
          if (vectors[i].length != spec.getDimension()) {
            throw configFailure("Jina returned " + vectors[i].length + "-dimension vectors, but the model's .dimensions is "
                + spec.getDimension());
          }
          call.results[request.get(i)] = EmbeddingResult.ok(vectors[i]);
        }
        break;
      case INPUT:
        if (request.size() == 1) {
          call.results[request.get(0)] = EmbeddingResult.permanent(answer.error());
          LOG.warn("EMBED-JINA-INPUT-REJECTED model={}: {}", spec.getName(), answer.error());
        } else {
          isolate.prepare();
        }
        break;
      case RETRYABLE:
        call.fill(request, EmbeddingResult.retryable(answer.error()));
        break;
      default:
        throw configFailure(answer.error());
    }
  }

  private Answer<float[][]> attempt(Call call, byte[] body, int expected) throws InterruptedException {
    HttpResponse<byte[]> response;
    try {
      HttpRequest request = HttpRequest.newBuilder(URI.create(config.endpoint() + "/embeddings"))
          .timeout(Duration.ofMillis(config.settings().requestTimeoutMs()))
          .header("Authorization", "Bearer " + key)
          .header("Content-Type", "application/json")
          .header("Accept", "application/json")
          .POST(HttpRequest.BodyPublishers.ofByteArray(body))
          .build();
      response = HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "Jina request failed: " + e.getClass().getSimpleName() + ": "
          + redact(e.getMessage()), -1);
    } catch (IllegalArgumentException e) {
      return Answer.failed(ProviderResponseKind.CONFIG, "the request cannot be sent: " + redact(e.getMessage()), -1);
    }
    int status = response.statusCode();
    return status / 100 == 2 ? okAnswer(call, response.body(), expected) : errorAnswer(response, status);
  }

  /** The answer of a 2xx: the parsed vectors; a non-dense answer is a configuration failure, a malformed one retryable. */
  private Answer<float[][]> okAnswer(Call call, byte[] body, int expected) {
    try {
      return Answer.ok(parse(call, body, expected));
    } catch (NonDenseAnswerException e) {
      return Answer.failed(ProviderResponseKind.CONFIG, e.getMessage(), -1);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: " + e.getMessage(), -1);
    }
  }

  /** The answer of an error status: classified by status and code, the provider's message kept unless it may echo the key. */
  private Answer<float[][]> errorAnswer(HttpResponse<byte[]> response, int status) {
    String detail = null;
    String code = null;
    try {
      JsonNode error = MAPPER.readTree(response.body());
      if (error != null && error.isObject()) {
        detail = error.path("detail").isTextual() ? error.path("detail").asText() : null;
        code = error.path("code").isTextual() ? error.path("code").asText() : null;
      }
    } catch (IOException e) {
      // not JSON: the raw text is the detail
    }
    if (detail == null) {
      detail = new String(response.body(), StandardCharsets.UTF_8);
    }
    ProviderResponseKind kind = classifyJina(status, code);
    // 401/403: providers echo part of the presented key in an auth error ("sk-ab***"), so only its code is kept
    String error = "Jina HTTP " + status + (code == null ? "" : " " + code)
        + (status == 401 || status == 403 ? " (provider message withheld)" : ": " + cut(redact(detail)));
    long hint = RetryAfter.hintMillis(null, response.headers().firstValue("Retry-After").orElse(null), clock.getAsLong());
    return Answer.failed(kind, error, hint);
  }

  /**
   * Jina {@code /v1/embeddings} errors (status codes and {@code code} values of its OpenAPI document, 2026.10.13.1624): 408,
   * 409 ({@code CONFLICT_RESOURCE_BUSY}), 429 and 5xx are transient; 401, 403 (including {@code AUTHZ_INSUFFICIENT_BALANCE}),
   * 404, a 400 {@value #JINA_MODEL_NOT_FOUND} and a 422 (the request failed schema validation, such as a dimension the model does not take) reject the configuration; any other
   * 400 (e.g. {@code INPUT_TOKEN_LIMIT_EXCEEDED}) and 413 reject an input of the request.
   */
  static ProviderResponseKind classifyJina(int status, String code) {
    if (status >= 200 && status < 300) {
      return ProviderResponseKind.OK;
    }
    if (status == 408 || status == 409 || status == 429 || status >= 500) {
      return ProviderResponseKind.RETRYABLE;
    }
    if (status == 400) {
      return JINA_MODEL_NOT_FOUND.equals(code) ? ProviderResponseKind.CONFIG : ProviderResponseKind.INPUT;
    }
    if (status == 413) {
      return ProviderResponseKind.INPUT;
    }
    return ProviderResponseKind.CONFIG;
  }

  private byte[] body(ArrayNode items) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", config.modelName());
    body.set("input", items);
    if (config.dimensionsParam() != null) {
      body.put("dimensions", config.dimensionsParam());
    }
    body.put("embedding_type", "float");
    try {
      return MAPPER.writeValueAsBytes(body);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot write a Jina request", e);
    }
  }

  /** {@code data[].index} / {@code data[].embedding}, every index once; {@code usage.total_tokens} when an integer. */
  private static float[][] parse(Call call, byte[] body, int expected) throws IOException {
    JsonNode root = MAPPER.readTree(body);
    JsonNode data = root == null ? null : root.get("data");
    if (data == null || !data.isArray()) {
      throw new IOException("no data array");
    }
    float[][] vectors = new float[expected][];
    for (JsonNode entry : data) {
      JsonNode index = entry.get("index");
      JsonNode embedding = entry.get("embedding");
      rejectNonDense(entry, index, embedding);
      if (index == null || !index.isInt() || index.asInt() < 0 || index.asInt() >= expected || vectors[index.asInt()] != null
          || embedding == null || !embedding.isArray() || embedding.size() == 0) {
        throw new IOException("data entry with index " + index + " for " + expected + " inputs, or without a float embedding");
      }
      vectors[index.asInt()] = floatVector(embedding);
    }
    for (int i = 0; i < expected; i++) {
      if (vectors[i] == null) {
        throw new IOException("no embedding for input " + i);
      }
    }
    JsonNode tokens = root.path("usage").path("total_tokens");
    if (tokens.isIntegralNumber() && tokens.canConvertToLong()) {
      call.tokens = Math.max(call.tokens, 0) + tokens.asLong();
    }
    return vectors;
  }

  /** Throws {@link NonDenseAnswerException} when a data entry is a multi-vector or sparse answer. */
  private static void rejectNonDense(JsonNode entry, JsonNode index, JsonNode embedding) {
    // the documented non-dense answers (MultiEmbeddingData's "embeddings", a sparse token map, an embedding object or array of
    // arrays): the model, not this answer, is wrong for a vector column
    boolean multi = entry.has("embeddings") || (embedding != null && embedding.isArray() && embedding.size() > 0
        && embedding.get(0).isArray());
    boolean sparse = (embedding != null && embedding.isObject()) || (embedding == null && index == null && isSparseMap(entry));
    if (multi || sparse) {
      throw new NonDenseAnswerException("Jina answered a " + (multi ? "multi-vector" : "sparse")
          + " embedding; the model must return one dense vector per input");
    }
  }

  private static boolean isSparseMap(JsonNode entry) {
    if (!entry.isObject() || entry.size() == 0) {
      return false;
    }
    for (JsonNode weight : entry) {
      if (!weight.isNumber()) {
        return false;
      }
    }
    return true;
  }

  /** Converts a non-empty JSON array of numbers to finite floats. */
  private static float[] floatVector(JsonNode embedding) throws IOException {
    float[] vector = new float[embedding.size()];
    for (int j = 0; j < vector.length; j++) {
      if (!embedding.get(j).isNumber()) {
        throw new IOException("an embedding holds a non-number");
      }
      vector[j] = (float) embedding.get(j).asDouble();
      if (!Float.isFinite(vector[j])) {
        throw new IOException("an embedding holds a non-finite float");
      }
    }
    return vector;
  }

  private EmbeddingClientConfigException configFailure(String error) {
    return new EmbeddingClientConfigException(PROVIDER + " " + config.endpoint() + " rejected the configuration of model "
        + spec.getName() + " (" + config.modelName() + ", credential " + config.describeCredential() + "): " + error);
  }

  private String redact(String text) {
    return text == null || key == null ? text : text.replace(key, REDACTED);
  }

  private static String cut(String text) {
    return text.length() <= MAX_ERROR_CHARS ? text : text.substring(0, MAX_ERROR_CHARS) + "…";
  }

  /** The API key: one printable token from the configured file or environment variable; the value never leaks. */
  private static String readKey(JinaClientConfig config, UnaryOperator<String> environment) {
    return SingleTokenReader.read(config.credentialFile(), config.credentialEnv(), environment, "Jina API key",
        config.describeCredential(), EmbeddingClientConfigException::new);
  }

  /** The configuration and credential SOURCE; never the key. */
  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + config + "}";
  }

  /** A well-formed 2xx whose embeddings are not dense vectors: the configured model cannot fill a VECTOR column. */
  private static final class NonDenseAnswerException extends RuntimeException {
    private NonDenseAnswerException(String message) {
      super(message);
    }
  }

  /** The per-call state: results by input position, isolation requests, reported tokens. */
  private static final class Call {
    private final EmbeddingResult[] results;
    private int isolationRequests;
    private long tokens = -1L;

    private Call(int size) {
      this.results = new EmbeddingResult[size];
    }

    private void fill(List<Integer> request, EmbeddingResult result) {
      for (int index : request) {
        results[index] = result;
      }
    }
  }
}
