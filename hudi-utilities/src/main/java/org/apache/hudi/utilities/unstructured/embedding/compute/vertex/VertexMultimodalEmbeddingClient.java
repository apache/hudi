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

package org.apache.hudi.utilities.unstructured.embedding.compute.vertex;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingComputePlatformException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.ImageFailures;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderRetryLoop.Answer;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.RetryAfter;
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * Vertex {@code multimodalembedding@001} {@code :predict} client: image and text inputs share one vector space, so a text query
 * can search the image vectors it writes. Each request carries one instance and the configured dimension:
 * <ul>
 *   <li>image row: {@code {"image":{"bytesBase64Encoded":…,"mimeType":…}}} with the image the compute flow prepared
 *       ({@link EmbeddingInput#image()}) and its type (image/jpeg by default, or image/png), read from
 *       {@code predictions[0].imageEmbedding}; a failure to prepare it is that input's outcome ({@link ImageFailures});</li>
 *   <li>text row: {@code {"text":…}}, read from {@code predictions[0].textEmbedding}. This endpoint shortens texts beyond 32
 *       tokens, so inputs must be chunked to at most 32 tokens.</li>
 * </ul>
 * A wrong output dimension or rejected configuration fails the call; an input rejected with HTTP 400 fails permanently.
 * Transient and malformed responses remain retryable. {@link VertexAccessTokens} refreshes tokens proactively and
 * after HTTP 401. Every send, including a refreshed resend, observes the rate limit and call deadline.
 */
public class VertexMultimodalEmbeddingClient implements EmbeddingClient {

  /** Provider id of this client ({@code hoodie.unstructured.embedding.model.<alias>.provider}). */
  public static final String PROVIDER = "vertex-multimodal";
  /** Error of an image input that reaches the client without a prepared image: a broken compute invariant. */
  public static final String IMAGE_WITHOUT_PREPARED_INPUT = "image input without a prepared image";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
  private static final int MAX_ERROR_BODY_CHARS = 300;
  /** The endpoint truncates text after this token count. */
  private static final String VERTEX_ENDPOINT = "Vertex endpoint ";
  /** google.rpc status of a 400 that rejects the configuration (model allowlisting, organization policy), not the input. */
  static final String FAILED_PRECONDITION = "FAILED_PRECONDITION";

  private final LongSupplier clock;
  private final RequestRateLimiter.Sleeper sleeper;
  private final UnaryOperator<String> environment;
  private final Random jitter = new SecureRandom();
  private EmbeddingModelSpec spec;
  private VertexClientConfig config;
  private VertexAccessTokens tokens;
  private RequestRateLimiter rateLimiter = RequestRateLimiter.unlimited();
  private ProviderRetryLoop loop;
  private EmbeddingCallStats lastCallStats = EmbeddingCallStats.NONE;
  /** Re-sends after a token refresh in the current call, and their rate-limit wait: the loop counts one request per attempt. */
  private int callResends;
  private long callResendWaitMillis;
  /** The bearer token of the last request sent, for the refresh its 401 asks for. */
  private String sentToken;

  /** Production wiring: the system clock, real sleeps and the process environment. */
  public VertexMultimodalEmbeddingClient() {
    this(System::currentTimeMillis, Thread::sleep);
  }

  /** Test seam: the clock and sleeper of the rate limiter and of the access tokens. */
  VertexMultimodalEmbeddingClient(LongSupplier clock, RequestRateLimiter.Sleeper sleeper) {
    this(clock, sleeper, System::getenv);
  }

  /** Test seam: also the environment the credential source reads. */
  VertexMultimodalEmbeddingClient(LongSupplier clock, RequestRateLimiter.Sleeper sleeper, UnaryOperator<String> environment) {
    this.clock = clock;
    this.sleeper = sleeper;
    this.environment = environment;
  }

  /** Text and images, whatever the model name. */
  @Override
  public Set<String> supportedModalities(String modelName) {
    return TEXT_AND_IMAGE;
  }

  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
    try {
      this.config = VertexClientConfig.from(spec, props);
    } catch (IllegalArgumentException e) {
      throw new EmbeddingClientConfigException(e.getMessage());
    }
    this.tokens = VertexAccessTokens.of(config, clock, sleeper, environment);
    this.rateLimiter = RequestRateLimiter.from(spec, props, clock, sleeper);
    this.loop = new ProviderRetryLoop("model=" + spec.getName() + " provider=" + PROVIDER, config.settings(), rateLimiter, clock,
        sleeper::sleep, jitter);
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
    callResends = 0;
    callResendWaitMillis = 0;
    try {
      List<EmbeddingResult> results = new ArrayList<>(inputs.size());
      for (EmbeddingInput input : inputs) {
        results.add(embedOne(input));
      }
      return results;
    } finally {
      // also when a config failure is thrown: the stats then describe the requests that led to it
      EmbeddingCallStats loopStats = loop.stats(0, -1L);
      lastCallStats = new EmbeddingCallStats(loopStats.getRequests() + callResends, loopStats.getRetries(), 0, -1L,
          loopStats.getWallMillis(), loopStats.getRateLimitWaitMillis() + callResendWaitMillis);
    }
  }

  private EmbeddingResult embedOne(EmbeddingInput input) throws InterruptedException {
    ObjectNode instance = MAPPER.createObjectNode();
    String vectorField;
    if (EmbeddingTableSchema.MODALITY_IMAGE.equals(input.getModality())) {
      if (input.image() == null) {
        // compute builds every image input of a multimodal client with its preparation: a missing one is a broken invariant
        throw new EmbeddingComputePlatformException(IMAGE_WITHOUT_PREPARED_INPUT + " (model " + spec.getName() + ")");
      }
      String notSent = loop.notSentReason();
      if (notSent != null) {
        return EmbeddingResult.retryable(notSent);
      }
      PreparedImage image;
      try {
        image = input.image().get();
      } catch (IOException | RuntimeException e) {
        return ImageFailures.result(input, e);
      }
      ObjectNode imageNode = instance.putObject("image");
      imageNode.put("bytesBase64Encoded", Base64.getEncoder().encodeToString(image.getEncoded()));
      imageNode.put("mimeType", image.getMimeType());
      vectorField = "imageEmbedding";
    } else if (EmbeddingTableSchema.MODALITY_TEXT.equals(input.getModality()) && input.getText() != null) {
      String notSent = loop.notSentReason();
      if (notSent != null) {
        return EmbeddingResult.retryable(notSent);
      }
      instance.put("text", input.getText());
      vectorField = "textEmbedding";
    } else {
      return EmbeddingResult.permanent("modality " + input.getModality() + " is not supported by " + PROVIDER);
    }
    ObjectNode request = MAPPER.createObjectNode();
    request.putArray("instances").add(instance);
    request.putObject("parameters").put("dimension", spec.getDimension());
    return call(request, vectorField);
  }

  private EmbeddingResult call(ObjectNode request, String vectorField) throws InterruptedException {
    byte[] body;
    try {
      body = MAPPER.writeValueAsBytes(request);
    } catch (IOException e) {
      return EmbeddingResult.retryable("Vertex request failed: " + e.getClass().getSimpleName() + ": " + e.getMessage());
    }
    Answer<float[]> answer = loop.send(() -> sentToken = tokens.current(), () -> attempt(body, vectorField));
    if (answer.isInterrupted()) {
      // an interrupt ends the call as an interrupt, never as the input's failure or an outage
      Thread.interrupted();
      throw new InterruptedException(answer.error());
    }
    switch (answer.kind()) {
      case OK:
        return EmbeddingResult.ok(answer.value());
      case INPUT:
        // each request answers one input, so a rejection is specific to that input
        return EmbeddingResult.permanent(answer.error());
      case RETRYABLE:
        return EmbeddingResult.retryable(answer.error());
      default:
        throw new EmbeddingClientConfigException(answer.error());
    }
  }

  /** One attempt of a request: a 401 refreshes the token and re-sends once, within the attempt. */
  private Answer<float[]> attempt(byte[] body, String vectorField) throws InterruptedException {
    HttpResponse<byte[]> response;
    try {
      response = send(body);
      if (response.statusCode() == 401 && tokens.refresh(sentToken)) {
        // the token expired while in use: the same request once more with the new one, after its own rate-limit token
        callResendWaitMillis += rateLimiter.acquire(loop.remainingMillis());
        if (!loop.pastDeadline()) {
          sentToken = tokens.current();
        }
        if (loop.pastDeadline()) {
          return Answer.failed(ProviderResponseKind.RETRYABLE, ProviderRetryLoop.DEADLINE_EXCEEDED + ": the request was not sent", -1);
        }
        callResends++;
        response = send(body);
      }
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "Vertex request failed: " + e.getClass().getSimpleName() + ": "
          + e.getMessage(), -1);
    }
    int status = response.statusCode();
    if (status == 401 || status == 403) {
      return Answer.failed(ProviderResponseKind.CONFIG, VERTEX_ENDPOINT + config.endpoint() + " rejected the credential from "
          + tokens.describe() + " with HTTP " + status, -1);
    }
    if (status == 400 && FAILED_PRECONDITION.equals(errorStatus(response.body()))) {
      // a model that needs allowlisting or that the organization's policy disallows: "the client should not retry until the system
      // state has been explicitly fixed" (google.rpc.Code), so it is the configuration, never the input's
      return Answer.failed(ProviderResponseKind.CONFIG, VERTEX_ENDPOINT + config.endpoint() + " answered HTTP 400 "
          + FAILED_PRECONDITION + ": " + errorBody(response.body()), -1);
    }
    if (status == 400) {
      return Answer.failed(ProviderResponseKind.INPUT, "Vertex rejected the input with HTTP 400: " + errorBody(response.body()), -1);
    }
    if (status >= 400 && status < 500 && status != 408 && status != 429) {
      // 404 (a model or endpoint that does not exist), 405, 409, 410,...: the configuration, which no retry repairs (class P)
      return Answer.failed(ProviderResponseKind.CONFIG, VERTEX_ENDPOINT + config.endpoint() + " answered HTTP " + status + ": "
          + errorBody(response.body()), -1);
    }
    if (status != 200) {
      long hint = RetryAfter.hintMillis(null, response.headers().firstValue("Retry-After").orElse(null), clock.getAsLong());
      return Answer.failed(ProviderResponseKind.RETRYABLE, "Vertex answered HTTP " + status + ": " + errorBody(response.body()),
          hint);
    }
    return vectorOf(response.body(), vectorField);
  }

  /**
   * The HTTP send of one request; its rate-limit token is taken first (by {@link ProviderRetryLoop}, or before a re-send by
   * {@link #attempt}), followed by bearer preparation and a final deadline check, so waiting or refreshing cannot start a
   * request after the call expires.
   */
  private HttpResponse<byte[]> send(byte[] body) throws IOException, InterruptedException {
    HttpRequest httpRequest = HttpRequest.newBuilder(URI.create(config.endpoint()))
        .timeout(Duration.ofMillis(config.requestTimeoutMs()))
        .header("Authorization", "Bearer " + sentToken)
        .header("Content-Type", "application/json; charset=utf-8")
        .POST(HttpRequest.BodyPublishers.ofByteArray(body))
        .build();
    return HTTP.send(httpRequest, HttpResponse.BodyHandlers.ofByteArray());
  }

  private Answer<float[]> vectorOf(byte[] body, String vectorField) {
    JsonNode vector;
    try {
      vector = MAPPER.readTree(body).path("predictions").path(0).path(vectorField);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: " + e.getMessage(), -1);
    }
    if (!vector.isArray()) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: expected predictions[0]." + vectorField + " of "
          + spec.getDimension() + " floats, got " + vector.getNodeType(), -1);
    }
    if (vector.size() != spec.getDimension()) {
      return Answer.failed(ProviderResponseKind.CONFIG, "Vertex returned " + vector.size() + "-dimension vectors, but model "
          + spec.getName() + "'s .dimensions is " + spec.getDimension(), -1);
    }
    float[] values = new float[vector.size()];
    for (int i = 0; i < values.length; i++) {
      JsonNode element = vector.get(i);
      if (!element.isNumber()) {
        return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: an embedding holds a non-number", -1);
      }
      values[i] = (float) element.asDouble();
      if (!Float.isFinite(values[i])) {
        return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: an embedding holds a non-finite float", -1);
      }
    }
    return Answer.ok(values);
  }

  /** The google.rpc status of an error body ({@code error.status}), or null when the body does not carry one. */
  private static String errorStatus(byte[] body) {
    try {
      JsonNode root = body.length == 0 ? null : MAPPER.readTree(body);
      JsonNode status = root == null ? null : root.path("error").path("status");
      return status != null && status.isTextual() ? status.asText() : null;
    } catch (IOException e) {
      return null;
    }
  }

  private static String errorBody(byte[] body) {
    String text = new String(body, StandardCharsets.UTF_8);
    return text.length() <= MAX_ERROR_BODY_CHARS ? text : text.substring(0, MAX_ERROR_BODY_CHARS) + "…";
  }
}
