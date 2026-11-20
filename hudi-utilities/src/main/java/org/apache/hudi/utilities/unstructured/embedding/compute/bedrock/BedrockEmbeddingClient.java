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

package org.apache.hudi.utilities.unstructured.embedding.compute.bedrock;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
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
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockInvoker.BedrockReply;
import org.apache.hudi.utilities.unstructured.embedding.image.PreparedImage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.function.LongSupplier;

/**
 * An {@link EmbeddingClient} over Amazon Bedrock InvokeModel for one model family ({@link BedrockModelFamily}), whose
 * modalities it reports ({@link #supportedModalities}).
 * Requests use the AWS SDK default credential chain through {@link SdkBedrockInvoker}.
 * Texts are packed within the family's request limits, and so are the images of a family that embeds images, in its own
 * request format, after their preparation ({@link EmbeddingInput#image()}); a failed preparation is that input's outcome
 * ({@link ImageFailures}), and an image the family cannot take (its type, encoded size or side) fails permanently without a
 * request, its error naming the row. Empty or unsupported inputs fail permanently without a request.
 * {@link ProviderRetryLoop} bounds retries, rate waits and deadlines; malformed answers remain retryable.
 * Rejected packed inputs are isolated. Before any successful embedding, a call whose every input sent to Bedrock (more than
 * one) was rejected fails configuration rather than quarantining rows; inputs settled without a request (empty, refused or
 * failed preparation) do not count. A rejection beside retryable inputs stays retryable.
 */
public abstract class BedrockEmbeddingClient implements EmbeddingClient {

  private static final Logger LOG = LoggerFactory.getLogger(BedrockEmbeddingClient.class);
  private static final String PREPARED_IMAGE_IS = "the prepared image is ";
  /** Provider id of the Bedrock clients ({@code hoodie.unstructured.embedding.model.<alias>.provider}). */
  public static final String PROVIDER = "bedrock";
  static final String EMPTY_INPUT = "empty input";
  /** Error of an image input that reaches the client without a prepared image: a broken compute invariant. */
  public static final String IMAGE_WITHOUT_PREPARED_INPUT = "image input without a prepared image";
  static final String COHERE_INPUT_TYPE = "search_document";
  /** Cohere's {@code truncate}: an over-long input is rejected, never silently cut. */
  static final String COHERE_TRUNCATE = "NONE";
  /** Bedrock: the request's content is rejected (400). */
  private static final String BEDROCK_VALIDATION = "ValidationException";
  private static final Set<String> BEDROCK_RETRYABLE = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "ThrottlingException", "ModelNotReadyException", "ModelTimeoutException", "ModelErrorException",
      "ServiceUnavailableException", "InternalServerException", "ServiceQuotaExceededException")));
  /**
   * The AWS SDK's clock-skew codes (AwsErrorCode, SDK 2.29.52; RequestExpired "The request is no longer valid due to expired
   * timestamps.", https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html): transient, since the SDK
   * adjusts its clock offset on the failed response and the next attempt is signed again. One that persists leaves its inputs
   * RETRYABLE, so the run fails; it is never an input's rejection.
   */
  private static final Set<String> BEDROCK_CLOCK_SKEW = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "RequestTimeTooSkewed", "RequestExpired", "RequestInTheFuture")));
  /** A signature the SDK made with a skewed clock ({@code InvalidSignatureException} "Signature expired: ..."): a clock skew too. */
  private static final String BEDROCK_INVALID_SIGNATURE = "InvalidSignatureException";
  private static final String SIGNATURE_EXPIRED = "Signature expired";
  /**
   * The request was aborted before Bedrock answered ({@code RequestAbortedException} 400, "aborted before a response could be
   * returned", https://docs.aws.amazon.com/bedrock/latest/APIReference/CommonErrors.html): transient, never the input's.
   */
  private static final String BEDROCK_REQUEST_ABORTED = "RequestAbortedException";
  private static final Set<String> BEDROCK_CONFIG = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      "AccessDeniedException", "ResourceNotFoundException", "UnrecognizedClientException", BEDROCK_INVALID_SIGNATURE,
      "ExpiredTokenException", "IncompleteSignature", "MissingAuthenticationToken", "NotAuthorized", "InvalidClientTokenId")));
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_ERROR_CHARS = 1024;
  /** Cohere caps a request at 128k tokens; a text counts one token per UTF-8 byte plus 8, an upper bound. */
  private static final long COHERE_MAX_REQUEST_TOKENS = 128_000L;
  private static final long COHERE_TOKENS_PER_INPUT = 8L;
  /** Cohere caps a request's payload at about 20 MB; images are packed so their data URIs stay within 20,000,000 bytes. */
  private static final long COHERE_MAX_REQUEST_PAYLOAD_BYTES = 20_000_000L;
  /** Cohere resamples an image above 2,458,624 pixels down to that size and one below 3,136 pixels up to it. */
  private static final long COHERE_MAX_IMAGE_PIXELS = 2_458_624L;
  private static final long COHERE_MIN_IMAGE_PIXELS = 3_136L;
  /** Cohere counts an image as {@code (pixels / 784) x 4} tokens. */
  private static final long COHERE_PIXELS_PER_TOKEN_UNIT = 784L;
  private static final long COHERE_TOKENS_PER_UNIT = 4L;

  private final Function<BedrockClientConfig, BedrockInvoker> invokers;
  private final LongSupplier clock;
  private final ProviderRetryLoop.Sleeper sleeper;
  private final Random jitter;
  private EmbeddingModelSpec spec;
  private BedrockClientConfig config;
  private BedrockInvoker invoker;
  private ProviderRetryLoop loop;
  /** Set once any request of this client returned vectors: the binding's configuration works. */
  private boolean embeddedOnce;
  private EmbeddingCallStats lastCallStats = EmbeddingCallStats.NONE;

  /** Production wiring: the SDK invoker, the system clock, real sleeps. */
  protected BedrockEmbeddingClient() {
    this(SdkBedrockInvoker::create, System::currentTimeMillis, Thread::sleep, new SecureRandom());
  }

  /** Test seam: the invoker factory, the clock and sleeper of the retry loop and rate limiter, the backoff jitter. */
  BedrockEmbeddingClient(Function<BedrockClientConfig, BedrockInvoker> invokers, LongSupplier clock,
                         ProviderRetryLoop.Sleeper sleeper, Random jitter) {
    this.invokers = invokers;
    this.clock = clock;
    this.sleeper = sleeper;
    this.jitter = jitter;
  }

  /** The family this client serves. */
  public abstract BedrockModelFamily family();

  /** The family's modalities: the client is chosen by family, so the model name adds nothing. */
  @Override
  public Set<String> supportedModalities(String modelName) {
    return family().modalities();
  }

  /**
   * @throws EmbeddingClientConfigException naming the offending key for an invalid configuration, or when the SDK client cannot
   *     be built (no region)
   */
  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
    try {
      this.config = BedrockClientConfig.from(spec, props, family());
    } catch (IllegalArgumentException e) {
      throw new EmbeddingClientConfigException(e.getMessage());
    }
    RequestRateLimiter rateLimiter = RequestRateLimiter.from(spec, props, clock, sleeper::sleep);
    this.loop = new ProviderRetryLoop("model=" + spec.getName() + " provider=" + PROVIDER + " family=" + family(),
        config.settings(), rateLimiter, clock, sleeper, jitter);
    this.invoker = invokers.apply(config);
  }

  @Override
  public EmbeddingModelSpec spec() {
    return spec;
  }

  @Override
  public EmbeddingCallStats lastCallStats() {
    return lastCallStats;
  }

  BedrockClientConfig config() {
    return config;
  }

  @Override
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) throws InterruptedException {
    lastCallStats = EmbeddingCallStats.NONE;
    if (loop.outageOpen()) {
      throw new EmbeddingProviderUnavailableException(ProviderRetryLoop.OUTAGE + " is open for model " + spec.getName() + " ("
          + PROVIDER + " " + config.modelId() + ") after " + loop.consecutiveFailures()
          + " consecutive failed requests; the remaining rows of this task are left for a later run");
    }
    loop.startCall();
    Call call = new Call(inputs.size());
    try {
      List<Integer> texts = new ArrayList<>();
      List<Integer> images = new ArrayList<>();
      route(inputs, call, texts, images);
      for (List<Integer> request : packTexts(inputs, texts)) {
        sendTexts(call, inputs, request);
      }
      sendImages(call, inputs, images);
      settleRejections(call);
    } finally {
      // also when a config failure is thrown: the stats then describe the requests that led to it
      lastCallStats = loop.stats(call.isolationRequests, call.tokens);
    }
    return Arrays.asList(call.results);
  }

  /**
   * Sorts the inputs of a call into the text and image requests; an empty text or a modality the model family does not take
   * gets its PERMANENT result here.
   *
   * @throws EmbeddingComputePlatformException for an image input without its preparation (a broken compute invariant)
   */
  private void route(List<EmbeddingInput> inputs, Call call, List<Integer> texts, List<Integer> images) {
    for (int i = 0; i < inputs.size(); i++) {
      EmbeddingInput input = inputs.get(i);
      if (EmbeddingTableSchema.MODALITY_TEXT.equals(input.getModality())) {
        if (input.getText() == null || input.getText().trim().isEmpty()) {
          call.results[i] = EmbeddingResult.invalidInput(EMPTY_INPUT);
        } else {
          texts.add(i);
        }
      } else if (EmbeddingTableSchema.MODALITY_IMAGE.equals(input.getModality())
          && family().modalities().contains(EmbeddingTableSchema.MODALITY_IMAGE)) {
        if (input.image() == null) {
          // compute builds every image input of a model that declares image with its preparation: a missing one is a broken invariant
          throw new EmbeddingComputePlatformException(IMAGE_WITHOUT_PREPARED_INPUT + " (model " + spec.getName() + ")");
        }
        images.add(i);
      } else {
        call.results[i] = EmbeddingResult.invalidInput("modality " + input.getModality() + " is not supported by " + PROVIDER
            + " " + family());
      }
    }
  }

  /** Text requests: consecutive texts up to the per-request input limit and, for Cohere, its token budget. */
  private List<List<Integer>> packTexts(List<EmbeddingInput> inputs, List<Integer> texts) {
    int maxInputs = Math.min(family().maxInputsPerRequest(), config.settings().maxInputs());
    List<List<Integer>> requests = new ArrayList<>();
    List<Integer> current = new ArrayList<>();
    long tokens = 0;
    for (int index : texts) {
      long cost = inputs.get(index).getText().getBytes(StandardCharsets.UTF_8).length + COHERE_TOKENS_PER_INPUT;
      if (!current.isEmpty() && (current.size() >= maxInputs || tokens + cost > COHERE_MAX_REQUEST_TOKENS)) {
        requests.add(current);
        current = new ArrayList<>();
        tokens = 0;
      }
      current.add(index);
      tokens += cost;
    }
    if (!current.isEmpty()) {
      requests.add(current);
    }
    return requests;
  }

  private void sendTexts(Call call, List<EmbeddingInput> inputs, List<Integer> request) throws InterruptedException {
    String notSent = loop.notSentReason();
    if (notSent != null) {
      call.fill(request, EmbeddingResult.retryable(notSent));
      return;
    }
    call.sent.addAll(request);
    List<String> texts = new ArrayList<>(request.size());
    for (int index : request) {
      texts.add(inputs.get(index).getText());
    }
    byte[] body = textBody(texts);
    Answer<float[][]> answer = loop.send(() -> attempt(call, body, texts.size()));
    if (answer.isInterrupted()) {
      Thread.interrupted();
      throw new InterruptedException(answer.error());
    }
    switch (answer.kind()) {
      case OK:
        accept(call, request, answer.value());
        break;
      case INPUT:
        if (request.size() == 1) {
          call.rejected.put(request.get(0), answer.error());
        } else {
          // isolate the rejected inputs: each one alone
          for (int index : request) {
            call.isolationRequests++;
            sendTexts(call, inputs, Collections.singletonList(index));
          }
        }
        break;
      case RETRYABLE:
        call.fill(request, EmbeddingResult.retryable(answer.error()));
        break;
      default:
        throw configFailure(answer.error());
    }
  }

  /**
   * Image requests, packed like texts: each image is prepared in input order (a failure is that input's outcome,
   * {@link ImageFailures}) and checked against the family's image limits, then added to the request being built, up to the
   * per-request input limit and, for Cohere, its payload and token budgets. A request full by count is sent before the next
   * image is prepared; one that the next image would take over the payload or token budget is sent once that image is
   * prepared. So the call holds the prepared images of one request, plus at most the next image. The payload counts the
   * request's JSON envelope and each data URI with its quotes and separator.
   */
  private void sendImages(Call call, List<EmbeddingInput> inputs, List<Integer> images) throws InterruptedException {
    int maxInputs = Math.min(family().maxInputsPerRequest(), config.settings().maxInputs());
    boolean cohere = family() == BedrockModelFamily.COHERE_EMBED_V4;
    long maxPayload = cohere ? COHERE_MAX_REQUEST_PAYLOAD_BYTES : Long.MAX_VALUE;
    long envelope = cohere ? imageBody(Collections.emptyList()).length : 0L;
    List<Integer> request = new ArrayList<>();
    List<PreparedImage> prepared = new ArrayList<>();
    long payload = envelope;
    long tokens = 0;
    for (int index : images) {
      if (request.size() >= maxInputs) {
        sendImageRequest(call, request, prepared);
        request = new ArrayList<>();
        prepared = new ArrayList<>();
        payload = envelope;
        tokens = 0;
      }
      PreparedImage image = sendable(call, inputs.get(index), index);
      if (image == null) {
        continue;
      }
      // the URI's two quotes, plus a comma after the first
      long bytes = dataUriLength(image) + (request.isEmpty() ? 2L : 3L);
      long cost = cohereImageTokens(image);
      if (!request.isEmpty() && (payload + bytes > maxPayload || tokens + cost > COHERE_MAX_REQUEST_TOKENS)) {
        sendImageRequest(call, request, prepared);
        request = new ArrayList<>();
        prepared = new ArrayList<>();
        payload = envelope;
        tokens = 0;
        bytes = dataUriLength(image) + 2L;
      }
      request.add(index);
      prepared.add(image);
      payload += bytes;
      tokens += cost;
    }
    if (!request.isEmpty()) {
      sendImageRequest(call, request, prepared);
    }
  }

  /**
   * The prepared image of input {@code index} when it can be sent; else null, with the input's result set: RETRYABLE when the
   * loop sends nothing now, the image failure's result when it cannot be prepared, PERMANENT when the model refuses it.
   */
  private PreparedImage sendable(Call call, EmbeddingInput input, int index) {
    String notSent = loop.notSentReason();
    if (notSent != null) {
      call.results[index] = EmbeddingResult.retryable(notSent);
      return null;
    }
    PreparedImage image;
    try {
      image = input.image().get();
    } catch (IOException | RuntimeException e) {
      // a malformed stored-input reference throws EmbeddingComputePlatformException from here
      call.results[index] = ImageFailures.result(input, e);
      return null;
    }
    String refused = imageRefusal(image);
    if (refused != null) {
      // the row is named, so alike refusals of rows of several files are never read as the provider rejecting the request
      call.results[index] = EmbeddingResult.permanent(input.describeIdentity() + ": " + refused);
      return null;
    }
    return image;
  }

  /** One image request; a rejected request of several images is isolated, each image alone. */
  private void sendImageRequest(Call call, List<Integer> request, List<PreparedImage> prepared) throws InterruptedException {
    String notSent = loop.notSentReason();
    if (notSent != null) {
      call.fill(request, EmbeddingResult.retryable(notSent));
      return;
    }
    call.sent.addAll(request);
    byte[] body = imageBody(prepared);
    Answer<float[][]> answer = loop.send(() -> attempt(call, body, request.size()));
    if (answer.isInterrupted()) {
      Thread.interrupted();
      throw new InterruptedException(answer.error());
    }
    switch (answer.kind()) {
      case OK:
        accept(call, request, answer.value());
        break;
      case INPUT:
        if (request.size() == 1) {
          call.rejected.put(request.get(0), answer.error());
        } else {
          for (int i = 0; i < request.size(); i++) {
            call.isolationRequests++;
            sendImageRequest(call, Collections.singletonList(request.get(i)), Collections.singletonList(prepared.get(i)));
          }
        }
        break;
      case RETRYABLE:
        call.fill(request, EmbeddingResult.retryable(answer.error()));
        break;
      default:
        throw configFailure(answer.error());
    }
  }

  /**
   * Cohere Embed v4's token count of an image, {@code (pixels / 784) x 4}, its pixels clamped to what Cohere resamples to
   * ([{@value #COHERE_MIN_IMAGE_PIXELS}, {@value #COHERE_MAX_IMAGE_PIXELS}]); 0 for another family.
   */
  private long cohereImageTokens(PreparedImage image) {
    if (family() != BedrockModelFamily.COHERE_EMBED_V4) {
      return 0L;
    }
    long pixels = Math.max(COHERE_MIN_IMAGE_PIXELS, Math.min(COHERE_MAX_IMAGE_PIXELS, (long) image.getWidth() * image.getHeight()));
    return (pixels + COHERE_PIXELS_PER_TOKEN_UNIT - 1) / COHERE_PIXELS_PER_TOKEN_UNIT * COHERE_TOKENS_PER_UNIT;
  }

  /** Why the family cannot take {@code image} (its type, encoded size or long side), or null when it can. */
  private String imageRefusal(PreparedImage image) {
    BedrockModelFamily family = family();
    if (!family.imageMimeTypes().contains(image.getMimeType())) {
      return PREPARED_IMAGE_IS + image.getMimeType() + ", which " + family + " does not accept (it accepts "
          + new TreeSet<>(family.imageMimeTypes()) + ")";
    }
    if (image.getEncoded().length > family.maxImageBytes()) {
      return PREPARED_IMAGE_IS + image.getEncoded().length + " bytes, over the " + family.maxImageBytes() + " bytes "
          + family + " accepts; lower " + EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key();
    }
    if (family.maxImageSide() > 0 && Math.max(image.getWidth(), image.getHeight()) > family.maxImageSide()) {
      return PREPARED_IMAGE_IS + image.getWidth() + "x" + image.getHeight() + " pixels, over the " + family.maxImageSide()
          + " pixels per side " + family + " accepts; lower " + EmbeddingTableConfig.COMPUTE_IMAGE_LONG_SIDE.key();
    }
    return null;
  }

  /** One attempt: invoke, classify, parse; never throws for a provider or transport failure. */
  private Answer<float[][]> attempt(Call call, byte[] body, int expected) throws InterruptedException {
    BedrockReply reply;
    try {
      reply = invoker.invoke(config.modelId(), body);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "Bedrock request failed: " + e.getMessage(), -1);
    }
    ProviderResponseKind kind = classifyBedrock(reply.status(), reply.errorCode(), reply.message());
    if (kind != ProviderResponseKind.OK) {
      String error = "Bedrock " + (reply.status() > 0 ? "HTTP " + reply.status() + " " : "") + reply.errorCode() + ": "
          + cut(reply.message());
      return Answer.failed(kind, error, RetryAfter.hintMillis(null, reply.retryAfter(), clock.getAsLong()));
    }
    try {
      float[][] vectors = parse(reply.body(), expected);
      long tokens = reply.inputTokens() >= 0 ? reply.inputTokens() : titanTokenCount(reply.body());
      if (tokens >= 0) {
        call.tokens = Math.max(call.tokens, 0) + tokens;
      }
      return Answer.ok(vectors);
    } catch (IOException e) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, "malformed answer: " + e.getMessage(), -1);
    }
  }

  /**
   * Amazon Bedrock InvokeModel errors, by the error code the SDK reads from the response
   * (https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_InvokeModel.html#API_runtime_InvokeModel_Errors):
   * throttling, a model not ready or timing out, a model error, server errors, a clock skew (an expired signature included) and an
   * aborted request are transient; access denied, a missing permission (NotAuthorized, a 400 on
   * https://docs.aws.amazon.com/bedrock/latest/userguide/troubleshooting-api-error-codes.html), an unknown model and a rejected or
   * expired credential reject the configuration; {@value #BEDROCK_VALIDATION} rejects an input. A code not
   * listed falls back to the status: 408, 429 and 5xx transient, 400/413/422 an input, anything else the configuration.
   *
   * @param status the HTTP status, or -1 when no response came back (the credentials could not be resolved)
   * @param errorCode the error code, or null
   * @param message the error message, or null
   */
  static ProviderResponseKind classifyBedrock(int status, String errorCode, String message) {
    if (status >= 200 && status < 300) {
      return ProviderResponseKind.OK;
    }
    if (errorCode != null) {
      if (BEDROCK_RETRYABLE.contains(errorCode) || BEDROCK_CLOCK_SKEW.contains(errorCode) || BEDROCK_REQUEST_ABORTED.equals(errorCode)
          || (BEDROCK_INVALID_SIGNATURE.equals(errorCode) && message != null && message.startsWith(SIGNATURE_EXPIRED))) {
        return ProviderResponseKind.RETRYABLE;
      }
      if (BEDROCK_CONFIG.contains(errorCode)) {
        return ProviderResponseKind.CONFIG;
      }
      if (BEDROCK_VALIDATION.equals(errorCode)) {
        return ProviderResponseKind.INPUT;
      }
    }
    if (status == 408 || status == 429 || status >= 500) {
      return ProviderResponseKind.RETRYABLE;
    }
    if (status == 400 || status == 413 || status == 422) {
      return ProviderResponseKind.INPUT;
    }
    return ProviderResponseKind.CONFIG;
  }

  /** OK vectors: a vector count other than the inputs' is malformed; a dimension other than the model's is configuration. */
  private void accept(Call call, List<Integer> request, float[][] vectors) {
    embeddedOnce = true;
    for (int i = 0; i < request.size(); i++) {
      if (vectors[i].length != spec.getDimension()) {
        throw configFailure("Bedrock returned " + vectors[i].length + "-dimension vectors, but the model's .dimensions is "
            + spec.getDimension());
      }
      call.results[request.get(i)] = EmbeddingResult.ok(vectors[i]);
    }
  }

  /**
   * Inputs Bedrock rejected alone become PERMANENT once this client has embedded anything, or when the call sent Bedrock one
   * input: one rejection cannot tell the row's fault from the binding's, and failing the run would fail every later run on the
   * same row. Otherwise (several inputs sent, every one rejected) the call fails as configuration, since a wrong model id or
   * binding is rejected the same way. Inputs settled without a request are not counted as sent.
   */
  private void settleRejections(Call call) {
    if (call.rejected.isEmpty()) {
      return;
    }
    if (!embeddedOnce && Arrays.stream(call.results).anyMatch(r -> r != null && r.getStatus() == EmbeddingResult.Status.RETRYABLE_FAILURE)) {
      // other inputs were only held back (throttled, not sent): nothing shows the configuration is wrong yet, so try again later
      for (Map.Entry<Integer, String> rejected : call.rejected.entrySet()) {
        call.results[rejected.getKey()] = EmbeddingResult.retryable(rejected.getValue()
            + " (rejected before this client embedded anything; retried)");
      }
      return;
    }
    if (!embeddedOnce && call.sent.size() > 1) {
      throw configFailure(call.rejected.size() + " input(s) rejected and nothing embedded yet by this client, so the "
          + "binding's configuration (model id, family, dimensions) may be the cause; first rejection: "
          + call.rejected.values().iterator().next());
    }
    for (Map.Entry<Integer, String> rejected : call.rejected.entrySet()) {
      call.results[rejected.getKey()] = EmbeddingResult.permanent(rejected.getValue());
    }
    if (LOG.isWarnEnabled()) {
      LOG.warn("EMBED-BEDROCK-INPUT-REJECTED model={} rejected={}: {}", spec.getName(), call.rejected.size(),
          call.rejected.values().iterator().next());
    }
  }

  private EmbeddingClientConfigException configFailure(String error) {
    return new EmbeddingClientConfigException("Bedrock rejected the configuration of model " + spec.getName() + " ("
        + config.modelId() + ", " + family() + ", region " + (config.region() == null ? "(default chain)" : config.region())
        + "): " + error);
  }

  private byte[] textBody(List<String> texts) {
    ObjectNode body = MAPPER.createObjectNode();
    switch (family()) {
      case TITAN_EMBED_TEXT_V2:
        body.put("inputText", texts.get(0));
        body.put("dimensions", spec.getDimension());
        break;
      case COHERE_EMBED_V4:
        cohereBody(body).putArray("texts").addAll(textNodes(texts));
        break;
      case TITAN_EMBED_IMAGE_V1:
        body.put("inputText", texts.get(0));
        titanMultimodalConfig(body);
        break;
      default:
        throw new UnsupportedOperationException("Unhandled Bedrock model family " + family());
    }
    return bytes(body);
  }

  /**
   * The request of {@code images}: Cohere Embed v4 {@code images}, a list of {@code data:<mime>;base64,…} URIs in input order
   * (never sent beside {@code texts}); Titan Multimodal G1 {@code inputImage}, the bare base64 of its one image.
   */
  private byte[] imageBody(List<PreparedImage> images) {
    ObjectNode body = MAPPER.createObjectNode();
    switch (family()) {
      case COHERE_EMBED_V4:
        ArrayNode uris = cohereBody(body).putArray("images");
        images.forEach(image -> uris.add(dataUri(image)));
        break;
      case TITAN_EMBED_IMAGE_V1:
        body.put("inputImage", Base64.getEncoder().encodeToString(images.get(0).getEncoded()));
        titanMultimodalConfig(body);
        break;
      default:
        throw new UnsupportedOperationException("Bedrock model family " + family() + " embeds no image");
    }
    return bytes(body);
  }

  /** The length of {@link #dataUri}, without building it: base64 writes 4 characters per 3 bytes, padded. */
  private static long dataUriLength(PreparedImage image) {
    return ("data:" + image.getMimeType() + ";base64,").length() + 4L * ((image.getEncoded().length + 2L) / 3L);
  }

  /** {@code data:<mime>;base64,<the encoded image>}. */
  private static String dataUri(PreparedImage image) {
    return "data:" + image.getMimeType() + ";base64," + Base64.getEncoder().encodeToString(image.getEncoded());
  }

  /** Titan Multimodal G1's {@code embeddingConfig.outputEmbeddingLength}: the model's dimension. */
  private void titanMultimodalConfig(ObjectNode body) {
    body.putObject("embeddingConfig").put("outputEmbeddingLength", spec.getDimension());
  }

  /** Cohere Embed v4: {@code truncate} NONE "returns an error if the input exceeds the limit"; the default is not documented. */
  private ObjectNode cohereBody(ObjectNode body) {
    body.put("input_type", COHERE_INPUT_TYPE);
    body.put("truncate", COHERE_TRUNCATE);
    body.putArray("embedding_types").add("float");
    body.put("output_dimension", spec.getDimension());
    return body;
  }

  private static ArrayNode textNodes(List<String> texts) {
    ArrayNode array = MAPPER.createArrayNode();
    texts.forEach(array::add);
    return array;
  }

  private static byte[] bytes(ObjectNode body) {
    try {
      return MAPPER.writeValueAsBytes(body);
    } catch (IOException e) {
      throw new IllegalStateException("Cannot write a Bedrock request", e);
    }
  }

  /** Titan (text and multimodal): {@code embedding}; Cohere: {@code embeddings.float} (or a bare {@code embeddings} list). */
  private float[][] parse(byte[] body, int expected) throws IOException {
    JsonNode root = MAPPER.readTree(body);
    if (root == null || !root.isObject()) {
      throw new IOException("the body is not a JSON object");
    }
    List<JsonNode> vectors = new ArrayList<>(expected);
    if (family() == BedrockModelFamily.COHERE_EMBED_V4) {
      JsonNode embeddings = root.path("embeddings");
      JsonNode floats = embeddings.isObject() ? embeddings.path("float") : embeddings;
      if (floats.isArray()) {
        floats.forEach(vectors::add);
      }
    } else {
      vectors.add(root.path("embedding"));
    }
    if (vectors.size() != expected) {
      throw new IOException("expected " + expected + " embeddings, got " + vectors.size());
    }
    float[][] out = new float[expected][];
    for (int i = 0; i < expected; i++) {
      out[i] = floatVector(vectors.get(i), i);
    }
    return out;
  }

  /** Converts embedding {@code i}, a non-empty JSON array of numbers, to finite floats. */
  private static float[] floatVector(JsonNode vector, int i) throws IOException {
    if (!vector.isArray() || vector.size() == 0) {
      throw new IOException("embedding " + i + " is not a non-empty array");
    }
    float[] floats = new float[vector.size()];
    for (int j = 0; j < floats.length; j++) {
      if (!vector.get(j).isNumber()) {
        throw new IOException("embedding " + i + " holds a non-number");
      }
      floats[j] = (float) vector.get(j).asDouble();
      if (!Float.isFinite(floats[j])) {
        throw new IOException("embedding " + i + " holds a non-finite float");
      }
    }
    return floats;
  }

  /** Titan's {@code inputTextTokenCount}, -1 when absent or for Cohere. */
  private long titanTokenCount(byte[] body) {
    if (family() == BedrockModelFamily.COHERE_EMBED_V4) {
      return -1L;
    }
    try {
      JsonNode count = MAPPER.readTree(body).path("inputTextTokenCount");
      return count.isIntegralNumber() ? count.asLong() : -1L;
    } catch (IOException e) {
      return -1L;
    }
  }

  private static String cut(String text) {
    if (text == null) {
      return "";
    }
    return text.length() <= MAX_ERROR_CHARS ? text : text.substring(0, MAX_ERROR_CHARS) + "…";
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "{" + config + "}";
  }

  /**
   * The per-call state: results by input position, inputs rejected alone, the distinct inputs sent to Bedrock, isolation
   * requests, reported input tokens.
   */
  private static final class Call {
    private final EmbeddingResult[] results;
    private final Map<Integer, String> rejected = new LinkedHashMap<>();
    private final Set<Integer> sent = new HashSet<>();
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
