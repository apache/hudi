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
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.ProviderResponseKind;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.RetryAfter;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.ErrorClassifier.ErrorBody;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * An {@link EmbeddingClient} for the endpoints that speak the OpenAI embeddings wire format
 * ({@code POST <endpoint>/embeddings}, results mapped by {@code data[].index}): OpenAI, Azure OpenAI and Hugging Face
 * Text Embeddings Inference, selected by the model's {@code provider} ({@link OpenAiProvider}); configuration in
 * {@link OpenAiClientConfig}.
 *
 * <p>One {@link #embed} call:
 * <ol>
 *   <li>refuses duplicate record keys (source_path_sha, source_modification_time, source_file_size, modality, chunk_id) among its inputs ({@link IllegalArgumentException})
 *   and, once the outage short-circuit is open, every call ({@link EmbeddingProviderUnavailableException});</li>
 *   <li>fails locally, as PERMANENT and without a request, an empty or non-text input and (OpenAI/Azure) an input over
 *   the per-input token limit, counted with the model's BPE encoding: inputs are never silently truncated;</li>
 *   <li>packs the rest into requests within the per-request input and token limits;</li>
 *   <li>retries a transient failure (429, 408, 409, 5xx, TEI 424, timeouts, I/O errors) with full-jitter exponential backoff,
 *   honouring {@code retry-after-ms} / {@code Retry-After} up to the max backoff, within the call's deadline; inputs of
 *   a request that stays failed are RETRYABLE;</li>
 *   <li>bisects a request the provider rejects for some input (400/413/422 not naming a request parameter) until each
 *   rejected input is alone, which is PERMANENT with the provider's message, within a budget of extra requests;</li>
 *   <li>throws {@link EmbeddingClientConfigException} when the provider rejects the configuration (401, 403, 404, a 400
 *   naming {@code model}/{@code dimensions}/...) or the account is out of quota or credit (a billing 429,
 *   {@link ProviderResponseKind#QUOTA}), so the compute run aborts without a commit.</li>
 * </ol>
 * Every request, each retry and bisection request included, first takes a token of the model's rate limit
 * ({@link RequestRateLimiter}, per client and so per compute task), when one is set; the wait is in {@link #lastCallStats()}.
 * After {@code outage.consecutive.failures} requests in a row stay RETRYABLE, the rest of the call is not sent and
 * every later call throws {@link EmbeddingProviderUnavailableException}: the client lives one Spark task, so the next
 * task or run tries the provider again.
 *
 * <p>The API key is read in {@link #init} from a file or a named env var ({@link CredentialSource}) and only ever put in
 * the auth header; provider text that echoes it (or its first {@value #KEY_PREFIX_CHARS} chars) is redacted before it
 * reaches a result, an exception or a log line.
 */
public class OpenAiCompatibleEmbeddingClient implements EmbeddingClient {

  private static final Logger LOG = LoggerFactory.getLogger(OpenAiCompatibleEmbeddingClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final JsonFactory JSON = MAPPER.getFactory();
  /** Length of a key prefix providers echo in auth errors ("Incorrect API key provided: sk-ab***"). */
  static final int KEY_PREFIX_CHARS = 8;
  static final String REDACTED = "***";
  static final String EMPTY_INPUT = "empty input";
  static final String OUTAGE = "provider outage short-circuit";
  static final String DEADLINE_EXCEEDED = "deadline exceeded";
  static final String BISECT_BUDGET_EXHAUSTED = "bisection budget exhausted";
  static final String INTERRUPTED = "interrupted";
  static final String REQUEST_REJECTED = "request rejected as a whole";
  private static final String NOT_SENT = ": the request was not sent";
  private static final int INITIAL_VECTOR_CAPACITY = 1024;
  private static final String MODEL_KEY_PREFIX = EmbeddingTableConfig.MODEL_PREFIX;
  private static final String DIMENSIONS_KEY_SUFFIX = EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX;

  /** Sleeps between retries; injectable so tests do not wait. */
  interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  private final HttpTransport transport;
  private final LongSupplier clock;
  private final Sleeper sleeper;
  private final Random jitter;
  private final UnaryOperator<String> environment;

  private EmbeddingModelSpec spec;
  private OpenAiClientConfig config;
  private String credentialSource;
  private String key;
  private OpenAiBpeTokenizer tokenizer;
  private int maxInputs;
  private long maxTokens;
  private int consecutiveFailures;
  private boolean outageOpen;
  private RequestRateLimiter rateLimiter = RequestRateLimiter.unlimited();
  private EmbeddingCallStats lastCallStats = EmbeddingCallStats.NONE;

  /** Production wiring: the JDK HTTP client, the system clock, real sleeps, the process environment. */
  public OpenAiCompatibleEmbeddingClient() {
    this(new JdkHttpTransport(), System::currentTimeMillis, Thread::sleep, new SecureRandom(), System::getenv);
  }

  OpenAiCompatibleEmbeddingClient(HttpTransport transport, LongSupplier clock, Sleeper sleeper, Random jitter,
                                  UnaryOperator<String> environment) {
    this.transport = transport;
    this.clock = clock;
    this.sleeper = sleeper;
    this.jitter = jitter;
    this.environment = environment;
  }

  /**
   * Validates the configuration, reads the API key and, for TEI, the served model and server limits from {@code GET /info}.
   *
   * @throws IllegalArgumentException for an invalid configuration
   * @throws IllegalStateException when the key cannot be read, or TEI's {@code /info} is unreachable, fails or does not
   *     describe an embedding model; the compute job then leaves the task's rows untouched
   */
  @Override
  public void init(EmbeddingModelSpec spec, TypedProperties props) {
    this.spec = spec;
    this.config = OpenAiClientConfig.from(spec, props);
    this.rateLimiter = RequestRateLimiter.from(spec, props, clock, sleeper::sleep);
    CredentialSource credentials = CredentialSource.of(config);
    this.credentialSource = credentials.describe();
    this.key = credentials.read(environment).orElse(null);
    this.tokenizer = config.encoding() == null ? null : new OpenAiBpeTokenizer(config.encoding());
    this.maxInputs = config.maxInputs();
    this.maxTokens = config.maxTokens();
    if (config.provider() == OpenAiProvider.HF_TEI) {
      readTeiInfo();
    }
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
  public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
    lastCallStats = EmbeddingCallStats.NONE;
    checkUniqueIds(inputs);
    if (outageOpen) {
      throw new EmbeddingProviderUnavailableException(OUTAGE + " is open for model " + spec.getName() + " ("
          + config.provider().id() + " " + config.endpoint() + ") after " + consecutiveFailures
          + " consecutive failed requests; the remaining rows of this task are left for a later run");
    }
    Call call = new Call(inputs, clock.getAsLong() + config.deadlineMs());
    List<Integer> sendable = new ArrayList<>(inputs.size());
    List<Integer> tokenCounts = new ArrayList<>(inputs.size());
    route(inputs, call, sendable, tokenCounts);
    int[] counts = tokenCounts.stream().mapToInt(Integer::intValue).toArray();
    try {
      for (int[] range : RequestPlanner.plan(counts, maxInputs, maxTokens)) {
        List<Integer> request = sendable.subList(range[0], range[1]);
        if (call.interrupted) {
          call.fill(request, EmbeddingResult.retryable(INTERRUPTED + NOT_SENT));
        } else if (outageOpen) {
          call.fill(request, EmbeddingResult.retryable(OUTAGE + NOT_SENT));
        } else if (call.pastDeadline()) {
          call.fill(request, EmbeddingResult.retryable(DEADLINE_EXCEEDED + NOT_SENT));
        } else {
          sendAndResolve(call, request);
        }
      }
    } finally {
      // also when a config failure is thrown: the stats then describe the requests that led to it
      lastCallStats = call.stats();
    }
    return Arrays.asList(call.results);
  }

  /**
   * Sorts the inputs of a call into the sendable ones (with their token counts); a non-text, empty or over-long input gets its
   * PERMANENT result here.
   */
  private void route(List<EmbeddingInput> inputs, Call call, List<Integer> sendable, List<Integer> tokenCounts) {
    for (int i = 0; i < inputs.size(); i++) {
      EmbeddingInput input = inputs.get(i);
      String text = input.getText();
      if (!EmbeddingTableSchema.MODALITY_TEXT.equals(input.getModality())) {
        call.results[i] = EmbeddingResult.invalidInput("modality " + input.getModality() + " is not supported by "
            + config.provider().id());
      } else if (text == null || text.trim().isEmpty()) {
        call.results[i] = EmbeddingResult.invalidInput(EMPTY_INPUT);
      } else {
        int tokens = tokenizer != null ? tokenizer.countTokens(text) : text.getBytes(StandardCharsets.UTF_8).length;
        if (config.maxInputTokens() > 0 && tokens > config.maxInputTokens()) {
          call.results[i] = EmbeddingResult.invalidInput("input has " + tokens + " tokens, over the per-input limit of "
              + config.maxInputTokens() + " for " + config.baseModel() + " (" + MODEL_KEY_PREFIX + spec.getName()
              + OpenAiClientConfig.REQUEST_MAX_INPUT_TOKENS_SUFFIX + " or the model's documented limit); it is not truncated");
        } else {
          sendable.add(i);
          tokenCounts.add(tokens);
        }
      }
    }
  }

  /** Sends {@code request} and settles its inputs, bisecting when the provider rejects one of them. */
  private void sendAndResolve(Call call, List<Integer> request) {
    resolveOutcome(call, request, sendWithRetries(call, request));
  }

  private void resolveOutcome(Call call, List<Integer> request, Outcome outcome) {
    switch (outcome.kind) {
      case OK:
        consecutiveFailures = 0;
        call.bisectSawSuccess = true;
        checkDimension(outcome.vectors);
        for (int i = 0; i < request.size(); i++) {
          call.results[request.get(i)] = vectorResult(outcome.vectors[i]);
        }
        break;
      case QUOTA:
        // out of quota is the account's state, which no retry within the run repairs: the run fails and the next one resumes from
        // the same checkpoint (class P)
        throw new EmbeddingClientConfigException(config.provider().id() + " " + config.endpoint() + " refused model "
            + spec.getName() + " (" + config.modelName() + ", credential " + credentialSource + ") for lack of quota: "
            + outcome.error);
      case RETRYABLE:
        call.fill(request, EmbeddingResult.retryable(outcome.error));
        if (outcome.interrupted) {
          // the task is being killed: stop sending, and do not blame the provider for it
          call.interrupted = true;
        } else if (!outcome.notSent) {
          recordFailedRequest(request.size(), outcome.error);
        }
        break;
      case CONFIG:
        throw new EmbeddingClientConfigException(config.provider().id() + " " + config.endpoint()
            + " rejected the configuration of model " + spec.getName() + " (" + config.modelName() + ", credential "
            + credentialSource + "): " + outcome.error);
      case INPUT:
        if (request.size() == 1) {
          rejectAlone(call, request.get(0), outcome.error);
        } else if (call.bisectRoot == null) {
          // a top-level request: track the bisection it starts, to tell a request-level rejection from bad inputs
          call.bisectRoot = request;
          call.bisectRootError = outcome.error;
          call.bisectSawSuccess = false;
          call.identicalSingletons = 0;
          try {
            bisect(call, request, outcome.error);
          } catch (RequestRejectedAsAWhole rejected) {
            // indistinguishable from a request whose probed inputs are all bad: retry later, never PERMANENT here; the caller's
            // per-input retry then tells bad inputs from a rejected request (MicroBatchEmbeddingIterator)
            String error = REQUEST_REJECTED + " (every input probed alone was rejected with the request's own error): "
                + outcome.error;
            call.fill(request, EmbeddingResult.retryable(error));
            LOG.warn("EMBED-OPENAI-REQUEST-REJECTED model={} inputs={}: {}", spec.getName(), request.size(), error);
            recordFailedRequest(request.size(), error);
          } finally {
            call.bisectRoot = null;
          }
        } else {
          bisect(call, request, outcome.error);
        }
        break;
      default:
        throw new UnsupportedOperationException("Unknown outcome " + outcome.kind);
    }
  }

  /**
   * Settles an input the provider rejected when sent alone: PERMANENT. A provider that rejects the REQUEST rather than
   * an input, with a 400 naming no parameter (an Azure deployment of a non-embedding model), would otherwise turn every
   * row into PERMANENT one by one. So when, with no success yet in this bisection, the first two inputs sent alone are
   * rejected with the top-level request's own error, the LAST input of that request is probed alone too. Rejected alike,
   * the request cannot be told apart from one whose probed inputs are all bad, so the whole request becomes RETRYABLE
   * ({@link RequestRejectedAsAWhole}) and counts toward the outage short-circuit; the caller retries each input alone and fails
   * the run when every input of the call is rejected alike with none embedded (MicroBatchEmbeddingIterator). Two bad
   * inputs that lead the batch are told apart by the probe succeeding. Needs a top-level request of 3+ inputs.
   */
  private void rejectAlone(Call call, int index, String error) {
    boolean sameAsRoot = call.bisectRoot != null && !call.bisectSawSuccess && error.equals(call.bisectRootError);
    if (sameAsRoot && ++call.identicalSingletons == 2) {
      int last = call.bisectRoot.get(call.bisectRoot.size() - 1);
      if (last != index && call.results[last] == null && call.bisectRequests < config.bisectMaxRequests()
          && !call.pastDeadline()) {
        call.bisectRequests++;
        Outcome probe = sendWithRetries(call, Collections.singletonList(last));
        if (probe.kind == ProviderResponseKind.INPUT && error.equals(probe.error)) {
          throw new RequestRejectedAsAWhole();
        }
        resolveOutcome(call, Collections.singletonList(last), probe);
      }
    }
    call.results[index] = EmbeddingResult.permanent(error);
    if (LOG.isWarnEnabled()) {
      LOG.warn("EMBED-OPENAI-INPUT-REJECTED model={} {}: {}", spec.getName(), call.inputs.get(index).describeIdentity(), error);
    }
  }

  private void bisect(Call call, List<Integer> request, String parentError) {
    int middle = request.size() / 2;
    for (List<Integer> whole : Arrays.asList(request.subList(0, middle), request.subList(middle, request.size()))) {
      List<Integer> half = unsettled(call, whole);
      if (half.isEmpty()) {
        continue;
      }
      if (call.interrupted) {
        call.fill(half, EmbeddingResult.retryable(INTERRUPTED + NOT_SENT));
      } else if (outageOpen) {
        call.fill(half, EmbeddingResult.retryable(OUTAGE + NOT_SENT));
      } else if (call.bisectRequests >= config.bisectMaxRequests()) {
        call.fill(half, EmbeddingResult.retryable(BISECT_BUDGET_EXHAUSTED + " (" + config.bisectMaxRequests()
            + " requests) isolating: " + parentError));
      } else if (call.pastDeadline()) {
        call.fill(half, EmbeddingResult.retryable(DEADLINE_EXCEEDED + " while isolating: " + parentError));
      } else {
        call.bisectRequests++;
        sendAndResolve(call, half);
      }
    }
  }

  /** The inputs of {@code request} that have no result yet (a probe may have settled one ahead of its turn). */
  private static List<Integer> unsettled(Call call, List<Integer> request) {
    List<Integer> open = new ArrayList<>(request.size());
    for (int index : request) {
      if (call.results[index] == null) {
        open.add(index);
      }
    }
    return open;
  }

  private void recordFailedRequest(int inputs, String error) {
    consecutiveFailures++;
    LOG.warn("EMBED-OPENAI-REQUEST-FAILED model={} inputs={} consecutiveFailures={}: {}", spec.getName(), inputs,
        consecutiveFailures, error);
    if (consecutiveFailures >= config.outageConsecutiveFailures() && !outageOpen) {
      outageOpen = true;
      LOG.warn("EMBED-OPENAI-OUTAGE model={} endpoint={}: {} consecutive requests failed; the client stops calling",
          spec.getName(), config.endpoint(), consecutiveFailures);
    }
  }

  /** One request with its retries; never throws for a provider or transport failure. */
  private Outcome sendWithRetries(Call call, List<Integer> request) {
    byte[] body = requestBody(call.inputs, request);
    Map<String, String> headers = headers();
    String url = config.endpoint() + "/embeddings";
    String lastError = null;
    for (int attempt = 1; attempt <= config.retryMaxAttempts(); attempt++) {
      Outcome notSent = awaitSendSlot(call, attempt, lastError);
      if (notSent != null) {
        return notSent;
      }
      if (attempt > 1) {
        call.retries++;
      }
      call.requests++;
      Attempt result = sendOnce(call, request, url, headers, body);
      if (result.outcome != null) {
        return result.outcome;
      }
      lastError = result.error;
      Outcome stop = retryOrStop(call, attempt, result);
      if (stop != null) {
        return stop;
      }
    }
    return Outcome.failed(ProviderResponseKind.RETRYABLE, lastError);
  }

  /**
   * The deadline checks around taking a rate-limit token for one attempt.
   *
   * @return the outcome when the attempt must not be sent; null when it may be sent
   */
  private Outcome awaitSendSlot(Call call, int attempt, String lastError) {
    if (call.pastDeadline()) {
      return deadlineBeforeSend(attempt, lastError);
    }
    try {
      // the one send point: every attempt of every request, bisection's included, takes a token first
      call.rateLimitWaitMillis += rateLimiter.acquire(Math.max(0L, call.deadline - clock.getAsLong()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Outcome.interrupted(lastError == null ? INTERRUPTED : lastError + " (" + INTERRUPTED + ")");
    }
    if (call.pastDeadline()) {
      return deadlineBeforeSend(attempt, lastError);
    }
    return null;
  }

  /** The deadline passed before an attempt: not sent at all on the first attempt, a retryable failure after. */
  private static Outcome deadlineBeforeSend(int attempt, String lastError) {
    return attempt == 1 ? Outcome.notSent(DEADLINE_EXCEEDED + NOT_SENT)
        : Outcome.failed(ProviderResponseKind.RETRYABLE, lastError + " (" + DEADLINE_EXCEEDED + " before the next attempt)");
  }

  /** One attempt of one request: the final outcome, or the failure that may be retried. */
  private Attempt sendOnce(Call call, List<Integer> request, String url, Map<String, String> headers, byte[] body) {
    try {
      return classifyReply(call, request, post(url, headers, body));
    } catch (IOException e) {
      return Attempt.failure(ProviderResponseKind.RETRYABLE, e.getClass().getSimpleName() + ": " + redact(e.getMessage()),
          null);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Attempt.done(Outcome.interrupted(INTERRUPTED));
    } catch (UnsendableRequestException e) {
      return Attempt.done(Outcome.failed(ProviderResponseKind.CONFIG, e.getMessage()));
    }
  }

  /** The vectors of a 2xx reply, or the classified failure of any reply. */
  private Attempt classifyReply(Call call, List<Integer> request, HttpReply reply) {
    ErrorBody error = reply.status() / 100 == 2 ? null : ErrorBody.parse(reply.body(), this::redact);
    ProviderResponseKind kind = ErrorClassifier.classify(config.provider(), reply.status(),
        error == null ? ErrorBody.parse(null) : error);
    if (kind != ProviderResponseKind.OK) {
      return Attempt.failure(kind, "HTTP " + reply.status() + (isAuthFailure(reply.status()) ? describeAuth(error)
          : describe(error)), reply);
    }
    try {
      Parsed parsed = parse(reply.body(), request.size());
      checkServedModel(parsed.model, config.baseModel(), "response model", OpenAiClientConfig.BASE_MODEL_SUFFIX);
      call.addPromptTokens(parsed.promptTokens);
      return Attempt.done(Outcome.ok(parsed.vectors));
    } catch (IOException e) {
      return Attempt.failure(ProviderResponseKind.RETRYABLE, "malformed response: " + redact(e.getMessage()), reply);
    }
  }

  /**
   * After a failed attempt: the final outcome when it is not retried (not retryable, attempts or deadline used up,
   * backoff over the limit, interrupted while waiting); null after waiting for the next attempt.
   */
  private Outcome retryOrStop(Call call, int attempt, Attempt failure) {
    ProviderResponseKind kind = failure.kind;
    String lastError = failure.error;
    if (kind != ProviderResponseKind.RETRYABLE) {
      return Outcome.failed(kind, lastError);
    }
    if (attempt == config.retryMaxAttempts()) {
      return Outcome.failed(kind, lastError + " (after " + attempt + " attempts)");
    }
    HttpReply reply = failure.reply;
    long hint = reply == null ? -1
        : RetryAfter.hintMillis(reply.header("retry-after-ms"), reply.header("Retry-After"), clock.getAsLong());
    if (hint > config.maxBackoffMs()) {
      return Outcome.failed(kind, lastError + " (provider asks to wait " + hint + " ms, over the "
          + config.maxBackoffMs() + " ms max backoff)");
    }
    long wait = hint >= 0 ? hint : backoff(attempt);
    if (clock.getAsLong() + wait >= call.deadline) {
      return Outcome.failed(kind, lastError + " (" + DEADLINE_EXCEEDED + " before the next attempt)");
    }
    LOG.debug("EMBED-OPENAI-RETRY model={} attempt={} waitMs={}: {}", spec.getName(), attempt, wait, lastError);
    try {
      sleeper.sleep(wait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Outcome.interrupted(lastError + " (" + INTERRUPTED + ")");
    }
    return null;
  }

  /** One attempt: a final outcome, or a failure with its kind, error and reply (null when nothing was received). */
  private static final class Attempt {
    private final Outcome outcome;
    private final ProviderResponseKind kind;
    private final String error;
    private final HttpReply reply;

    private Attempt(Outcome outcome, ProviderResponseKind kind, String error, HttpReply reply) {
      this.outcome = outcome;
      this.kind = kind;
      this.error = error;
      this.reply = reply;
    }

    private static Attempt done(Outcome outcome) {
      return new Attempt(outcome, null, null, null);
    }

    private static Attempt failure(ProviderResponseKind kind, String error, HttpReply reply) {
      return new Attempt(null, kind, error, reply);
    }
  }

  /**
   * The transport call alone. An {@link IllegalArgumentException} from it means the HTTP client refused to build the
   * request (a URL or header it cannot send): retrying cannot help, and its message may quote a header value, so it is
   * redacted.
   */
  private HttpReply post(String url, Map<String, String> headers, byte[] body) throws IOException, InterruptedException {
    try {
      return transport.post(url, headers, body, config.requestTimeoutMs());
    } catch (IllegalArgumentException e) {
      throw new UnsendableRequestException("the request cannot be sent: " + redact(e.getMessage()));
    }
  }

  /** Signals, inside one bisection, that the provider rejects the request as a whole (see {@link #rejectAlone}). */
  private static final class RequestRejectedAsAWhole extends RuntimeException {
    private RequestRejectedAsAWhole() {
      super(null, null, false, false);
    }
  }

  /** The HTTP client refused to build a request; the message is already redacted. */
  private static final class UnsendableRequestException extends RuntimeException {
    private UnsendableRequestException(String message) {
      super(message);
    }
  }

  /** Full jitter: uniform in [0, min(max, initial * 2^(attempt-1))]. */
  private long backoff(int attempt) {
    long ceiling = (long) Math.min(config.maxBackoffMs(), config.initialBackoffMs() * Math.pow(2, attempt - 1.0));
    return Math.floorMod(jitter.nextLong(), ceiling + 1);
  }

  private String describe(ErrorBody error) {
    if (error == null || error.message() == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(": ").append(redact(error.message()));
    if (error.param() != null) {
      sb.append(" (param ").append(error.param()).append(')');
    }
    if (error.code() != null) {
      sb.append(" (code ").append(error.code()).append(')');
    }
    return sb.toString();
  }

  /**
   * A response whose vectors all have one length other than the configured dimension means the configuration is wrong
   * (e.g. {@code .dimensions} of a TEI or unknown model, which get no {@code dimensions} parameter): retrying cannot
   * help and marking the rows PERMANENT would poison them, so the run aborts.
   */
  private void checkDimension(float[][] vectors) {
    int common = -1;
    for (float[] vector : vectors) {
      if (vector == null) {
        continue;
      }
      if (common >= 0 && vector.length != common) {
        return;
      }
      common = vector.length;
    }
    if (common >= 0 && common != spec.getDimension()) {
      throw new EmbeddingClientConfigException(config.provider().id() + " " + config.endpoint() + " returned "
          + common + "-dimension vectors for model " + spec.getName() + " (" + config.modelName() + "), but "
          + MODEL_KEY_PREFIX + spec.getName() + DIMENSIONS_KEY_SUFFIX + "=" + spec.getDimension());
    }
  }

  /** 401/403: providers echo parts of the presented key in the message (OpenAI: a prefix and the last 4 chars). */
  private static boolean isAuthFailure(int status) {
    return status == 401 || status == 403;
  }

  /** An auth error's code and type only; its message, which may quote the key, is dropped. */
  private static String describeAuth(ErrorBody error) {
    if (error == null) {
      return "";
    }
    StringBuilder sb = new StringBuilder(" (provider message withheld");
    if (error.code() != null) {
      sb.append("; code ").append(error.code());
    }
    if (error.type() != null) {
      sb.append("; type ").append(error.type());
    }
    return sb.append(')').toString();
  }

  /** A vector of the configured dimension is OK; a missing one, or one of a different length than its batch-mates, is a
   *  malformed response the caller retries later (the model's width itself is checked by {@link #checkDimension}). */
  private EmbeddingResult vectorResult(float[] vector) {
    if (vector == null) {
      return EmbeddingResult.retryable("malformed response: no embedding for this input");
    }
    if (vector.length != spec.getDimension()) {
      return EmbeddingResult.retryable("malformed response: expected " + spec.getDimension() + " floats, got "
          + vector.length);
    }
    return EmbeddingResult.ok(vector);
  }

  private Map<String, String> headers() {
    Map<String, String> headers = new HashMap<>();
    headers.put("Content-Type", "application/json");
    headers.put("Accept", "application/json");
    if (key != null) {
      config.provider().authorize(headers, key);
    }
    return headers;
  }

  private byte[] requestBody(List<EmbeddingInput> inputs, List<Integer> request) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try (JsonGenerator json = JSON.createGenerator(out)) {
      json.writeStartObject();
      json.writeStringField("model", config.modelName());
      json.writeArrayFieldStart("input");
      for (int index : request) {
        json.writeString(inputs.get(index).getText());
      }
      json.writeEndArray();
      json.writeStringField("encoding_format", "float");
      if (config.dimensionsParam() != null) {
        json.writeNumberField("dimensions", config.dimensionsParam());
      }
      json.writeEndObject();
    } catch (IOException e) {
      throw new IllegalStateException("Cannot write an embeddings request", e);
    }
    return out.toByteArray();
  }

  /** Streams the served model, {@code data[].index} / {@code data[].embedding} and usage out of a 2xx body. */
  private static Parsed parse(byte[] body, int expected) throws IOException {
    float[][] vectors = new float[expected][];
    long promptTokens = -1;
    String model = null;
    try (JsonParser json = JSON.createParser(body)) {
      if (json.nextToken() != JsonToken.START_OBJECT) {
        throw new IOException("the body is not a JSON object");
      }
      while (json.nextToken() == JsonToken.FIELD_NAME) {
        String field = json.getCurrentName();
        JsonToken value = json.nextToken();
        if ("data".equals(field) && value == JsonToken.START_ARRAY) {
          while (json.nextToken() == JsonToken.START_OBJECT) {
            readEntry(json, vectors);
          }
        } else if ("model".equals(field) && value == JsonToken.VALUE_STRING) {
          model = json.getText();
        } else if ("usage".equals(field) && value == JsonToken.START_OBJECT) {
          promptTokens = readPromptTokens(json, promptTokens);
        } else {
          json.skipChildren();
        }
      }
    }
    return new Parsed(vectors, promptTokens, model);
  }

  /**
   * Reads a {@code usage} object: its {@code prompt_tokens}, or {@code promptTokens} as it was when there is none. Usage is
   * optional metadata: anything but an integer leaves it unreported, never fails the vectors.
   */
  private static long readPromptTokens(JsonParser json, long promptTokens) throws IOException {
    long tokens = promptTokens;
    while (json.nextToken() == JsonToken.FIELD_NAME) {
      String usageField = json.getCurrentName();
      json.nextToken();
      if ("prompt_tokens".equals(usageField) && json.currentToken() == JsonToken.VALUE_NUMBER_INT
          && json.getNumberType() != JsonParser.NumberType.BIG_INTEGER) {
        tokens = json.getLongValue();
      } else {
        json.skipChildren();
      }
    }
    return tokens;
  }

  private static void readEntry(JsonParser json, float[][] vectors) throws IOException {
    int index = -1;
    float[] vector = null;
    while (json.nextToken() == JsonToken.FIELD_NAME) {
      String field = json.getCurrentName();
      JsonToken value = json.nextToken();
      if ("index".equals(field) && value == JsonToken.VALUE_NUMBER_INT) {
        index = json.getIntValue();
      } else if ("embedding".equals(field) && value == JsonToken.START_ARRAY) {
        vector = readFloats(json);
      } else {
        json.skipChildren();
      }
    }
    if (index < 0 || index >= vectors.length || vector == null || vectors[index] != null) {
      throw new IOException("data entry with index " + index + " for " + vectors.length + " inputs"
          + (vector == null ? ", without a float embedding" : ""));
    }
    vectors[index] = vector;
  }

  private static float[] readFloats(JsonParser json) throws IOException {
    float[] buffer = new float[INITIAL_VECTOR_CAPACITY];
    int size = 0;
    for (JsonToken token = json.nextToken(); token != JsonToken.END_ARRAY; token = json.nextToken()) {
      if (token != JsonToken.VALUE_NUMBER_FLOAT && token != JsonToken.VALUE_NUMBER_INT) {
        throw new IOException("an embedding holds a non-number " + token);
      }
      float value = json.getFloatValue();
      if (!Float.isFinite(value)) {
        throw new IOException("an embedding holds a non-finite number");
      }
      if (size == buffer.length) {
        buffer = Arrays.copyOf(buffer, size * 2);
      }
      buffer[size++] = value;
    }
    return Arrays.copyOf(buffer, size);
  }

  /** TEI: the served identity and server limits from {@code GET /info}, which must describe the configured embedding model. */
  private void readTeiInfo() {
    String url = OpenAiProvider.HF_TEI.infoUrl(config.endpoint());
    HttpReply reply;
    try {
      reply = transport.get(url, headers(), config.requestTimeoutMs());
    } catch (IOException e) {
      throw new IllegalStateException("TEI GET " + url + " failed: " + e.getClass().getSimpleName() + ": "
          + redact(e.getMessage()));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted reading TEI GET " + url);
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException("TEI GET " + url + " cannot be sent: " + redact(e.getMessage()));
    }
    if (reply.status() != 200) {
      ErrorBody error = ErrorBody.parse(reply.body(), this::redact);
      throw new IllegalStateException("TEI GET " + url + " returned HTTP " + reply.status()
          + (isAuthFailure(reply.status()) ? describeAuth(error) : describe(error)));
    }
    JsonNode info;
    try {
      info = MAPPER.readTree(reply.body());
    } catch (IOException e) {
      throw new IllegalStateException("TEI GET " + url + " returned a body that is not JSON");
    }
    JsonNode modelType = info == null ? null : info.get("model_type");
    if (modelType == null || !modelType.has("embedding")) {
      throw new IllegalStateException("TEI at " + config.endpoint() + " does not serve an embedding model (model_type "
          + modelType + ")");
    }
    JsonNode modelId = info.get("model_id");
    checkServedModel(modelId != null && modelId.isTextual() ? modelId.asText() : null, config.modelName(), "model_id",
        EmbeddingTableConfig.MODEL_NAME_SUFFIX);
    int serverInputs = info.path("max_client_batch_size").asInt(-1);
    long serverTokens = info.path("max_batch_tokens").asLong(-1);
    if (serverInputs <= 0 || serverTokens <= 0) {
      throw new IllegalStateException("TEI GET " + url + " lacks max_client_batch_size / max_batch_tokens");
    }
    maxInputs = maxInputs > 0 ? Math.min(maxInputs, serverInputs) : serverInputs;
    maxTokens = maxTokens > 0 ? Math.min(maxTokens, serverTokens) : serverTokens;
  }

  /** A deployment alias or endpoint must not silently serve another vector space of the same dimension. */
  private void checkServedModel(String served, String expected, String field, String expectedConfigSuffix) {
    if (!expected.equals(served)) {
      throw new EmbeddingClientConfigException(config.provider().id() + " " + config.endpoint() + " served model "
          + field + "=" + redact(served) + " does not match configured model " + expected
          + " (" + MODEL_KEY_PREFIX + spec.getName() + expectedConfigSuffix + ")");
    }
  }

  /**
   * Refuses two inputs of one record key (source_path_sha, source_modification_time, source_file_size, modality, chunk_id).
   */
  private static void checkUniqueIds(List<EmbeddingInput> inputs) {
    Set<List<Object>> ids = new HashSet<>();
    for (EmbeddingInput input : inputs) {
      if (!ids.add(Arrays.asList(input.getSourcePathSha(), input.getSourceModificationTime(), input.getSourceFileSize(),
          input.getModality(), input.getChunkId()))) {
        throw new IllegalArgumentException("Duplicate embedding input " + input.describeIdentity() + " in one call");
      }
    }
  }

  /** Provider or transport text with the API key and its first {@value #KEY_PREFIX_CHARS} chars replaced. */
  private String redact(String text) {
    if (text == null || key == null) {
      return text;
    }
    String redacted = text.replace(key, REDACTED);
    return key.length() > KEY_PREFIX_CHARS ? redacted.replace(key.substring(0, KEY_PREFIX_CHARS), REDACTED) : redacted;
  }

  /** The configuration and credential SOURCE; never the key. */
  @Override
  public String toString() {
    return "OpenAiCompatibleEmbeddingClient{" + config + ", " + rateLimiter + "}";
  }

  /** The per-call state: results by input position and the request counters. */
  private final class Call {
    private final List<EmbeddingInput> inputs;
    private final EmbeddingResult[] results;
    private final long deadline;
    private final long startMillis;
    private int requests;
    private int retries;
    private int bisectRequests;
    private long promptTokens = -1;
    private long rateLimitWaitMillis;
    /** The top-level request being bisected, or null; with its error, whether any part succeeded, and how many
     *  inputs sent alone were rejected with that same error. */
    private List<Integer> bisectRoot;
    private String bisectRootError;
    private boolean bisectSawSuccess;
    private int identicalSingletons;
    /** Set once the calling thread was interrupted: nothing more is sent in this call. */
    private boolean interrupted;

    private Call(List<EmbeddingInput> inputs, long deadline) {
      this.inputs = inputs;
      this.results = new EmbeddingResult[inputs.size()];
      this.deadline = deadline;
      this.startMillis = clock.getAsLong();
    }

    private boolean pastDeadline() {
      return clock.getAsLong() >= deadline;
    }

    private void fill(List<Integer> request, EmbeddingResult result) {
      for (int index : request) {
        results[index] = result;
      }
    }

    private void addPromptTokens(long tokens) {
      if (tokens >= 0) {
        promptTokens = Math.max(promptTokens, 0) + tokens;
      }
    }

    private EmbeddingCallStats stats() {
      return new EmbeddingCallStats(requests, retries, bisectRequests, promptTokens, clock.getAsLong() - startMillis,
          rateLimitWaitMillis);
    }
  }

  /** What a request (with its retries) came to. */
  private static final class Outcome {
    private final ProviderResponseKind kind;
    private final float[][] vectors;
    private final String error;
    /** The calling thread was interrupted: RETRYABLE, but not a provider failure. */
    private final boolean interrupted;
    private final boolean notSent;

    private Outcome(ProviderResponseKind kind, float[][] vectors, String error, boolean interrupted, boolean notSent) {
      this.kind = kind;
      this.vectors = vectors;
      this.error = error;
      this.interrupted = interrupted;
      this.notSent = notSent;
    }

    private static Outcome ok(float[][] vectors) {
      return new Outcome(ProviderResponseKind.OK, vectors, null, false, false);
    }

    private static Outcome failed(ProviderResponseKind kind, String error) {
      return new Outcome(kind, null, error, false, false);
    }

    private static Outcome notSent(String error) {
      return new Outcome(ProviderResponseKind.RETRYABLE, null, error, false, true);
    }

    private static Outcome interrupted(String error) {
      return new Outcome(ProviderResponseKind.RETRYABLE, null, error, true, false);
    }
  }

  /** A parsed 2xx body. */
  private static final class Parsed {
    private final float[][] vectors;
    private final long promptTokens;
    private final String model;

    private Parsed(float[][] vectors, long promptTokens, String model) {
      this.vectors = vectors;
      this.promptTokens = promptTokens;
      this.model = model;
    }
  }
}
