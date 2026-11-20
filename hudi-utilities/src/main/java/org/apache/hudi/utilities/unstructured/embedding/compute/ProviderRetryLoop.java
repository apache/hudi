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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.function.LongSupplier;

/**
 * Sends one request with its retries: every attempt first takes a token of {@link RequestRateLimiter}, then a transient answer
 * ({@link ProviderResponseKind#RETRYABLE}) is retried with full-jitter exponential backoff, honouring the answer's retry hint
 * up to {@code .retry.max.backoff.ms}, within the call's
 * {@code .request.deadline.ms} and {@code .retry.max.attempts} ({@link ProviderClientSettings}). After
 * {@code .outage.consecutive.failures} requests in a row stay RETRYABLE, {@link #outageOpen()} is true and every later request
 * is not sent. Any other answer ends the request at once.
 *
 * <p>Not thread safe. {@link #startCall} opens a call, {@link #stats} reports it.
 */
public final class ProviderRetryLoop {

  private static final Logger LOG = LoggerFactory.getLogger(ProviderRetryLoop.class);
  public static final String OUTAGE = "provider outage short-circuit";
  public static final String DEADLINE_EXCEEDED = "deadline exceeded";
  public static final String INTERRUPTED = "interrupted";
  private static final String NOT_SENT = ": the request was not sent";

  /** Waits between attempts; injectable so tests advance a fake clock instead of sleeping. */
  @FunctionalInterface
  public interface Sleeper {
    void sleep(long millis) throws InterruptedException;
  }

  /** Potentially blocking request preparation, after the permit wait and before the final deadline check. */
  @FunctionalInterface
  public interface Preparation {
    void prepare() throws InterruptedException;
  }

  /** One attempt of a request. */
  @FunctionalInterface
  public interface Sender<T> {
    /** Never throws for a provider or transport failure: those are an {@link Answer} of their kind. */
    Answer<T> send() throws InterruptedException;
  }

  /** What one attempt, or a request with its retries, came to. */
  public static final class Answer<T> {
    private final ProviderResponseKind kind;
    private final T value;
    private final String error;
    private final long retryAfterMillis;
    private final boolean byInterrupt;

    private Answer(ProviderResponseKind kind, T value, String error, long retryAfterMillis, boolean interrupted) {
      this.kind = kind;
      this.value = value;
      this.error = error;
      this.retryAfterMillis = retryAfterMillis;
      this.byInterrupt = interrupted;
    }

    public static <T> Answer<T> ok(T value) {
      return new Answer<>(ProviderResponseKind.OK, value, null, -1, false);
    }

    /** @param retryAfterMillis the provider's retry hint, -1 for none */
    public static <T> Answer<T> failed(ProviderResponseKind kind, String error, long retryAfterMillis) {
      return new Answer<>(kind, null, error, retryAfterMillis, false);
    }

    static <T> Answer<T> interrupted(String error) {
      return new Answer<>(ProviderResponseKind.RETRYABLE, null, error, -1, true);
    }

    public ProviderResponseKind kind() {
      return kind;
    }

    public T value() {
      return value;
    }

    public String error() {
      return error;
    }

    /** The calling thread was interrupted: RETRYABLE, but not a provider failure. */
    public boolean isInterrupted() {
      return byInterrupt;
    }
  }

  private final String name;
  private final ProviderClientSettings settings;
  private final RequestRateLimiter rateLimiter;
  private final LongSupplier clock;
  private final Sleeper sleeper;
  private final Random jitter;
  private int consecutiveFailures;
  private boolean outageOpen;
  private boolean interruptedInCall;
  private long deadline;
  private long callStart;
  private int requests;
  private int retries;
  private long rateLimitWaitMillis;

  /** @param name the model and provider, for log lines */
  public ProviderRetryLoop(String name, ProviderClientSettings settings, RequestRateLimiter rateLimiter, LongSupplier clock,
                           Sleeper sleeper, Random jitter) {
    this.name = name;
    this.settings = settings;
    this.rateLimiter = rateLimiter;
    this.clock = clock;
    this.sleeper = sleeper;
    this.jitter = jitter;
  }

  /** Opens a call: resets its counters and starts its deadline. */
  public void startCall() {
    callStart = clock.getAsLong();
    deadline = callStart + settings.deadlineMs();
    requests = 0;
    retries = 0;
    rateLimitWaitMillis = 0;
    interruptedInCall = false;
  }

  /** The current call's counts. */
  public EmbeddingCallStats stats() {
    return new EmbeddingCallStats(requests, retries, rateLimitWaitMillis);
  }

  /** The current call's cost; {@code isolationRequests} is reported as bisection requests, {@code promptTokens} -1 if unknown. */
  public EmbeddingCallStats stats(int isolationRequests, long promptTokens) {
    return new EmbeddingCallStats(requests, retries, isolationRequests, promptTokens, clock.getAsLong() - callStart,
        rateLimitWaitMillis);
  }

  public boolean outageOpen() {
    return outageOpen;
  }

  public int consecutiveFailures() {
    return consecutiveFailures;
  }

  /** Set once the calling thread was interrupted in this call: nothing more is sent. */
  public boolean wasInterrupted() {
    return interruptedInCall;
  }

  /** The non-negative time left for this call, including permit waiting. */
  public long remainingMillis() {
    return Math.max(0L, deadline - clock.getAsLong());
  }

  public boolean pastDeadline() {
    return clock.getAsLong() >= deadline;
  }

  /**
   * Why a request of this call is not sent (the call was interrupted, the outage short-circuit is open, or the deadline passed),
   * or null when it may be sent.
   */
  public String notSentReason() {
    if (interruptedInCall) {
      return INTERRUPTED + NOT_SENT;
    }
    if (outageOpen) {
      return OUTAGE + NOT_SENT;
    }
    if (pastDeadline()) {
      return DEADLINE_EXCEEDED + NOT_SENT;
    }
    return null;
  }

  /** One request with its retries; a RETRYABLE end counts toward the outage short-circuit, an OK one resets it. */
  public <T> Answer<T> send(Sender<T> sender) {
    return send(() -> { }, sender);
  }

  /** Prepares each attempt before checking the deadline and counting its actual send. */
  public <T> Answer<T> send(Preparation preparation, Sender<T> sender) {
    int requestsBefore = requests;
    Answer<T> answer = attempts(preparation, sender);
    if (answer.kind() == ProviderResponseKind.OK) {
      consecutiveFailures = 0;
    } else if (answer.kind() == ProviderResponseKind.RETRYABLE && !answer.isInterrupted() && requests > requestsBefore) {
      consecutiveFailures++;
      LOG.warn("EMBED-PROVIDER-REQUEST-FAILED {} consecutiveFailures={}: {}", name, consecutiveFailures, answer.error());
      if (consecutiveFailures >= settings.outageConsecutiveFailures() && !outageOpen) {
        outageOpen = true;
        LOG.warn("EMBED-PROVIDER-OUTAGE {}: {} consecutive requests failed; later requests are not sent", name, consecutiveFailures);
      }
    } else if (answer.isInterrupted()) {
      interruptedInCall = true;
    }
    return answer;
  }

  private <T> Answer<T> attempts(Preparation preparation, Sender<T> sender) {
    String lastError = null;
    for (int attempt = 1; attempt <= settings.retryMaxAttempts(); attempt++) {
      Answer<T> stop = beforeSend(preparation, lastError);
      if (stop != null) {
        return stop;
      }
      if (attempt > 1) {
        retries++;
      }
      requests++;
      Answer<T> answer;
      try {
        answer = sender.send();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return Answer.interrupted(INTERRUPTED);
      }
      if (answer.kind() != ProviderResponseKind.RETRYABLE) {
        return answer;
      }
      lastError = answer.error();
      stop = retryOrStop(attempt, answer.retryAfterMillis, lastError);
      if (stop != null) {
        return stop;
      }
    }
    return Answer.failed(ProviderResponseKind.RETRYABLE, lastError, -1);
  }

  /**
   * Before a request: nothing is sent once the call is stopped; else waits for the rate limit and prepares the request.
   * Returns the answer that ends the call (not sent, interrupted, or past the deadline), or null to send.
   */
  private <T> Answer<T> beforeSend(Preparation preparation, String lastError) {
    String notSent = notSentReason();
    if (notSent != null) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, notSent, -1);
    }
    try {
      rateLimitWaitMillis += rateLimiter.acquire(remainingMillis());
      if (!pastDeadline()) {
        preparation.prepare();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Answer.interrupted(lastError == null ? INTERRUPTED : lastError + " (" + INTERRUPTED + ")");
    }
    if (pastDeadline()) {
      String notSentMessage = DEADLINE_EXCEEDED + NOT_SENT;
      return Answer.failed(ProviderResponseKind.RETRYABLE, lastError == null ? notSentMessage : lastError + " (" + notSentMessage + ")", -1);
    }
    return null;
  }

  /**
   * After a RETRYABLE answer of attempt {@code attempt}: the answer that ends the call (attempts exhausted, a provider hint
   * over the max backoff, the deadline before the next attempt, or an interrupt while waiting), or null after the wait.
   */
  private <T> Answer<T> retryOrStop(int attempt, long hint, String lastError) {
    if (attempt == settings.retryMaxAttempts()) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, lastError + " (after " + attempt + " attempts)", -1);
    }
    if (hint > settings.maxBackoffMs()) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, lastError + " (provider asks to wait " + hint + " ms, over the "
          + settings.maxBackoffMs() + " ms max backoff)", -1);
    }
    long wait = hint >= 0 ? hint : backoff(attempt);
    if (clock.getAsLong() + wait >= deadline) {
      return Answer.failed(ProviderResponseKind.RETRYABLE, lastError + " (" + DEADLINE_EXCEEDED + " before the next attempt)",
          -1);
    }
    LOG.debug("EMBED-PROVIDER-RETRY {} attempt={} waitMs={}: {}", name, attempt, wait, lastError);
    try {
      sleeper.sleep(wait);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return Answer.interrupted(lastError + " (" + INTERRUPTED + ")");
    }
    return null;
  }

  /** Full jitter: uniform in [0, min(max, initial * 2^(attempt-1))]. */
  private long backoff(int attempt) {
    long ceiling = (long) Math.min(settings.maxBackoffMs(), settings.initialBackoffMs() * Math.pow(2, attempt - 1.0));
    return Math.floorMod(jitter.nextLong(), ceiling + 1);
  }
}
