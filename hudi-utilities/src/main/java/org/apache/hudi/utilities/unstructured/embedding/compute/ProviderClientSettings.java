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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import java.io.Serializable;

/**
 * The request, retry and outage settings of one model binding, the one declaration and parse every provider client shares. Read
 * from {@code hoodie.unstructured.embedding.model.<alias>.*} under the key suffixes declared here: {@code .request.timeout.ms}
 * (30000), {@code .request.deadline.ms} (300000), {@code .retry.max.attempts} (5), {@code .retry.initial.backoff.ms} (500),
 * {@code .retry.max.backoff.ms} (60000), {@code .outage.consecutive.failures} (3) and {@code .request.max.inputs} (the
 * provider's default, never above its maximum).
 */
public final class ProviderClientSettings implements Serializable {

  private static final long serialVersionUID = 1L;
  /** Inputs per request at most. */
  public static final String REQUEST_MAX_INPUTS_SUFFIX = ".request.max.inputs";
  /** One attempt's timeout, in ms. */
  public static final String REQUEST_TIMEOUT_MS_SUFFIX = ".request.timeout.ms";
  /** A call's deadline over all its attempts, in ms. */
  public static final String REQUEST_DEADLINE_MS_SUFFIX = ".request.deadline.ms";
  /** Attempts of one request at most. */
  public static final String RETRY_MAX_ATTEMPTS_SUFFIX = ".retry.max.attempts";
  /** The first backoff between attempts, in ms. */
  public static final String RETRY_INITIAL_BACKOFF_MS_SUFFIX = ".retry.initial.backoff.ms";
  /** The longest backoff between attempts, in ms. */
  public static final String RETRY_MAX_BACKOFF_MS_SUFFIX = ".retry.max.backoff.ms";
  /** Requests in a row that stay retryable before {@link ProviderRetryLoop#outageOpen()} is true. */
  public static final String OUTAGE_CONSECUTIVE_FAILURES_SUFFIX = ".outage.consecutive.failures";
  static final int DEFAULT_REQUEST_TIMEOUT_MS = 30_000;
  static final int DEFAULT_REQUEST_DEADLINE_MS = 300_000;
  static final int DEFAULT_RETRY_MAX_ATTEMPTS = 5;
  static final int DEFAULT_RETRY_INITIAL_BACKOFF_MS = 500;
  static final int DEFAULT_RETRY_MAX_BACKOFF_MS = 60_000;
  static final int DEFAULT_OUTAGE_CONSECUTIVE_FAILURES = 3;

  private final String alias;
  private final int requestTimeoutMs;
  private final int deadlineMs;
  private final int retryMaxAttempts;
  private final int initialBackoffMs;
  private final int maxBackoffMs;
  private final int outageConsecutiveFailures;
  private final int maxInputs;

  /** {@link #maxInputs()} of settings that do not bound the inputs per request ({@link #withoutRequestBound}). */
  public static final int NO_INPUT_BOUND = -1;

  private ProviderClientSettings(String alias, TypedProperties props, int defaultMaxInputs, int providerMaxInputs) {
    this.alias = alias;
    this.requestTimeoutMs = positiveInt(props, key(REQUEST_TIMEOUT_MS_SUFFIX), DEFAULT_REQUEST_TIMEOUT_MS);
    this.deadlineMs = positiveInt(props, key(REQUEST_DEADLINE_MS_SUFFIX), DEFAULT_REQUEST_DEADLINE_MS);
    this.retryMaxAttempts = positiveInt(props, key(RETRY_MAX_ATTEMPTS_SUFFIX), DEFAULT_RETRY_MAX_ATTEMPTS);
    this.initialBackoffMs = positiveInt(props, key(RETRY_INITIAL_BACKOFF_MS_SUFFIX), DEFAULT_RETRY_INITIAL_BACKOFF_MS);
    this.maxBackoffMs = positiveInt(props, key(RETRY_MAX_BACKOFF_MS_SUFFIX), DEFAULT_RETRY_MAX_BACKOFF_MS);
    this.outageConsecutiveFailures =
        positiveInt(props, key(OUTAGE_CONSECUTIVE_FAILURES_SUFFIX), DEFAULT_OUTAGE_CONSECUTIVE_FAILURES);
    if (defaultMaxInputs == NO_INPUT_BOUND) {
      this.maxInputs = NO_INPUT_BOUND;
      return;
    }
    this.maxInputs = positiveInt(props, key(REQUEST_MAX_INPUTS_SUFFIX), defaultMaxInputs);
    if (maxInputs > providerMaxInputs) {
      throw new IllegalArgumentException(key(REQUEST_MAX_INPUTS_SUFFIX) + "=" + maxInputs
          + " exceeds the provider maximum " + providerMaxInputs);
    }
  }

  /**
   * @throws IllegalArgumentException naming the offending key: a value that is not a positive integer
   */
  public static ProviderClientSettings from(EmbeddingModelSpec spec, TypedProperties props) {
    return withoutRequestBound(spec, props);
  }

  /**
   * The settings of a client that bounds its requests itself, as the OpenAI-compatible client does with its own inputs and
   * tokens maxima: {@code .request.max.inputs} is not read here and {@link #maxInputs()} is
   * {@link #NO_INPUT_BOUND}.
   *
   * @throws IllegalArgumentException naming the offending key: a value that is not a positive integer
   */
  public static ProviderClientSettings withoutRequestBound(EmbeddingModelSpec spec, TypedProperties props) {
    return new ProviderClientSettings(spec.getName(), props, NO_INPUT_BOUND, NO_INPUT_BOUND);
  }

  /**
   * @param defaultMaxInputs  inputs per request when {@code .request.max.inputs} is unset
   * @param providerMaxInputs the most inputs the provider takes in one request
   * @throws IllegalArgumentException naming the offending key: a value that is not a positive integer, or more inputs than the
   *     provider takes
   */
  public static ProviderClientSettings from(EmbeddingModelSpec spec, TypedProperties props, int defaultMaxInputs,
                                            int providerMaxInputs) {
    return new ProviderClientSettings(spec.getName(), props, defaultMaxInputs, providerMaxInputs);
  }

  /** {@code hoodie.unstructured.embedding.model.<alias><suffix>}. */
  private String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + alias + suffix;
  }

  /**
   * Parses a positive integer setting: {@code defaultValue} when {@code key}
   * is not set; a set value that is blank, not a number, not positive or above {@link Integer#MAX_VALUE} is refused.
   *
   * @throws IllegalArgumentException naming the key
   */
  public static int positiveInt(TypedProperties props, String key, int defaultValue) {
    if (!props.containsKey(key)) {
      return defaultValue;
    }
    String raw = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue()).trim();
    if (raw.isEmpty()) {
      throw new IllegalArgumentException(key + " is set but blank: set a positive integer, or remove the key for the default "
          + defaultValue);
    }
    long value;
    try {
      value = Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + "=" + raw + " is not a number");
    }
    if (value <= 0 || value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(key + "=" + value + " must be > 0 and fit an int");
    }
    return (int) value;
  }

  public int requestTimeoutMs() {
    return requestTimeoutMs;
  }

  /** Wall-clock budget of one call, retries included. */
  public int deadlineMs() {
    return deadlineMs;
  }

  /** Attempts per request, the first included. */
  public int retryMaxAttempts() {
    return retryMaxAttempts;
  }

  public int initialBackoffMs() {
    return initialBackoffMs;
  }

  public int maxBackoffMs() {
    return maxBackoffMs;
  }

  public int outageConsecutiveFailures() {
    return outageConsecutiveFailures;
  }

  /** Most inputs packed into one request (texts; an image always travels alone); {@link #NO_INPUT_BOUND} when not read. */
  public int maxInputs() {
    return maxInputs;
  }

  @Override
  public String toString() {
    return "timeoutMs=" + requestTimeoutMs + ", deadlineMs=" + deadlineMs + ", retryMaxAttempts=" + retryMaxAttempts
        + ", backoffMs=" + initialBackoffMs + ".." + maxBackoffMs + ", outageConsecutiveFailures=" + outageConsecutiveFailures
        + ", maxInputs=" + maxInputs;
  }
}
