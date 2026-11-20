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

import java.io.Serializable;

/**
 * What one {@link EmbeddingClient#embed} call cost at the provider: request totals that have no per-input home.
 */
public final class EmbeddingCallStats implements Serializable {

  private static final long serialVersionUID = 1L;

  /** The stats of a client that reports none, or of a call that sent nothing. */
  public static final EmbeddingCallStats NONE = new EmbeddingCallStats(0, 0, 0, -1L, 0L);

  private final int requests;
  private final int retries;
  private final int bisectRequests;
  private final long providerPromptTokens;
  private final long wallMillis;
  private final long rateLimitWaitMillis;

  /**
   * Stats of a call that reports only its request counts and rate-limit wait: no bisection requests, no provider token count
   * (-1) and no wall time (0, not measured), so the wait is not included in {@link #getWallMillis()}.
   */
  public EmbeddingCallStats(int requests, int retries, long rateLimitWaitMillis) {
    this(requests, retries, 0, -1L, 0L, rateLimitWaitMillis);
  }

  /** Stats of a call with no rate-limit wait. */
  public EmbeddingCallStats(int requests, int retries, int bisectRequests, long providerPromptTokens, long wallMillis) {
    this(requests, retries, bisectRequests, providerPromptTokens, wallMillis, 0L);
  }

  /**
   * @param requests             provider requests issued, retries and bisection included
   * @param retries              requests repeated after a retryable failure
   * @param bisectRequests       extra requests spent isolating inputs the provider rejected
   * @param providerPromptTokens input tokens the provider reported (e.g. OpenAI {@code usage.prompt_tokens}) summed
   *                             over the successful requests; -1 when it reported none
   * @param wallMillis           elapsed time of the call
   * @param rateLimitWaitMillis  time the call waited for request tokens of the model's rate limit
   *                             ({@link RequestRateLimiter}), included in {@code wallMillis}
   */
  public EmbeddingCallStats(int requests, int retries, int bisectRequests, long providerPromptTokens, long wallMillis,
                            long rateLimitWaitMillis) {
    this.requests = requests;
    this.retries = retries;
    this.bisectRequests = bisectRequests;
    this.providerPromptTokens = providerPromptTokens;
    this.wallMillis = wallMillis;
    this.rateLimitWaitMillis = rateLimitWaitMillis;
  }

  public int getRequests() {
    return requests;
  }

  public int getRetries() {
    return retries;
  }

  public int getBisectRequests() {
    return bisectRequests;
  }

  /** Input tokens the provider reported, or -1 when it reported none. */
  public long getProviderPromptTokens() {
    return providerPromptTokens;
  }

  public long getWallMillis() {
    return wallMillis;
  }

  /** Time the call waited for rate-limit tokens before its requests; 0 when the model has no rate limit. */
  public long getRateLimitWaitMillis() {
    return rateLimitWaitMillis;
  }

  @Override
  public String toString() {
    return "EmbeddingCallStats{requests=" + requests + ", retries=" + retries + ", bisectRequests=" + bisectRequests
        + ", providerPromptTokens=" + providerPromptTokens + ", wallMillis=" + wallMillis + ", rateLimitWaitMillis=" + rateLimitWaitMillis + "}";
  }
}
