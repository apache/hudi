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

import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;
import org.apache.hudi.utilities.unstructured.embedding.compute.SingleTokenReader;

import java.util.function.LongSupplier;
import java.util.function.UnaryOperator;

/**
 * Where {@link VertexMultimodalEmbeddingClient} gets its bearer token, per {@value VertexClientConfig#CREDENTIAL_SOURCE_SUFFIX}:
 * <ul>
 *   <li>{@value VertexClientConfig#CREDENTIAL_SOURCE_TOKEN}: an access token in a file or an environment variable. A refresh
 *       reads the same source again, so a file that an external refresher rewrites (a sidecar, a cron) is picked up;</li>
 *   <li>{@value VertexClientConfig#CREDENTIAL_SOURCE_GOOGLE}: a Google credential that mints its own tokens
 *       ({@link GoogleAccessTokens}).</li>
 * </ul>
 * One instance per client, so per compute task. A token value never appears in a message.
 */
abstract class VertexAccessTokens {

  /**
   * @param clock the client's clock, for a token's expiry
   * @param sleeper waits between attempts at the token endpoint
   * @param environment looks up an environment variable ({@code System::getenv} in production)
   * @throws EmbeddingClientConfigException when the configured source holds no usable token, no usable service-account key, or
   *     names a token endpoint that is not an http(s) URL; a metadata server that does not exist is found at the first token
   *     request ({@link #current()}), and is the configuration too
   */
  static VertexAccessTokens of(VertexClientConfig config, LongSupplier clock, RequestRateLimiter.Sleeper sleeper,
                               UnaryOperator<String> environment) {
    return VertexClientConfig.CREDENTIAL_SOURCE_GOOGLE.equals(config.credentialSource())
        ? new GoogleAccessTokens(config, clock, sleeper, environment) : new TokenSource(config, environment);
  }

  /** Where the tokens actually come from, for messages: never a token. */
  abstract String describe();

  /**
   * The token to send now; a Google credential first refreshes a token that is expired or about to expire.
   *
   * @throws EmbeddingClientConfigException the credential is rejected or unusable (class P: retrying cannot help)
   * @throws org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException the token endpoint
   *     cannot be reached (an outage)
   */
  abstract String current() throws InterruptedException;

  /**
   * Called when the endpoint answered HTTP 401 to {@code rejected}: obtains a new token.
   *
   * @return whether the current token now differs from {@code rejected}, i.e. a re-send can succeed
   * @throws EmbeddingClientConfigException the credential is rejected or unusable
   * @throws org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException the token endpoint
   *     cannot be reached (an outage)
   */
  abstract boolean refresh(String rejected) throws InterruptedException;

  /** {@value VertexClientConfig#CREDENTIAL_SOURCE_TOKEN}: the token in a file or an environment variable, read again on refresh. */
  private static final class TokenSource extends VertexAccessTokens {

    private final VertexClientConfig config;
    private final UnaryOperator<String> environment;
    private String token;

    TokenSource(VertexClientConfig config, UnaryOperator<String> environment) {
      this.config = config;
      this.environment = environment;
      this.token = read();
    }

    @Override
    String describe() {
      return config.describeCredential();
    }

    @Override
    String current() {
      return token;
    }

    @Override
    boolean refresh(String rejected) {
      token = read();
      return !token.equals(rejected);
    }

    /** One printable token, from the configured file or environment variable; the value never leaks. */
    private String read() {
      return SingleTokenReader.read(config.credentialFile(), config.credentialEnv(), environment, "Vertex access token",
          config.describeCredential(), EmbeddingClientConfigException::new);
    }
  }
}
