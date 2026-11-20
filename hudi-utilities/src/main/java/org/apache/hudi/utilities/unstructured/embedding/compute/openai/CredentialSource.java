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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.unstructured.embedding.compute.SingleTokenReader;

import java.util.function.UnaryOperator;
import java.util.regex.Pattern;

/**
 * Where an endpoint key comes from: a file (e.g. a mounted Kubernetes secret) or an environment variable named by
 * config; never a Hudi property. Only {@link #describe()} (the source, never the value) may appear in logs or messages.
 * {@link #checkEnvName} is the one check, for every provider client, that a configured variable is a name.
 */
public final class CredentialSource {

  private static final Pattern ENV_VAR_NAME = Pattern.compile("[A-Za-z_]\\w*");

  private final String file;
  private final String env;

  private CredentialSource(String file, String env) {
    this.file = file;
    this.env = env;
  }

  /** The configured file, else the configured or provider-default env var name, else none. */
  static CredentialSource of(OpenAiClientConfig config) {
    return new CredentialSource(config.credentialFile(), config.credentialFile() == null ? config.credentialEnv() : null);
  }

  /**
   * Refuses a {@code .credential.env} value that is not an environment variable name, without echoing it: a key or token
   * pasted there instead of a variable name must not reach a log or message.
   *
   * @param key the config key, named in the message
   * @throws IllegalArgumentException when {@code name} is not letters, digits and underscores, not starting with a digit
   */
  public static void checkEnvName(String key, String name) {
    if (!ENV_VAR_NAME.matcher(name).matches()) {
      throw new IllegalArgumentException(key + " must be an environment variable NAME (letters, digits, underscores), never "
          + "the key itself");
    }
  }

  /** {@code file <path>}, {@code env <NAME>} or {@code none}. */
  String describe() {
    if (file != null) {
      return "file " + file;
    }
    return env != null ? "env " + env : "none";
  }

  /**
   * @param environment looks up an environment variable ({@code System::getenv} in production)
   * @return the trimmed key, or empty when no source is configured
   * @throws IllegalStateException when the configured source is missing, unreadable, empty or holds anything but one
   *     printable ASCII token (a multi-line file, an inner space); the message names the source only
   */
  Option<String> read(UnaryOperator<String> environment) {
    if (file == null && env == null) {
      return Option.empty();
    }
    return Option.of(SingleTokenReader.read(file, env, environment, "API key", describe(), IllegalStateException::new));
  }
}
