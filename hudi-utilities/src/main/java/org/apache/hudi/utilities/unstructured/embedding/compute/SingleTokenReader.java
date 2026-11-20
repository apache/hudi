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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Reads one secret token (an API key, an access token) from a file (e.g. a mounted Kubernetes secret) or an environment
 * variable named by config, for every provider client. The value never appears in a message: a failure names the source only.
 */
public final class SingleTokenReader {

  private SingleTokenReader() {
  }

  /**
   * @param file        the file holding the token, or null to read {@code env}
   * @param env         the environment variable holding the token, read when {@code file} is null
   * @param environment looks up an environment variable ({@code System::getenv} in production)
   * @param what        the token's name in messages ("API key", "Jina API key", ...)
   * @param source      where the token comes from, for messages ({@code file <path>} or {@code env <NAME>}): never the value
   * @param failure     the caller's exception for a message
   * @return the trimmed token
   * @throws RuntimeException from {@code failure} when the source is unreadable, empty or holds anything but one printable ASCII
   *     token (a multi-line file, an inner space)
   */
  public static String read(String file, String env, UnaryOperator<String> environment, String what, String source,
                            Function<String, ? extends RuntimeException> failure) {
    String value;
    if (file != null) {
      try {
        value = new String(Files.readAllBytes(Paths.get(file)), StandardCharsets.UTF_8);
      } catch (IOException e) {
        throw failure.apply("Cannot read the " + what + " from " + source + ": " + e.getClass().getSimpleName());
      }
    } else {
      value = environment.apply(env);
    }
    if (value == null || value.trim().isEmpty()) {
      throw failure.apply("No " + what + " in " + source);
    }
    String trimmed = value.trim();
    if (!isSinglePrintableToken(trimmed)) {
      // an HTTP client would echo such a token in its "invalid header value" error; never name the character or value
      throw failure.apply("The " + what + " in " + source + " contains whitespace, control or non-ASCII characters; it must be "
          + "a single printable token");
    }
    return trimmed;
  }

  /**
   * One printable ASCII token, nothing else: an HTTP client echoes an invalid header value in its error, so a value that fails
   * this is never sent and never quoted.
   */
  public static boolean isSinglePrintableToken(String value) {
    if (value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c < '!' || c > '~') {
        return false;
      }
    }
    return true;
  }
}
