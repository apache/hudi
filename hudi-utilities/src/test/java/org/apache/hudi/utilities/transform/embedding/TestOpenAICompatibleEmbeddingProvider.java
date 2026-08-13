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

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.config.EmbeddingTransformerConfig;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Direct unit tests for {@link OpenAICompatibleEmbeddingProvider} against a stub
 * server (no Spark): request shape and auth header, vector parsing, retry on 5xx
 * honoring Retry-After, retry on connection-level IOException, retry exhaustion,
 * fail-fast on 4xx, and response/input count mismatch.
 */
public class TestOpenAICompatibleEmbeddingProvider {

  private static HttpServer server;
  private static final AtomicInteger REQUEST_COUNT = new AtomicInteger();
  private static final AtomicInteger REMAINING_FAILURES = new AtomicInteger();
  private static volatile int failureStatus = 500;
  private static volatile boolean abortConnection = false;
  private static volatile int vectorsToReturn = -1; // -1 = one per input
  private static volatile String retryAfterHeader = "1";
  private static final AtomicReference<String> LAST_BODY = new AtomicReference<>();
  private static final AtomicReference<String> LAST_AUTH = new AtomicReference<>();

  @BeforeAll
  public static void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/v1/embeddings", exchange -> {
      REQUEST_COUNT.incrementAndGet();
      LAST_AUTH.set(exchange.getRequestHeaders().getFirst("Authorization"));
      byte[] request = new byte[8192];
      int n = exchange.getRequestBody().read(request);
      LAST_BODY.set(new String(request, 0, Math.max(0, n), StandardCharsets.UTF_8));
      if (REMAINING_FAILURES.getAndUpdate(f -> Math.max(0, f - 1)) > 0) {
        if (abortConnection) {
          exchange.close(); // no response -> client-side IOException
          return;
        }
        exchange.getResponseHeaders().add("Retry-After", retryAfterHeader);
        exchange.sendResponseHeaders(failureStatus, -1);
        exchange.close();
        return;
      }
      java.util.regex.Matcher inputArray =
          java.util.regex.Pattern.compile("\"input\":\\[(.*?)\\]").matcher(LAST_BODY.get());
      long inputs = inputArray.find() ? inputArray.group(1).split("\",\"").length : 0;
      long count = vectorsToReturn >= 0 ? vectorsToReturn : inputs;
      StringBuilder data = new StringBuilder("{\"data\":[");
      for (int i = 0; i < count; i++) {
        data.append(i > 0 ? "," : "").append("{\"index\":").append(i)
            .append(",\"embedding\":[1.5,-2.0]}");
      }
      byte[] response = data.append("]}").toString().getBytes(StandardCharsets.UTF_8);
      exchange.sendResponseHeaders(200, response.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response);
      }
    });
    server.start();
  }

  @AfterAll
  public static void stopServer() {
    server.stop(0);
  }

  @BeforeEach
  public void reset() {
    REQUEST_COUNT.set(0);
    REMAINING_FAILURES.set(0);
    failureStatus = 500;
    abortConnection = false;
    vectorsToReturn = -1;
    retryAfterHeader = "1";
  }

  private OpenAICompatibleEmbeddingProvider provider(String apiKeyEnv) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTransformerConfig.ENDPOINT_URL.key(),
        "http://localhost:" + server.getAddress().getPort() + "/v1/embeddings");
    props.setProperty(EmbeddingTransformerConfig.MODEL.key(), "stub-model");
    if (apiKeyEnv != null) {
      props.setProperty(EmbeddingTransformerConfig.API_KEY_ENV.key(), apiKeyEnv);
    }
    OpenAICompatibleEmbeddingProvider provider = new OpenAICompatibleEmbeddingProvider();
    provider.init(props);
    return provider;
  }

  @Test
  public void testRequestShapeAndVectorParsing() {
    List<float[]> vectors = provider(null).embed(Arrays.asList("first text", "second"));
    assertEquals(2, vectors.size());
    assertArrayEquals(new float[] {1.5f, -2.0f}, vectors.get(0));
    assertTrue(LAST_BODY.get().contains("\"model\":\"stub-model\""));
    assertTrue(LAST_BODY.get().contains("\"input\":[\"first text\",\"second\"]"));
  }

  @Test
  public void testBearerHeaderFromEnvironmentAndMissingEnvFails() {
    // PATH is always present: its value must be forwarded as the bearer token
    provider("PATH").embed(Arrays.asList("authed"));
    assertEquals("Bearer " + System.getenv("PATH"), LAST_AUTH.get());

    assertThrows(HoodieException.class,
        () -> provider("HOODIE_TEST_NO_SUCH_ENV_VAR").embed(Arrays.asList("x")));
  }

  @Test
  public void testRetriesTransientFailuresHonoringRetryAfter() {
    // two 503s (Retry-After: 1) then success
    REMAINING_FAILURES.set(2);
    failureStatus = 503;
    long startMs = System.currentTimeMillis();
    assertEquals(1, provider(null).embed(Arrays.asList("flaky")).size());
    assertEquals(3, REQUEST_COUNT.get());
    assertTrue(System.currentTimeMillis() - startMs >= 2000); // two Retry-After sleeps

    // connection aborted mid-request retries the same way
    REMAINING_FAILURES.set(1);
    abortConnection = true;
    assertEquals(1, provider(null).embed(Arrays.asList("dropped")).size());
  }

  @Test
  public void testFailFastOn4xxAndExhaustionOn5xx() {
    // non-retriable client error fails on the first attempt
    REMAINING_FAILURES.set(Integer.MAX_VALUE);
    failureStatus = 404;
    assertThrows(HoodieException.class, () -> provider(null).embed(Arrays.asList("gone")));
    assertEquals(1, REQUEST_COUNT.get());

    // persistent 5xx exhausts all attempts then fails
    REQUEST_COUNT.set(0);
    failureStatus = 502;
    assertThrows(HoodieException.class, () -> provider(null).embed(Arrays.asList("down")));
    assertEquals(5, REQUEST_COUNT.get());
  }

  @Test
  public void testResponseCountMismatchFails() {
    vectorsToReturn = 1;
    assertThrows(HoodieException.class,
        () -> provider(null).embed(Arrays.asList("one", "two")));
  }

  @Test
  public void testUnparsableRetryAfterFallsBackToExponentialBackoff() {
    // two 429s whose Retry-After cannot be parsed as seconds, then success
    REMAINING_FAILURES.set(2);
    failureStatus = 429;
    retryAfterHeader = "abc";
    long startMs = System.currentTimeMillis();
    assertEquals(1, provider(null).embed(Arrays.asList("throttled")).size());
    assertEquals(3, REQUEST_COUNT.get());
    // the unparsable header is ignored in favour of exponential backoff: 1000ms then 2000ms.
    // honoring "abc" as one second would only add up to 2000ms in total.
    assertTrue(System.currentTimeMillis() - startMs >= 3000);
  }

  @Test
  public void testProviderInitDefaultsToNoOp() {
    // an implementation that needs no configuration inherits the interface's no-op init
    EmbeddingProvider inMemoryProvider = texts -> Collections.singletonList(new float[] {1.0f});
    assertDoesNotThrow(() -> inMemoryProvider.init(new TypedProperties()));
  }
}
