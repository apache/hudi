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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingCallStats;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;
import org.apache.hudi.utilities.unstructured.embedding.compute.RequestRateLimiter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The request rate limit at the send point of {@link VertexMultimodalEmbeddingClient} (one request per input), against
 * {@link VertexStubServer} recording each arrival on the test's fake clock; a sleep only advances that clock.
 */
class TestVertexClientRateLimits {

  private static final String ALIAS = "mm";
  private static final int DIM = 128;

  @TempDir
  Path tempDir;
  private VertexStubServer stub;
  private TypedProperties props;
  private final AtomicLong now = new AtomicLong(2_000_000L);
  private final List<Long> sleeps = new ArrayList<>();

  @BeforeEach
  void setUp() throws IOException {
    stub = new VertexStubServer();
    stub.recordArrivalsOn(now::get);
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, "recorded-test-token-0002\n".getBytes(StandardCharsets.UTF_8));
    props = new TypedProperties();
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.endpoint());
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
  }

  @AfterEach
  void tearDown() {
    stub.close();
  }

  /** On the Vertex send point: at 4 requests/s (burst 2) no half-open 1 s window holds more than 5 requests. */
  @Test
  void testVertexRateIsRespected() throws Exception {
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "4");
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_BURST_SUFFIX), "2");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingResult> results = client.embed(texts(14));

    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.toString());
    }
    List<Long> arrivals = stub.arrivals();
    assertEquals(14, arrivals.size());
    assertEquals(5, maxInAnyWindow(arrivals), "4/s plus a burst of 2: " + arrivals);
    EmbeddingCallStats stats = client.lastCallStats();
    assertEquals(14, stats.getRequests());
    assertEquals(arrivals.get(13) - arrivals.get(0), stats.getRateLimitWaitMillis());
    assertEquals(12 * 250L, stats.getRateLimitWaitMillis(), "two burst tokens, then 250 ms per request");
  }

  /** On the Vertex send point: no key, no wait, no sleep; the call's requests are reported. */
  @Test
  void testVertexNoSettingDoesNotPace() throws Exception {
    VertexMultimodalEmbeddingClient client = client();
    client.embed(texts(6));
    assertEquals(Collections.singletonList(2_000_000L), distinct(stub.arrivals()));
    assertEquals(Collections.emptyList(), sleeps);
    assertEquals(6, client.lastCallStats().getRequests());
    assertEquals(0L, client.lastCallStats().getRateLimitWaitMillis());
  }

  /** On the Vertex send point: a bad rate fails {@code init} with the key named, before any request. */
  @Test
  void testVertexBadRateFailsInit() {
    props.setProperty(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX), "-1");
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient(now::get, sleeps::add);
    EmbeddingModelSpec model = spec();
    EmbeddingClientConfigException thrown = assertThrows(EmbeddingClientConfigException.class, () -> client.init(model, props));
    assertTrue(thrown.getMessage().contains(key(RequestRateLimiter.RATE_LIMIT_REQUESTS_PER_SECOND_SUFFIX)), thrown.getMessage());
    assertEquals(0, stub.requests().size());
  }

  private VertexMultimodalEmbeddingClient client() {
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient(now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    });
    client.init(spec(), props);
    return client;
  }

  private static int maxInAnyWindow(List<Long> times) {
    List<Long> sorted = new ArrayList<>(times);
    sorted.sort(null);
    int max = 0;
    int start = 0;
    for (int end = 0; end < sorted.size(); end++) {
      while (sorted.get(end) - sorted.get(start) >= 1_000L) {
        start++;
      }
      max = Math.max(max, end - start + 1);
    }
    return max;
  }

  private static List<Long> distinct(List<Long> values) {
    List<Long> distinct = new ArrayList<>();
    for (Long value : values) {
      if (!distinct.contains(value)) {
        distinct.add(value);
      }
    }
    return distinct;
  }

  private static List<EmbeddingInput> texts(int count) {
    List<EmbeddingInput> inputs = new ArrayList<>();
    for (int i = 0; i < count; i++) {
      inputs.add(new EmbeddingInput("f" + i, i, EmbeddingTableSchema.MODALITY_TEXT, "text " + i));
    }
    return inputs;
  }

  private static EmbeddingModelSpec spec() {
    return new EmbeddingModelSpec(ALIAS, "emb_mm", DIM, null, VertexMultimodalEmbeddingClient.PROVIDER, "multimodalembedding@001");
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
