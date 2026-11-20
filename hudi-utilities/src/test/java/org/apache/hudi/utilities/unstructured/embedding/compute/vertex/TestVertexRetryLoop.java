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
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientConfigException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingInput;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingProviderUnavailableException;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingResult;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Vertex client under throttling, an outage and a wrong-size answer: it honours Retry-After,
 * the call deadline and the outage short-circuit as the OpenAI, Bedrock and Jina clients do, and a vector of the wrong
 * dimension is the configuration's, failing the run.
 */
class TestVertexRetryLoop {

  private static final String ALIAS = "mm";
  private static final String BUSY = "{\"error\":{\"code\":503,\"status\":\"UNAVAILABLE\"}}";

  @TempDir
  Path tempDir;
  private VertexStubServer stub;
  private TypedProperties props;
  private final AtomicLong now = new AtomicLong(1_000_000L);
  private final List<Long> sleeps = new ArrayList<>();

  @BeforeEach
  void setUp() throws IOException {
    stub = new VertexStubServer();
    Path tokenFile = tempDir.resolve("token");
    Files.write(tokenFile, "recorded-test-token-0001\n".getBytes(StandardCharsets.UTF_8));
    props = new TypedProperties();
    props.setProperty(key(VertexClientConfig.ENDPOINT_SUFFIX), stub.endpoint());
    props.setProperty(key(VertexClientConfig.CREDENTIAL_FILE_SUFFIX), tokenFile.toString());
  }

  @AfterEach
  void tearDown() {
    stub.close();
  }

  @Test
  void throttledRequestWaitsTheRetryAfterHintThenSucceeds() throws Exception {
    stub.enqueueStatus(429, BUSY, "2");
    EmbeddingResult result = client().embed(Collections.singletonList(text("a"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
    assertEquals(2, stub.requests().size());
    assertEquals(Collections.singletonList(2000L), sleeps);
  }

  @Test
  void retryThatWouldPassTheDeadlineIsNotSent() throws Exception {
    props.setProperty(key(".request.deadline.ms"), "1000");
    stub.enqueueStatus(503, BUSY, "2");
    EmbeddingResult result = client().embed(Collections.singletonList(text("a"))).get(0);
    assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    assertTrue(result.getError().contains("deadline exceeded"), result.getError());
    assertEquals(1, stub.requests().size());
  }

  @Test
  void consecutiveFailedRequestsOpenTheOutageShortCircuit() throws Exception {
    props.setProperty(key(".retry.max.attempts"), "1");
    props.setProperty(key(".outage.consecutive.failures"), "2");
    for (int i = 0; i < 3; i++) {
      stub.enqueueStatus(503, BUSY);
    }
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingResult> results = client.embed(Arrays.asList(text("a"), text("b"), text("c")));
    assertEquals(2, stub.requests().size());
    for (EmbeddingResult result : results) {
      assertEquals(EmbeddingResult.Status.RETRYABLE_FAILURE, result.getStatus());
    }
    assertTrue(results.get(2).getError().contains("provider outage short-circuit"), results.get(2).getError());
    List<EmbeddingInput> next = Collections.singletonList(text("d"));
    assertThrows(EmbeddingProviderUnavailableException.class, () -> client.embed(next));
    assertEquals(2, stub.requests().size());
  }

  @Test
  void vectorOfTheWrongDimensionFailsTheRun() {
    stub.enqueueBody("{\"predictions\":[{\"textEmbedding\":[0.1,0.2,0.3]}]}");
    VertexMultimodalEmbeddingClient client = client();
    List<EmbeddingInput> inputs = Collections.singletonList(text("a"));
    EmbeddingClientConfigException e = assertThrows(EmbeddingClientConfigException.class, () -> client.embed(inputs));
    assertTrue(e.getMessage().contains("3") && e.getMessage().contains("128"), e.getMessage());
    assertEquals(1, stub.requests().size());
  }

  private VertexMultimodalEmbeddingClient client() {
    VertexMultimodalEmbeddingClient client = new VertexMultimodalEmbeddingClient(now::get, millis -> {
      sleeps.add(millis);
      now.addAndGet(millis);
    });
    client.init(new EmbeddingModelSpec(ALIAS, "emb_mm", 128, null, VertexMultimodalEmbeddingClient.PROVIDER,
        "multimodalembedding@001"), props);
    return client;
  }

  private static EmbeddingInput text(String fid) {
    return new EmbeddingInput(fid, 0, EmbeddingTableSchema.MODALITY_TEXT, "a cat on a mat");
  }

  private static String key(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + ALIAS + suffix;
  }
}
