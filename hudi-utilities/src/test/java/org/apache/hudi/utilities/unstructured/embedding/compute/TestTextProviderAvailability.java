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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Exercises built-in provider registration through a real local HTTP call, with no provider class reference. */
class TestTextProviderAvailability {

  @TempDir
  Path temp;

  @Test
  void openAiFactorySendsTextAndMapsVectorsByResponseIndex() throws Exception {
    AtomicReference<String> body = new AtomicReference<>();
    AtomicReference<String> authorization = new AtomicReference<>();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/v1/embeddings", exchange -> {
      body.set(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      authorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
      byte[] response = ("{\"model\":\"text-embedding-3-small\",\"data\":[{\"index\":1,\"embedding\":[5,6,7,8]},"
          + "{\"index\":0,\"embedding\":[1,2,3,4]}]}").getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().set("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.length);
      exchange.getResponseBody().write(response);
      exchange.close();
    });
    server.start();
    try {
      Path key = temp.resolve("fake.key");
      Files.write(key, "fake-local-key\n".getBytes(StandardCharsets.UTF_8));
      TypedProperties props = new TypedProperties();
      String prefix = "hoodie.unstructured.embedding.model.local";
      props.setProperty(prefix + ".endpoint", "http://127.0.0.1:" + server.getAddress().getPort() + "/v1");
      props.setProperty(prefix + ".credential.file", key.toString());
      EmbeddingModelSpec spec = new EmbeddingModelSpec("local", "emb_local", 4, null, "openai", "text-embedding-3-small");
      try (EmbeddingClient client = EmbeddingClientFactory.create(spec, props)) {
        List<EmbeddingResult> results = client.embed(Arrays.asList(
            new EmbeddingInput("a", 0, "text", "first"), new EmbeddingInput("b", 0, "text", "second")));
        assertEquals(2, results.size());
        assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
        assertEquals(EmbeddingResult.Status.OK, results.get(1).getStatus());
        assertArrayEquals(new float[] {1, 2, 3, 4}, results.get(0).getVector());
        assertArrayEquals(new float[] {5, 6, 7, 8}, results.get(1).getVector());
      }
      JsonNode sent = new ObjectMapper().readTree(body.get());
      assertEquals("text-embedding-3-small", sent.path("model").asText());
      assertEquals(4, sent.path("dimensions").asInt());
      assertEquals(Arrays.asList("first", "second"), new ObjectMapper().convertValue(sent.path("input"), List.class));
      assertEquals("Bearer fake-local-key", authorization.get());
    } finally {
      server.stop(0);
    }
  }

  @ParameterizedTest
  @CsvSource({
      "bedrock,amazon.titan-embed-text-v2:0,256,bedrock.BedrockTitanTextEmbeddingClient",
      "bedrock,cohere.embed-v4:0,256,bedrock.BedrockCohereEmbeddingClient",
      "vertex-multimodal,multimodalembedding@001,128,vertex.VertexMultimodalEmbeddingClient",
      "jina,jina-embeddings-v3,256,jina.JinaEmbeddingClient"
  })
  void remainingTextProvidersInitializeThroughFactory(String provider, String model, int dimensions,
                                                            String expectedClass) throws Exception {
    Path key = temp.resolve("fake-provider.key");
    Files.writeString(key, "fake-local-token");
    TypedProperties props = new TypedProperties();
    String prefix = "hoodie.unstructured.embedding.model.local";
    props.setProperty(prefix + ".endpoint", "http://127.0.0.1:1/publishers/google/models/multimodalembedding@001:predict");
    props.setProperty(prefix + ".credential.file", key.toString());
    props.setProperty(prefix + ".bedrock.region", "us-east-1");
    EmbeddingModelSpec spec = new EmbeddingModelSpec("local", "emb_local", dimensions, null, provider, model);
    EmbeddingClientFactory.checkConfig(spec, props);
    try (EmbeddingClient client = EmbeddingClientFactory.create(spec, props)) {
      assertEquals("org.apache.hudi.utilities.unstructured.embedding.compute." + expectedClass, client.getClass().getName());
    }
  }

  @Test
  void dummyFactoryStillEmbedsWithoutAnyProviderConfiguration() throws Exception {
    EmbeddingModelSpec spec = new EmbeddingModelSpec("local", "emb_local", 4, null, "dummy", "local");
    try (EmbeddingClient client = EmbeddingClientFactory.create(spec, new TypedProperties())) {
      List<EmbeddingResult> results = client.embed(Collections.singletonList(new EmbeddingInput("a", 0, "text", "first")));
      assertEquals(1, results.size());
      assertEquals(EmbeddingResult.Status.OK, results.get(0).getStatus());
      assertEquals(4, results.get(0).getVector().length);
    }
  }
}
