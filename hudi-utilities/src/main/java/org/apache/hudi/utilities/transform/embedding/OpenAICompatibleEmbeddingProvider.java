/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.HoodieException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.hudi.common.util.ConfigUtils.getLongWithAltKeys;
import static org.apache.hudi.common.util.ConfigUtils.getStringWithAltKeys;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.API_KEY_ENV;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.ENDPOINT_URL;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.MODEL;
import static org.apache.hudi.utilities.config.EmbeddingTransformerConfig.TIMEOUT_MS;

/**
 * {@link EmbeddingProvider} for any OpenAI-compatible {@code /v1/embeddings} endpoint
 * (Ollama, TEI, vLLM, OpenAI, Voyage). One POST per record batch; transient failures
 * (429 and 5xx) are retried with exponential backoff honoring {@code Retry-After}.
 */
public class OpenAICompatibleEmbeddingProvider implements EmbeddingProvider {

  private static final long serialVersionUID = 1L;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int MAX_ATTEMPTS = 5;
  private static final long BASE_BACKOFF_MS = 1_000L;

  // One client per JVM per connect timeout, shared by every partition on the executor.
  // java.net.http.HttpClient is not Closeable before Java 21 and owns a selector thread
  // plus a connection pool, so an instance-level client would accumulate one of each per
  // Spark task with no way to release them.
  private static final ConcurrentHashMap<Long, HttpClient> CLIENTS = new ConcurrentHashMap<>();

  private String endpointUrl;
  private String model;
  private String apiKeyEnv;
  private long timeoutMs;

  @Override
  public void init(TypedProperties props) {
    this.endpointUrl = getStringWithAltKeys(props, ENDPOINT_URL);
    this.model = getStringWithAltKeys(props, MODEL);
    this.apiKeyEnv = getStringWithAltKeys(props, API_KEY_ENV, true);
    this.timeoutMs = getLongWithAltKeys(props, TIMEOUT_MS);
  }

  @Override
  public List<float[]> embed(List<String> texts) {
    HttpRequest request = buildRequest(texts);
    for (int attempt = 1; ; attempt++) {
      try {
        HttpResponse<String> response = client().send(request, HttpResponse.BodyHandlers.ofString());
        int status = response.statusCode();
        if (status == 200) {
          return parseVectors(response.body(), texts.size());
        }
        boolean retriable = status == 429 || status >= 500;
        if (!retriable || attempt == MAX_ATTEMPTS) {
          throw new HoodieException("Embeddings API returned HTTP " + status
              + " (attempt " + attempt + "/" + MAX_ATTEMPTS + "): " + truncate(response.body()));
        }
        sleep(backoffMs(response, attempt));
      } catch (IOException e) {
        if (attempt == MAX_ATTEMPTS) {
          throw new HoodieException("Embeddings API unreachable after " + MAX_ATTEMPTS + " attempts", e);
        }
        sleep(backoffMs(null, attempt));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new HoodieException("Interrupted while calling embeddings API", e);
      }
    }
  }

  // HttpClient is thread-safe and keep-alives/pools connections internally, so sharing
  // one across concurrent batch requests reuses sockets rather than contending
  @VisibleForTesting
  HttpClient client() {
    return CLIENTS.computeIfAbsent(timeoutMs,
        timeout -> HttpClient.newBuilder().connectTimeout(Duration.ofMillis(timeout)).build());
  }

  private HttpRequest buildRequest(List<String> texts) {
    ObjectNode body = MAPPER.createObjectNode();
    body.put("model", model);
    ArrayNode input = body.putArray("input");
    texts.forEach(input::add);
    HttpRequest.Builder builder = HttpRequest.newBuilder()
        .uri(URI.create(endpointUrl))
        .timeout(Duration.ofMillis(timeoutMs))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(body.toString()));
    if (apiKeyEnv != null && !apiKeyEnv.isEmpty()) {
      String apiKey = System.getenv(apiKeyEnv);
      if (apiKey == null || apiKey.isEmpty()) {
        throw new HoodieException("Environment variable " + apiKeyEnv + " (from "
            + API_KEY_ENV.key() + ") is not set");
      }
      builder.header("Authorization", "Bearer " + apiKey);
    }
    return builder.build();
  }

  private List<float[]> parseVectors(String responseBody, int expectedCount) throws IOException {
    JsonNode data = MAPPER.readTree(responseBody).path("data");
    if (!data.isArray() || data.size() != expectedCount) {
      throw new HoodieException("Embeddings API returned " + data.size()
          + " vectors for " + expectedCount + " inputs");
    }
    List<float[]> vectors = new ArrayList<>(expectedCount);
    // OpenAI-compatible APIs return entries ordered by `index`
    for (JsonNode entry : data) {
      JsonNode embedding = entry.path("embedding");
      float[] vector = new float[embedding.size()];
      for (int i = 0; i < vector.length; i++) {
        vector[i] = (float) embedding.get(i).asDouble();
      }
      vectors.add(vector);
    }
    return vectors;
  }

  private static long backoffMs(HttpResponse<String> response, int attempt) {
    if (response != null) {
      // honor Retry-After (seconds form) when the server provides it
      long retryAfter = response.headers().firstValue("Retry-After")
          .map(v -> {
            try {
              return Long.parseLong(v.trim()) * 1000L;
            } catch (NumberFormatException e) {
              return -1L;
            }
          }).orElse(-1L);
      if (retryAfter > 0) {
        return retryAfter;
      }
    }
    return BASE_BACKOFF_MS * (1L << (attempt - 1));
  }

  private static void sleep(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new HoodieException("Interrupted during embeddings API backoff", e);
    }
  }

  private static String truncate(String body) {
    return body == null ? "" : body.substring(0, Math.min(body.length(), 500));
  }
}
