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

import java.util.Map;

/**
 * The providers that serve the OpenAI embeddings wire format ({@code POST <endpoint>/embeddings}, {@code data[].index}),
 * each with its endpoint default, credential header and per-request limits.
 */
public enum OpenAiProvider {
  /** api.openai.com; {@code Authorization: Bearer <key>}; 2048 inputs and 300,000 tokens per request. */
  OPENAI("openai", "https://api.openai.com/v1", "OPENAI_API_KEY", 2048, 300_000L),
  /** An Azure OpenAI resource's v1 route ({@code https://<resource>.openai.azure.com/openai/v1}); {@code api-key} header. */
  AZURE_OPENAI("azure-openai", null, "AZURE_OPENAI_API_KEY", 2048, 300_000L),
  /**
   * A Hugging Face Text Embeddings Inference server's OpenAI-compatible route ({@code http://<host>/v1}); optional
   * bearer token; per-request limits read from its {@code GET /info}.
   */
  HF_TEI("hf-tei", null, null, -1, -1L);

  private static final String TEI_ROUTE_SUFFIX = "/v1";

  private final String id;
  private final String defaultEndpoint;
  private final String defaultCredentialEnv;
  private final int maxInputsPerRequest;
  private final long maxTokensPerRequest;

  OpenAiProvider(String id, String defaultEndpoint, String defaultCredentialEnv, int maxInputsPerRequest,
                 long maxTokensPerRequest) {
    this.id = id;
    this.defaultEndpoint = defaultEndpoint;
    this.defaultCredentialEnv = defaultCredentialEnv;
    this.maxInputsPerRequest = maxInputsPerRequest;
    this.maxTokensPerRequest = maxTokensPerRequest;
  }

  /** The {@code hoodie.unstructured.embedding.model.<alias>.provider} value. */
  public String id() {
    return id;
  }

  public static Option<OpenAiProvider> fromId(String id) {
    for (OpenAiProvider provider : values()) {
      if (provider.id.equals(id)) {
        return Option.of(provider);
      }
    }
    return Option.empty();
  }

  /** The endpoint used when none is configured; null when one is required. */
  String defaultEndpoint() {
    return defaultEndpoint;
  }

  /** The env var holding the key when no credential is configured; null when the provider needs no key. */
  String defaultCredentialEnv() {
    return defaultCredentialEnv;
  }

  /** Most inputs in one request; -1 when the server reports it ({@link #HF_TEI}). */
  int maxInputsPerRequest() {
    return maxInputsPerRequest;
  }

  /** Most summed input tokens in one request; -1 when the server reports it ({@link #HF_TEI}). */
  long maxTokensPerRequest() {
    return maxTokensPerRequest;
  }

  /** Adds the header carrying {@code key}. */
  void authorize(Map<String, String> headers, String key) {
    if (this == AZURE_OPENAI) {
      headers.put("api-key", key);
    } else {
      headers.put("Authorization", "Bearer " + key);
    }
  }

  /** The TEI server's {@code GET /info} URL for an OpenAI-compatible endpoint ({@code …/v1} → {@code …/info}). */
  String infoUrl(String endpoint) {
    String root = endpoint.endsWith(TEI_ROUTE_SUFFIX)
        ? endpoint.substring(0, endpoint.length() - TEI_ROUTE_SUFFIX.length()) : endpoint;
    return root + "/info";
  }
}
