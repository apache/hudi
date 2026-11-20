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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Resolves configured embedding clients by provider id or explicit client class.
 */
public final class EmbeddingClientNames {

  private static final String PKG = "org.apache.hudi.utilities.unstructured.embedding.compute.";

  /** The ids of the OpenAI-compatible providers ({@code openai.OpenAiProvider}), in declaration order. */
  static final List<String> OPENAI_PROVIDERS = Collections.unmodifiableList(Arrays.asList("openai", "azure-openai", "hf-tei"));
  static final String OPENAI_COMPATIBLE_CLIENT = PKG + "openai.OpenAiCompatibleEmbeddingClient";
  static final String VERTEX_PROVIDER = "vertex-multimodal";
  static final String VERTEX_CLIENT = PKG + "vertex.VertexMultimodalEmbeddingClient";
  static final String BEDROCK_PROVIDER = "bedrock";
  /** The client class of each Bedrock model family, by the family's base model id ({@code bedrock.BedrockModelFamily}). */
  static final Map<String, String> BEDROCK_CLIENTS;
  static final String JINA_PROVIDER = "jina";
  static final String JINA_CLIENT = PKG + "jina.JinaEmbeddingClient";
  /** The provider ids with a built-in client, as {@code hoodie.unstructured.embedding.model.<alias>.provider} names them. */
  public static final List<String> PROVIDERS = Collections.unmodifiableList(Stream.of(
      Stream.of(DummyEmbeddingClient.PROVIDER, DummyMultimodalEmbeddingClient.PROVIDER), OPENAI_PROVIDERS.stream(),
      Stream.of(VERTEX_PROVIDER, BEDROCK_PROVIDER, JINA_PROVIDER))
      .flatMap(s -> s).collect(Collectors.toList()));

  static {
    Map<String, String> bedrock = new LinkedHashMap<>();
    bedrock.put("amazon.titan-embed-text-v2", PKG + "bedrock.BedrockTitanTextEmbeddingClient");
    bedrock.put("cohere.embed-v4", PKG + "bedrock.BedrockCohereEmbeddingClient");
    bedrock.put("amazon.titan-embed-image-v1", PKG + "bedrock.BedrockTitanMultimodalEmbeddingClient");
    BEDROCK_CLIENTS = Collections.unmodifiableMap(bedrock);
  }

  private EmbeddingClientNames() {
  }

  /**
   * The client class of {@code spec}, loaded and checked but not instantiated; the compute job calls it on the driver
   * so a misconfigured model fails the run before any Spark work instead of on every task.
   *
   * @throws IllegalArgumentException if the configured {@code .client.class} does not load or is not an
   *     {@link EmbeddingClient}
   * @throws UnsupportedOperationException if no client class is set and the provider has no client yet
   */
  public static String clientClassName(EmbeddingModelSpec spec) {
    if (!StringUtils.isNullOrEmpty(spec.getClientClass())) {
      String className = spec.getClientClass().trim();
      String key = EmbeddingTableConfig.MODEL_PREFIX + spec.getName() + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX;
      Class<?> clazz;
      try {
        clazz = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
      } catch (ClassNotFoundException | LinkageError e) {
        throw new IllegalArgumentException("Config " + key + " names class " + className + ", which cannot be loaded", e);
      }
      if (!EmbeddingClient.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException("Config " + key + " names class " + className + ", which is not an "
            + EmbeddingClient.class.getName());
      }
      return className;
    }
    if (DummyEmbeddingClient.PROVIDER.equals(spec.getProvider())) {
      return DummyEmbeddingClient.class.getName();
    }
    if (DummyMultimodalEmbeddingClient.PROVIDER.equals(spec.getProvider())) {
      return DummyMultimodalEmbeddingClient.class.getName();
    }
    if (OPENAI_PROVIDERS.contains(spec.getProvider())) {
      return OPENAI_COMPATIBLE_CLIENT;
    }
    if (VERTEX_PROVIDER.equals(spec.getProvider())) {
      return VERTEX_CLIENT;
    }
    if (BEDROCK_PROVIDER.equals(spec.getProvider())) {
      String family = bedrockFamily(spec.getModelName());
      if (family == null) {
        throw new UnsupportedOperationException("Bedrock model id " + spec.getModelName() + " of model " + spec.getName()
            + " names no supported model family (" + String.join(", ", BEDROCK_CLIENTS.keySet()) + "); for an application "
            + "inference profile or provisioned-throughput ARN set " + EmbeddingTableConfig.MODEL_PREFIX + spec.getName()
            + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX + " to the family's client: "
            + String.join(", ", BEDROCK_CLIENTS.values().stream().sorted().collect(Collectors.toList())));
      }
      return BEDROCK_CLIENTS.get(family);
    }
    if (JINA_PROVIDER.equals(spec.getProvider())) {
      return JINA_CLIENT;
    }
    throw new UnsupportedOperationException("provider " + spec.getProvider() + " of model " + spec.getName()
        + " has no built-in embedding client; providers: " + String.join(", ", PROVIDERS) + ", or set " + EmbeddingTableConfig.MODEL_PREFIX + spec.getName()
        + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX + " to an " + EmbeddingClient.class.getName() + " implementation");
  }

  /**
   * The base model id of the Bedrock family {@code modelId} names, or null: the id after the last {@code /} (an ARN) and
   * before the {@code :} version, equal to a family's base id or ending in {@code .<base id>} (a cross-region profile), the
   * rule of {@code bedrock.BedrockModelFamily#fromModelId}.
   */
  static String bedrockFamily(String modelId) {
    if (modelId == null) {
      return null;
    }
    String id = modelId.trim();
    id = id.substring(id.lastIndexOf('/') + 1);
    int version = id.indexOf(':');
    String base = version < 0 ? id : id.substring(0, version);
    for (String family : BEDROCK_CLIENTS.keySet()) {
      if (base.equals(family) || base.endsWith("." + family)) {
        return family;
      }
    }
    return null;
  }
}
