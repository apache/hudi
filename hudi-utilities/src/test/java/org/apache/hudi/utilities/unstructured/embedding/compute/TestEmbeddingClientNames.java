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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockCohereEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockModelFamily;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockTitanMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.bedrock.BedrockTitanTextEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.jina.JinaEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiCompatibleEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiProvider;
import org.apache.hudi.utilities.unstructured.embedding.compute.vertex.VertexMultimodalEmbeddingClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The name table the table config resolves clients by stays the providers' own ids and classes. */
class TestEmbeddingClientNames {

  @Test
  void namesAreTheProviderClasses() {
    List<String> openAiProviders = EmbeddingClientNames.OPENAI_PROVIDERS;
    assertEquals(Arrays.stream(OpenAiProvider.values()).map(OpenAiProvider::id).collect(Collectors.toList()), openAiProviders);
    assertEquals(EmbeddingClientNames.OPENAI_COMPATIBLE_CLIENT, OpenAiCompatibleEmbeddingClient.class.getName());
    assertEquals(VertexMultimodalEmbeddingClient.PROVIDER, EmbeddingClientNames.VERTEX_PROVIDER);
    assertEquals(EmbeddingClientNames.VERTEX_CLIENT, VertexMultimodalEmbeddingClient.class.getName());
    assertEquals(BedrockEmbeddingClient.PROVIDER, EmbeddingClientNames.BEDROCK_PROVIDER);
    assertEquals(JinaEmbeddingClient.PROVIDER, EmbeddingClientNames.JINA_PROVIDER);
    assertEquals(EmbeddingClientNames.JINA_CLIENT, JinaEmbeddingClient.class.getName());
  }

  @Test
  void bedrockTableIsTheModelFamilies() {
    Map<BedrockModelFamily, String> clients = new EnumMap<>(BedrockModelFamily.class);
    clients.put(BedrockModelFamily.TITAN_EMBED_TEXT_V2, BedrockTitanTextEmbeddingClient.class.getName());
    clients.put(BedrockModelFamily.COHERE_EMBED_V4, BedrockCohereEmbeddingClient.class.getName());
    clients.put(BedrockModelFamily.TITAN_EMBED_IMAGE_V1, BedrockTitanMultimodalEmbeddingClient.class.getName());
    assertEquals(Arrays.stream(BedrockModelFamily.values()).map(BedrockModelFamily::baseModelId).collect(Collectors.toList()),
        EmbeddingClientNames.BEDROCK_CLIENTS.keySet().stream().collect(Collectors.toList()));
    for (BedrockModelFamily family : BedrockModelFamily.values()) {
      assertEquals(clients.get(family), EmbeddingClientNames.BEDROCK_CLIENTS.get(family.baseModelId()));
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"amazon.titan-embed-text-v2:0", "us.amazon.titan-embed-text-v2:0",
      "cohere.embed-v4:0", "arn:aws:bedrock:us-west-2::foundation-model/cohere.embed-v4:0",
      "arn:aws:bedrock:us-east-1:123456789012:inference-profile/us.cohere.embed-v4:0",
      "arn:aws:bedrock:us-west-2:123456789012:application-inference-profile/abcdef123456", "amazon.titan-embed-text-v1",
      " cohere.embed-v4 "})
  void bedrockFamilyMatchesTheModelFamilyRule(String modelId) {
    assertEquals(BedrockModelFamily.fromModelId(modelId).map(BedrockModelFamily::baseModelId).orElse(null),
        EmbeddingClientNames.bedrockFamily(modelId));
  }

  @ParameterizedTest
  @ValueSource(strings = {"openai", "azure-openai", "hf-tei"})
  void resolvesEachOpenAiProviderWithoutAnExplicitClient(String provider) {
    assertEquals(OpenAiCompatibleEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(spec(provider, null)));
  }

  @Test
  void explicitCustomClientWinsOverAnUnsupportedProvider() {
    assertEquals(DummyEmbeddingClient.class.getName(),
        EmbeddingClientFactory.clientClassName(spec("custom", DummyEmbeddingClient.class.getName())));
    EmbeddingModelSpec notAClient = spec("openai", String.class.getName());
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.clientClassName(notAClient));
    EmbeddingModelSpec unsupported = spec("unsupported", null);
    assertThrows(UnsupportedOperationException.class, () -> EmbeddingClientFactory.clientClassName(unsupported));
  }

  private static EmbeddingModelSpec spec(String provider, String clientClass) {
    return new EmbeddingModelSpec("m", "emb_m", 4, clientClass, provider, "text-embedding-3-small");
  }
}
