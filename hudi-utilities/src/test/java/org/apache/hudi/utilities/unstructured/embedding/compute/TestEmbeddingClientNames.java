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
import org.apache.hudi.utilities.unstructured.embedding.compute.openai.OpenAiCompatibleEmbeddingClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** The name table the table config resolves clients by stays the providers' own ids and classes. */
public class TestEmbeddingClientNames {

  @ParameterizedTest
  @ValueSource(strings = {"openai", "azure-openai", "hf-tei"})
  public void resolvesEachOpenAiProviderWithoutAnExplicitClient(String provider) {
    assertEquals(OpenAiCompatibleEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(spec(provider, null)));
  }

  @Test
  public void explicitCustomClientWinsOverAnUnsupportedProvider() {
    assertEquals(DummyEmbeddingClient.class.getName(),
        EmbeddingClientFactory.clientClassName(spec("custom", DummyEmbeddingClient.class.getName())));
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.clientClassName(spec("openai", String.class.getName())));
    assertThrows(UnsupportedOperationException.class, () -> EmbeddingClientFactory.clientClassName(spec("unsupported", null)));
  }

  private static EmbeddingModelSpec spec(String provider, String clientClass) {
    return new EmbeddingModelSpec("m", "emb_m", 4, clientClass, provider, "text-embedding-3-small");
  }
}
