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
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestVertexPropertyPreflight {
  @Test
  void rejectsTextBudgetBeforeClientInitialization() {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientFactory.checkConfig(model("text"), properties()));
    assertTrue(failure.getMessage().contains(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()));
  }

  @Test
  void rejectsEndpointNamingAnotherModel() {
    TypedProperties properties = properties();
    properties.setProperty("hoodie.unstructured.embedding.model.m.endpoint",
        "http://127.0.0.1:1/publishers/google/models/other-model:predict");
    assertThrows(IllegalArgumentException.class, () -> EmbeddingClientFactory.checkConfig(model("image"), properties));
  }

  @Test
  void imageOnlyModelKeepsItsTextBudgetExemption() {
    assertDoesNotThrow(() -> EmbeddingClientFactory.checkConfig(model("image"), properties()));
  }

  @Test
  void textBudgetAtTheEndpointLimitPassesPreflight() {
    TypedProperties properties = properties();
    properties.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "32");
    assertDoesNotThrow(() -> EmbeddingClientFactory.checkConfig(model("text"), properties));
  }

  @Test
  void parsedMixedCaseTextDeclarationChecksTheBudget() {
    TypedProperties properties = modelProperties("TeXt");
    assertThrows(IllegalArgumentException.class,
        () -> EmbeddingClientFactory.checkConfig(EmbeddingTableConfig.from(properties).activeModel(), properties));
  }

  @Test
  void parsedImageDeclarationKeepsTheTextBudgetExemption() {
    TypedProperties properties = modelProperties("IMAGE");
    assertDoesNotThrow(() -> EmbeddingClientFactory.checkConfig(EmbeddingTableConfig.from(properties).activeModel(), properties));
  }

  private static TypedProperties modelProperties(String modality) {
    TypedProperties properties = properties();
    properties.setProperty(EmbeddingTableConfig.MODELS.key(), "m");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m.dimensions", "128");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m.provider", "vertex-multimodal");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m.name", "multimodalembedding@001");
    properties.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m.modalities", modality);
    properties.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "33");
    return properties;
  }

  private static EmbeddingModelSpec model(String modality) {
    return new EmbeddingModelSpec("m", "emb_m", 128, null, "vertex-multimodal", "multimodalembedding@001",
        Collections.singleton(modality));
  }

  private static TypedProperties properties() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty("hoodie.unstructured.embedding.model.m.endpoint",
        "http://127.0.0.1:1/publishers/google/models/multimodalembedding@001:predict");
    return properties;
  }
}
