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

package org.apache.hudi.utilities.unstructured.embedding;

import org.apache.hudi.common.config.TypedProperties;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The active-model selector validates every configured alias before choosing a declared model. */
class TestEmbeddingActiveModels {
  @Test
  void declaredSelectionStillChoosesTheKnownModel() {
    EmbeddingTableConfig config = config("m1");
    assertEquals(Collections.singletonList("m1"), config.activeModels().stream().map(EmbeddingModelSpec::getName).collect(Collectors.toList()));
    assertEquals("m1", config.activeModel().getName());
  }

  @Test
  void selectionRejectsAnUnknownAliasAlongsideAKnownOne() {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> config("m1,typo").activeModels());
    assertTrue(failure.getMessage().contains("typo"), failure.getMessage());
  }

  @Test
  void singleModelEntryPointRejectsTheSameUnknownAlias() {
    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, () -> config("m1,typo").activeModel());
    assertTrue(failure.getMessage().contains("typo"), failure.getMessage());
  }

  private static EmbeddingTableConfig config(String active) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.ACTIVE_MODELS.key(), active);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "3");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    return EmbeddingTableConfig.from(props);
  }
}
