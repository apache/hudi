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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * The production dummy client reads no failure-injection key: those keys belong to test code, as
 * the prep dummy preprocessors' key does.
 */
class TestDummyEmbeddingClientKeys {

  private static final String ALIAS = "m";

  @Test
  void productionDummyClientIgnoresTheTestFaultKeys() {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ".dummy.fail.source_path_shas", "a");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + ALIAS + ".dummy.permanent.source_path_shas", "a");
    DummyEmbeddingClient client = new DummyEmbeddingClient();
    client.init(new EmbeddingModelSpec(ALIAS, "emb_m", 4, null, "dummy", ALIAS), props);
    EmbeddingResult result = client.embed(Collections.singletonList(
        new EmbeddingInput("a", 0, EmbeddingTableSchema.MODALITY_TEXT, "text"))).get(0);
    assertEquals(EmbeddingResult.Status.OK, result.getStatus(), result.getError());
  }

  @Test
  void productionDummyClientDeclaresNoDummyKey() throws IllegalAccessException {
    for (Field field : DummyEmbeddingClient.class.getDeclaredFields()) {
      if (Modifier.isStatic(field.getModifiers()) && field.getType() == String.class) {
        field.setAccessible(true);
        Object value = field.get(null);
        assertFalse(String.valueOf(value).startsWith(".dummy."), field.getName() + "=" + value);
      }
    }
  }
}
