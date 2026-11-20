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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Resolves an explicitly named dummy provider. */
public class TestEmbeddingConfigRulings {

  private static final String PROVIDER_KEY = "hoodie.unstructured.embedding.model.m1.provider";

  @Test
  public void dummyProviderWorksWhenNamed() {
    List<EmbeddingModelSpec> models = EmbeddingTableConfig.from(oneModel("dummy")).models();
    assertEquals(1, models.size());
    assertEquals("dummy", models.get(0).getProvider());
    assertEquals(DummyEmbeddingClient.class.getName(), EmbeddingClientFactory.clientClassName(models.get(0)));
  }

  @Test
  public void invalidResolvedColumnNamesTheModelColumnKey() {
    TypedProperties props = oneModel("dummy");
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "text-embedding-3-small");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "text-embedding-3-small.dimensions", "8");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "text-embedding-3-small.provider", "dummy");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableConfig.from(props).models());
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.MODEL_PREFIX + "text-embedding-3-small.column"));
    assertTrue(refused.getMessage().contains("emb_text-embedding-3-small"));
  }

  @Test
  public void modelColumnsCannotCollideIgnoringCase() {
    TypedProperties props = oneModel("dummy");
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1,m2");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.column", "vec");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m2.column", "VEC");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m2.dimensions", "8");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m2.provider", "dummy");
    HoodieException refused = assertThrows(HoodieException.class, () -> EmbeddingTableConfig.from(props).models());
    assertTrue(refused.getMessage().contains("m1") && refused.getMessage().contains("m2"));
  }

  @Test
  public void modelColumnCannotReplaceAFixedColumn() {
    TypedProperties props = oneModel("dummy");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.column", "chunk_id");
    HoodieException refused = assertThrows(HoodieException.class, () -> EmbeddingTableConfig.from(props).models());
    assertTrue(refused.getMessage().contains("chunk_id"));
  }

  private static TypedProperties oneModel(String provider) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty("hoodie.unstructured.embedding.model.m1.dimensions", "8");
    if (provider != null) {
      props.setProperty(PROVIDER_KEY, provider);
    }
    return props;
  }
}
