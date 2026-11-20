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
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.EmbeddingClientFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Validates configured client classes through the compute factory. */
public class TestEmbeddingCompute {

  private static final String MODEL = "m1";
  private TypedProperties props;

  @BeforeEach
  public void setUp() {
    props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), MODEL);
    props.setProperty(modelKey(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX), "4");
    props.setProperty(modelKey(EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX), "dummy");
  }

  @Test
  public void testFactoryRejectsAClientClassThatIsNotAnEmbeddingClient() {
    String key = modelKey(EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    for (String className : new String[] {String.class.getName(), "org.apache.hudi.no.such.Client"}) {
      props.setProperty(key, className);
      IllegalArgumentException invalid = assertThrows(IllegalArgumentException.class,
          () -> EmbeddingClientFactory.clientClassName(config().model(MODEL)), className);
      assertTrue(invalid.getMessage().contains(key) && invalid.getMessage().contains(className), invalid.getMessage());
    }
    props.setProperty(key, InitFailingClient.class.getName());
    assertEquals(InitFailingClient.class.getName(), EmbeddingClientFactory.clientClassName(config().model(MODEL)));
  }

  private EmbeddingTableConfig config() {
    return EmbeddingTableConfig.from(props);
  }

  private static String modelKey(String suffix) {
    return EmbeddingTableConfig.MODEL_PREFIX + MODEL + suffix;
  }

  /** A valid client type whose init would fail if class validation instantiated it. */
  public static class InitFailingClient extends DummyEmbeddingClient {
    static final String MESSAGE = "endpoint credentials rejected";

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      throw new IllegalStateException(MESSAGE);
    }
  }
}
