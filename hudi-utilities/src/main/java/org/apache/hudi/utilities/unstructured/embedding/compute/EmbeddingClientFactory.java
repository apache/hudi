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
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;

/**
 * Builds the {@link EmbeddingClient} of a model: the {@code hoodie.unstructured.embedding.model.<alias>.client.class}
 * when set, else the built-in client of the model's provider.
 */
public final class EmbeddingClientFactory {

  private EmbeddingClientFactory() {
  }

  /**
   * Instantiates and {@link EmbeddingClient#init initializes} the client of {@code spec}.
   *
   * @throws UnsupportedOperationException if no client class is set and the provider has no built-in client
   * @throws HoodieException if the client class cannot be instantiated or is not an {@link EmbeddingClient}
   */
  public static EmbeddingClient create(EmbeddingModelSpec spec, TypedProperties props) {
    String className = clientClassName(spec);
    Object instance = ReflectionUtils.loadClass(className);
    if (!(instance instanceof EmbeddingClient)) {
      throw new HoodieException("Embedding client class " + className + " of model " + spec.getName()
          + " is not an " + EmbeddingClient.class.getName());
    }
    EmbeddingClient client = (EmbeddingClient) instance;
    try {
      client.init(spec, props);
    } catch (RuntimeException e) {
      // a client whose init fails is closed; a failing close is kept on the init failure
      try {
        client.close();
      } catch (RuntimeException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
    return client;
  }

  /**
   * The client class of {@code spec}, loaded and checked but not instantiated.
   *
   * @throws IllegalArgumentException if the configured {@code .client.class} does not load or is not an
   *     {@link EmbeddingClient}
   * @throws UnsupportedOperationException if no client class is set and the provider has no built-in client
   */
  public static String clientClassName(EmbeddingModelSpec spec) {
    return EmbeddingClientNames.clientClassName(spec);
  }

}
