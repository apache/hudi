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
import java.util.List;

/**
 * The client class of a model, resolved from a table of provider ids and class NAMES, so the table config and the model
 * identity can name a model's client without linking any provider package (or its SDK). The client factory
 * ({@code EmbeddingClientFactory}) resolves through it.
 */
public final class EmbeddingClientNames {

  private static final String PKG = "org.apache.hudi.utilities.unstructured.embedding.compute.";

  /** The provider ids with a built-in client, as {@code hoodie.unstructured.embedding.model.<alias>.provider} names them. */
  public static final List<String> PROVIDERS = Collections.unmodifiableList(Arrays.asList(DummyEmbeddingClient.PROVIDER, DummyMultimodalEmbeddingClient.PROVIDER));

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
    throw new UnsupportedOperationException("provider " + spec.getProvider() + " of model " + spec.getName()
        + " has no built-in embedding client; providers: " + String.join(", ", PROVIDERS) + ", or set " + EmbeddingTableConfig.MODEL_PREFIX + spec.getName()
        + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX + " to an " + EmbeddingClient.class.getName() + " implementation");
  }

}
