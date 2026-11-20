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

import java.io.Serializable;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

/**
 * One declared embedding model and the VECTOR column it fills, with the modalities it is declared to embed
 * ({@code hoodie.unstructured.embedding.model.<alias>.modalities}) for provider preflight validation.
 */
public class EmbeddingModelSpec implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String name;
  private final String column;
  private final int dimension;
  private final String clientClass;
  private final String provider;
  private final String modelName;
  /** The declared modalities, sorted; empty when the model declares none. */
  private final Set<String> modalities;

  /** A model without an explicit input-modality declaration ({@link #getModalities()}). */
  public EmbeddingModelSpec(String name, String column, int dimension, String clientClass, String provider,
                            String modelName) {
    this(name, column, dimension, clientClass, provider, modelName, Collections.emptySet());
  }

  /**
   * @param modalities the modalities the model is declared to embed ({@code text}, {@code image}); empty when undeclared
   */
  public EmbeddingModelSpec(String name, String column, int dimension, String clientClass, String provider,
                            String modelName, Set<String> modalities) {
    this.name = name;
    this.column = column;
    this.dimension = dimension;
    this.clientClass = clientClass;
    this.provider = provider;
    this.modelName = modelName;
    this.modalities = Collections.unmodifiableSet(new TreeSet<>(modalities));
  }

  public String getName() {
    return name;
  }

  public String getColumn() {
    return column;
  }

  public int getDimension() {
    return dimension;
  }

  /** The configured EmbeddingClient class, or null to use the client of {@link #getProvider()}. */
  public String getClientClass() {
    return clientClass;
  }

  /** The embedding provider serving this model (e.g. {@code dummy}, {@code openai}). */
  public String getProvider() {
    return provider;
  }

  /** The provider-side model name; defaults to the alias {@link #getName()}. */
  public String getModelName() {
    return modelName;
  }

  /**
   * The modalities the model is declared to embed, sorted ({@code hoodie.unstructured.embedding.model.<alias>.modalities});
   * empty when it declares none. Provider configuration validates limits for explicitly declared inputs.
   */
  public Set<String> getModalities() {
    return modalities;
  }

  /** Whether the model is declared to embed rows of {@code modality}. */
  public boolean declares(String modality) {
    return modalities.contains(modality);
  }

  @Override
  public String toString() {
    return name + "(" + column + ", VECTOR(" + dimension + "), " + clientClass + ", " + provider + "/" + modelName + ", "
        + modalities + ")";
  }
}
