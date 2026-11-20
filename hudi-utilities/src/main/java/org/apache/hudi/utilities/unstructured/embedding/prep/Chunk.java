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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable embedding input: modality, text, token count and aux. */
public class Chunk {

  private final String modality;
  private final String text;
  private final Integer tokenCount;
  private final Map<String, String> aux;

  /**
   * @param modality   embedding-table modality, e.g. {@code text} or {@code image}
   * @param text       chunk text, null for non-text modalities
   * @param tokenCount tokens in {@code text} as counted by the configured
   *                   {@link org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer}, null when no text
   * @param aux        modality-specific provenance (page, size, ...); never raw file bytes; copied
   */
  public Chunk(String modality, String text, Integer tokenCount, Map<String, String> aux) {
    this.modality = modality;
    this.text = text;
    this.tokenCount = tokenCount;
    this.aux = aux == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(aux));
  }

  /** A builder of a chunk of {@code modality}; every other field is optional. */
  public static Builder builder(String modality) {
    return new Builder(modality);
  }

  public String getModality() {
    return modality;
  }

  public String getText() {
    return text;
  }

  public Integer getTokenCount() {
    return tokenCount;
  }

  public Map<String, String> getAux() {
    return aux;
  }

  /** Builds an immutable {@link Chunk}; the chunk copies the aux map. */
  public static final class Builder {
    private final String modality;
    private String text;
    private Integer tokenCount;
    private Map<String, String> aux = Collections.emptyMap();

    private Builder(String modality) {
      this.modality = Objects.requireNonNull(modality, "modality");
    }

    /** Chunk text, null for non-text modalities. */
    public Builder text(String text) {
      this.text = text;
      return this;
    }

    /** Tokens in the text as counted by the configured {@link org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer}. */
    public Builder tokenCount(Integer tokenCount) {
      this.tokenCount = tokenCount;
      return this;
    }

    /** Modality-specific provenance (page, size, ...); never raw file bytes. */
    public Builder aux(Map<String, String> aux) {
      this.aux = aux == null ? Collections.emptyMap() : aux;
      return this;
    }

    public Chunk build() {
      return new Chunk(modality, text, tokenCount, aux);
    }
  }
}
