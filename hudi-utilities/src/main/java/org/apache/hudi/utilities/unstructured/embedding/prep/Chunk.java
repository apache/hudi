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

import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable embedding input: modality, text, token count, aux and, for a non-text row, its payload: the model input the
 * embedding table stores in {@code prepared_image}.
 */
public class Chunk {

  private final String modality;
  private final String text;
  private final Integer tokenCount;
  private final Map<String, String> aux;
  private final BlobBytesSupplier payload;
  private final long payloadSizeBytes;
  private final String payloadContentType;
  private final boolean payloadIsSource;

  /**
   * @param modality   embedding-table modality, e.g. {@code text} or {@code image}
   * @param text       chunk text, null for non-text modalities
   * @param tokenCount tokens in {@code text} as counted by the configured
   *                   {@link org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer}, null when no text
   * @param aux        modality-specific provenance (page, size, ...); never raw file bytes; copied
   */
  public Chunk(String modality, String text, Integer tokenCount, Map<String, String> aux) {
    this(modality, text, tokenCount, aux == null ? Collections.emptyMap() : Collections.unmodifiableMap(new HashMap<>(aux)),
        null, 0L, null, false);
  }

  private Chunk(String modality, String text, Integer tokenCount, Map<String, String> aux, BlobBytesSupplier payload,
                long payloadSizeBytes, String payloadContentType, boolean payloadIsSource) {
    this.modality = modality;
    this.text = text;
    this.tokenCount = tokenCount;
    this.aux = aux;
    this.payload = payload;
    this.payloadSizeBytes = payloadSizeBytes;
    this.payloadContentType = payloadContentType;
    this.payloadIsSource = payloadIsSource;
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

  /** The model input to store in {@code prepared_image} (an image or a video frame); null when none. */
  public BlobBytesSupplier getPayload() {
    return payload;
  }

  /** Size of {@link #getPayload()} in bytes; 0 when there is no payload. */
  public long getPayloadSizeBytes() {
    return payloadSizeBytes;
  }

  /** Content type of {@link #getPayload()} (e.g. image/png); null when there is no payload. No column stores it. */
  public String getPayloadContentType() {
    return payloadContentType;
  }

  /**
   * Whether {@link #getPayload()} is the file's whole source, unmodified (a standalone image): the framework then stores the
   * source row's own blob value (its inline bytes, or its reference to the object) instead of reading the bytes.
   */
  public boolean isPayloadSource() {
    return payloadIsSource;
  }

  /** Builds an immutable {@link Chunk}; {@link #build()} copies the aux map. */
  public static final class Builder {
    private final String modality;
    private String text;
    private Integer tokenCount;
    private Map<String, String> aux = Collections.emptyMap();
    private BlobBytesSupplier payload;
    private long payloadSizeBytes;
    private String payloadContentType;
    private boolean payloadIsSource;

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

    /**
     * A derived model input to store in {@code prepared_image} (a video frame): its bytes, read once, whole, by the framework, which
     * stores them inline; their exact size and content type.
     *
     * @throws IllegalArgumentException if {@code payloadSizeBytes} is negative
     */
    public Builder payload(BlobBytesSupplier payload, long payloadSizeBytes, String contentType) {
      ValidationUtils.checkArgument(payloadSizeBytes >= 0, "payloadSizeBytes must be >= 0 but is " + payloadSizeBytes);
      this.payload = payload;
      this.payloadSizeBytes = payload == null ? 0L : payloadSizeBytes;
      this.payloadContentType = payload == null ? null : contentType;
      this.payloadIsSource = false;
      return this;
    }

    /**
     * {@link #payload(BlobBytesSupplier, long, String)} of a payload that is the file's whole source, unmodified ({@code source}
     * opens the file's bytes, {@code sizeBytes} is its source_file_size): the framework stores the source row's own blob value
     * and never reads these bytes.
     */
    public Builder sourcePayload(BlobBytesSupplier source, long sizeBytes, String contentType) {
      payload(source, sizeBytes, contentType);
      this.payloadIsSource = source != null;
      return this;
    }

    public Chunk build() {
      return new Chunk(modality, text, tokenCount, Collections.unmodifiableMap(new HashMap<>(aux)), payload,
          payloadSizeBytes, payloadContentType, payloadIsSource);
    }
  }
}
