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

import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

import java.io.Serializable;

/**
 * One embedding-table row handed to an {@link EmbeddingClient}, with its five-column source version key. A text row carries
 * {@code text} and no bytes. An image row (modality {@code image}) carries its STORED input, {@link #storedImage()} over the
 * row's {@code prepared_image}, and for a model that declares {@code image} the compute flow's bounded, in-memory resize of it
 * into the model input, {@link #image()}.
 */
public class EmbeddingInput implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourcePathSha;
  private final long sourceModificationTime;
  private final long sourceFileSize;
  private final long chunkId;
  private final String modality;
  private final String text;
  /** The row's stored input ({@code prepared_image}); null when it has none (text rows). */
  private final transient BlobBytesSupplier stored;
  private final transient PreparedImageSupplier image;

  /** A standalone provider input without source version metadata; both version fields default to zero. */
  public EmbeddingInput(String sourcePathSha, long chunkId, String modality, String text) {
    this(sourcePathSha, 0L, 0L, modality, chunkId, text);
  }

  /** An embedding row's full source version identity, in key-column order, and its prepared text. */
  public EmbeddingInput(String sourcePathSha, long sourceModificationTime, long sourceFileSize, String modality, long chunkId, String text) {
    this.sourcePathSha = sourcePathSha;
    this.sourceModificationTime = sourceModificationTime;
    this.sourceFileSize = sourceFileSize;
    this.chunkId = chunkId;
    this.modality = modality;
    this.text = text;
    this.stored = null;
    this.image = null;
  }

  /** {@code row}'s identity and text with {@code stored} and {@code image}. */
  private EmbeddingInput(EmbeddingInput row, BlobBytesSupplier stored, PreparedImageSupplier image) {
    this.sourcePathSha = row.sourcePathSha;
    this.sourceModificationTime = row.sourceModificationTime;
    this.sourceFileSize = row.sourceFileSize;
    this.chunkId = row.chunkId;
    this.modality = row.modality;
    this.text = row.text;
    this.stored = stored;
    this.image = image;
  }

  /**
   * The input of an image row: {@code stored} opens the row's {@code prepared_image}, and {@code image} prepares the model input
   * from it for a model that declares {@code image} (null until image compute provides one). Both version fields default to
   * zero, as for a standalone text input.
   */
  public static EmbeddingInput storedImage(String sourcePathSha, long chunkId, String modality, BlobBytesSupplier stored,
                                           PreparedImageSupplier image) {
    return storedImage(sourcePathSha, 0L, 0L, modality, chunkId, stored, image);
  }

  /** The input of an image row with its full source version identity, in key-column order; see the overload above. */
  public static EmbeddingInput storedImage(String sourcePathSha, long sourceModificationTime, long sourceFileSize, String modality,
                                           long chunkId, BlobBytesSupplier stored, PreparedImageSupplier image) {
    return new EmbeddingInput(new EmbeddingInput(sourcePathSha, sourceModificationTime, sourceFileSize, modality, chunkId, null),
        stored, image);
  }

  public String getSourcePathSha() {
    return sourcePathSha;
  }

  public long getSourceModificationTime() {
    return sourceModificationTime;
  }

  public long getSourceFileSize() {
    return sourceFileSize;
  }

  public long getChunkId() {
    return chunkId;
  }

  public String getModality() {
    return modality;
  }

  public String getText() {
    return text;
  }

  /** The five key columns and their values, in key order, for diagnostics. */
  public String describeIdentity() {
    return "source_path_sha=" + sourcePathSha + " source_modification_time=" + sourceModificationTime
        + " source_file_size=" + sourceFileSize + " modality=" + modality + " chunk_id=" + chunkId;
  }

  /**
   * Opens the row's stored input, {@code prepared_image} (each call a fresh stream the caller closes); null when the input is not
   * a stored one (text rows).
   */
  public BlobBytesSupplier storedImage() {
    return stored;
  }

  /**
   * Prepares the resized model input of an image row from its stored input (each call prepares it again); null for text rows and
   * while no image preparation is configured.
   */
  public PreparedImageSupplier image() {
    return image;
  }

}
