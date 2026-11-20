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

import java.io.Serializable;

/**
 * The five-column source version key and text passed to an embedding client.
 */
public class EmbeddingInput implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourcePathSha;
  private final long sourceModificationTime;
  private final long sourceFileSize;
  private final long chunkId;
  private final String modality;
  private final String text;

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

}
