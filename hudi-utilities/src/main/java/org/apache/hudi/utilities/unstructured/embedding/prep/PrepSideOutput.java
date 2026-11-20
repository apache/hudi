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

import java.io.Serializable;
import java.util.Objects;

/**
 * One input a prep round does not turn into embedding rows: a file whose preprocessing failed or a chunk whose payload could
 * not be stored ({@link #STAGE_PREP}), or a file that yields no chunk or has no preprocessor ({@link #STAGE_EMPTY}, error
 * {@link #NO_TEXT} or {@link #NO_PREPROCESSOR}). Goes to the job's error table when it is on; never an embedding row.
 */
public final class PrepSideOutput implements Serializable {

  private static final long serialVersionUID = 1L;

  public static final String STAGE_PREP = "prep";
  public static final String STAGE_EMPTY = "empty";
  /** The error of a {@link #STAGE_EMPTY} file whose preprocessor finished without error and without any chunk. */
  public static final String NO_TEXT = "no_text";
  /** The error of a {@link #STAGE_EMPTY} file whose extension no preprocessor is mapped to. */
  public static final String NO_PREPROCESSOR = "no_preprocessor";

  private final String sourcePathSha;
  /** The source row's version tag. */
  private final Long sourceModificationTime;
  private final Long sourceFileSize;
  private final String sourcePath;
  private final String modality;
  private final Long chunkId;
  private final String stage;
  private final String error;

  public PrepSideOutput(String sourcePathSha, Long sourceModificationTime, Long sourceFileSize, String sourcePath, String modality,
                        Long chunkId, String stage, String error) {
    this.sourcePathSha = sourcePathSha;
    this.sourceModificationTime = sourceModificationTime;
    this.sourceFileSize = sourceFileSize;
    this.sourcePath = sourcePath;
    this.modality = modality;
    this.chunkId = chunkId;
    this.stage = Objects.requireNonNull(stage, "stage");
    this.error = error;
  }

  public String getSourcePathSha() {
    return sourcePathSha;
  }

  public Long getSourceModificationTime() {
    return sourceModificationTime;
  }

  public Long getSourceFileSize() {
    return sourceFileSize;
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public String getModality() {
    return modality;
  }

  /** The chunk of an entry that fails one chunk only; null for a whole file. */
  public Long getChunkId() {
    return chunkId;
  }

  public String getStage() {
    return stage;
  }

  public String getError() {
    return error;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PrepSideOutput)) {
      return false;
    }
    PrepSideOutput other = (PrepSideOutput) o;
    return Objects.equals(sourcePathSha, other.sourcePathSha) && Objects.equals(sourceModificationTime, other.sourceModificationTime)
        && Objects.equals(sourceFileSize, other.sourceFileSize) && Objects.equals(sourcePath, other.sourcePath) && Objects.equals(modality, other.modality)
        && Objects.equals(chunkId, other.chunkId) && stage.equals(other.stage) && Objects.equals(error, other.error);
  }

  @Override
  public int hashCode() {
    return Objects.hash(sourcePathSha, sourceModificationTime, sourceFileSize, sourcePath, modality, chunkId, stage, error);
  }

  @Override
  public String toString() {
    return "PrepSideOutput{stage=" + stage + ", source_path_sha=" + sourcePathSha + ", source_modification_time=" + sourceModificationTime
        + ", source_file_size=" + sourceFileSize + ", source_file_path=" + sourcePath + ", modality="
        + modality + ", chunk_id=" + chunkId + ", error=" + error + "}";
  }
}
