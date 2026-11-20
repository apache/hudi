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

package org.apache.hudi.utilities.unstructured.embedding.source;

import java.io.Serializable;

/**
 * The source-row columns of one file a {@link org.apache.hudi.utilities.unstructured.embedding.prep.FilePreprocessor} may look at (the row minus its bytes).
 */
public class FileMeta implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourcePathSha;
  /** The source object's modification time in ms, half of the row's version tag; 0 when the source row carries none. */
  private final long sourceModificationTime;
  private final String fileExtension;
  private final long fileSize;

  public FileMeta(String sourcePathSha, long sourceModificationTime, String fileExtension, long fileSize) {
    this.sourcePathSha = sourcePathSha;
    this.sourceModificationTime = sourceModificationTime;
    this.fileExtension = fileExtension;
    this.fileSize = fileSize;
  }

  /** A file whose version tag is not known (preprocessor tests): modification time 0. */
  public FileMeta(String sourcePathSha, String fileExtension, long fileSize) {
    this(sourcePathSha, 0L, fileExtension, fileSize);
  }

  public String getSourcePathSha() {
    return sourcePathSha;
  }

  public long getSourceModificationTime() {
    return sourceModificationTime;
  }

  public String getFileExtension() {
    return fileExtension;
  }

  public long getFileSize() {
    return fileSize;
  }

  /**
   * The file as a failure reason names it: {@code source_path_sha <source_path_sha> (<extension>, <size> bytes)}. The source_path_sha is the
   * source row's key, the column a quarantine entry carries (a reason holds no identity hash).
   */
  @Override
  public String toString() {
    return "source_path_sha " + sourcePathSha + " (" + fileExtension + ", " + fileSize + " bytes)";
  }
}
