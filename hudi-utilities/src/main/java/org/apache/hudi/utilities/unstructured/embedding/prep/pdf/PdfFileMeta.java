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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import java.io.Serializable;

/** What the chunker knows about one PDF besides its bytes. */
public final class PdfFileMeta implements Serializable {

  private static final long serialVersionUID = 1L;

  private final String sourcePathSha;
  private final String sourcePath;
  private final long fileSize;

  /**
   * @param sourcePathSha copied unchanged into every record
   * @param sourcePath used only in log lines
   * @param fileSize   declared size in bytes, -1 when unknown
   */
  public PdfFileMeta(String sourcePathSha, String sourcePath, long fileSize) {
    this.sourcePathSha = sourcePathSha;
    this.sourcePath = sourcePath;
    this.fileSize = fileSize;
  }

  public String sourcePathSha() {
    return sourcePathSha;
  }

  public String sourcePath() {
    return sourcePath;
  }

  public long fileSize() {
    return fileSize;
  }
}
