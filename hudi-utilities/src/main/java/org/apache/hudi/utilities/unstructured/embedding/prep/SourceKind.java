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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

/** The kind of source file read by a preprocessor and its output modality. */
public enum SourceKind {
  /** A document whose rows are text chunks (a PDF). */
  DOCUMENT(EmbeddingTableSchema.MODALITY_TEXT);

  private final String markerModality;

  SourceKind(String markerModality) {
    this.markerModality = markerModality;
  }

  /**
   * The modality of the quarantine entry of a failed or empty file of this kind: its main target modality, so no row ever
   * carries a source type ({@code text} for a document).
   */
  public String markerModality() {
    return markerModality;
  }
}
