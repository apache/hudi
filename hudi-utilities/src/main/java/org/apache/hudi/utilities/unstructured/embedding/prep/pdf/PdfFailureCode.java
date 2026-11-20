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

import org.apache.hudi.utilities.unstructured.SourceCauses;
import org.apache.hudi.utilities.unstructured.embedding.prep.ErrorText;

/**
 * Failure codes of the PDF chunker. File-level codes end a file (a quarantine entry, or the
 * {@code file_error} of the loss summary); page-level codes appear in WARN lines and the loss counters only.
 */
public enum PdfFailureCode {
  // file level
  INPUT_LIMIT, SOURCE_IO, ENCRYPTED, LOAD_ERROR, ALL_PAGES_FAILED, PAGE_LIMIT, TOKENIZER_ERROR, INTERNAL,
  // page level
  PAGE_ERROR, PAGE_TRUNCATED, PAGE_GLYPH_LIMIT, DECODE_LIMIT, DECODE_ERROR, GRAPHICS_DEPTH_LIMIT;

  /** Maximum length of a formatted detail (the embedding table's {@code prep_error} limit). */
  public static final int MAX_DETAIL_CHARS = 1024;

  /**
   * "CODE: exception class: message" of the ROOT cause (what actually failed, e.g. the missing file's path),
   * followed by the chunker's own message in parentheses when it wrapped that cause; cut to {@link #MAX_DETAIL_CHARS}. The
   * chunker never puts extracted text into an exception it creates.
   */
  public String format(Throwable cause) {
    if (cause == null) {
      return name();
    }
    Throwable root = SourceCauses.getRootCause(cause);
    if (root == null) {
      // getRootCause returns null only for a null failure, handled above
      root = cause;
    }
    String detail = name() + ": " + root.getClass().getName() + ": " + root.getMessage()
        + (root == cause ? "" : " (" + cause.getMessage() + ")");
    return ErrorText.truncate(detail, MAX_DETAIL_CHARS);
  }
}
