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

/**
 * Fails the current page with a page-level {@link PdfFailureCode} ({@code PAGE_GLYPH_LIMIT}, {@code DECODE_LIMIT},
 * {@code GRAPHICS_DEPTH_LIMIT}, {@code PAGE_ERROR}). Unchecked, so PDFBox's operator error handling, which swallows
 * {@code IOException}s raised under some operators, never hides it.
 */
public class PageFailure extends RuntimeException {

  private final PdfFailureCode code;

  public PageFailure(PdfFailureCode code, String message) {
    super(message);
    this.code = code;
  }

  public PageFailure(PdfFailureCode code, String message, Throwable cause) {
    super(message, cause);
    this.code = code;
  }

  public PdfFailureCode code() {
    return code;
  }
}
