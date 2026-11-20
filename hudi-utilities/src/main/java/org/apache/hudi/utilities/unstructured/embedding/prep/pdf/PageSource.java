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

import java.io.Closeable;
import java.io.IOException;

/**
 * The parser seam of the chunker: extracts one page at a time. Everything above it (normalization, packing, records,
 * error isolation) is parser-free; the in-JVM PDFBox implementation
 * ({@link PdfBoxPageSource}) can be replaced by an out-of-process one without any caller
 * change.
 *
 * <p>{@link #open()} and {@link #next()} signal a file-level failure with {@link PdfFileFailure}. Any other exception (or
 * {@link StackOverflowError}) thrown by {@link #next()} fails only that page ({@code PAGE_ERROR}): the source has already
 * moved past it.
 */
public interface PageSource extends Closeable {

  /** Stages and loads the file. */
  void open() throws IOException;

  /** Whether another page exists, without extracting it. */
  boolean hasMorePages();

  /** Extracts the next page. */
  PageText next() throws IOException;

  /** Bytes staged to local disk for this file. */
  long stagedBytes();

  /** Deletes the staged file and scratch; idempotent. */
  @Override
  void close();
}
