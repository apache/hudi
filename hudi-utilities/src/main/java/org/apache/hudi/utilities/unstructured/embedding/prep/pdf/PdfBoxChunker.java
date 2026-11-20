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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The production {@link PdfChunker}: the parser-free iterator over a {@link PageSource}; by default the in-JVM PDFBox
 * source with its best-effort guard. Stateless and reusable across files.
 */
public class PdfBoxChunker implements PdfChunker {

  private static final Logger LOG = LoggerFactory.getLogger(PdfBoxChunker.class);

  private final PageSourceFactory pageSources;

  /** The in-JVM PDFBox chunker, with the best-effort guard. */
  public PdfBoxChunker() {
    this(PdfBoxPageSource::new);
  }

  /** Chunks with the given page source, e.g. an out-of-process one or a test double. */
  public PdfBoxChunker(PageSourceFactory pageSources) {
    this.pageSources = pageSources;
  }

  @Override
  public PdfChunkIterator open(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config) {
    return new PdfChunkIterator(source, meta, config, pageSources, LOG::warn);
  }

  /** Same as {@link #open} with the WARN lines sent to {@code warnings} (tests). */
  PdfChunkIterator open(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config,
                        PdfChunkIterator.WarnSink warnings) {
    return new PdfChunkIterator(source, meta, config, pageSources, warnings);
  }
}
