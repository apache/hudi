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

import org.apache.hudi.common.util.collection.ClosableIterator;

/**
 * Turns one PDF into text chunks, pulled one at a time. Implementations hold no per-file state, so
 * one instance serves consecutive files of a task; each returned iterator is used by one thread.
 */
public interface PdfChunker {

  /**
   * Returns the chunk iterator of one file. Does no I/O and starts no thread: the first {@code hasNext()} opens the source.
   * The caller closes the iterator.
   */
  ClosableIterator<PdfChunk> open(InputStreamSupplier source, PdfFileMeta meta, PdfChunkerConfig config);
}
