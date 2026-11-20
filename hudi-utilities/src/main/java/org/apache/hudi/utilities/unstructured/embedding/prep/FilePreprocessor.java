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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * Turns one file into embedding inputs, incrementally.
 *
 * <p>Contract (incremental, with memory use hard capped regardless of the file size):
 * <ul>
 *   <li>{@link #open} returns quickly-iterable state; each {@code next()} produces ONE chunk; nothing that grows
 *       with the file size may be buffered (no list of chunks, pages or the whole text);</li>
 *   <li>a preprocessor bounds each chunk's content (text + aux) itself, by its own token, word or window limits; the
 *       framework does not re-check chunk size;</li>
 *   <li>any {@link Exception} from {@code open}, {@code hasNext} or {@code next} fails only this file: the framework
 *       logs a warning, keeps the chunks already emitted, quarantines the file (stage prep, no row) and continues;</li>
 *   <li>a file without embeddable content yields an iterator without chunks; the framework quarantines it as empty
 *       (no row). A preprocessor never emits marker rows: it throws to fail a file and emits nothing for a file without
 *       text;</li>
 *   <li>the framework always calls {@code close()} (file switch, failure, or Spark task completion).</li>
 * </ul>
 * The framework stamps every stored row with the table-pinned {@code prep_logic_version}, replacing the raw chunks'
 * legacy input_logic/video_logic/audio_logic hints.
 * Implementations need a public no-arg constructor; one instance serves many files of one Spark task, sequentially.
 */
public interface FilePreprocessor {

  /**
   * Stable implementation name + version, pinned on the table with the class of every extension it prepares
   * ({@code hoodie.table.embedding.prep.extension.<ext>}). Bump it whenever emitted records change. The table pins no setting of
   * a custom implementation: its settings live outside the prep prefixes ({@code hoodie.unstructured.chunk.},
   * {@code hoodie.unstructured.pdf.}, {@code hoodie.unstructured.embedding.prep.}, where an undeclared key is refused at job
   * start), and a setting that changes its records must change this name.
   */
  String name();

  /**
   * The kind of source file this preprocessor reads; its {@link SourceKind#markerModality()} is the modality of the file's
   * quarantine entry when it fails or yields nothing.
   */
  SourceKind sourceKind();

  ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
      throws IOException;

  /**
   * Whether this preprocessor's chunks may carry a payload, a model input the embedding table stores in {@code prepared_image}
   * (an image's or a video frame's bytes). A preprocessor that emits a payload must declare it; the framework does not check.
   * Default false: text preprocessors store no input.
   */
  default boolean storesInputs() {
    return false;
  }

  /**
   * Validates this preprocessor's configuration on the DRIVER before the job reads any file, so a bad key fails the job
   * once instead of failing every file as an isolated error.
   *
   * @throws IllegalArgumentException for an invalid configuration
   */
  default void validateConfig(TypedProperties props, Tokenizer tokenizer) {
  }

  /**
   * The modalities of the rows this preprocessor can produce for a file under {@code props}. Default: the
   * marker modality of its {@link #sourceKind()}.
   */
  default Set<String> modalities(TypedProperties props) {
    return Collections.singleton(sourceKind().markerModality());
  }
}
