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

package org.apache.hudi.utilities.unstructured.embedding.prep.video;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;

import java.io.IOException;

/**
 * Picks the frames of one video that become its {@code video} rows of the embedding table ("Video -> Process
 * frame -> frame extraction strategy, chunk id, source_path_sha ... Keep the interface"). Data prep's
 * {@link org.apache.hudi.utilities.unstructured.embedding.prep.VideoFramePreprocessor} runs one per video; the default is
 * {@code video.FfmpegKeyframeVideoFrameSource} (the closest keyframe every X, consecutive repeats dropped).
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #name()} is the strategy's logic version (e.g. {@code video-keyframe-v1}), recorded in every frame row's aux
 *       ({@code video_logic}). It names the logic, never the class or library. Its parameters are preprocessing
 *       style keys ({@code prep.video.*}), so a table never mixes frame logics;</li>
 *   <li>{@link #frames} yields the KEPT frames in presentation order (never decode order), one at a time, their indexes dense
 *       from 0 (frame k becomes the row {@code (source_path_sha, image, chunk_id = k)}) and their times strictly increasing, measured from the
 *       container's start (the origin speech is timed from); the same video, configuration and native build always yield the
 *       same frames with the same encoded bytes, so a re-run after an interruption writes the same keys and inputs;</li>
 *   <li>at most one decoded frame and its encoded image are held in memory, so memory stays independent of the video's length
 *       (memory use is hard capped regardless of the file size);</li>
 *   <li>failures are thrown and classified by the caller: an {@link java.io.UncheckedIOException} over a storage failure of the
 *       source is an interruption unless the source is gone, short or names no readable range; anything else (a corrupt or
 *       refused video) fails only this file. The caller always closes the iterator.</li>
 * </ul>
 * Implementations need a public no-arg constructor; one instance serves the videos of one Spark task, sequentially.
 */
public interface FrameExtractionStrategy {

  /** The strategy's logic version, e.g. {@code video-keyframe-v1}. */
  String name();

  /**
   * Checks this strategy's configuration on the driver, before any Spark work.
   *
   * @throws IllegalArgumentException naming the offending key
   */
  default void validateConfig(TypedProperties props) {
  }

  /** Opens the kept frames of {@code video}; each {@code next()} decodes, encodes and returns ONE frame. */
  ClosableIterator<ExtractedFrame> frames(BlobBytesSupplier video, FileMeta file, TypedProperties props) throws IOException;
}
