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

package org.apache.hudi.utilities.unstructured.embedding.audio;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.IOException;

/**
 * Demux, decode, resample and voice-activity steps for ONE video: turns its audio track into bounded
 * {@link AudioSegment}s, one at a time ("Process audio -> text chunk, chunk id, fid"; "memory usage must be
 * hard capped regardless of the file size"). The audio counterpart of the video frame seam
 * The real implementation decodes with FFmpeg through JavaCV; a stub
 * row may return fixed segments.
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #logicVersion()} names the logic and its style-relevant settings, never the class; it is part of
 *       the audio logic version recorded on every transcript row ({@link AudioRows#AUX_AUDIO_LOGIC});</li>
 *   <li>{@link #validate} runs on the driver before any Spark work; an {@link IllegalArgumentException} names the key;</li>
 *   <li>{@link #segments} streams: each {@code next()} decodes only as far as the next segment, so heap and native memory
 *       stay bounded by one decode window plus one segment payload, independent of the video's length;</li>
 *   <li>deterministic: the same video and configuration yield the same segments (a crashed prep run is redone from
 *       segment 0 and must yield the same rows), and {@code startSegment = k} returns segments {@code k, k+1, ...} with
 *       their true indexes and spans. Data prep reads a video whole and always passes {@code startSegment = 0},
 *       {@code startMs = 0}; the resume parameters stay in the seam for a caller that splits a video;</li>
 *   <li>resume cost: {@code startMs} is the END of segment {@code startSegment - 1} as this source reported it (0 when
 *       {@code startSegment = 0}); segment {@code startSegment} starts at or after it. A source SHOULD seek to it instead of
 *       decoding from the start (a resume that re-decodes from 0 makes a long video cost quadratic in its length); it may
 *       do so only when its segmentation does not depend on audio before {@code startMs} (e.g. voice-activity windows
 *       aligned to the absolute timeline and state reset at every segment start); a source that cannot seek
 *       deterministically decodes from 0 and skips;</li>
 *   <li>an empty iterator ends the video: with {@code startSegment = 0} the video has no audio stream or no speech (no
 *       speech row); with {@code startSegment > 0} every segment is already done;</li>
 *   <li>failures, per video, classified by CAUSE and not by exception class (a decoder's own exceptions may extend
 *       {@link IOException}, e.g. JavaCV's {@code FrameGrabber.Exception}): an interruption of the prep run (nothing
 *       committed, the rerun reads the video again) only when a
 *       {@link org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException} is in the cause chain (the video's
 *       bytes could not be read, and the source is not gone or short) or the source's own task-local spill I/O failed and it
 *       says so by throwing {@link java.io.UncheckedIOException}; every other exception from opening or from {@code next()},
 *       decoder and container errors included, is a content failure of this video (its quarantine entry); an
 *       {@link IllegalArgumentException} from {@code segments()} itself is a configuration error and fails the task, so a
 *       decoder must not throw one, or a subtype of it, for corrupt content;</li>
 *   <li>read failures must not look like the end of the audio: a decoder may swallow an error of the stream it reads
 *       (JavaCV 1.5.11's {@code FFmpegFrameGrabber} read callback prints it and returns end-of-file).
 *       So the source wraps {@link AudioInput#bytes()} itself: the first {@link IOException} from opening or reading the
 *       bytes is recorded as a {@link org.apache.hudi.utilities.unstructured.embedding.source.BlobReadException} and rethrown
 *       from {@code hasNext()}/{@code next()} as soon as the decoder reports end of stream or fails, so a storage error
 *       part-way through a video interrupts the prep run, never ends it as a short transcript;</li>
 *   <li>the caller always closes the iterator; one no-arg instance serves one Spark task's videos sequentially.</li>
 * </ul>
 */
public interface AudioSegmentSource {

  /** Logic version of segmentation, e.g. {@code ffmpeg-vad-energy-v1}; a stub uses {@code audio-stub-v0}. */
  String logicVersion();

  /** Checks this source's configuration on the driver; throws {@link IllegalArgumentException} naming the key. */
  void validate(TypedProperties props);

  /**
   * Opens the speech segments of {@code audio} in time order, starting at segment {@code startSegment} (0 = all).
   *
   * @param audio        the video whose audio track is read
   * @param startSegment the first segment index the caller needs (data prep: 0)
   * @param startMs      end of segment {@code startSegment - 1} on the video's timeline as reported before, 0 when
   *                     {@code startSegment = 0}; a seek hint
   * @param props        the job's properties ({@link AudioConfigKeys})
   * @return the segments, one decoded at a time; the caller closes it
   * @throws IOException when the video bytes cannot be opened
   */
  ClosableIterator<AudioSegment> segments(AudioInput audio, long startSegment, long startMs, TypedProperties props)
      throws IOException;
}
