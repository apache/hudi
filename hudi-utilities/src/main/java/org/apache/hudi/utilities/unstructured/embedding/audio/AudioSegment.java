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

import java.io.IOException;
import java.io.InputStream;

/**
 * One speech segment of a video's audio track: a time span the voice activity step kept, with its audio payload ready for
 * a {@link SegmentTranscriber}.
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #index()} is dense from 0 in time order over one video and its configuration; the same video and configuration
 *   always yield the same segments;</li>
 *   <li>times are milliseconds on the VIDEO's timeline, {@code 0 <= startMs() < endMs()}, and
 *       {@code endMs() - startMs() <= }{@link AudioConfigKeys#SEGMENT_MAX_MS};</li>
 *   <li>the payload is bounded: {@link #payloadBytes()} {@code <= }{@link AudioConfigKeys#SEGMENT_MAX_BYTES}; it is held by
 *       the source (heap or a task-local spill file) only until the next segment is requested.</li>
 * </ul>
 */
public interface AudioSegment {

  /** Position of this segment in the video's segment sequence, dense from 0. */
  long index();

  /** Start of the speech span, milliseconds on the video's timeline. */
  long startMs();

  /** End of the speech span (exclusive), milliseconds on the video's timeline. */
  long endMs();

  /** Sample rate of the payload in Hz (default {@link AudioConfigKeys#DEFAULT_SAMPLE_RATE}). */
  int sampleRate();

  /** Channel count of the payload (default 1, mono). */
  int channels();

  /** Payload encoding, e.g. {@code pcm_s16le} (raw little-endian 16-bit) or {@code flac}; part of the audio logic version. */
  String encoding();

  /** Exact byte length of the payload. */
  long payloadBytes();

  /** Opens the payload; callable more than once (a transcriber retry re-reads it); the caller closes the stream. */
  InputStream openPayload() throws IOException;
}
