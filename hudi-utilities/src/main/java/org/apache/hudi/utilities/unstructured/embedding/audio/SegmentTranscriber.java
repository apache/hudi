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
 * Speech-to-text for ONE bounded {@link AudioSegment}: yields the speaker-labelled utterances of that segment
 * (a text chunk contains at least speaker and content); the default is whisper. Whether the engine is a local library or a provider endpoint is an operator decision;
 * both fit this seam. It takes one segment rather than the whole media, so no call ever needs
 * more than {@link AudioConfigKeys#SEGMENT_MAX_BYTES} of audio, unlike the whole-media
 * {@code prep.video.Transcriber} skeleton it superseded (retired).
 *
 * <p>Contract:
 * <ul>
 *   <li>{@link #id()} names the engine and model (e.g. {@code <engine>/<model>}); it is part of the audio logic version;</li>
 *   <li>{@link #transcribe} yields utterances in non-decreasing {@link TranscriptSegment#getStartMs()} order, with times on
 *       the VIDEO's timeline (segment start already added) and inside the segment's span;</li>
 *   <li>speaker labels are scoped to ONE segment unless {@link #fileScopedSpeakers()} is true: the same label in two
 *       segments need not be the same person;</li>
 *   <li>text is returned as the engine produced it; normalisation is the chunker's job;</li>
 *   <li>failures: an {@link IOException} interrupts the prep run (endpoint unavailable, throttled, timed out, payload read:
 *       nothing is committed and the rerun transcribes the video again), so an implementation must NOT let an engine's data
 *       error (an audio the engine refuses or cannot decode) surface as an {@link IOException}: it throws
 *       {@link IllegalStateException} or another non-IO exception for it, a content failure of the video (its quarantine entry); an
 *       {@link IllegalArgumentException} from {@code transcribe()} itself is a configuration error and fails the task; a
 *       failure of the engine's own runtime that cannot be told from a data error, or a segment that breaks what the segment
 * source promises, is a {@code PrepPlatformException}, which fails the round (class P). The caller always closes the
 *       iterator.</li>
 * </ul>
 */
public interface SegmentTranscriber {

  /** Engine and model of the transcription, stable across runs of one table. */
  String id();

  /** Whether speaker labels identify the same speaker across segments of one video. */
  boolean fileScopedSpeakers();

  /** Checks this transcriber's configuration on the driver; throws {@link IllegalArgumentException} naming the key. */
  void validate(TypedProperties props);

  /**
   * Transcribes {@code segment}.
   *
   * @param segment the speech segment, read through {@link AudioSegment#openPayload()}
   * @param props   the job's properties ({@link AudioConfigKeys})
   * @return the utterances in time order; the caller closes it
   * @throws IOException when the engine cannot be reached or the payload cannot be read (an interruption of the prep run)
   */
  ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) throws IOException;
}
