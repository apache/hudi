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

/**
 * Speaker labels of the audio pass, v1 (a text chunk contains at least speaker and content).
 *
 * <p>v1 has no diarization: a transcriber that cannot tell speakers apart labels EVERY utterance of a video with the one
 * file-scoped label {@link #FILE_SPEAKER} and returns {@code true} from {@link SegmentTranscriber#fileScopedSpeakers()}, so
 * the transcript rows carry {@code [speaker-1] <utterance>} lines and aux {@code speaker_scope=file}.
 *
 * <p>The diarization seam: a transcriber that does separate speakers puts its own label on each
 * {@link org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment} and declares, through
 * {@link SegmentTranscriber#fileScopedSpeakers()}, whether a label names the same person across the video's segments
 * ({@code file}) or only within one segment ({@code segment}). A label is a generic id: no whitespace, control char or
 * bracket ({@link TranscriptPacker} fails the video otherwise); the chunker writes the label as it arrives and data prep records
 * the scope; nothing else changes when a diarizer arrives.
 */
public final class SpeakerLabels {

  /** The one speaker label of every utterance when the transcriber has no diarization. */
  public static final String FILE_SPEAKER = "speaker-1";

  /** {@link AudioRows#AUX_SPEAKER_SCOPE} value: labels identify the same speaker across the whole video. */
  public static final String SCOPE_FILE = "file";

  /** {@link AudioRows#AUX_SPEAKER_SCOPE} value: labels are only meaningful within one segment. */
  public static final String SCOPE_SEGMENT = "segment";

  private SpeakerLabels() {
  }

  /** The {@link AudioRows#AUX_SPEAKER_SCOPE} value of {@code transcriber}'s labels. */
  public static String scopeOf(SegmentTranscriber transcriber) {
    return transcriber.fileScopedSpeakers() ? SCOPE_FILE : SCOPE_SEGMENT;
  }

  /** One line of transcript chunk text: {@code [<speaker>] <utterance>}. */
  public static String line(String speaker, String utterance) {
    return "[" + speaker + "] " + utterance;
  }
}
