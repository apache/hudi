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
 * The shape of the speech rows data prep writes into the embedding table for a video: their aux keys and
 * the audio logic line. Constants only.
 *
 * <p>A speech row is an ordinary text row: {@code modality = text}, {@code chunk_id} dense from 0 per
 * (source_path_sha, text) through the configured {@code ChunkIdScheme}, like a PDF chunk. The record key
 * (source_path_sha, modality, chunk_id) keeps it apart from the video's frame row of the same chunk_id
 * ({@code modality = image}); no id range is reserved.
 */
public final class AudioRows {

  /** The logic version of the speech rows' layout, the {@code speech} part of the audio logic. */
  public static final String SPEECH_LOGIC_VERSION = "prep-v1";

  /**
   * Aux key, on speech rows: the audio logic version, see {@link #AUDIO_LOGIC_FORMAT}. Legacy raw-chunk hint; stored rows carry
   * the composed prep_logic_version instead.
   */
  public static final String AUX_AUDIO_LOGIC = "audio_logic";

  /**
   * Format of the audio logic version: segment source logic, transcriber id, chunker logic, tokenizer id, and where speech is
   * written ({@code speech=prep-v1}: by data prep with the video's frames). It is part of the table's preprocessing style.
   * A change is not detected; use a new table for a different preprocessing style. It does not join a model column's identity,
   * because the transcript text it produces is persisted in
   * {@code prepared_text_chunk}.
   */
  public static final String AUDIO_LOGIC_FORMAT = "audio=%s;asr=%s;pack=%s;tokenizer=%s;speech=" + SPEECH_LOGIC_VERSION;

  /** Aux key, on speech rows: start of the chunk's first utterance, ms on the video's timeline (from the container start). */
  public static final String AUX_START_MS = "start_ms";

  /** Aux key, on speech rows: the latest end of the chunk's utterances, ms on the video's timeline. */
  public static final String AUX_END_MS = "end_ms";

  /** Aux key, on speech rows: distinct speaker labels of the chunk, comma-separated, in order of first appearance. */
  public static final String AUX_SPEAKERS = "speakers";

  /** Aux key, on speech rows: whether speaker labels are per segment or per file ({@code segment} | {@code file}). */
  public static final String AUX_SPEAKER_SCOPE = "speaker_scope";

  /** Aux key, on speech rows: index of the first audio segment the chunk draws from. */
  public static final String AUX_FIRST_SEGMENT = "first_segment";

  /** Aux key, on speech rows: index of the last audio segment the chunk draws from. */
  public static final String AUX_LAST_SEGMENT = "last_segment";

  private AudioRows() {
  }
}
