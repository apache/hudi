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

import java.util.List;

/**
 * One packed transcript chunk: the {@code prepared_text_chunk} of one transcript row of the embedding table;
 * built by {@link TranscriptPacker}.
 *
 * <p>The row it becomes: {@code source_path_sha}, {@code modality = text}, {@code chunk_id =
 * ChunkIdScheme.chunkId(ordinal())} (dense from 0 per video, like a PDF chunk), {@code prepared_text_chunk = text()},
 * {@code chunk_token_count = tokenCount()}, and aux {@link AudioRows#AUX_START_MS}, {@link AudioRows#AUX_END_MS},
 * {@link AudioRows#AUX_SPEAKERS}, {@link AudioRows#AUX_SPEAKER_SCOPE}, {@link AudioRows#AUX_FIRST_SEGMENT}, {@link AudioRows#AUX_LAST_SEGMENT},
 * {@link AudioRows#AUX_AUDIO_LOGIC}.
 */
public interface TranscriptChunk {

  /** Dense per-video position of this chunk in time order, from 0; one video is packed whole. */
  long ordinal();

  /**
   * Normalised text with each speaker turn as one line {@code [<speaker>] <utterance>}, so the embedded text carries the
   * speaker and the content; it starts with the speaker label of its first
   * utterance even when it starts inside a turn.
   */
  String text();

  /** Tokens of {@link #text()} under the table's tokenizer; at most the table's chunk max tokens. */
  int tokenCount();

  /**
   * Start of the first utterance any of whose text, overlap included, is in this chunk, milliseconds on the video's timeline
   * (a piece of a split utterance carries its utterance's start: no per-word times).
   */
  long startMs();

  /** The latest end of the utterances in this chunk (ends need not be in order), milliseconds on the video's timeline. */
  long endMs();

  /** Distinct speaker labels in this chunk, in order of first appearance. */
  List<String> speakers();

  /** Index of the first {@link AudioSegment} this chunk draws text from. */
  long firstSegment();

  /** Index of the last {@link AudioSegment} this chunk draws text from. */
  long lastSegment();
}
