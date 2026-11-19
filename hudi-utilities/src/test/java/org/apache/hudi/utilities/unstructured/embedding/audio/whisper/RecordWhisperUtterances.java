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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Records the utterance fixture of the transcript packer tests ({@code audio/transcript/selfish-giant-whisper-base-en.txt}):
 * whisper base.en over the first {@link #SECONDS} s of the public-domain clip of {@link WhisperTestClips}, cut into
 * {@link #SEGMENT_SECONDS} s segments. Gated twice: the model cache ({@link WhisperTestClips#CACHE_DIR_PROPERTY}) and the output
 * file ({@link #OUT_PROPERTY}); CI sets neither, and the committed fixture is never rewritten by a test run.
 */
@EnabledIfSystemProperty(named = RecordWhisperUtterances.OUT_PROPERTY, matches = ".+")
public class RecordWhisperUtterances {

  static final String OUT_PROPERTY = "hudi.test.whisper.record.out";
  static final int SECONDS = 180;
  static final int SEGMENT_SECONDS = 30;

  @Test
  public void record() throws IOException {
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), "base.en");
    props.setProperty(WhisperConfig.MODEL_DIR.key(), WhisperTestClips.cacheDir().toString());
    short[] pcm = WhisperTestClips.clipPcm(SECONDS);
    assertEquals(SECONDS * WhisperConfig.WHISPER_SAMPLE_RATE, pcm.length);
    int perSegment = SEGMENT_SECONDS * WhisperConfig.WHISPER_SAMPLE_RATE;
    Path out = Paths.get(System.getProperty(OUT_PROPERTY));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber();
         Writer writer = Files.newBufferedWriter(out, StandardCharsets.UTF_8)) {
      transcriber.validate(props);
      writer.write("# whisper utterances recorded with " + transcriber.id() + ", the first " + SECONDS
          + " s of " + WhisperTestClips.CLIP_FILE + "\n");
      writer.write("# (LibriVox, public domain, https://archive.org/download/hpot_librivox/hpot_03_selfish_giant.mp3, sha-256 "
          + "b12913bc2655b02bc8649766c506cc862e790d613afbc5dd6cceec0f5c70540f) in " + SEGMENT_SECONDS + " s segments\n");
      writer.write("# columns: segment index, start ms, end ms, text (video timeline; tab separated)\n");
      for (int s = 0; s * perSegment < pcm.length; s++) {
        short[] samples = Arrays.copyOfRange(pcm, s * perSegment, Math.min(pcm.length, (s + 1) * perSegment));
        WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(s, (long) s * SEGMENT_SECONDS * 1000, samples);
        try (ClosableIterator<TranscriptSegment> it = transcriber.transcribe(segment, props)) {
          while (it.hasNext()) {
            TranscriptSegment u = it.next();
            writer.write(s + "\t" + u.getStartMs() + "\t" + u.getEndMs() + "\t" + u.getText().replace('\t', ' ') + "\n");
          }
        }
      }
    }
  }
}
