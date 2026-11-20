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
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whisper transcriber on real ggml models over 30 s of a public-domain reading. Gated: runs only when
 * {@code -Dhudi.test.whisper.cache.dir} names a directory filled by fetch-inputs.sh (CI does not set it).
 */
@EnabledIfSystemProperty(named = WhisperTestClips.CACHE_DIR_PROPERTY, matches = ".+")
class TestWhisperTranscriberModel {

  private static final Logger LOG = LoggerFactory.getLogger(TestWhisperTranscriberModel.class);

  /** Sanity bound on the story-body word error rate of one 30 s segment (the benchmark measured 2.1% / 1.3% over 10 min). */
  private static final double MAX_WER = 0.10;

  private static Path renamedModel(Path dir) throws IOException {
    return Files.createSymbolicLink(dir.resolve("custom.bin"), WhisperTestClips.model("base.en").toAbsolutePath());
  }

  private static WhisperConfig modelAt(Path file, String language, String task) {
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), file.toAbsolutePath().toString());
    props.setProperty(WhisperConfig.LANGUAGE.key(), language);
    props.setProperty(WhisperConfig.TASK.key(), task);
    return WhisperConfig.from(props);
  }

  @Test
  void renamedEnglishModelRefusesAnotherLanguageAfterReuse(@TempDir Path dir) throws IOException {
    Path model = renamedModel(dir);
    WhisperConfig english = modelAt(model, "en", "transcribe");
    WhisperConfig german = modelAt(model, "de", "transcribe");
    float[] pcm = new float[32000];
    try (NativeWhisperEngine engine = NativeWhisperEngine.open(english)) {
      assertDoesNotThrow(() -> engine.transcribe(english, pcm, pcm.length));
      assertThrows(IllegalArgumentException.class, () -> engine.transcribe(german, pcm, pcm.length));
    }
  }

  @Test
  void renamedEnglishModelRefusesTranslationAfterReuse(@TempDir Path dir) throws IOException {
    Path model = renamedModel(dir);
    WhisperConfig english = modelAt(model, "en", "transcribe");
    WhisperConfig translation = modelAt(model, "en", "translate");
    float[] pcm = new float[32000];
    try (NativeWhisperEngine engine = NativeWhisperEngine.open(english)) {
      assertDoesNotThrow(() -> engine.transcribe(english, pcm, pcm.length));
      assertThrows(IllegalArgumentException.class, () -> engine.transcribe(translation, pcm, pcm.length));
    }
  }

  @Test
  void baseEnTranscribesThirtySeconds() throws IOException {
    transcribeAndCheck("base.en");
  }

  @Test
  void smallEnTranscribesThirtySeconds() throws IOException {
    Assumptions.assumeTrue(WhisperTestClips.hasModel("small.en"), "small.en not in the cache");
    transcribeAndCheck("small.en");
  }

  /**
   * Where the model exists, random PCM reaching the native engine either transcribes or is refused without an
   * {@link IllegalArgumentException}, which prep reads as a configuration error. A payload that ends early or declares an odd
   * byte count breaks the segment source's invariant and must fail as a {@link PrepPlatformException} (class P).
   */
  @Test
  void corruptInputIsNeverAnIllegalArgumentException() throws IOException {
    assertTrue(WhisperTestClips.hasModel("base.en"), "missing " + WhisperTestClips.model("base.en"));
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), "base.en");
    props.setProperty(WhisperConfig.MODEL_DIR.key(), WhisperTestClips.cacheDir().toString());
    byte[] noise = new byte[10 * 32_000];
    new Random(11).nextBytes(noise);
    List<WhisperTestSegments.TestSegment> corrupt = new ArrayList<>();
    corrupt.add(new WhisperTestSegments.TestSegment(0, 0, 10_000, 16000, 1, PcmPayload.PCM_S16LE, noise.length,
        () -> new ByteArrayInputStream(noise)));
    corrupt.add(new WhisperTestSegments.TestSegment(1, 10_000, 20_000, 16000, 1, PcmPayload.PCM_S16LE, noise.length,
        () -> new ByteArrayInputStream(noise, 0, noise.length / 3)));
    corrupt.add(new WhisperTestSegments.TestSegment(2, 20_000, 30_000, 16000, 1, PcmPayload.PCM_S16LE, noise.length - 1,
        () -> new ByteArrayInputStream(noise, 0, noise.length - 1)));
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber()) {
      transcriber.validate(props);
      for (WhisperTestSegments.TestSegment segment : corrupt) {
        try (ClosableIterator<TranscriptSegment> it = transcriber.transcribe(segment, props)) {
          while (it.hasNext()) {
            TranscriptSegment u = it.next();
            assertTrue(u.getStartMs() >= segment.startMs() && u.getStartMs() < segment.endMs(), u.toString());
          }
          LOG.info("corrupt segment {} transcribed without failure", segment.index());
        } catch (RuntimeException failure) {
          assertFalse(failure instanceof IllegalArgumentException, "segment " + segment.index() + ": " + failure);
          if (segment.index() == 0) {
            assertTrue(failure instanceof PrepPlatformException || failure instanceof IllegalStateException,
                "native noise segment: " + failure);
          } else {
            assertTrue(failure instanceof PrepPlatformException, "segment " + segment.index() + ": " + failure);
          }
          LOG.info("corrupt segment {} refused: {}", segment.index(), failure.toString());
        }
      }
    }
  }

  private static void transcribeAndCheck(String model) throws IOException {
    assertTrue(WhisperTestClips.hasModel(model), "missing " + WhisperTestClips.model(model));
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), model);
    props.setProperty(WhisperConfig.MODEL_DIR.key(), WhisperTestClips.cacheDir().toString());
    short[] pcm = WhisperTestClips.clipPcm(WhisperTestClips.CLIP_SECONDS);
    assertEquals(WhisperTestClips.CLIP_SECONDS * 16000, pcm.length);
    WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, pcm);

    int liveBefore = NativeWhisperEngine.liveContexts();
    List<TranscriptSegment> utterances = new ArrayList<>();
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber()) {
      transcriber.validate(props);
      assertEquals("whisper-cpp-1.7.1-" + model + "/en/transcribe", transcriber.id());
      try (ClosableIterator<TranscriptSegment> it = transcriber.transcribe(segment, props)) {
        it.forEachRemaining(utterances::add);
      }
      assertEquals(liveBefore + 1, NativeWhisperEngine.liveContexts());
    }
    assertEquals(liveBefore, NativeWhisperEngine.liveContexts());

    assertFalse(utterances.isEmpty());
    StringBuilder text = new StringBuilder();
    long previousStart = 0;
    for (TranscriptSegment u : utterances) {
      assertEquals("speaker-1", u.getSpeakerLabel());
      assertFalse(u.getText().trim().isEmpty());
      assertTrue(u.getStartMs() >= previousStart, "starts must not decrease: " + u);
      assertTrue(u.getStartMs() >= segment.startMs() && u.getEndMs() <= segment.endMs() && u.getStartMs() <= u.getEndMs(),
          "outside the segment span: " + u);
      previousStart = u.getStartMs();
      text.append(u.getText()).append(' ');
    }
    WhisperWer.Result wer = WhisperWer.body(WhisperTestClips.reference(), text.toString());
    LOG.info("{}: {} utterances, {}; text: {}", model, utterances.size(), wer, text);
    assertTrue(wer.rate() <= MAX_WER, model + " " + wer + " text: " + text);
    assertTrue(wer.referenceWords >= 20, wer.toString());
  }
}
