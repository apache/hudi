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
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Keys, defaults, validation and the engine identity of the whisper transcriber. */
class TestWhisperConfig {

  private static TypedProperties props(String... keyValues) {
    TypedProperties props = new TypedProperties();
    for (int i = 0; i < keyValues.length; i += 2) {
      props.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return props;
  }

  private static void assertRefusedNaming(String key, TypedProperties props) {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> WhisperConfig.from(props));
    assertTrue(e.getMessage().contains(key), e.getMessage());
  }

  @Test
  void defaultsResolveToBaseEn() {
    WhisperConfig config = WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/models"));
    assertEquals("base.en", config.modelName());
    assertEquals(Paths.get("/models", "ggml-base.en.bin"), config.modelPath());
    assertEquals("en", config.languageCode());
    assertEquals("transcribe", config.taskName());
    assertFalse(config.translate());
    assertEquals(1, config.threadCount());
    assertEquals("whisper-cpp-1.7.1-base.en/en/transcribe", config.id());
  }

  @Test
  void keysLiveUnderTheTranscriberPrefix() {
    assertEquals("hoodie.unstructured.embedding.audio.transcriber.whisper.", WhisperConfig.PREFIX);
    assertEquals(WhisperConfig.PREFIX + "model", WhisperConfig.MODEL.key());
    assertEquals(WhisperConfig.PREFIX + "model.dir", WhisperConfig.MODEL_DIR.key());
    assertEquals(WhisperConfig.PREFIX + "language", WhisperConfig.LANGUAGE.key());
    assertEquals(WhisperConfig.PREFIX + "task", WhisperConfig.TASK.key());
    assertEquals(WhisperConfig.PREFIX + "threads", WhisperConfig.THREADS.key());
    assertTrue(WhisperConfig.PREFIX.startsWith(AudioConfigKeys.TRANSCRIBER_CLASS.key().replace("class", "")));
  }

  @Test
  void absolutePathModelNamedByBasename() {
    WhisperConfig config = WhisperConfig.from(props(WhisperConfig.MODEL.key(), "/m/ggml-small.en.bin"));
    assertEquals("small.en", config.modelName());
    assertEquals(Paths.get("/m/ggml-small.en.bin"), config.modelPath());
    assertEquals("whisper-cpp-1.7.1-small.en/en/transcribe", config.id());
    assertEquals("custom", WhisperConfig.from(props(WhisperConfig.MODEL.key(), "/m/custom.bin")).modelName());
  }

  @Test
  void namedModelRequiresModelDir() {
    assertRefusedNaming(WhisperConfig.MODEL_DIR.key(), props());
    assertRefusedNaming(WhisperConfig.MODEL_DIR.key(), props(WhisperConfig.MODEL.key(), "small.en"));
  }

  @Test
  void badModelSyntaxRefused() {
    assertRefusedNaming(WhisperConfig.MODEL.key(), props(WhisperConfig.MODEL.key(), "relative/ggml-base.en.bin"));
    assertRefusedNaming(WhisperConfig.MODEL.key(), props(WhisperConfig.MODEL.key(), "/abs/model.gguf"));
    assertRefusedNaming(WhisperConfig.MODEL.key(), props(WhisperConfig.MODEL.key(), "base en", WhisperConfig.MODEL_DIR.key(), "/m"));
    assertRefusedNaming(WhisperConfig.MODEL.key(), props(WhisperConfig.MODEL.key(), "/abs/ggml-.bin"));
  }

  @Test
  void englishOnlyModelRequiresEnglishTranscribe() {
    assertRefusedNaming(WhisperConfig.LANGUAGE.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.LANGUAGE.key(), "de"));
    assertRefusedNaming(WhisperConfig.TASK.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.TASK.key(), "translate"));
    assertRefusedNaming(WhisperConfig.LANGUAGE.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.LANGUAGE.key(), "auto"));
  }

  @Test
  void multilingualModelTakesLanguageAndTask() {
    WhisperConfig config = WhisperConfig.from(props(WhisperConfig.MODEL.key(), "small", WhisperConfig.MODEL_DIR.key(), "/m",
        WhisperConfig.LANGUAGE.key(), "auto", WhisperConfig.TASK.key(), "translate"));
    assertTrue(config.translate());
    assertEquals("whisper-cpp-1.7.1-small/auto/translate", config.id());
    assertRefusedNaming(WhisperConfig.LANGUAGE.key(), props(WhisperConfig.MODEL.key(), "small", WhisperConfig.MODEL_DIR.key(), "/m",
        WhisperConfig.LANGUAGE.key(), "EN"));
    assertRefusedNaming(WhisperConfig.TASK.key(), props(WhisperConfig.MODEL.key(), "small", WhisperConfig.MODEL_DIR.key(), "/m",
        WhisperConfig.TASK.key(), "summarise"));
  }

  @Test
  void languageMustBeOneWhisperKnows() {
    for (String known : new String[] {"de", "haw", "yue", "he", "jw"}) {
      assertEquals(known, WhisperConfig.from(props(WhisperConfig.MODEL.key(), "small", WhisperConfig.MODEL_DIR.key(), "/m",
          WhisperConfig.LANGUAGE.key(), known)).languageCode());
    }
    for (String unknown : new String[] {"xx", "eng", "iw"}) {
      assertRefusedNaming(WhisperConfig.LANGUAGE.key(), props(WhisperConfig.MODEL.key(), "small", WhisperConfig.MODEL_DIR.key(), "/m",
          WhisperConfig.LANGUAGE.key(), unknown));
    }
  }

  @Test
  void segmentMaxBytesHasAnUpperBound() {
    assertRefusedNaming(AudioConfigKeys.SEGMENT_MAX_BYTES.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", AudioConfigKeys.SEGMENT_MAX_BYTES.key(),
        String.valueOf(5L * 1024 * 1024 * 1024)));
    assertEquals(WhisperConfig.MAX_SEGMENT_BYTES, WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m",
        AudioConfigKeys.SEGMENT_MAX_BYTES.key(), String.valueOf(WhisperConfig.MAX_SEGMENT_BYTES))).segmentMaxBytes());
  }

  @Test
  void threadsMustBePositive() {
    assertEquals(4, WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.THREADS.key(), "4")).threadCount());
    assertRefusedNaming(WhisperConfig.THREADS.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.THREADS.key(), "0"));
    assertRefusedNaming(WhisperConfig.THREADS.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.THREADS.key(), "many"));
  }

  @Test
  void sampleRateMustBe16000() {
    assertRefusedNaming(AudioConfigKeys.SAMPLE_RATE.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", AudioConfigKeys.SAMPLE_RATE.key(), "8000"));
    assertEquals("base.en", WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m", AudioConfigKeys.SAMPLE_RATE.key(), "16000")).modelName());
  }

  @Test
  void segmentMaxBytesDefaultsTo1MiB() {
    assertEquals(1024L * 1024L, WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m")).segmentMaxBytes());
    assertEquals(4096L, WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m", AudioConfigKeys.SEGMENT_MAX_BYTES.key(), "4096"))
        .segmentMaxBytes());
    assertRefusedNaming(AudioConfigKeys.SEGMENT_MAX_BYTES.key(), props(WhisperConfig.MODEL_DIR.key(), "/m", AudioConfigKeys.SEGMENT_MAX_BYTES.key(), "0"));
  }

  @Test
  void equalityFollowsTheResolvedSettings() {
    assertEquals(WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m")), WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m")));
    assertNotEquals(WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m")),
        WhisperConfig.from(props(WhisperConfig.MODEL_DIR.key(), "/m", WhisperConfig.THREADS.key(), "2")));
  }
}
