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

import org.bytedeco.javacv.FFmpegFrameGrabber;
import org.bytedeco.javacv.Frame;
import org.bytedeco.javacv.FrameGrabber;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Inputs of the model tests, from a cache directory named by the system property {@link #CACHE_DIR_PROPERTY} and filled by the
 * fetch-inputs.sh (kept outside the repository, never committed): the ggml models and a public-domain LibriVox reading,
 * "The Selfish Giant" (Oscar Wilde) read by Joy Chan, https://archive.org/download/hpot_librivox/hpot_03_selfish_giant.mp3
 * (sha-256 b12913bc2655b02bc8649766c506cc862e790d613afbc5dd6cceec0f5c70540f). The tests read its first 30 s only.
 */
final class WhisperTestClips {

  /** System property naming the cache directory; the model tests are skipped when it is not set. */
  static final String CACHE_DIR_PROPERTY = "hudi.test.whisper.cache.dir";

  static final String CLIP_FILE = "hpot_03_selfish_giant.mp3";

  /** Seconds of the clip the tests transcribe: the reader's preamble, the title and the story's first paragraph. */
  static final int CLIP_SECONDS = 30;

  private WhisperTestClips() {
  }

  static Path cacheDir() {
    return Paths.get(System.getProperty(CACHE_DIR_PROPERTY));
  }

  static Path model(String name) {
    return cacheDir().resolve("ggml-" + name + ".bin");
  }

  static boolean hasModel(String name) {
    return System.getProperty(CACHE_DIR_PROPERTY) != null && Files.isRegularFile(model(name));
  }

  /** The first {@code seconds} of the clip, decoded and resampled to 16 kHz mono 16-bit by FFmpeg. */
  static short[] clipPcm(int seconds) throws IOException {
    Path clip = cacheDir().resolve(CLIP_FILE);
    assertTrue(Files.isRegularFile(clip), "missing " + clip + " (run fetch-inputs.sh)");
    int wanted = seconds * WhisperConfig.WHISPER_SAMPLE_RATE;
    short[] pcm = new short[wanted];
    int got = 0;
    try (FFmpegFrameGrabber grabber = new FFmpegFrameGrabber(clip.toFile())) {
      grabber.setSampleRate(WhisperConfig.WHISPER_SAMPLE_RATE);
      grabber.setAudioChannels(1);
      grabber.setSampleMode(FrameGrabber.SampleMode.SHORT);
      grabber.start();
      Frame frame;
      while (got < wanted && (frame = grabber.grabSamples()) != null) {
        ShortBuffer samples = (ShortBuffer) frame.samples[0];
        while (samples.hasRemaining() && got < wanted) {
          pcm[got++] = samples.get();
        }
      }
      grabber.stop();
    }
    return got == wanted ? pcm : Arrays.copyOf(pcm, got);
  }

  /** The reference text of the clip's first 30 s (test resource, '#' lines are provenance). */
  static String reference() throws IOException {
    StringBuilder text = new StringBuilder();
    try (InputStream in = WhisperTestClips.class.getResourceAsStream("selfish-giant-opening.txt");
         BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (!line.startsWith("#")) {
          text.append(line).append('\n');
        }
      }
    }
    return text.toString();
  }
}
