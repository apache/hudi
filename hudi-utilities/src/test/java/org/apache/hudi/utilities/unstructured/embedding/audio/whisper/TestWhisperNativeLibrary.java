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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The whisper-jni native library on the platform the tests run on (linux-arm64 locally, linux-x86_64 in CI), and how model
 * loading failures are classified. Needs no model file.
 */
class TestWhisperNativeLibrary {

  private static WhisperConfig modelAt(Path file) {
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), file.toAbsolutePath().toString());
    return WhisperConfig.from(props);
  }

  @Test
  void nativeLibraryLoadsOnThisPlatform() {
    assertNotNull(NativeWhisperEngine.library());
    assertTrue(NativeWhisperEngine.library().getSystemInfo().contains("BLAS"));
  }

  @Test
  void missingModelRefusedAsConfiguration(@TempDir Path dir) {
    WhisperConfig config = modelAt(dir.resolve("ggml-absent.bin"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> NativeWhisperEngine.open(config));
    assertTrue(e.getMessage().startsWith(WhisperConfig.MODEL.key()), e.getMessage());
  }

  @Test
  void badMagicModelRefusedAsConfiguration(@TempDir Path dir) throws IOException {
    Path model = Files.write(dir.resolve("ggml-broken.bin"), new byte[] {1, 2, 3, 4, 5});
    int live = NativeWhisperEngine.liveContexts();
    WhisperConfig config = modelAt(model);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> NativeWhisperEngine.open(config));
    assertTrue(e.getMessage().contains("not a ggml whisper model"), e.getMessage());
    assertEquals(live, NativeWhisperEngine.liveContexts());
  }

  @Test
  void modelPathDirectoryRefused(@TempDir Path dir) throws IOException {
    Path model = Files.createDirectory(dir.resolve("ggml-folder.bin"));
    WhisperConfig config = modelAt(model);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> NativeWhisperEngine.open(config));
    assertTrue(e.getMessage().contains("no readable model file"), e.getMessage());
  }

  @Test
  void transcriberSurfacesModelErrorAsConfiguration(@TempDir Path dir) throws IOException {
    Path model = Files.write(dir.resolve("ggml-broken.bin"), new byte[] {9, 9, 9, 9});
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL.key(), model.toString());
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber()) {
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(2));
      assertThrows(IllegalArgumentException.class, () -> transcriber.transcribe(segment, props));
    }
    assertEquals(0, WhisperContextPool.shared().open());
  }
}
