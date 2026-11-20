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

import java.util.List;

/**
 * One loaded speech-to-text model context: the seam between {@link WhisperSegmentTranscriber} and the native library, so the
 * transcriber's mapping, bounds and lifecycle are testable without a model file. Not thread-safe: {@link WhisperContextPool}
 * lends an engine to one call at a time.
 */
interface WhisperEngine extends AutoCloseable {

  /**
   * Transcribes {@code samples} mono 16 kHz samples in {@code [-1, 1]}.
   *
   * @return the utterances, timed in centiseconds from the first sample
   * @throws org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException when the engine's run fails (whisper
   *     cannot tell its own allocation or compute failure from audio it cannot take)
   */
  List<RawUtterance> transcribe(WhisperConfig config, float[] pcm, int samples);

  /** Frees the context; idempotent. */
  @Override
  void close();

  /** Opens an engine on the model of a configuration. */
  interface Factory {

    /**
     * @throws IllegalArgumentException when the model or the native library cannot be loaded (a configuration error)
     */
    WhisperEngine open(WhisperConfig config);
  }

  /** One utterance as the engine timed it. */
  final class RawUtterance {
    private final long startCentis;
    private final long endCentis;
    private final String text;

    RawUtterance(long startCentis, long endCentis, String text) {
      this.startCentis = startCentis;
      this.endCentis = endCentis;
      this.text = text;
    }

    long startCentis() {
      return startCentis;
    }

    long endCentis() {
      return endCentis;
    }

    String text() {
      return text;
    }
  }
}
