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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every whisper context (a native model and its buffers) the pool opens is closed once the last transcriber of its model closes,
 * whatever the transcriptions did. The engines are test doubles: no native library is loaded.
 */
class TestWhisperContextPoolHandleRelease {

  /** Engines that record their close; each may be told to fail its transcription or its close. */
  private static final class Engines implements WhisperEngine.Factory {
    final List<Engine> opened = new ArrayList<>();
    volatile RuntimeException transcribeFailure;
    volatile boolean closeFails;
    volatile boolean closeErrors;

    @Override
    public synchronized WhisperEngine open(WhisperConfig config) {
      Engine engine = new Engine(opened.size() + 1);
      opened.add(engine);
      return engine;
    }

    synchronized List<String> unclosed() {
      List<String> open = new ArrayList<>();
      for (Engine engine : opened) {
        if (!engine.closed) {
          open.add("whisper context #" + engine.number);
        }
      }
      return open;
    }

    private final class Engine implements WhisperEngine {
      private final int number;
      private boolean closed;

      private Engine(int number) {
        this.number = number;
      }

      @Override
      public List<RawUtterance> transcribe(WhisperConfig config, float[] pcm, int samples) {
        if (transcribeFailure != null) {
          throw transcribeFailure;
        }
        return Collections.singletonList(new RawUtterance(0, 100, "word"));
      }

      @Override
      public void close() {
        closed = true;
        if (closeErrors) {
          throw new LinkageError("test: whisper context #" + number + " failed to free");
        }
        if (closeFails) {
          throw new IllegalStateException("test: whisper context #" + number + " failed to free");
        }
      }
    }
  }

  private final Engines engines = new Engines();
  private final WhisperContextPool pool = new WhisperContextPool(engines);

  @Test
  void transcriberCloseFreesTheContextItUsed() throws IOException {
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      for (int i = 0; i < 3; i++) {
        drain(transcriber.transcribe(WhisperTestSegments.pcm(i, i * 2_000L, WhisperTestSegments.tone(1)), props()));
      }
    }
    assertEquals(1, engines.opened.size());
    assertEquals(0, pool.open());
    assertEquals(Collections.emptyList(), engines.unclosed(), "unclosed contexts after the transcriber closed");
  }

  @Test
  void failedTranscriptionReturnsTheContextAndCloseFreesIt() {
    RuntimeException failure = new IllegalStateException("test: full() failed");
    engines.transcribeFailure = failure;
    try (WhisperSegmentTranscriber transcriber = new WhisperSegmentTranscriber(pool)) {
      WhisperTestSegments.TestSegment segment = WhisperTestSegments.pcm(0, 0, WhisperTestSegments.tone(1));
      TypedProperties props = props();
      assertEquals(failure, assertThrows(IllegalStateException.class, () -> transcriber.transcribe(segment, props)));
      assertEquals(1, pool.idle(WhisperConfig.from(props())), "the context went back to the pool");
    }
    assertEquals(Collections.emptyList(), engines.unclosed(), "unclosed contexts after a failed transcription");
  }

  /** A context released after its last transcriber closed (a transcription in flight at close) is freed at once. */
  @Test
  void contextReleasedAfterTheLastUnregisterIsFreed() {
    WhisperConfig config = WhisperConfig.from(props());
    pool.register(config);
    WhisperEngine engine = pool.acquire(config);
    pool.unregister(config);
    pool.release(config, engine);
    assertEquals(0, pool.open());
    assertEquals(Collections.emptyList(), engines.unclosed());
  }

  /** Two idle contexts of one model: the first fails to free, the second must still be freed. */
  @Test
  void contextThatFailsToFreeDoesNotKeepTheOthersAlive() {
    WhisperConfig config = WhisperConfig.from(props());
    pool.register(config);
    WhisperEngine first = pool.acquire(config);
    WhisperEngine second = pool.acquire(config);
    pool.release(config, first);
    pool.release(config, second);
    assertEquals(2, pool.idle(config));
    engines.closeFails = true;
    try {
      pool.unregister(config);
    } catch (IllegalStateException expected) {
      // whether the free failure is thrown or logged is not under test; that every context is freed is
    }
    assertEquals(Collections.emptyList(), engines.unclosed(), "unclosed contexts after one failed to free");
  }

  /**
   * Three idle contexts of one model, each failing to free with an {@link Error}: every context is still freed, and the first
   * failure is rethrown with the others suppressed.
   */
  @Test
  void contextsThatFailToFreeWithAnErrorAreAllFreedAndTheFirstErrorIsRethrown() {
    WhisperConfig config = WhisperConfig.from(props());
    pool.register(config);
    List<WhisperEngine> acquired = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      acquired.add(pool.acquire(config));
    }
    for (WhisperEngine engine : acquired) {
      pool.release(config, engine);
    }
    assertEquals(3, pool.idle(config));
    engines.closeErrors = true;
    LinkageError error = assertThrows(LinkageError.class, () -> pool.unregister(config));
    assertEquals(Collections.emptyList(), engines.unclosed(), "unclosed contexts after an Error freeing one");
    assertEquals(2, error.getSuppressed().length, "the other two failures are suppressed on the first");
    for (Throwable suppressed : error.getSuppressed()) {
      assertTrue(suppressed instanceof LinkageError, String.valueOf(suppressed));
    }
  }

  private static TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    return props;
  }

  private static void drain(ClosableIterator<TranscriptSegment> it) {
    try {
      it.forEachRemaining(segment -> { });
    } finally {
      it.close();
    }
  }
}
