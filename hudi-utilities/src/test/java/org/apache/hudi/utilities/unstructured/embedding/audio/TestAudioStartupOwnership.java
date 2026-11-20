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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Startup owns every constructed audio component even when a later component cannot be constructed. */
class TestAudioStartupOwnership {
  @BeforeEach
  void reset() {
    Source.CREATED.set(0);
    Source.CLOSED.set(0);
    Source.closeFailure = null;
    Transcriber.CLOSED.set(0);
    Chunker.CLOSED.set(0);
  }

  @Test
  void validationReleasesSourceWhenTranscriberConstructionFails() {
    AudioConfig config = AudioConfig.from(props(FailingTranscriber.class));
    assertThrows(RuntimeException.class, config::validate);
    assertEquals(1, Source.CREATED.get());
    assertEquals(1, Source.CLOSED.get(), "the acquired source is closed after partial construction");
  }

  @Test
  void logicLineReleasesSourceWhenTranscriberConstructionFails() {
    AudioConfig config = AudioConfig.from(props(FailingTranscriber.class));
    assertThrows(RuntimeException.class, config::logicLine);
    assertEquals(1, Source.CREATED.get());
    assertEquals(1, Source.CLOSED.get(), "identity preparation also owns partially constructed components");
  }

  @Test
  void successfulValidationAndIdentityPreparationCloseEveryComponent() {
    AudioConfig config = AudioConfig.from(props(Transcriber.class));
    config.validate();
    config.logicLine();
    assertEquals(2, Source.CREATED.get());
    assertEquals(2, Source.CLOSED.get());
    assertEquals(2, Transcriber.CLOSED.get());
    assertEquals(2, Chunker.CLOSED.get());
  }

  @Test
  void wrongRoleClassIsRejectedBeforeConstruction() {
    AudioConfig config = AudioConfig.from(props(Source.class));
    assertThrows(IllegalArgumentException.class, config::validate);
    assertEquals(1, Source.CREATED.get(), "the wrong-role class must not be constructed");
    assertEquals(1, Source.CLOSED.get());
  }

  @Test
  void cleanupContinuesAfterNativeLinkageFailure() {
    Source.closeFailure = new UnsatisfiedLinkError("native teardown unavailable");
    AudioConfig config = AudioConfig.from(props(Transcriber.class));
    assertDoesNotThrow(config::validate);
    assertEquals(1, Source.CLOSED.get());
    assertEquals(1, Transcriber.CLOSED.get());
    assertEquals(1, Chunker.CLOSED.get());
  }

  @Test
  void constructionFailureSurvivesNativeCleanupFailure() {
    Source.closeFailure = new UnsatisfiedLinkError("native teardown unavailable");
    AudioConfig config = AudioConfig.from(props(FailingTranscriber.class));
    assertThrows(IllegalArgumentException.class, config::validate);
    assertEquals(1, Source.CLOSED.get());
  }

  private static TypedProperties props(Class<?> transcriber) {
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), Source.class.getName());
    props.setProperty(AudioConfigKeys.TRANSCRIBER_CLASS.key(), transcriber.getName());
    props.setProperty(AudioConfigKeys.CHUNKER_CLASS.key(), Chunker.class.getName());
    props.setProperty(EmbeddingTableConfig.TOKENIZER_TYPE.key(), TokenizerFactory.WORD_COUNT);
    return props;
  }

  public static class Source extends StubAudioSegmentSource implements AutoCloseable {
    private static final AtomicInteger CREATED = new AtomicInteger();
    private static final AtomicInteger CLOSED = new AtomicInteger();
    private static LinkageError closeFailure;

    public Source() {
      CREATED.incrementAndGet();
    }

    @Override
    public void close() {
      CLOSED.incrementAndGet();
      if (closeFailure != null) {
        throw closeFailure;
      }
    }
  }

  public static class Transcriber extends StubSegmentTranscriber implements AutoCloseable {
    private static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }

  public static class FailingTranscriber extends StubSegmentTranscriber {
    public FailingTranscriber() {
      throw new IllegalStateException("transcriber construction failed");
    }
  }

  public static class Chunker extends StubTranscriptChunker implements AutoCloseable {
    private static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public void close() {
      CLOSED.incrementAndGet();
    }
  }
}
