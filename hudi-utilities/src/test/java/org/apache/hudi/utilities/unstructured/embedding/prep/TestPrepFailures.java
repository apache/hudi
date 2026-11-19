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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.source.PreprocessorConfigurationException;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link PrepFailures}: which failures of a file fail the prep round, found through cause and suppressed chains. */
public class TestPrepFailures {

  private static final SourceReadException SOURCE_READ = new SourceReadException("read failed", new IOException("reset"));
  private static final PreprocessorConfigurationException CONFIGURATION =
      new PreprocessorConfigurationException("model missing", null);
  private static final PrepPlatformException PLATFORM = new PrepPlatformException("scratch disk full", null);
  private static final TokenizerException RETRYABLE =
      new TokenizerException(TokenizerException.Kind.RETRYABLE, "tokenizer service timed out", null);

  static Stream<Arguments> roundFailures() {
    return Stream.of(
        // name, round failure, the failure that holds it
        Arguments.of("source read itself", SOURCE_READ, SOURCE_READ),
        Arguments.of("configuration as a cause", CONFIGURATION, new IllegalStateException("decoder", CONFIGURATION)),
        Arguments.of("platform two causes deep", PLATFORM,
            new IllegalStateException("decoder", new IOException("page", PLATFORM))),
        Arguments.of("source read as a suppressed", SOURCE_READ, suppressing(new IllegalStateException("decoder"), SOURCE_READ)),
        Arguments.of("platform suppressed under a cause", PLATFORM,
            new IllegalStateException("decoder", suppressing(new IOException("close"), PLATFORM))),
        Arguments.of("retryable tokenizer failure as a cause", RETRYABLE, new IllegalStateException("pack", RETRYABLE)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("roundFailures")
  void findsTheRoundFailure(String name, RuntimeException expected, Throwable failure) {
    assertSame(expected, PrepFailures.roundFailure(failure), name);
  }

  static Stream<Arguments> badInput() {
    return Stream.of(
        Arguments.of("plain unchecked failure", new IllegalStateException("bad page")),
        Arguments.of("storage failure", new IOException("corrupt")),
        Arguments.of("permanent tokenizer failure", new IllegalStateException("pack",
            new TokenizerException(TokenizerException.Kind.PERMANENT, "uncountable text", null))),
        Arguments.of("default tokenizer failure", new TokenizerException("uncountable text")),
        Arguments.of("cause cycle", cycle()));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("badInput")
  void badInputOfAFileFailsNoRound(String name, Throwable failure) {
    assertNull(PrepFailures.roundFailure(failure), name);
    assertDoesNotThrow(() -> PrepFailures.rethrowRoundFailure(failure), name);
  }

  /**
   * A JVM failure a decoder wrapped or added as suppressed (an OutOfMemoryError, an InternalError, a LinkageError) says nothing
   * about the file: it fails the round as a platform failure carrying what wrapped it, never the file's bad input. A
   * StackOverflowError (the file's nesting causes it, on every attempt) stays the file's bad input.
   */
  @Test
  void jvmFailureUnderADecoderFailsTheRoundNotTheFile() {
    RuntimeException suppressed = new RuntimeException("frame failed");
    suppressed.addSuppressed(new UnsatisfiedLinkError("no jniavcodec"));
    for (RuntimeException failure : new RuntimeException[] {
        new PreprocessingException("video v cannot be read", new IOException("decode", new OutOfMemoryError("Java heap space"))),
        new IllegalStateException("decode failed", new InternalError("a JVM fault")),
        suppressed}) {
      PrepPlatformException round = assertThrows(PrepPlatformException.class, () -> PrepFailures.rethrowRoundFailure(failure),
          failure.toString());
      assertSame(failure, round.getCause());
    }
    // control: a decoder's own failure with no JVM failure under it is the file's bad input
    assertDoesNotThrow(() -> PrepFailures.rethrowRoundFailure(new PreprocessingException("corrupt", new IOException("bad data"))));
    assertDoesNotThrow(() -> PrepFailures.rethrowRoundFailure(new PreprocessingException("too deep", new StackOverflowError())));
  }

  /**
   * A source read failure with a JVM failure under it is thrown unchecked, as a platform failure carrying it, before a decoder
   * could take the read's IOException for an end of data; a plain read failure stays a source read failure, a missing source the
   * file's own.
   */
  @Test
  void sourceReadFailureWithAJvmFailureUnderItIsThrownUnchecked() {
    IOException underHeap = new IOException("read failed", new OutOfMemoryError("Java heap space"));
    PrepPlatformException round = assertThrows(PrepPlatformException.class, () -> PreprocessingIterator.classified(underHeap));
    assertSame(underHeap, round.getCause());
    IOException plain = new IOException("connection reset");
    assertSame(plain, assertThrows(SourceReadException.class, () -> PreprocessingIterator.classified(plain)).getCause());
    FileNotFoundException missing = new FileNotFoundException("gone");
    assertSame(missing, PreprocessingIterator.classified(missing));
  }

  @Test
  void rethrowsTheRoundFailureItselfAsItIs() {
    assertSame(SOURCE_READ, assertThrows(SourceReadException.class, () -> PrepFailures.rethrowRoundFailure(SOURCE_READ)));
    assertSame(PLATFORM, assertThrows(PrepPlatformException.class, () -> PrepFailures.rethrowRoundFailure(PLATFORM)));
    assertSame(RETRYABLE, assertThrows(TokenizerException.class, () -> PrepFailures.rethrowRoundFailure(RETRYABLE)));
  }

  static Stream<Arguments> wrapped() {
    return Stream.of(
        // name, held round failure, the type rethrown around the failure that holds it
        Arguments.of("source read", SOURCE_READ, SourceReadException.class),
        Arguments.of("configuration", CONFIGURATION, PreprocessorConfigurationException.class),
        Arguments.of("platform", PLATFORM, PrepPlatformException.class),
        Arguments.of("retryable tokenizer failure", RETRYABLE, PrepPlatformException.class));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("wrapped")
  void rethrowsAHeldRoundFailureCarryingWhatHeldIt(String name, RuntimeException held, Class<? extends RuntimeException> type) {
    IllegalStateException failure = new IllegalStateException("decoder failed", held);
    RuntimeException thrown = assertThrows(RuntimeException.class, () -> PrepFailures.rethrowRoundFailure(failure), name);
    assertEquals(type, thrown.getClass(), name);
    assertSame(failure, thrown.getCause() instanceof IOException ? thrown.getCause().getCause() : thrown.getCause(), name);
    assertTrue(thrown.getMessage().contains("IllegalStateException: decoder failed"), thrown.getMessage());
    assertTrue(causes(thrown, held), "the held failure stays in the cause chain: " + name);
  }

  private static boolean causes(Throwable failure, Throwable expected) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause == expected) {
        return true;
      }
    }
    return false;
  }

  private static <T extends Throwable> T suppressing(T failure, Throwable suppressed) {
    failure.addSuppressed(suppressed);
    return failure;
  }

  /** Two failures, each the other's cause and suppressed: the walk must stop. */
  private static IllegalStateException cycle() {
    IllegalStateException first = new IllegalStateException("first");
    IllegalStateException second = new IllegalStateException("second", first);
    first.initCause(second);
    second.addSuppressed(first);
    return first;
  }
}
