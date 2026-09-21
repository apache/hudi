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

package org.apache.hudi.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestAutoCloseableUtils {

  static Stream<Exception> closeFailures() {
    return Stream.of(new IOException("IO failure"), new IllegalStateException("runtime failure"),
        new Exception("checked failure"));
  }

  @Test
  void testNullAndSuccessfulClose() {
    assertDoesNotThrow(() -> AutoCloseableUtils.closeWithSuppressed(null, null));
    assertDoesNotThrow(() -> AutoCloseableUtils.closeQuietlyWithSuppressed(null, null));
    AtomicBoolean closed = new AtomicBoolean();
    assertDoesNotThrow(() -> AutoCloseableUtils.closeWithSuppressed(() -> closed.set(true), null));
    assertTrue(closed.get());
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  void testPreservesOriginalFailure(Exception closeFailure) {
    IOException failure = new IOException("write failed");
    assertDoesNotThrow(() -> AutoCloseableUtils.closeWithSuppressed(() -> {
      throw closeFailure;
    }, failure));
    assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  void testPropagatesCloseFailureWithoutOriginalFailure(Exception closeFailure) {
    Exception actual = assertThrows(Exception.class, () -> AutoCloseableUtils.closeWithSuppressed(() -> {
      throw closeFailure;
    }, null));
    if (closeFailure instanceof IOException || closeFailure instanceof RuntimeException) {
      assertSame(closeFailure, actual);
    } else {
      assertTrue(actual instanceof IOException);
      assertSame(closeFailure, actual.getCause());
    }
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  void testQuietCloseHandlesCheckedAndUncheckedFailures(Exception closeFailure) {
    AutoCloseable closeable = () -> {
      throw closeFailure;
    };
    assertDoesNotThrow(() -> AutoCloseableUtils.closeQuietlyWithSuppressed(closeable, null));
    IOException failure = new IOException("write failed");
    assertDoesNotThrow(() -> AutoCloseableUtils.closeQuietlyWithSuppressed(closeable, failure));
    assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  void testDoesNotSuppressFailureOnItself(Exception failure) {
    assertDoesNotThrow(() -> AutoCloseableUtils.closeWithSuppressed(() -> {
      throw failure;
    }, failure));
    assertArrayEquals(new Throwable[0], failure.getSuppressed());
  }
}
