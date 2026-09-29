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

package org.apache.hudi.common.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestCloseableUtils {

  static Stream<Throwable> closeFailures() {
    return Stream.of(new IOException("IO failure"), new IllegalStateException("runtime failure"),
        new Exception("checked failure"), new AssertionError("error"));
  }

  @Test
  void testNullAndSuccessfulClose() {
    IOException failure = new IOException("write failed");
    assertDoesNotThrow(() -> CloseableUtils.closeSuppressing(null, failure));
    AtomicBoolean closed = new AtomicBoolean();
    CloseableUtils.closeSuppressing(() -> closed.set(true), failure);
    assertTrue(closed.get());
    assertArrayEquals(new Throwable[0], failure.getSuppressed());
  }

  @ParameterizedTest
  @MethodSource("closeFailures")
  void testPreservesOriginalFailure(Throwable closeFailure) {
    IOException failure = new IOException("write failed");
    AutoCloseable closeable = () -> {
      if (closeFailure instanceof Error) {
        throw (Error) closeFailure;
      }
      throw (Exception) closeFailure;
    };
    assertDoesNotThrow(() -> CloseableUtils.closeSuppressing(closeable, failure));
    assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
  }

  @Test
  void testDoesNotSuppressFailureOnItself() {
    IOException failure = new IOException("write failed");
    assertDoesNotThrow(() -> CloseableUtils.closeSuppressing(() -> {
      throw failure;
    }, failure));
    assertArrayEquals(new Throwable[0], failure.getSuppressed());
  }
}
