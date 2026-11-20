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

package org.apache.hudi.utilities.unstructured.embedding.image;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The reader walk keeps the first JDK candidate and disposes every other; an Error out of the walk disposes the chosen one too and
 * propagates as itself, with a failure of that dispose kept as suppressed.
 */
class TestImageHeaderReaderFirstJdk {

  /** Yields {@code items}, then throws {@code failure} instead of answering whether there is more. */
  private static Iterator<String> failingAfter(List<String> items, Error failure) {
    Iterator<String> it = items.iterator();
    return new Iterator<String>() {
      @Override
      public boolean hasNext() {
        if (!it.hasNext()) {
          throw failure;
        }
        return true;
      }

      @Override
      public String next() {
        if (!it.hasNext()) {
          throw new NoSuchElementException();
        }
        return it.next();
      }
    };
  }

  @Test
  void anErrorOutOfTheWalkDisposesTheChosenCandidateAndPropagates() {
    UnsatisfiedLinkError failure = new UnsatisfiedLinkError("provider native missing (injected)");
    List<String> disposed = new ArrayList<>();
    Iterator<String> candidates = failingAfter(Arrays.asList("jdk", "plugin"), failure);
    assertSame(failure, assertThrows(UnsatisfiedLinkError.class,
        () -> ImageHeaderReader.firstJdk(candidates, "jdk"::equals, disposed::add)));
    assertEquals(Arrays.asList("plugin", "jdk"), disposed, "the other candidate and then the chosen one are disposed");
  }

  @Test
  void failingDisposeOfTheChosenCandidateIsKeptAsSuppressed() {
    UnsatisfiedLinkError failure = new UnsatisfiedLinkError("provider native missing (injected)");
    LinkageError disposeFailure = new LinkageError("dispose failed (injected)");
    Iterator<String> candidates = failingAfter(Arrays.asList("jdk"), failure);
    UnsatisfiedLinkError thrown = assertThrows(UnsatisfiedLinkError.class, () -> ImageHeaderReader.firstJdk(candidates, "jdk"::equals,
        candidate -> {
          throw disposeFailure;
        }));
    assertSame(failure, thrown);
    assertEquals(1, thrown.getSuppressed().length);
    assertSame(disposeFailure, thrown.getSuppressed()[0]);
  }
}
