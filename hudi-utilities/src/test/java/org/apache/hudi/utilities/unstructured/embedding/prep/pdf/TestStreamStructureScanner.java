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

package org.apache.hudi.utilities.unstructured.embedding.prep.pdf;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Content-stream element bounds count nested containers as well as scalar values. */
class TestStreamStructureScanner {

  @ParameterizedTest
  @ValueSource(strings = {"[]", "<<>>", "[[]]"})
  void nestedContainersAboveElementCapAreRefused(String child) {
    int elementsPerChild = "[[]]".equals(child) ? 2 : 1;
    String content = "[" + child.repeat(StreamStructureScanner.MAX_OPERANDS / elementsPerChild + 1) + "] n";
    StreamStructureScanner.Refusal failure = assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content));
    assertEquals(PdfFailureCode.DECODE_LIMIT, failure.code());
    assertTrue(failure.getMessage().contains("elements"), failure.getMessage());
  }

  @ParameterizedTest
  @ValueSource(strings = {"[]", "<<>>", "[[]]"})
  void nestedContainersAtElementCapAreAccepted(String child) {
    int elementsPerChild = "[[]]".equals(child) ? 2 : 1;
    String content = "[" + child.repeat(StreamStructureScanner.MAX_OPERANDS / elementsPerChild) + "] n";
    assertDoesNotThrow(() -> scan(content));
  }

  @ParameterizedTest
  @ValueSource(strings = {"[0]", "<</A 0>>"})
  void childContainersAndScalarsShareTheElementCap(String child) {
    int elementsPerChild = "[0]".equals(child) ? 2 : 3;
    String content = "[" + child.repeat(StreamStructureScanner.MAX_OPERANDS / elementsPerChild + 1) + "] n";
    assertThrows(StreamStructureScanner.Refusal.class, () -> scan(content));
  }

  private static void scan(String content) {
    try (StreamStructureScanner.Operands scanner = new StreamStructureScanner.Operands("test content stream", Long.MAX_VALUE)) {
      byte[] bytes = content.getBytes(StandardCharsets.US_ASCII);
      scanner.write(bytes, 0, bytes.length);
    }
  }
}
