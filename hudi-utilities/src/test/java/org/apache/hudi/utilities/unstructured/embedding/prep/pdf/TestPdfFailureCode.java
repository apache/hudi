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

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link PdfFailureCode#format}. */
class TestPdfFailureCode {

  @Test
  void cycleInPdfFailureDetailsTerminates() {
    IOException first = new IOException("first");
    IOException second = new IOException("second", first);
    first.initCause(second);
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> assertTrue(PdfFailureCode.INTERNAL.format(first).contains("second")));
  }

  private static final String EMOJI = new String(Character.toChars(0x1F600));

  @Test
  void formatNeverSplitsASurrogatePair() {
    // the 1024-char cut lands between the two halves of a supplementary character
    String prefix = "PAGE_ERROR: java.io.IOException: ";
    String pad = repeat('a', PdfFailureCode.MAX_DETAIL_CHARS - prefix.length() - 1);
    String cut = PdfFailureCode.PAGE_ERROR.format(new IOException(pad + EMOJI + "tail"));
    assertEquals(PdfFailureCode.MAX_DETAIL_CHARS - 1, cut.length(), "backs off one char rather than keep a lone high half");
    assertFalse(Character.isHighSurrogate(cut.charAt(cut.length() - 1)));
    // neighbours: a pair that ends exactly at the limit is kept whole; plain text is cut at the limit
    String whole = PdfFailureCode.PAGE_ERROR.format(new IOException(repeat('a', pad.length() - 1) + EMOJI + "tail"));
    assertEquals(PdfFailureCode.MAX_DETAIL_CHARS, whole.length());
    assertEquals(EMOJI, whole.substring(whole.length() - 2));
    String plain = PdfFailureCode.PAGE_ERROR.format(new IOException(repeat('a', 2000)));
    assertEquals(PdfFailureCode.MAX_DETAIL_CHARS, plain.length());
  }

  static String repeat(char c, int n) {
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) {
      sb.append(c);
    }
    return sb.toString();
  }
}
