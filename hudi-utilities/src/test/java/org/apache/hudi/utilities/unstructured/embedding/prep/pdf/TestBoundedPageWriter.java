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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** {@link BoundedPageWriter} keeps at most its char budget and never half of a surrogate pair. */
class TestBoundedPageWriter {

  private static final String EMOJI = new String(Character.toChars(0x1F600));

  @Test
  void stringCutBeforeAHighSurrogateDropsTheWholePair() throws IOException {
    BoundedPageWriter writer = new BoundedPageWriter(3);
    writer.write("ab" + EMOJI);
    assertEquals("ab", writer.text());
    assertTrue(writer.truncated());
  }

  @Test
  void charArrayCutBeforeAHighSurrogateDropsTheWholePair() {
    BoundedPageWriter writer = new BoundedPageWriter(3);
    char[] chars = ("x" + "ab" + EMOJI + "y").toCharArray();
    writer.write(chars, 1, 4);
    assertEquals("ab", writer.text());
    assertTrue(writer.truncated());
  }

  @Test
  void pairEndingAtTheBudgetIsKeptWhole() throws IOException {
    BoundedPageWriter string = new BoundedPageWriter(4);
    string.write("ab" + EMOJI + "c");
    assertEquals("ab" + EMOJI, string.text());
    assertTrue(string.truncated());
    BoundedPageWriter array = new BoundedPageWriter(4);
    char[] chars = ("ab" + EMOJI + "c").toCharArray();
    array.write(chars, 0, chars.length);
    assertEquals("ab" + EMOJI, array.text());
    assertTrue(array.truncated());
  }

  @Test
  void noRoomLeftKeepsTheTextAndMarksTruncation() throws IOException {
    BoundedPageWriter string = new BoundedPageWriter(2);
    string.write("ab");
    assertFalse(string.truncated(), "text that fits exactly is not truncated");
    string.write(EMOJI);
    assertEquals("ab", string.text());
    assertTrue(string.truncated());
    BoundedPageWriter array = new BoundedPageWriter(2);
    array.write("ab".toCharArray(), 0, 2);
    assertFalse(array.truncated());
    array.write(EMOJI.toCharArray(), 0, 2);
    assertEquals("ab", array.text());
    assertTrue(array.truncated());
  }

  @Test
  void writesAfterTruncationAreDropped() throws IOException {
    BoundedPageWriter writer = new BoundedPageWriter(3);
    writer.write("abcd");
    writer.write("e");
    writer.write("f".toCharArray(), 0, 1);
    assertEquals("abc", writer.text());
    assertTrue(writer.truncated());
  }
}
