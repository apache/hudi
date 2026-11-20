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

package org.apache.hudi.utilities.unstructured.embedding.text;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The OpenAI BPE tokenizer against the counts measured with jtokkit 1.1.0 and tiktoken on the same {@code sample.txt}. */
class TestOpenAiBpeTokenizer {

  private static final String SAMPLE = "/unstructured/tokenizer/sample.txt";

  private static String sample() throws IOException {
    try (InputStream in = TestOpenAiBpeTokenizer.class.getResourceAsStream(SAMPLE)) {
      // trimmed, as the reference counts read it (the file ends with a newline)
      return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
    }
  }

  @Test
  void testCountsMatchTiktoken() throws IOException {
    String sample = sample();
    assertEquals(691, sample.length());
    // tiktoken counts the same sample as 154 (cl100k_base) and 152 (o200k_base)
    assertEquals(154, new OpenAiBpeTokenizer().countTokens(sample));
    assertEquals(152, new OpenAiBpeTokenizer("o200k_base").countTokens(sample));
  }

  @Test
  void testPrefixEndIsTheInclusiveIndexPlusOne() throws IOException {
    String sample = sample();
    OpenAiBpeTokenizer tokenizer = new OpenAiBpeTokenizer();
    // measured: encodeOrdinary with at most 32 tokens reports lastProcessedCharacterIndex 181, an inclusive index.
    // The first 181 chars count 34 tokens, the first 182 count 32
    assertEquals(34, tokenizer.countTokens(sample.substring(0, 181)));
    int end = tokenizer.prefixEndWithin(sample, 32);
    assertEquals(182, end);
    assertTrue(tokenizer.countTokens(sample.substring(0, end)) <= 32);
  }

  @Test
  void testPrefixEndOfShortTextIsItsLength() {
    OpenAiBpeTokenizer tokenizer = new OpenAiBpeTokenizer();
    assertEquals(11, tokenizer.prefixEndWithin("hello world", 32));
    assertEquals(0, tokenizer.prefixEndWithin("", 5));
    assertEquals(0, tokenizer.prefixEndWithin(null, 5));
  }

  @Test
  void testPrefixEndNeverSplitsASurrogatePair() {
    OpenAiBpeTokenizer tokenizer = new OpenAiBpeTokenizer();
    String emojis = "😀😁😂😃";
    for (int max = 1; max <= 8; max++) {
      int end = tokenizer.prefixEndWithin(emojis, max);
      assertEquals(0, end % 2, "cut inside a surrogate pair at max=" + max + ": " + end);
      assertTrue(tokenizer.countTokens(emojis.substring(0, end)) <= max);
    }
  }

  @Test
  void testLoneSurrogateIsNormalised() {
    OpenAiBpeTokenizer tokenizer = new OpenAiBpeTokenizer();
    // measured: jtokkit counts "ab\uD83Dcd" as 5 tokens, tiktoken encode_ordinary as 3; the tokenizer replaces
    // the lone surrogate with U+FFFD first, which jtokkit counts as tiktoken does
    assertEquals("ab\uFFFDcd", OpenAiBpeTokenizer.normalise("ab\uD83Dcd")); // escapes: lone or paired UTF-16 surrogates, U+FFFD
    assertEquals("x\uFFFD", OpenAiBpeTokenizer.normalise("x\uDE00")); // escapes: lone or paired UTF-16 surrogates, U+FFFD
    assertEquals("😀", OpenAiBpeTokenizer.normalise("😀"));
    assertEquals(tokenizer.countTokens("ab\uFFFDcd"), tokenizer.countTokens("ab\uD83Dcd")); // escapes: lone or paired UTF-16 surrogates, U+FFFD
    assertEquals(3, tokenizer.countTokens("ab\uD83Dcd")); // escapes: lone or paired UTF-16 surrogates, U+FFFD
  }

  @Test
  void testSpecialTokenTextIsOrdinary() {
    // jtokkit encode() throws on special-token text; the ordinary methods count it as text
    assertTrue(new OpenAiBpeTokenizer().countTokens("<|endoftext|>") > 1);
  }

  @Test
  void testIdAndUnit() {
    assertEquals("openai-bpe:cl100k_base", new OpenAiBpeTokenizer().id());
    assertEquals("openai-bpe:o200k_base", new OpenAiBpeTokenizer("o200k_base").id());
    assertEquals(TokenUnit.MODEL_TOKENS, new OpenAiBpeTokenizer().unit());
    assertEquals(0, new OpenAiBpeTokenizer().countTokens(null));
    assertEquals(0, new OpenAiBpeTokenizer().countTokens(""));
  }

  @Test
  void testUnknownEncodingIsRejected() {
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> new OpenAiBpeTokenizer("nope"));
    assertTrue(e.getMessage().contains("nope"), e.getMessage());
    assertTrue(e.getMessage().contains("cl100k_base"), e.getMessage());
  }
}
