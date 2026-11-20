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

import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.EncodingRegistry;
import com.knuddels.jtokkit.api.EncodingResult;
import com.knuddels.jtokkit.api.EncodingType;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Counts tokens with the byte-pair encoding of OpenAI's embedding models, through jtokkit, a pure-Java port of tiktoken
 * whose counts match tiktoken's on the same text.
 *
 * <p>Only jtokkit's ORDINARY methods are used: text that spells a special token ({@code <|endoftext|>}) is counted as
 * text instead of failing the file. A lone UTF-16 surrogate, which jtokkit and tiktoken count differently, is replaced
 * by U+FFFD first ({@link #normalise}). BPE counts are not monotone in prefix length: a longer prefix can have FEWER
 * tokens, so a count of {@code a + b} is derived from the counts of {@code a} and {@code b} only where {@code b} starts
 * at a word space ({@link #addsUpAtWordSpaces}).
 */
public class OpenAiBpeTokenizer implements Tokenizer {

  /** Prefix of {@link #id()}; also the {@code hoodie.unstructured.chunk.tokenizer.type} of this tokenizer. */
  public static final String TYPE = "openai-bpe";
  /**
   * Encoding of text-embedding-3-small, text-embedding-3-large and text-embedding-ada-002. A literal (jtokkit's
   * {@code EncodingType.CL100K_BASE} name), so classes inlining it never load jtokkit.
   */
  public static final String DEFAULT_ENCODING = "cl100k_base";
  /**
   * The logic of encoding with jtokkit (its ORDINARY methods, the surrogate replacement, the inclusive prefix end), pinned in
   * the text and audio prep logic versions instead of the jtokkit jar version. Bump only when a dependency upgrade or code change
   * alters prep output; the golden test below proves it: {@code TestTextPackingPins} pins the chunk streams this tokenizer
   * packs on a fixed PDF, and {@code TestOpenAiBpeTokenizer} its counts.
   */
  public static final String ENCODING_LOGIC_VERSION = "jtokkit-bpe-encoding-v1";
  private static final char REPLACEMENT_CHARACTER = '�'; // U+FFFD, the Unicode replacement character
  /** One registry per JVM: it is thread-safe and loads each vocabulary once, on first use. */
  private static final EncodingRegistry REGISTRY = Encodings.newLazyEncodingRegistry();

  private final String encodingName;
  private final Encoding encoding;

  /** The {@link #DEFAULT_ENCODING} tokenizer; the no-arg constructor {@code tokenizer.class} needs. */
  public OpenAiBpeTokenizer() {
    this(DEFAULT_ENCODING);
  }

  /**
   * @param encodingName a jtokkit encoding name, e.g. {@code cl100k_base} or {@code o200k_base}
   * @throws IllegalArgumentException for an encoding jtokkit does not know
   */
  public OpenAiBpeTokenizer(String encodingName) {
    this.encodingName = encodingName;
    this.encoding = REGISTRY.getEncoding(encodingName).orElseThrow(() -> new IllegalArgumentException(
        "Unknown OpenAI BPE encoding '" + encodingName + "'; expected one of "
            + Arrays.stream(EncodingType.values()).map(EncodingType::getName).collect(Collectors.joining(", "))));
  }

  @Override
  public int countTokens(String text) {
    if (text == null || text.isEmpty()) {
      return 0;
    }
    try {
      return encoding.countTokensOrdinary(normalise(text));
    } catch (RuntimeException e) {
      throw new TokenizerException("Encoding " + encodingName + " failed to count a text of " + text.length() + " chars", e);
    }
  }

  /**
   * Returns an end index {@code e} of {@code text} with {@code countTokens(text.substring(0, e)) <= maxTokens}, never
   * inside a surrogate pair: {@code text.length()} when the whole text fits, 0 when nothing does. It is a large such
   * index, not promised to be the largest (counts are not monotone).
   *
   * <p>jtokkit's {@link EncodingResult#getLastProcessedCharacterIndex()} is INCLUSIVE, so the candidate end is that index
   * + 1 (e.g. on the test sample.txt with 32 tokens: index 181, end 182; the 181-char prefix counts 34 tokens, the 182-char
   * prefix 32). The candidate is re-counted and moved back one code point at a time until it fits.
   */
  @Override
  public int prefixEndWithin(String text, int maxTokens) {
    if (text == null || text.isEmpty() || maxTokens <= 0) {
      return 0;
    }
    String normalised = normalise(text);
    EncodingResult result;
    try {
      result = encoding.encodeOrdinary(normalised, maxTokens);
    } catch (RuntimeException e) {
      throw new TokenizerException("Encoding " + encodingName + " failed to encode a text of " + text.length() + " chars", e);
    }
    int end = result.isTruncated() ? Math.min(result.getLastProcessedCharacterIndex() + 1, text.length()) : text.length();
    end = Utf16.floor(text, end);
    while (end > 0 && countTokens(text.substring(0, end)) > maxTokens) {
      end = text.offsetByCodePoints(end, -1);
    }
    return end;
  }

  /** Replaces every lone UTF-16 surrogate (a high one not followed by a low one, or a low one alone) by U+FFFD. */
  public static String normalise(String text) {
    StringBuilder normalised = null;
    int i = 0;
    while (i < text.length()) {
      int units = unitsAt(text, i);
      boolean lone = units == 1 && Character.isSurrogate(text.charAt(i));
      if (lone && normalised == null) {
        normalised = new StringBuilder(text.length()).append(text, 0, i);
      }
      if (normalised != null) {
        if (lone) {
          normalised.append(REPLACEMENT_CHARACTER);
        } else {
          normalised.append(text, i, i + units);
        }
      }
      i += units;
    }
    return normalised == null ? text : normalised.toString();
  }

  /** The number of UTF-16 units of the code point at {@code i}: 2 for a high surrogate followed by a low one, else 1. */
  private static int unitsAt(String text, int i) {
    return Character.isHighSurrogate(text.charAt(i)) && i + 1 < text.length() && Character.isLowSurrogate(text.charAt(i + 1))
        ? 2 : 1;
  }

  /**
   * True: every jtokkit encoding splits the text into pieces before the byte-pair merges, which never cross a piece, and
   * no piece holds a U+0020 after a non-whitespace char (cl100k_base's parser and the o200k, p50k and r50k patterns start
   * a piece there), so the count of {@code a + b} is the count of {@code a} plus that of {@code b} at such a space. The
   * surrogate replacement of {@link #normalise} is decided per char pair and never pairs across that space. A subclass may
   * count differently, so it adds up only when it says so itself. TestIncrementalTokenCounting pins this against every
   * encoding; a jtokkit upgrade must keep it green.
   */
  @Override
  public boolean addsUpAtWordSpaces() {
    return getClass() == OpenAiBpeTokenizer.class;
  }

  @Override
  public String id() {
    return TYPE + ":" + encodingName;
  }

  @Override
  public TokenUnit unit() {
    return TokenUnit.MODEL_TOKENS;
  }
}
