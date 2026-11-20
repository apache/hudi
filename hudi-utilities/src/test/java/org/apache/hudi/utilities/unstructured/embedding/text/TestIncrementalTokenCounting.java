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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Incremental token counting gives the counts and the chunks of full counting. {@link TokenBudget#prefixes} and
 * {@link TokenBudget#suffixes} are checked against full counts of every prefix and suffix of random texts, and the
 * {@link ChunkPacker} is run twice over the same text: with a tokenizer that adds up at word spaces (incremental) and with
 * the same tokenizer reporting that it does not (every count a full count, as before incremental counting).
 */
public class TestIncrementalTokenCounting {

  private static final PackFailures FAILURES = (kind, message, cause) -> new IllegalStateException(kind + ": " + message, cause);

  /** Pieces random texts are built of: many whitespace kinds, contractions, digits, punctuation, CJK, marks, surrogates. */
  private static final String[] PIECES = {
      "a", "word", "Hello", " ", " ", " ", "  ", "\n", "\n\n", "\t", // the repeated spaces weight the random choice
      "\u00A0", "\u3000", "\u2009", "\u0085", "\u001F", "\u000B", "\f", "\u001C", "\u001D", "\u001E", // spaces
      "\u1680", "\u2028", "\u2029", "\u202F", "\u205F", // more whitespace kinds
      "'s", "'ll", "'RE", "don't", "123", "45678", "3½", ".", ",", "...", "!?", "(x)", "\"", "-", "/", "中文",
      "日本語", "한국", "é", "́", "😀", "\uD83D", "\uDE00", "�", "été", // lone high and low surrogates
      "АБ", "אב", "ال", "<|endoftext|>", "x"};

  @ParameterizedTest
  @ValueSource(strings = {"cl100k_base", "o200k_base", "p50k_base", "r50k_base", "word-count"})
  void prefixAndSuffixMeasuresMatchFullCounts(String encoding) {
    Tokenizer tokenizer = tokenizer(encoding);
    assertTrue(tokenizer.addsUpAtWordSpaces());
    TokenBudget budget = new TokenBudget(tokenizer, 40, 12, 400, FAILURES);
    assertTrue(budget.countsIncrementally());
    Random random = new Random(7);
    for (int round = 0; round < 60; round++) {
      String text = randomText(random, 1 + random.nextInt(300));
      TokenBudget.Prefixes prefixes = budget.prefixes(text);
      // ends asked in a random order, as the paragraph preference asks them in descending order
      List<Integer> ends = new ArrayList<>();
      for (int end = 0; end <= text.length(); end++) {
        ends.add(end);
      }
      Collections.shuffle(ends, random);
      for (int end : ends) {
        String prefix = text.substring(0, end);
        String context = encoding + " prefix " + end + " of " + escape(text);
        assertEquals(budget.count(prefix), prefixes.count(end), context);
        assertEquals(TokenBudget.words(prefix), prefixes.words(end), context);
        assertEquals(budget.fits(prefix), prefixes.fits(end), context);
      }
      int from = random.nextInt(text.length() + 1);
      TokenBudget.Suffixes suffixes = budget.suffixes(text, from);
      for (int start = text.length(); start >= from; start--) {
        String suffix = text.substring(start);
        String context = encoding + " suffix " + start + " (from " + from + ") of " + escape(text);
        assertEquals(budget.count(suffix), suffixes.count(start), context);
        assertEquals(TokenBudget.words(suffix), suffixes.words(start), context);
      }
    }
  }

  @Test
  void tokenizerThatDoesNotAddUpIsCountedInFull() {
    TokenBudget budget = new TokenBudget(new FullCounting(new OpenAiBpeTokenizer()), 40, 12, 400, FAILURES);
    assertFalse(budget.countsIncrementally());
    // a subclass may count differently: it adds up only when it says so itself
    assertFalse(new OpenAiBpeTokenizer() { }.addsUpAtWordSpaces());
    assertFalse(new WordCountTokenizer() { }.addsUpAtWordSpaces());
    assertTrue(TokenBudget.isWordSpace("a b", 1));
    assertFalse(TokenBudget.isWordSpace("a  b", 2));
    assertFalse(TokenBudget.isWordSpace("a\u00A0 b", 2)); // a no-break space before the space
    assertFalse(TokenBudget.isWordSpace("a\u0085 b", 2));
    assertFalse(TokenBudget.isWordSpace(" b", 0));
  }

  /** Text without a sentence break: every chunk is split at words, and its overlap is found by the word-start scan. */
  @Test
  void noSentenceBreaks() {
    Random random = new Random(1);
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 6000; i++) {
      text.append(i == 0 ? "" : " ").append(randomWord(random));
    }
    assertSameChunks(pages(text.toString(), 900), 256, 128, 256, 8192);
    assertSameChunks(pages(text.toString(), 900), 64, 32, 256, 2048);
    assertSameChunks(pages(text.toString(), 900), 256, 0, 256, 8192);
  }

  /** Words longer than a chunk: the code-point split after the word scan. */
  @Test
  void veryLongWords() {
    Random random = new Random(2);
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 400; i++) {
      int length = random.nextInt(10) == 0 ? 300 + random.nextInt(3000) : 1 + random.nextInt(12);
      StringBuilder word = new StringBuilder();
      for (int c = 0; c < length; c++) {
        word.append((char) ('a' + random.nextInt(26)));
      }
      text.append(i == 0 ? "" : " ").append(word).append(random.nextInt(30) == 0 ? "." : "");
    }
    assertSameChunks(pages(text.toString(), 700), 256, 128, 256, 8192);
    assertSameChunks(pages(text.toString(), 700), 16, 8, 256, 512);
  }

  /** CJK text, without spaces (one long word) and with spaces between short runs, and sentence marks of its own. */
  @Test
  void cjk() {
    Random random = new Random(3);
    String[] runs = {"中文字", "日本語の文", "한국어", "漢字", "。",
        "，", "、"};
    StringBuilder spaced = new StringBuilder();
    StringBuilder unspaced = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      String run = runs[random.nextInt(runs.length)];
      spaced.append(i == 0 ? "" : " ").append(run);
      unspaced.append(run);
    }
    assertSameChunks(pages(spaced.toString(), 500), 256, 128, 256, 8192);
    assertSameChunks(pages(unspaced.toString(), 500), 256, 128, 256, 8192);
    assertSameChunks(pages(spaced.toString(), 500), 32, 16, 256, 1024);
  }

  /** Many whitespace kinds between words, including those a tokenizer splits differently from Java. */
  @ParameterizedTest
  @ValueSource(strings = {"cl100k_base", "o200k_base", "word-count"})
  void mixedWhitespaceAndSymbols(String encoding) {
    Random random = new Random(4);
    assertSameChunks(encoding, pages(randomText(random, 40000), 1200), 256, 128, 256, 8192);
    assertSameChunks(encoding, pages(randomText(random, 20000), 600), 48, 24, 20, 600);
  }

  /** Prose with sentences and paragraphs: the whole-sentence path, the paragraph preference and boundary overlaps. */
  @ParameterizedTest
  @ValueSource(strings = {"cl100k_base", "o200k_base", "word-count"})
  void prose(String encoding) {
    Random random = new Random(5);
    StringBuilder text = new StringBuilder();
    for (int s = 0; s < 2500; s++) {
      if (s > 0) {
        text.append(random.nextInt(6) == 0 ? "\n\n" : " ");
      }
      int words = 1 + random.nextInt(random.nextInt(8) == 0 ? 400 : 25);
      for (int w = 0; w < words; w++) {
        text.append(w == 0 ? "" : " ").append(randomWord(random));
      }
      text.append(random.nextBoolean() ? "." : "?");
    }
    assertSameChunks(encoding, pages(text.toString(), 2000), 256, 128, 256, 8192);
    assertSameChunks(encoding, pages(text.toString(), 2000), 100, 50, 256, 4000);
  }

  /** Token limits at and near their edges, with and without overlap. */
  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 7, 8, 255, 256, 257, 1024})
  void tokenLimitEdges(int maxTokens) {
    Random random = new Random(maxTokens);
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 3000; i++) {
      text.append(i == 0 ? "" : " ").append(randomWord(random)).append(random.nextInt(50) == 0 ? "." : "");
    }
    assertSameChunks(pages(text.toString(), 800), maxTokens, maxTokens / 2, 256, 8192);
    assertSameChunks(pages(text.toString(), 800), maxTokens, 0, 256, 8192);
    assertSameChunks(pages(text.toString(), 800), maxTokens, maxTokens / 2, Math.max(1, maxTokens / 3), 8192);
  }

  /** The declared-boundary mode (transcripts), with a chunk prefix counted in every fit test. */
  @Test
  void declaredBoundariesWithAPrefix() {
    Random random = new Random(6);
    List<String> pieces = new ArrayList<>();
    for (int i = 0; i < 1500; i++) {
      StringBuilder piece = new StringBuilder();
      int words = 1 + random.nextInt(random.nextInt(10) == 0 ? 300 : 20);
      for (int w = 0; w < words; w++) {
        piece.append(randomWord(random)).append(' ');
      }
      pieces.add(piece.toString());
    }
    for (int maxTokens : new int[] {64, 256}) {
      List<String> incremental = packDeclared(new OpenAiBpeTokenizer(), maxTokens, pieces);
      List<String> full = packDeclared(new FullCounting(new OpenAiBpeTokenizer()), maxTokens, pieces);
      assertEquals(full, incremental, "declared boundaries, max tokens " + maxTokens);
    }
  }

  /** On text without a sentence break, the chars the tokenizer is given grow with the text, not with its square. */
  @Test
  void tokenizerWorkGrowsWithTheText() {
    Random random = new Random(8);
    StringBuilder text = new StringBuilder();
    for (int i = 0; i < 20000; i++) {
      text.append(i == 0 ? "" : " ").append(randomWord(random));
    }
    List<String> pages = pages(text.toString(), 2000);
    CharCounting incremental = new CharCounting(new OpenAiBpeTokenizer(), true);
    CharCounting full = new CharCounting(new OpenAiBpeTokenizer(), false);
    assertEquals(pack(full, pages, 256, 128, 256, 8192), pack(incremental, pages, 256, 128, 256, 8192));
    long length = text.length();
    assertTrue(incremental.chars < 8 * length, "incremental: " + incremental.chars + " chars counted for " + length);
    assertTrue(full.chars > 50 * length, "full: " + full.chars + " chars counted for " + length);
  }

  // ------------------------------------------------------------------ helpers

  private static void assertSameChunks(List<String> pages, int maxTokens, int overlapTokens, int maxWords, int window) {
    assertSameChunks("cl100k_base", pages, maxTokens, overlapTokens, maxWords, window);
  }

  private static void assertSameChunks(String encoding, List<String> pages, int maxTokens, int overlapTokens, int maxWords,
                                       int window) {
    String context = encoding + ", max tokens " + maxTokens + ", overlap " + overlapTokens + ", max words " + maxWords
        + ", window " + window;
    List<String> incremental = pack(tokenizer(encoding), pages, maxTokens, overlapTokens, maxWords, window);
    List<String> full = pack(new FullCounting(tokenizer(encoding)), pages, maxTokens, overlapTokens, maxWords, window);
    assertEquals(full.size(), incremental.size(), context);
    for (int i = 0; i < full.size(); i++) {
      assertEquals(full.get(i), incremental.get(i), context + ", chunk " + i);
    }
    assertTrue(full.size() > 1, context);
  }

  /** The chunks of the PDF mode as lines (text, overlap, prefix, marks, tokens, words), then the forced splits or the failure. */
  private static List<String> pack(Tokenizer tokenizer, List<String> pages, int maxTokens, int overlapTokens, int maxWords,
                                   int window) {
    ChunkPacker packer = new ChunkPacker(new TokenBudget(tokenizer, maxTokens, maxWords, window, FAILURES), overlapTokens,
        Locale.ENGLISH);
    List<PackedChunk> chunks = new ArrayList<>();
    List<String> lines = new ArrayList<>();
    try {
      for (int page = 1; page <= pages.size(); page++) {
        packer.appendPage(pages.get(page - 1), page);
        while (!packer.needsMoreText()) {
          chunks.add(packer.cut(false));
        }
      }
      while (packer.hasPending()) {
        chunks.add(packer.cut(true));
      }
      lines.addAll(describe(chunks));
      lines.add("forced splits " + packer.forcedSplits());
    } catch (IllegalStateException e) {
      lines.addAll(describe(chunks));
      lines.add("failed: " + e.getMessage());
    }
    return lines;
  }

  private static List<String> packDeclared(Tokenizer tokenizer, int maxTokens, List<String> pieces) {
    ChunkPacker packer = ChunkPacker.withDeclaredBoundaries(new TokenBudget(tokenizer, maxTokens, 256, 8192, FAILURES),
        maxTokens / 2, (firstMark, start) -> "[speaker " + (firstMark % 3) + "] ");
    List<PackedChunk> chunks = new ArrayList<>();
    for (int i = 0; i < pieces.size(); i++) {
      ChunkPacker.Boundary after = i % 7 == 6 ? ChunkPacker.Boundary.PARAGRAPH
          : i % 2 == 1 ? ChunkPacker.Boundary.SENTENCE : ChunkPacker.Boundary.NONE;
      packer.appendSegment(pieces.get(i), i + 1, after);
      while (!packer.needsMoreText()) {
        chunks.add(packer.cut(false));
      }
    }
    while (packer.hasPending()) {
      chunks.add(packer.cut(true));
    }
    List<String> lines = describe(chunks);
    lines.add("forced splits " + packer.forcedSplits());
    return lines;
  }

  private static List<String> describe(List<PackedChunk> chunks) {
    List<String> lines = new ArrayList<>();
    for (PackedChunk c : chunks) {
      lines.add(c.overlapChars() + "|" + c.prefixChars() + "|" + c.firstMark() + "-" + c.lastMark() + "|" + c.tokenCount()
          + "|" + c.wordCount() + "|" + c.text());
    }
    return lines;
  }

  /** Cuts {@code text} into pages of about {@code size} chars at spaces, as normalized pages are joined by one space. */
  private static List<String> pages(String text, int size) {
    List<String> pages = new ArrayList<>();
    int start = 0;
    while (start < text.length()) {
      int end = Math.min(text.length(), start + size);
      int space = end < text.length() ? text.indexOf(' ', end) : -1;
      end = space < 0 ? text.length() : space;
      String page = text.substring(start, end).trim();
      if (!page.isEmpty()) {
        pages.add(page);
      }
      start = end;
    }
    return pages;
  }

  private static Tokenizer tokenizer(String encoding) {
    return "word-count".equals(encoding) ? new WordCountTokenizer() : new OpenAiBpeTokenizer(encoding);
  }

  private static String randomWord(Random random) {
    String[] words = {"the", "of", "table", "Revenue", "2024", "Q3", "$1,204.50", "(see", "note)", "don't", "it's", "naïve",
        "co-operate", "e.g.,", "§", "—", "ID-4471", "x", "approximately", "Übersicht", "中文"};
    return words[random.nextInt(words.length)];
  }

  private static String randomText(Random random, int length) {
    StringBuilder text = new StringBuilder();
    while (text.length() < length) {
      text.append(PIECES[random.nextInt(PIECES.length)]);
    }
    return text.toString();
  }

  private static String escape(String s) {
    StringBuilder out = new StringBuilder();
    for (char c : s.toCharArray()) {
      out.append(c >= 0x20 && c < 0x7F ? String.valueOf(c) : String.format("\\u%04X", (int) c));
    }
    return out.toString();
  }

  /** The wrapped tokenizer, reporting that its counts do not add up: every count is a full count. */
  private static class FullCounting implements Tokenizer {
    private final Tokenizer delegate;

    FullCounting(Tokenizer delegate) {
      this.delegate = delegate;
    }

    @Override
    public int countTokens(String text) {
      return delegate.countTokens(text);
    }

    @Override
    public int prefixEndWithin(String text, int maxTokens) {
      return delegate.prefixEndWithin(text, maxTokens);
    }

    @Override
    public String id() {
      return delegate.id();
    }

    @Override
    public TokenUnit unit() {
      return delegate.unit();
    }
  }

  /** The wrapped tokenizer, adding up or not as told, summing the chars it is asked to count. */
  private static final class CharCounting extends FullCounting {
    private final boolean addsUp;
    private long chars;

    CharCounting(Tokenizer delegate, boolean addsUp) {
      super(delegate);
      this.addsUp = addsUp;
    }

    @Override
    public int countTokens(String text) {
      chars += text.length();
      return super.countTokens(text);
    }

    @Override
    public boolean addsUpAtWordSpaces() {
      return addsUp;
    }
  }
}
