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

import org.apache.hudi.utilities.unstructured.embedding.text.ChunkPacker;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.PackedChunk;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenBudget;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenUnit;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.text.BreakIterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The packing rules, the chunk record's span and counts, the tokenizer window rule and the pull rule, on the
 * parser-free {@link ChunkPacker} fed with normalized pages.
 */
class TestChunkPacker {

  private static final Tokenizer WORDS = new WordCountTokenizer();
  private static final Tokenizer BPE = new OpenAiBpeTokenizer();
  private static final String[] VOCAB = ("the of and to in is that for it as was with be by on not he this are or his from at "
      + "which but have an they you were her she there been one all we can has would more if will no who some into its "
      + "data lake table record index stream chunk page file vector model search").split(" ");

  /** A packer with the given limits; overlap is in the tokenizer's unit. */
  static ChunkPacker packer(Tokenizer tokenizer, int maxTokens, int overlap, int maxWords, int window) {
    return new ChunkPacker(new TokenBudget(tokenizer, maxTokens, maxWords, window, PdfFileFailure.PACK_FAILURES), overlap,
        Locale.ENGLISH);
  }

  /** The chunks of one packing run and the text stream the packer saw. */
  static final class Packed extends ArrayList<PackedChunk> {
    final StringBuilder stream = new StringBuilder();
    /** The page owning each stream char; a join space belongs to the page before it. */
    final List<Integer> owner = new ArrayList<>();
  }

  /** Drives the packer the way the iterator does: pull pages while the pull rule says so, then cut. */
  static List<PackedChunk> pack(ChunkPacker packer, List<String> pages, int maxPageChars, int window) {
    Packed chunks = new Packed();
    int next = 0;
    boolean ended = false;
    while (true) {
      if (!ended && packer.needsMoreText()) {
        assertTrue(packer.pendingLength() < 2 * window, "pull bound: pending " + packer.pendingLength() + " before a pull");
        if (next < pages.size()) {
          String page = pages.get(next);
          // the packer puts one space before a page only while it holds text (pending text or a carried overlap)
          if (!page.isEmpty() && chunks.stream.length() > 0 && packer.retainedChars() > 0) {
            chunks.stream.append(' ');
            chunks.owner.add(chunks.owner.get(chunks.owner.size() - 1));
          }
          chunks.stream.append(page);
          for (int i = 0; i < page.length(); i++) {
            chunks.owner.add(next + 1);
          }
          packer.appendPage(page, next + 1);
          next++;
        } else {
          ended = true;
        }
        continue;
      }
      assertTrue(packer.retainedChars() <= 2 * maxPageChars + 4 * window, "retained " + packer.retainedChars());
      assertTrue(packer.pageMarkers() <= packer.retainedChars() + 1);
      PackedChunk chunk = packer.cut(ended);
      if (chunk == null) {
        assertTrue(ended, "cut returned null before the end of input");
        return chunks;
      }
      chunks.add(chunk);
    }
  }

  /** Every per-chunk packing and record invariant for one packing run. */
  static void checkInvariants(List<PackedChunk> chunks, Tokenizer tokenizer, int maxTokens, int overlap, int maxWords, int window) {
    String stream = ((Packed) chunks).stream.toString();
    StringBuilder covered = new StringBuilder();
    int lastPageStart = 0;
    PackedChunk previous = null;
    for (PackedChunk chunk : chunks) {
      String text = chunk.text();
      assertTrue(text.length() <= window, "chunk within the char window");
      assertTrue(TokenBudget.words(text) <= maxWords, "chunk within the word budget");
      assertEquals(tokenizer.countTokens(text), chunk.tokenCount(), "record token count is exact");
      assertEquals(TokenBudget.words(text), chunk.wordCount(), "record word count is exact");
      assertTrue(chunk.tokenCount() <= maxTokens, "chunk within the token budget");
      assertTrue(text.length() > chunk.overlapChars(), "every chunk has new text");
      assertFalse(text.trim().isEmpty(), "chunk text is non-blank");
      if (chunk.overlapChars() > 0) {
        assertNotNull(previous);
        String prefix = text.substring(0, chunk.overlapChars());
        assertTrue(previous.text().endsWith(prefix), "overlap prefix is the previous suffix");
        assertTrue(tokenizer.countTokens(prefix) <= overlap, "overlap within the overlap tokens");
        assertTrue(TokenBudget.words(prefix) <= maxWords / 2, "overlap within half the word budget");
        assertTrue(prefix.length() <= window / 2, "overlap within half the char window");
      }
      int newStart = covered.length();
      String newText = text.substring(chunk.overlapChars());
      assertEquals(stream.substring(newStart, Math.min(stream.length(), newStart + newText.length())), newText,
          "chunk text is an exact slice of the stream");
      assertEquals(stream.substring(newStart - chunk.overlapChars(), newStart), text.substring(0, chunk.overlapChars()));
      covered.append(newText);
      assertTrue(chunk.firstMark() >= 1 && chunk.firstMark() <= chunk.lastMark(), "page span is ordered");
      assertTrue(chunk.firstMark() >= lastPageStart, "page_start never decreases");
      lastPageStart = chunk.firstMark();
      previous = chunk;
    }
    assertEquals(stream, covered.toString(), "chunks cover the whole stream");
  }

  private static List<String> randomPages(Random random, int pages, int maxPageChars) {
    TextNormalizer normalizer = new TextNormalizer(maxPageChars);
    List<String> result = new ArrayList<>();
    for (int p = 0; p < pages; p++) {
      StringBuilder page = new StringBuilder();
      int sentences = random.nextInt(30);
      for (int s = 0; s < sentences; s++) {
        int words = 1 + random.nextInt(random.nextInt(10) == 0 ? 120 : 15);
        for (int w = 0; w < words; w++) {
          page.append(w == 0 ? capitalize(VOCAB[random.nextInt(VOCAB.length)]) : VOCAB[random.nextInt(VOCAB.length)]);
          page.append(w + 1 < words ? " " : "");
        }
        if (s + 1 < sentences || random.nextBoolean()) {
          page.append(random.nextInt(5) == 0 ? "?" : ".");   // pages may end mid-sentence
        }
        page.append(random.nextInt(4) == 0 ? "\n\n" : random.nextInt(3) == 0 ? "\n" : " ");
      }
      result.add(normalizer.normalize(page.toString()).text());
    }
    return result;
  }

  private static String capitalize(String word) {
    return Character.toUpperCase(word.charAt(0)) + word.substring(1);
  }

  private static String words(String prefix, int count, String end) {
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= count; i++) {
      sb.append(i == 1 ? capitalize(prefix) : prefix).append(i);
      sb.append(i < count ? " " : end);
    }
    return sb.toString();
  }

  // ------------------------------------------------------------------ budgets, coverage, exact slices and records

  @Test
  void everyChunkFits() {
    int[][] configs = {{256, 128, 256, 8192}, {64, 0, 256, 8192}, {512, 128, 256, 8192}, {20, 10, 16, 256}};
    for (Tokenizer tokenizer : Arrays.asList(WORDS, BPE)) {
      for (int[] cfg : configs) {
        Random random = new Random(42);
        for (int round = 0; round < 6; round++) {
          List<String> pages = randomPages(random, 1 + random.nextInt(12), 262_144);
          List<PackedChunk> chunks = pack(packer(tokenizer, cfg[0], cfg[1], cfg[2], cfg[3]), pages, 262_144, cfg[3]);
          checkInvariants(chunks, tokenizer, cfg[0], cfg[1], cfg[2], cfg[3]);
        }
      }
    }
  }

  @Test
  void reconstructsNormalizedStream() {
    List<String> pages = randomPages(new Random(7), 20, 262_144);
    pages.add(3, "");                                  // an empty page contributes nothing
    List<PackedChunk> chunks = pack(packer(BPE, 256, 128, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, BPE, 256, 128, 256, 8192);
    assertTrue(chunks.size() > 3);
  }

  @Test
  void chunkTextIsExactSlice() {
    List<String> pages = Arrays.asList("First sentence here.  Second one\n\nnew paragraph.", "Next page text. End");
    List<PackedChunk> chunks = pack(packer(WORDS, 4, 2, 16, 256), pages, 262_144, 256);
    checkInvariants(chunks, WORDS, 4, 2, 16, 256);
    assertEquals("First sentence here.  ", chunks.get(0).text(), "nothing is trimmed: the chunk keeps its trailing spaces");
  }

  @Test
  void overlapPrefixIsPreviousSuffix() {
    List<String> pages = randomPages(new Random(11), 8, 262_144);
    List<PackedChunk> chunks = pack(packer(WORDS, 30, 15, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, WORDS, 30, 15, 256, 8192);
    assertTrue(chunks.stream().anyMatch(c -> c.overlapChars() > 0), "the fixture exercises overlaps");
  }

  @Test
  void countsRecomputeEqual() {
    List<String> pages = randomPages(new Random(3), 6, 262_144);
    for (PackedChunk chunk : pack(packer(BPE, 64, 32, 256, 8192), pages, 262_144, 8192)) {
      assertEquals(BPE.countTokens(chunk.text()), chunk.tokenCount());
    }
  }

  // ------------------------------------------------------------------ paragraph and sentence cuts

  @Test
  void cutsAtParagraphWhenHalfFull() {
    String page = words("a", 12, ".") + "\n\n" + words("b", 5, ".") + " " + words("c", 6, ".");
    List<PackedChunk> chunks = pack(packer(WORDS, 20, 0, 256, 256), Collections.singletonList(page), 262_144, 256);
    assertEquals(words("a", 12, ".") + "\n\n", chunks.get(0).text(), "cut at the paragraph: 12 new tokens >= (20 - 0) / 2");
    assertEquals(words("b", 5, ".") + " " + words("c", 6, "."), chunks.get(1).text());
  }

  @Test
  void cutsAtSentenceOtherwise() {
    String page = words("a", 8, ".") + "\n\n" + words("b", 5, ".") + " " + words("c", 8, ".");
    List<PackedChunk> chunks = pack(packer(WORDS, 20, 0, 256, 256), Collections.singletonList(page), 262_144, 256);
    assertEquals(words("a", 8, ".") + "\n\n" + words("b", 5, ".") + " ", chunks.get(0).text(),
        "8 tokens before the paragraph < 10: cut before the sentence that does not fit");
    assertEquals(words("c", 8, "."), chunks.get(1).text());
  }

  @Test
  void paragraphRightAfterOverlapNotUsed() {
    // chunk 0 ends with a paragraph; its overlap (the last sentence + "\n\n") is followed by short new paragraphs: the
    // cut never falls at the end of the overlap (no new text), and every chunk has new text
    String page = words("a", 6, ".") + " " + words("b", 6, ".") + "\n\n" + words("c", 3, ".") + "\n\n" + words("d", 9, ".")
        + "\n\n" + words("e", 9, ".");
    List<String> pages = Collections.singletonList(page);
    List<PackedChunk> chunks = pack(packer(WORDS, 16, 8, 16, 256), pages, 262_144, 256);
    checkInvariants(chunks, WORDS, 16, 8, 16, 256);
    for (PackedChunk chunk : chunks) {
      assertTrue(chunk.text().length() > chunk.overlapChars());
    }
  }

  @Test
  void jdkSentenceBehaviourRecorded() {
    String text = "Dr. Smith paid 3.5 USD. Then left.";
    BreakIterator iterator = BreakIterator.getSentenceInstance(Locale.ENGLISH);
    iterator.setText(text);
    List<String> sentences = new ArrayList<>();
    int start = iterator.first();
    for (int end = iterator.next(); end != BreakIterator.DONE; start = end, end = iterator.next()) {
      sentences.add(text.substring(start, end));
    }
    // RECORDED JDK 11 behaviour, not "fixed": "Dr. " ends a sentence
    assertEquals(Arrays.asList("Dr. ", "Smith paid 3.5 USD. ", "Then left."), sentences);
  }

  @Test
  void unfinishedSentenceCarriedAcrossPages() {
    List<String> pages = Arrays.asList(words("a", 30, ".") + " The quick", "brown fox jumps. " + words("b", 30, "."));
    List<PackedChunk> chunks = pack(packer(WORDS, 40, 0, 256, 256), pages, 262_144, 256);
    checkInvariants(chunks, WORDS, 40, 0, 256, 256);
    PackedChunk across = chunks.stream().filter(c -> c.text().contains("The quick brown fox jumps.")).findFirst().orElse(null);
    assertNotNull(across, "the sentence split by the page break stays whole in one chunk");
    assertEquals(1, across.firstMark());
    assertEquals(2, across.lastMark());
  }

  // ------------------------------------------------------------------ forced splits

  @Test
  void giantSentenceSplitAtWords() {
    List<String> pages = Collections.singletonList(words("w", 1000, ""));
    ChunkPacker packer = packer(WORDS, 50, 0, 256, 8192);
    List<PackedChunk> chunks = pack(packer, pages, 262_144, 8192);
    checkInvariants(chunks, WORDS, 50, 0, 256, 8192);
    assertEquals(20, chunks.size());
    assertTrue(packer.forcedSplits() >= 19);
    for (PackedChunk chunk : chunks) {
      assertTrue(chunk.text().endsWith(" ") || chunk == chunks.get(chunks.size() - 1), "word splits keep whole words");
    }
  }

  @Test
  void giantWordSplitAtCodePoints() {
    StringBuilder word = new StringBuilder();
    for (int i = 0; i < 5000; i++) {
      word.append("A\uD83D\uDE00B");  // escaped non-ASCII test data
    }
    List<String> pages = Collections.singletonList(word.toString());
    List<PackedChunk> chunks = pack(packer(BPE, 64, 0, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, BPE, 64, 0, 256, 8192);
    for (PackedChunk chunk : chunks) {
      assertFalse(Character.isHighSurrogate(chunk.text().charAt(chunk.text().length() - 1)), "a split inside a surrogate pair");
      assertFalse(Character.isLowSurrogate(chunk.text().charAt(0)));
    }
  }

  @Test
  void forcedSplitCounter() {
    StringBuilder word = new StringBuilder();
    for (int i = 0; i < 1000; i++) {
      word.append('x');
    }
    ChunkPacker packer = packer(WORDS, 256, 0, 256, 256);
    List<PackedChunk> chunks = pack(packer, Collections.singletonList(word.toString()), 262_144, 256);
    assertEquals(Arrays.asList(256, 256, 256, 232), lengths(chunks));
    assertEquals(3, packer.forcedSplits(), "the last piece fits at the end of input, so it is not a forced split");
  }

  // ------------------------------------------------------------------ overlap

  @Test
  void overlapSentenceAligned() {
    StringBuilder page = new StringBuilder();
    for (int s = 1; s <= 10; s++) {
      page.append(words("s" + s + "w", 4, ".")).append(s < 10 ? " " : "");
    }
    List<String> pages = Collections.singletonList(page.toString());
    List<PackedChunk> chunks = pack(packer(WORDS, 20, 10, 256, 256), pages, 262_144, 256);
    checkInvariants(chunks, WORDS, 20, 10, 256, 256);
    // chunk 0 = sentences 1-5 (20 words); the longest sentence-aligned suffix with <= 10 tokens is sentences 4-5
    assertTrue(chunks.get(0).text().startsWith("S1w1") && chunks.get(0).text().endsWith("s5w4. "));
    assertTrue(chunks.get(1).text().startsWith("S4w1 "), chunks.get(1).text());
    assertEquals((words("s4w", 4, ".") + " " + words("s5w", 4, ".") + " ").length(), chunks.get(1).overlapChars());
  }

  @Test
  void overlapUsesStreamBoundaries() {
    // sentences of at most 6 words: a sentence-aligned overlap within 20 tokens always exists, so every overlap must start
    // right after a sentence end recorded while packing, never at a mere word start
    Random random = new Random(5);
    StringBuilder page = new StringBuilder();
    for (int s = 0; s < 200; s++) {
      page.append(words("s" + s + "w", 1 + random.nextInt(6), random.nextBoolean() ? "." : "?")).append(' ');
    }
    List<String> pages = Collections.singletonList(page.toString().trim());
    List<PackedChunk> chunks = pack(packer(WORDS, 40, 20, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, WORDS, 40, 20, 256, 8192);
    String stream = ((Packed) chunks).stream.toString();
    int newStart = 0;
    int overlaps = 0;
    for (PackedChunk chunk : chunks) {
      if (chunk.overlapChars() > 0) {
        overlaps++;
        int start = newStart - chunk.overlapChars();
        char end = stream.charAt(start - 2);
        assertTrue(stream.charAt(start - 1) == ' ' && (end == '.' || end == '?'),
            "overlap does not start at a sentence boundary: '" + chunk.text().substring(0, chunk.overlapChars()) + "'");
      }
      newStart += chunk.text().length() - chunk.overlapChars();
    }
    assertTrue(overlaps > 5);
  }

  @Test
  void pageSpansMatchCharOwnership() {
    // oracle: the page owning each stream char (a join space belongs to the page before it); page_start/page_end of every
    // chunk are the owners of its first and last char, across overlaps, forced splits and cuts at a page start
    for (int seed = 0; seed < 300; seed++) {
      Random random = new Random(seed);
      List<String> pages = randomPages(random, 1 + random.nextInt(8), 262_144);
      int maxTokens = 16 + random.nextInt(60);
      int overlap = random.nextInt(maxTokens / 2 + 1);
      int window = 256 + random.nextInt(800);
      Packed chunks = (Packed) pack(packer(WORDS, maxTokens, overlap, 16 + random.nextInt(60), window), pages, 262_144,
          window);
      List<Integer> owner = chunks.owner;
      int newStart = 0;
      for (PackedChunk chunk : chunks) {
        int start = newStart - chunk.overlapChars();
        int end = newStart + chunk.text().length() - chunk.overlapChars();
        assertEquals((int) owner.get(start), chunk.firstMark(), "seed " + seed + " page_start");
        assertEquals((int) owner.get(end - 1), chunk.lastMark(), "seed " + seed + " page_end");
        newStart = end;
      }
    }
  }

  @Test
  void giantSupplementaryWordSplitAtWindowEdge() {
    // word-count accepts a whole window-long word, and a window of odd length ends between the halves of a pair
    StringBuilder word = new StringBuilder("AAAAAAA");
    for (int i = 0; i < 400; i++) {
      word.append("\uD835\uDC00");                      // U+1D400 MATHEMATICAL BOLD CAPITAL A
    }
    List<String> pages = Collections.singletonList(word.toString());
    for (int window : new int[] {257, 258, 301}) {
      List<PackedChunk> chunks = pack(packer(WORDS, 256, 0, 256, window), pages, 262_144, window);
      checkInvariants(chunks, WORDS, 256, 0, 256, window);
      for (PackedChunk chunk : chunks) {
        assertFalse(Character.isHighSurrogate(chunk.text().charAt(chunk.text().length() - 1)), "window " + window);
        assertFalse(Character.isLowSurrogate(chunk.text().charAt(0)), "window " + window);
      }
    }
  }

  @Test
  void overlapFallsBackToWords() {
    // a 30-word sentence is force-split (chunk 0 = its first 20 words); its remaining 10 words plus a short sentence fit
    // after a 5-token overlap. No sentence boundary lies inside chunk 0, so the overlap is its last 5 whole words.
    List<String> pages = Collections.singletonList(words("w", 30, ".") + " Next short one.");
    List<PackedChunk> chunks = pack(packer(WORDS, 20, 5, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, WORDS, 20, 5, 256, 8192);
    assertEquals(2, chunks.size());
    assertEquals("w16 w17 w18 w19 w20 ", chunks.get(1).text().substring(0, chunks.get(1).overlapChars()));
  }

  @Test
  void overlapZero() {
    List<String> pages = randomPages(new Random(9), 5, 262_144);
    List<PackedChunk> chunks = pack(packer(WORDS, 30, 0, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, WORDS, 30, 0, 256, 8192);
    assertTrue(chunks.stream().allMatch(c -> c.overlapChars() == 0));
  }

  @Test
  void noOverlapOnlyChunk() {
    for (int seed = 0; seed < 20; seed++) {
      List<String> pages = randomPages(new Random(seed), 3, 262_144);
      for (PackedChunk chunk : pack(packer(WORDS, 16, 8, 16, 256), pages, 262_144, 256)) {
        assertTrue(chunk.text().length() > chunk.overlapChars());
      }
    }
  }

  @Test
  void smallMaxTokensDefaultOverlapProgresses() {
    List<String> pages = randomPages(new Random(13), 10, 262_144);
    List<PackedChunk> chunks = pack(packer(BPE, 20, 10, 256, 8192), pages, 262_144, 8192);
    checkInvariants(chunks, BPE, 20, 10, 256, 8192);
  }

  @Test
  @Timeout(120)
  void terminatesOnAdversarialInput() {
    StringBuilder dots = new StringBuilder();
    for (int i = 0; i < 20_000; i++) {
      dots.append(". ");
    }
    StringBuilder bigWord = new StringBuilder();
    for (int i = 0; i < 262_144; i++) {
      bigWord.append((char) ('a' + i % 26));
    }
    String paragraphAfterOverlap = words("a", 10, ".") + "\n\n" + words("b", 2, ".") + "\n\n" + words("c", 10, ".");
    List<List<String>> inputs = Arrays.asList(
        Collections.singletonList(dots.toString().trim()),
        Arrays.asList(bigWord.toString(), bigWord.toString(), bigWord.toString(), bigWord.toString()),
        Collections.singletonList(paragraphAfterOverlap),
        Collections.singletonList(""));
    for (List<String> pages : inputs) {
      for (Tokenizer tokenizer : Arrays.asList(WORDS, BPE)) {
        List<PackedChunk> chunks = pack(packer(tokenizer, 16, 8, 16, 256), pages, 262_144, 256);
        checkInvariants(chunks, tokenizer, 16, 8, 16, 256);
      }
    }
  }

  // ------------------------------------------------------------------ tokenizer window and pull bound

  @Test
  void neverCalledAboveWindow() {
    AtomicInteger longest = new AtomicInteger();
    Tokenizer spy = new Tokenizer() {
      @Override
      public int countTokens(String text) {
        longest.accumulateAndGet(text.length(), Math::max);
        return BPE.countTokens(text);
      }

      @Override
      public int prefixEndWithin(String text, int maxTokens) {
        longest.accumulateAndGet(text.length(), Math::max);
        return Tokenizer.super.prefixEndWithin(text, maxTokens);
      }

      @Override
      public String id() {
        return "spy";
      }

      @Override
      public TokenUnit unit() {
        return TokenUnit.MODEL_TOKENS;
      }
    };
    StringBuilder bigWord = new StringBuilder();
    for (int i = 0; i < 100_000; i++) {
      bigWord.append('z');
    }
    List<String> pages = new ArrayList<>(randomPages(new Random(1), 5, 262_144));
    pages.add(bigWord.toString());
    pack(packer(spy, 256, 128, 256, 1024), pages, 262_144, 1024);
    assertTrue(longest.get() > 0 && longest.get() <= 1024, "longest tokenizer input " + longest.get());
  }

  @Test
  void zeroPrefixTestDoubleFailsFile() {
    Tokenizer nothingFits = new Tokenizer() {
      @Override
      public int countTokens(String text) {
        return text.isEmpty() ? 0 : 1000;
      }

      @Override
      public String id() {
        return "nothing-fits";
      }

      @Override
      public TokenUnit unit() {
        return TokenUnit.MODEL_TOKENS;
      }
    };
    ChunkPacker packer = packer(nothingFits, 16, 0, 256, 256);
    List<String> pages = Collections.singletonList("Hello world.");
    PdfFileFailure failure = assertThrows(PdfFileFailure.class, () -> pack(packer, pages, 262_144, 256));
    assertEquals(PdfFailureCode.TOKENIZER_ERROR, failure.code());
  }

  private static List<Integer> lengths(List<PackedChunk> chunks) {
    List<Integer> lengths = new ArrayList<>();
    for (PackedChunk chunk : chunks) {
      lengths.add(chunk.text().length());
    }
    return lengths;
  }

}
