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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link ChunkPacker} packing PDF page text, driven as the PDF chunker drives it: append a page, cut while no more
 * text is needed, then cut what is left at the end of input. Word-count tokens keep the expected chunks readable; the
 * word limit is set above the token limit so the token limit is the one that binds.
 */
public class TestChunkPackerLimits {

  private static final PackFailures FAILURES = (kind, message, cause) -> new IllegalStateException(kind + ": " + message, cause);
  private static final int MAX_WORDS = 100;

  @Test
  void packsWholeSentencesUpToTheTokenLimit() {
    ChunkPacker packer = packer(5, 0, 200);
    List<PackedChunk> chunks = pack(packer, "One two three. Four five six. Seven eight.");
    assertEquals(Arrays.asList("One two three. ", "Four five six. Seven eight."), texts(chunks));
    assertEquals(Arrays.asList(3, 5), tokens(chunks));
    assertEquals(0, packer.forcedSplits());
  }

  @Test
  void theTokenLimitBindsBelowTheWordLimit() {
    List<PackedChunk> chunks = pack(packer(3, 0, 200), "One two three. Four five six.");
    assertEquals(Arrays.asList("One two three. ", "Four five six."), texts(chunks));
  }

  @Test
  void prefersAParagraphEndWithEnoughNewText() {
    // (maxTokens - overlap) / 2 = 4 tokens before the paragraph end: the cut moves back to it
    List<PackedChunk> chunks = pack(packer(8, 0, 200), "One two three four.\n\nFive six. Seven eight nine.");
    assertEquals(Arrays.asList("One two three four.\n\n", "Five six. Seven eight nine."), texts(chunks));
  }

  @Test
  void ignoresAParagraphEndWithTooLittleNewText() {
    List<PackedChunk> chunks = pack(packer(8, 0, 200), "One.\n\nTwo three four five. Six seven eight nine.");
    assertEquals(Arrays.asList("One.\n\nTwo three four five. ", "Six seven eight nine."), texts(chunks));
  }

  @Test
  void splitsASentenceLongerThanTheLimitAtWords() {
    ChunkPacker packer = packer(4, 0, 200);
    List<PackedChunk> chunks = pack(packer, "w1 w2 w3 w4 w5 w6 w7 w8 w9 w10.");
    assertEquals(Arrays.asList("w1 w2 w3 w4 ", "w5 w6 w7 w8 ", "w9 w10."), texts(chunks));
    assertEquals(2, packer.forcedSplits());
  }

  @Test
  void splitsAWordLongerThanTheWindowAtCodePointsKeepingSurrogatePairs() {
    ChunkPacker packer = packer(4, 0, 16);
    // a surrogate pair at offsets 15-16 straddles the 16-char window: the first cut stops before it
    List<PackedChunk> chunks = pack(packer, "abcdefghijklmno😀pqrstuvwxyz0123");
    assertEquals(Arrays.asList("abcdefghijklmno", "😀pqrstuvwxyz012", "3"), texts(chunks));
    assertEquals(2, packer.forcedSplits());
  }

  @Test
  void repeatsWholeSentencesOfThePreviousChunkAsOverlap() {
    List<PackedChunk> chunks = pack(packer(6, 2, 200), "One two three. Four five six. Seven eight nine. Ten eleven twelve.");
    assertEquals(Arrays.asList("One two three. Four five six. ", "five six. Seven eight nine. ", "eight nine. Ten eleven twelve."),
        texts(chunks));
    assertEquals(Arrays.asList(0, 10, 12), overlaps(chunks));
    assertEquals(Arrays.asList(6, 5, 5), tokens(chunks));
  }

  @Test
  void dropsTheOverlapWhenTheNextSentenceNoLongerFitsAfterIt() {
    List<PackedChunk> chunks = pack(packer(6, 2, 200), "One two three four. Five six seven eight nine ten.");
    assertEquals(Arrays.asList("One two three four. ", "Five six seven eight nine ten."), texts(chunks));
    assertEquals(Arrays.asList(0, 0), overlaps(chunks));
  }

  @Test
  void anOverlapFromTheNextPageStartsTheChunkOnThatPage() {
    ChunkPacker packer = packer(6, 2, 200);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "One two three.", 1);
    feed(packer, chunks, "Four five six. Seven eight nine.", 2);
    finish(packer, chunks);
    assertEquals(Arrays.asList("One two three. Four five six. ", "five six. Seven eight nine."), texts(chunks));
    assertEquals(Arrays.asList(1, 2), firstMarks(chunks));
    assertEquals(Arrays.asList(2, 2), lastMarks(chunks));
  }

  @Test
  void joinsPagesWithOneSpaceAndSkipsEmptyPages() {
    ChunkPacker packer = packer(50, 0, 400);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "Alpha beta.", 1);
    feed(packer, chunks, "", 2);
    feed(packer, chunks, "Gamma delta.", 3);
    finish(packer, chunks);
    assertEquals(Arrays.asList("Alpha beta. Gamma delta."), texts(chunks));
    assertEquals(Arrays.asList(1), firstMarks(chunks));
    assertEquals(Arrays.asList(3), lastMarks(chunks));
    assertEquals(Arrays.asList(4), tokens(chunks));
  }

  @Test
  void chunksNameThePagesTheirTextComesFrom() {
    ChunkPacker packer = packer(3, 0, 200);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "One two three.", 1);
    feed(packer, chunks, "Four five six.", 2);
    finish(packer, chunks);
    assertEquals(Arrays.asList("One two three. ", "Four five six."), texts(chunks));
    assertEquals(Arrays.asList(1, 2), firstMarks(chunks));
    assertEquals(Arrays.asList(1, 2), lastMarks(chunks));
  }

  @Test
  void chunkEndingAPageExactlyLeavesTheNextPageItsOwnFirstMark() {
    // page 1 is one finished sentence of exactly a window: the cut takes all of it before page 2 arrives
    ChunkPacker packer = packer(4, 0, 16);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "abc def ghi jkl.", 1);
    assertEquals(0, packer.pendingLength());
    feed(packer, chunks, "Next.", 2);
    finish(packer, chunks);
    assertEquals(Arrays.asList("abc def ghi jkl.", "Next."), texts(chunks));
    assertEquals(Arrays.asList(1, 2), firstMarks(chunks));
    assertEquals(Arrays.asList(1, 2), lastMarks(chunks));
  }

  @Test
  void overlapCarriedOverAnExactPageEndKeepsTheJoinSpace() {
    ChunkPacker packer = packer(4, 2, 16);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "abc def ghi jkl.", 1);
    feed(packer, chunks, "Next.", 2);
    finish(packer, chunks);
    assertEquals(Arrays.asList("abc def ghi jkl.", "ghi jkl. Next."), texts(chunks));
    assertEquals(Arrays.asList(0, 8), overlaps(chunks));
    assertEquals(Arrays.asList(1, 1), firstMarks(chunks));
    assertEquals(Arrays.asList(1, 2), lastMarks(chunks));
  }

  /**
   * The overlap carried over an exact page end is dropped when the next page's first sentence does not fit after it: the join
   * space goes with the overlap, so the chunk starts with that page's own text and names that page as its first.
   */
  @Test
  void overlapDroppedAfterAnExactPageEndTakesTheJoinSpaceWithIt() {
    ChunkPacker packer = packer(4, 2, 16);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "abc def ghi jkl.", 1);
    feed(packer, chunks, "One two three.", 2);
    finish(packer, chunks);
    assertEquals(Arrays.asList("abc def ghi jkl.", "One two three."), texts(chunks));
    assertEquals(Arrays.asList(0, 0), overlaps(chunks));
    assertEquals(Arrays.asList(1, 2), firstMarks(chunks));
    assertEquals(Arrays.asList(1, 2), lastMarks(chunks));
  }

  /**
   * The same dropped overlap at a mid-stream cut: the next page is longer than the window, so the cut is made before the end of
   * input, and without the join space its first sentence still fits whole.
   */
  @Test
  void overlapDroppedAtAMidStreamCutTakesTheJoinSpaceWithIt() {
    ChunkPacker packer = packer(4, 2, 16);
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, "abc def ghi jkl.", 1);
    feed(packer, chunks, "One two three. Four.", 2);
    assertEquals(2, chunks.size(), "the second chunk was cut before the end of input");
    finish(packer, chunks);
    assertEquals("One two three. ", chunks.get(1).text());
    assertEquals(0, chunks.get(1).overlapChars());
    assertEquals(2, chunks.get(1).firstMark());
    assertEquals(0, packer.forcedSplits());
    for (PackedChunk chunk : chunks) {
      assertTrue(!chunk.text().startsWith(" "), "a chunk starts with a join space: '" + chunk.text() + "'");
    }
  }

  /**
   * Many pages of many sentences through a window far shorter than the text, so most cuts are made mid-stream
   * ({@code cut(false)}) under the pull rule: every chunk keeps the limits, its overlap repeats the end of the chunk before it,
   * the chunks without their overlaps rejoin to the stream (each page joined with one space to the text still held, or
   * with none after a cut that held nothing), and every chunk names the pages of its first and last chars.
   */
  @Test
  void streamsManyPagesThroughASmallWindowWithAndWithoutOverlap() {
    List<String> pages = pages(new Random(7), 14);
    for (int overlapTokens : new int[] {0, 2}) {
      List<PackedChunk> chunks = packAndCheck(new WordCountTokenizer(), 6, overlapTokens, 48, pages);
      assertTrue(chunks.size() > 10, "chunks: " + chunks.size());
      if (overlapTokens > 0) {
        assertTrue(overlaps(chunks).stream().anyMatch(chars -> chars > 0), "some chunk carries an overlap");
      }
    }
  }

  /** The same stream under the default BPE tokenizer, whose counts are not monotone in the prefix length. */
  @Test
  void streamsManyPagesUnderTheDefaultBpeTokenizer() {
    List<String> pages = pages(new Random(11), 10);
    for (int overlapTokens : new int[] {0, 4}) {
      packAndCheck(new OpenAiBpeTokenizer(), 12, overlapTokens, 64, pages);
    }
  }

  private static List<PackedChunk> packAndCheck(Tokenizer tokenizer, int maxTokens, int overlapTokens, int window,
                                                List<String> pages) {
    ChunkPacker packer = new ChunkPacker(new TokenBudget(tokenizer, maxTokens, MAX_WORDS, window, FAILURES), overlapTokens,
        Locale.ROOT);
    List<PackedChunk> chunks = new ArrayList<>();
    StringBuilder stream = new StringBuilder();
    List<Integer> pageOfChar = new ArrayList<>();
    for (int page = 1; page <= pages.size(); page++) {
      String text = pages.get(page - 1);
      // the packer joins a page to held text (pending or a carried overlap) with one space, and to nothing without one
      if (stream.length() > 0 && packer.retainedChars() > 0) {
        stream.append(' ');
        pageOfChar.add(page - 1);                     // the join space belongs to the previous page
      }
      stream.append(text);
      for (int i = 0; i < text.length(); i++) {
        pageOfChar.add(page);
      }
      feed(packer, chunks, text, page);
      assertTrue(packer.pendingLength() < 2 * window, "pending " + packer.pendingLength() + " after page " + page);
    }
    finish(packer, chunks);
    String context = "tokenizer " + tokenizer.id() + ", overlap " + overlapTokens;

    StringBuilder rejoined = new StringBuilder();
    String previous = "";
    for (PackedChunk chunk : chunks) {
      String text = chunk.text();
      assertTrue(text.length() <= window && chunk.tokenCount() <= maxTokens, context + ": over the limits: " + text);
      assertEquals(tokenizer.countTokens(text), chunk.tokenCount(), context);
      assertEquals(0, chunk.prefixChars(), context);
      String overlap = text.substring(0, chunk.overlapChars());
      assertTrue(previous.endsWith(overlap), context + ": overlap '" + overlap + "' is not the end of '" + previous + "'");
      int start = rejoined.length() - overlap.length();
      rejoined.append(text.substring(chunk.overlapChars()));
      assertEquals(pageOfChar.get(start).intValue(), chunk.firstMark(), context + ": first mark of '" + text + "'");
      assertEquals(pageOfChar.get(rejoined.length() - 1).intValue(), chunk.lastMark(), context + ": last mark of '" + text + "'");
      previous = text;
    }
    assertEquals(stream.toString(), rejoined.toString(), context);
    assertEquals(1, chunks.get(0).firstMark());
    assertEquals(pages.size(), chunks.get(chunks.size() - 1).lastMark());
    return chunks;
  }

  /** Normalized pages of one to five sentences of two to eight words, some ending a paragraph. */
  private static List<String> pages(Random random, int count) {
    String[] words = {"alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta", "theta", "iota", "kappa", "lambda", "mu"};
    List<String> pages = new ArrayList<>();
    for (int page = 0; page < count; page++) {
      StringBuilder text = new StringBuilder();
      int sentences = 1 + random.nextInt(5);
      for (int s = 0; s < sentences; s++) {
        if (s > 0) {
          text.append(random.nextInt(4) == 0 ? "\n\n" : " ");
        }
        int length = 2 + random.nextInt(7);
        for (int w = 0; w < length; w++) {
          String word = words[random.nextInt(words.length)];
          text.append(w == 0 ? Character.toUpperCase(word.charAt(0)) + word.substring(1) : " " + word);
        }
        text.append('.');
      }
      pages.add(text.toString());
    }
    return pages;
  }

  private static ChunkPacker packer(int maxTokens, int overlapTokens, int window) {
    return new ChunkPacker(new TokenBudget(new WordCountTokenizer(), maxTokens, MAX_WORDS, window, FAILURES), overlapTokens,
        Locale.ROOT);
  }

  private static List<PackedChunk> pack(ChunkPacker packer, String page) {
    List<PackedChunk> chunks = new ArrayList<>();
    feed(packer, chunks, page, 1);
    finish(packer, chunks);
    return chunks;
  }

  private static void feed(ChunkPacker packer, List<PackedChunk> chunks, String page, int number) {
    packer.appendPage(page, number);
    while (!packer.needsMoreText()) {
      chunks.add(packer.cut(false));
    }
  }

  private static void finish(ChunkPacker packer, List<PackedChunk> chunks) {
    while (packer.hasPending()) {
      chunks.add(packer.cut(true));
    }
  }

  private static List<String> texts(List<PackedChunk> chunks) {
    List<String> texts = new ArrayList<>();
    chunks.forEach(chunk -> texts.add(chunk.text()));
    return texts;
  }

  private static List<Integer> tokens(List<PackedChunk> chunks) {
    List<Integer> counts = new ArrayList<>();
    chunks.forEach(chunk -> counts.add(chunk.tokenCount()));
    return counts;
  }

  private static List<Integer> overlaps(List<PackedChunk> chunks) {
    List<Integer> chars = new ArrayList<>();
    chunks.forEach(chunk -> chars.add(chunk.overlapChars()));
    return chars;
  }

  private static List<Integer> firstMarks(List<PackedChunk> chunks) {
    List<Integer> marks = new ArrayList<>();
    chunks.forEach(chunk -> marks.add(chunk.firstMark()));
    return marks;
  }

  private static List<Integer> lastMarks(List<PackedChunk> chunks) {
    List<Integer> marks = new ArrayList<>();
    chunks.forEach(chunk -> marks.add(chunk.lastMark()));
    return marks;
  }
}
