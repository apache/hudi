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

import java.text.BreakIterator;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.Objects;

/**
 * Cuts the normalized text stream of one file into chunks, holding only bounded state.
 *
 * <p>The caller appends normalized pages while {@link #needsMoreText()} holds and pages remain, then calls
 * {@link #cut(boolean)}. A chunk starts with the overlap carried from the previous chunk and appends whole
 * sentences while the result {@link TokenBudget#fits fits}; when the next sentence does not fit it is cut at
 * the last paragraph boundary that leaves at least {@code (maxTokens - overlapTokens) / 2} tokens of new text, else before
 * that sentence. A sentence that does not fit even alone (overlap dropped) is split at words, and a word at
 * code points. In the PDF mode every chunk is an exact slice of the stream; in the declared-boundary
 * mode a chunk is the caller's prefix plus such a slice. Every chunk is re-counted after the cut.
 *
 * <p>The fit tests, the paragraph preference and the overlap choice count each growing candidate by its new part
 * ({@link TokenBudget#prefixes}, {@link TokenBudget#suffixes}) when the tokenizer adds up at word spaces, so a chunk costs
 * work in its length rather than in its length times its words. Those counts equal full counts exactly
 * ({@link Tokenizer#addsUpAtWordSpaces}), so the chunks are those of full counting; a tokenizer that does not add up
 * is counted in full.
 *
 * <p>Stream boundaries are the boundaries one {@link BreakIterator} sentence instance returns over the pending text,
 * and a paragraph ends where a sentence end follows {@code \n\n}; the boundaries inside a chunk are recorded
 * when it is cut and are the only overlap starts considered.
 *
 * <p>Two modes. The PDF chunker's ({@link #ChunkPacker(TokenBudget, int, Locale)}): pages appended with
 * {@link #appendPage}, boundaries from the {@link BreakIterator}, no chunk prefix, failures of type {@code PdfFileFailure}; every
 * chunk is an exact slice of the stream. The DECLARED-boundary mode ({@link #withDeclaredBoundaries}), for text whose units
 * the caller knows (the transcript packer: an utterance end is a sentence boundary, a speaker turn a paragraph boundary):
 * pieces appended with {@link #appendSegment} name the boundary after them, which replaces the {@link BreakIterator} and
 * the {@code \n\n} test; a {@link ChunkPrefix} may put a prefix before each chunk (counted in every fit test, the forced
 * split and the re-count, never part of the overlap). Both modes run the same pull, cut, split and overlap rules.
 *
 * <p>Marks: every appended piece carries an int mark, which the PDF mode names a page (hence {@code pageAt}, {@code lastPage},
 * {@link #pageMarkers()}) and the transcript packer an utterance number; {@link #appendSegment} appends one piece (for the
 * transcript packer, one utterance with its separator), not an audio segment.
 */
public final class ChunkPacker {

  private static final int[] NONE = new int[0];

  /** The boundary after a piece of declared-boundary text. */
  public enum Boundary {
    /** No boundary: the next piece continues the sentence. */
    NONE,
    /** A sentence ends after the piece. */
    SENTENCE,
    /** A sentence and a paragraph end after the piece. */
    PARAGRAPH
  }

  /** The prefix put before a chunk in declared-boundary mode. */
  @FunctionalInterface
  public interface ChunkPrefix {

    /**
     * The text to put before the chunk whose first stream char was appended under {@code firstMark} and whose stream text
     * starts with {@code start} (its overlap, else at least its first sentence); empty for none. Called before every cut.
     */
    String prefixFor(int firstMark, String start);
  }

  private final TokenBudget budget;
  private final int maxTokens;
  private final int overlapTokens;
  private final int maxWords;
  private final int window;
  /** The PDF mode's sentence boundaries; null in declared-boundary mode. */
  private final BreakIterator sentences;
  /** The declared-boundary mode's chunk prefix; null in the PDF mode. */
  private final ChunkPrefix prefix;
  /**
   * Declared-boundary mode: the boundary at the end of the pending text (after the last appended piece), as a
   * {@link Boundary} ordinal. A boundary inside the pending text is kept on the mark of the piece that starts there (its
   * third element), so it needs no container of its own and leaves with that mark.
   */
  private int endBoundary;
  /** The sentence cursor over the scanned text, created on first use. */
  private SentenceCursor cursor;

  /** Normalized text not yet emitted as new text; bounded by the pull rule. */
  private final StringBuilder pending = new StringBuilder();
  /** Sentence boundaries found by the last {@link #usableEnds} scan, ascending (the chunk's stream boundaries). */
  private int[] scanBoundaries = NONE;
  /** Page starts inside {@link #pending}: {offset, 1-based page}; at most pending chars + 1 entries. */
  private final ArrayDeque<int[]> marks = new ArrayDeque<>();
  private boolean pendingStartIsBoundary = true;
  /** Whether the last cut fell inside a word (a code-point split), so the pending text starts mid-word. */
  private boolean cutInsideWord;
  private boolean pendingStartsMidWord;
  /** Whether the pending text starts with the join space put after a carried-over overlap (the last cut took all pending). */
  private boolean pendingStartsWithOverlapJoin;
  private boolean streamHasText;
  private int lastPage;

  private String overlap = "";
  private boolean overlapStartIsBoundary;
  private int[] overlapBoundaries = NONE;
  private int[] overlapMarkOffsets = NONE;
  private int[] overlapMarkPages = NONE;

  private long forcedSplits;

  /** The PDF chunker's packer: {@link BreakIterator} sentences of {@code locale}, no chunk prefix. */
  public ChunkPacker(TokenBudget budget, int overlapTokens, Locale locale) {
    this(budget, overlapTokens, BreakIterator.getSentenceInstance(locale), null);
  }

  private ChunkPacker(TokenBudget budget, int overlapTokens, BreakIterator sentences, ChunkPrefix prefix) {
    this.budget = budget;
    this.maxTokens = budget.maxTokens();
    this.overlapTokens = overlapTokens;
    this.maxWords = budget.maxWords();
    this.window = budget.windowChars();
    this.sentences = sentences;
    this.prefix = prefix;
  }

  /**
   * A packer of text appended with {@link #appendSegment}, whose sentence and paragraph boundaries the caller declares, and
   * whose chunks start with {@code prefix}'s text.
   *
   * @param budget        the chunk limits; its failures are this packer's
   * @param overlapTokens tokens of overlap at most
   * @param prefix        the chunk prefix
   */
  public static ChunkPacker withDeclaredBoundaries(TokenBudget budget, int overlapTokens, ChunkPrefix prefix) {
    return new ChunkPacker(budget, overlapTokens, null, Objects.requireNonNull(prefix, "prefix"));
  }

  /**
   * Appends one non-empty piece of normalized text under {@code mark} (declared-boundary mode); pieces join with nothing,
   * so the caller supplies the separators, and {@code after} names the boundary at the piece's end.
   *
   * @throws IllegalStateException in the PDF mode
   * @throws IllegalArgumentException for an empty piece
   */
  public void appendSegment(String text, int mark, Boundary after) {
    if (sentences != null) {
      throw new IllegalStateException("appendSegment is the declared-boundary mode's; the PDF mode appends pages");
    }
    if (text.isEmpty()) {
      throw new IllegalArgumentException("an appended piece must not be empty (mark " + mark + ")");
    }
    marks.addLast(new int[] {pending.length(), mark, pending.length() == 0 ? 0 : endBoundary});
    pending.append(text);
    streamHasText = true;
    lastPage = mark;
    endBoundary = after.ordinal();
  }

  /**
   * Appends one normalized page; pages join with ONE space and an empty page contributes nothing. A page that starts the
   * next chunk with nothing before it (the last cut took all pending text and kept no overlap) gets no join space, so the
   * chunk starts with the page's own text and names that page as its first. The join space after a carried-over overlap
   * belongs to the overlap: a cut that drops the overlap drops that space too.
   */
  public void appendPage(String normalizedText, int page) {
    if (sentences == null) {
      throw new IllegalStateException("appendPage is the PDF mode's; the declared-boundary mode appends segments");
    }
    if (normalizedText.isEmpty()) {
      return;
    }
    if (streamHasText && (pending.length() > 0 || !overlap.isEmpty())) {
      if (pending.length() == 0) {
        marks.addLast(new int[] {0, lastPage});
        pendingStartsWithOverlapJoin = true;
      }
      pending.append(' ');                            // the join space belongs to the previous page
    }
    marks.addLast(new int[] {pending.length(), page});
    pending.append(normalizedText);
    streamHasText = true;
    lastPage = page;
  }

  /**
   * The pull rule: more text is needed while the pending text is shorter than the window, or its unfinished last
   * sentence starts inside the window and is shorter than the window.
   */
  public boolean needsMoreText() {
    int len = pending.length();
    if (len < window) {
      return true;
    }
    if (len >= 2 * window) {
      return false;                                   // the last sentence cannot both start inside the window and be shorter
    }
    int lastStart = lastSentenceStart();
    return lastStart < window && len - lastStart < window;
  }

  /** Whether text is pending that {@link #cut} would emit. */
  public boolean hasPending() {
    return pending.length() > 0;
  }

  /** The tokens of a cut chunk, re-counted; a chunk over any limit is an internal failure. */
  private int recountedTokens(String chunkText, int words) {
    int tokens = chunkText.length() <= window ? budget.count(chunkText) : Integer.MAX_VALUE;
    if (chunkText.length() > window || words > maxWords || tokens > maxTokens) {
      throw budget.failure(PackFailures.Kind.INTERNAL, "re-count after cut: " + chunkText.length() + " chars, " + words
          + " words, " + tokens + " tokens exceed the limits");
    }
    return tokens;
  }

  /** The overlap the next chunk starts with: none at the end of the file or without overlap tokens. */
  private void nextOverlap(String slice, String ov, int cutPos, boolean fileDone) {
    if (overlapTokens > 0 && !fileDone) {
      // the overlap comes from the stream slice, never from the prefix
      chooseOverlap(slice, chunkBoundaries(ov, cutPos), chunkMarks(ov, cutPos), ov.isEmpty() && pendingStartsMidWord);
    } else {
      clearOverlap();
    }
  }

  /**
   * Cuts the next chunk, or returns null when nothing is pending. Call only when {@link #needsMoreText()} is false or
   * {@code endOfInput} is true.
   */
  public PackedChunk cut(boolean endOfInput) {
    int len = pending.length();
    if (len == 0) {
      return null;
    }
    // a chunk is at most a window long, so the cut never needs text beyond 2 windows: scan only that prefix, so the
    // work per cut is bounded by the config and not by the page size
    String text = pending.substring(0, Math.min(len, 2 * window + 1));
    int[] ends = usableEnds(text, len, endOfInput);
    String ov = overlap;
    String pre = prefixFor(ov, text);
    int cutPos = greedy(text, ends, pre + ov);
    if (cutPos == 0 && !ov.isEmpty()) {
      ov = "";                                        // the overlap is "at max", drop it for this chunk
      if (pendingStartsWithOverlapJoin) {
        // the join space and its page mark belong to the dropped overlap: the chunk starts with the next page's own text
        consume(1);
        pendingStartsMidWord = false;
        len = pending.length();
        text = pending.substring(0, Math.min(len, 2 * window + 1));
        ends = usableEnds(text, len, endOfInput);
      }
      pre = prefixFor(ov, text);
      cutPos = greedy(text, ends, pre);
    }
    boolean forced = cutPos == 0;
    cutInsideWord = false;
    if (forced) {
      cutPos = forcedSplit(text, ends.length > 0 ? ends[0] : text.length(), pre);
      forcedSplits++;
    } else if (cutPos < len) {
      cutPos = preferParagraph(text, ends, cutPos);
    }

    String slice = ov.isEmpty() ? text.substring(0, cutPos) : ov + text.substring(0, cutPos);
    String chunkText = pre.isEmpty() ? slice : pre + slice;
    int words = TokenBudget.words(chunkText);
    int tokens = recountedTokens(chunkText, words);
    int pageStart = ov.isEmpty() ? pageAt(0) : overlapMarkPages[0];
    int pageEnd = pageAt(cutPos - 1);
    PackedChunk chunk = new PackedChunk(chunkText, ov.length(), pre.length(), pageStart, pageEnd, tokens, words);

    nextOverlap(slice, ov, cutPos, endOfInput && cutPos == len);
    consume(cutPos);
    pendingStartIsBoundary = !forced;
    pendingStartsMidWord = cutInsideWord;
    return chunk;
  }

  /** Cuts so far whose first sentence did not fit alone and was split at words or code points. */
  public long forcedSplits() {
    return forcedSplits;
  }

  /** Chars of pending text; below 2 windows whenever {@link #needsMoreText()} asks for a page. */
  public int pendingLength() {
    return pending.length();
  }

  /** Chars held: pending text plus the overlap (part of the retained bound). */
  public int retainedChars() {
    return pending.length() + overlap.length();
  }

  /** Page markers held. */
  public int pageMarkers() {
    return marks.size() + overlapMarkOffsets.length;
  }

  /**
   * The smallest mark the next chunk can carry: its overlap's first mark, else the pending text's first mark; -1 when nothing
   * is held. A caller keeping per-mark data may drop every mark below it.
   */
  public int firstHeldMark() {
    if (!overlap.isEmpty()) {
      return overlapMarkPages[0];
    }
    return pending.length() > 0 ? pageAt(0) : -1;
  }

  // ------------------------------------------------------------------ sentences

  /** The chunk prefix for a chunk starting with {@code ov} (else with {@code text}); empty in the PDF mode. */
  private String prefixFor(String ov, String text) {
    if (prefix == null) {
      return "";
    }
    String result = prefix.prefixFor(ov.isEmpty() ? pageAt(0) : overlapMarkPages[0], ov.isEmpty() ? text : ov);
    return result == null ? "" : result;
  }

  private SentenceCursor cursor() {
    if (cursor == null) {
      cursor = sentences != null ? new BreakIteratorCursor(sentences) : new DeclaredCursor();
    }
    return cursor;
  }

  /** Start of the last sentence of the pending text, which is shorter than 2 windows here. */
  private int lastSentenceStart() {
    SentenceCursor last = cursor();
    last.setText(pending);
    last.last();
    int start = last.previous();
    return start == BreakIterator.DONE ? 0 : start;
  }

  /**
   * Sentence ends usable for this cut, ascending: every boundary before the end of the pending text, up to and including
   * the first one beyond the window, plus the end itself when the last sentence is finished — the input has ended, or it
   * starts at or beyond the window, or it is at least a window long. {@code text} is the scanned prefix of the
   * pending text ({@code pendingLength} chars); the end of a shorter prefix is not a boundary. The boundaries found are
   * kept in {@link #scanBoundaries}.
   */
  private int[] usableEnds(String text, int pendingLength, boolean endOfInput) {
    int len = text.length();
    int[] ends = new int[16];
    int count = 0;
    SentenceCursor scan = cursor();
    scan.setText(text);
    int start = scan.first();
    for (int b = scan.next(); b != BreakIterator.DONE && sentenceFinished(b, len, pendingLength, endOfInput, start); b = scan.next()) {
      if (count == ends.length) {
        ends = Arrays.copyOf(ends, count * 2);
      }
      ends[count++] = b;
      if (b > window) {
        break;
      }
      start = b;
    }
    scanBoundaries = Arrays.copyOf(ends, count);
    return scanBoundaries;
  }

  /**
   * Whether the sentence ending at {@code b} of a scanned prefix of {@code len} chars is finished: it ends inside the prefix, or the
   * prefix is the whole pending text and the input has ended, the sentence starts at or beyond the window or is a window long.
   */
  private boolean sentenceFinished(int b, int len, int pendingLength, boolean endOfInput, int start) {
    return b < len || (len == pendingLength && (endOfInput || start >= window || len - start >= window));
  }

  /**
   * The longest run of whole sentences that fits after {@code ov}; 0 when not even the first does. The candidates share
   * one {@link TokenBudget.Prefixes}, so each is counted by its new part.
   */
  private int greedy(String text, int[] ends, String ov) {
    TokenBudget.Prefixes candidates =
        budget.prefixes(ov + text.substring(0, Math.min(text.length(), Math.max(0, window - ov.length()))));
    int cut = 0;
    for (int end : ends) {
      if (ov.length() + end > window || !candidates.fits(ov.length() + end)) {
        break;
      }
      cut = end;
    }
    return cut;
  }

  /**
   * The paragraph preference: the LAST sentence end at or before {@code cut} that closes a paragraph
   * ({@code \n\n}) and leaves at least {@code (maxTokens - overlapTokens) / 2} tokens of new text before it; else {@code cut}.
   */
  private int preferParagraph(String text, int[] ends, int cut) {
    int minNewTokens = (maxTokens - overlapTokens) / 2;
    TokenBudget.Prefixes heads = budget.prefixes(text.substring(0, cut));
    for (int i = ends.length - 1; i >= 0; i--) {
      int end = ends[i];
      if (end > cut || !endsParagraph(text, end)) {
        continue;
      }
      if (heads.count(end) >= minNewTokens) {
        return end;
      }
    }
    return cut;
  }

  /** Whether the sentence end {@code end} of the scanned {@code text} also ends a paragraph. */
  private boolean endsParagraph(String text, int end) {
    if (sentences != null) {
      return end >= 2 && text.charAt(end - 1) == '\n' && text.charAt(end - 2) == '\n';
    }
    return declaredAt(end) == Boundary.PARAGRAPH.ordinal();
  }

  /**
   * The first sentence ({@code text[0, sentenceEnd)}) does not fit alone after the chunk prefix {@code pre}, so
   * append its words while the candidate fits; if not even the first word fits, cut that word at code points. A prefix that
   * does not fit alone, or not even one code point after it, is a {@link PackFailures.Kind#TOKENIZER_ERROR}.
   */
  private int forcedSplit(String text, int sentenceEnd, String pre) {
    if (!pre.isEmpty() && !budget.fits(pre)) {
      throw budget.failure(PackFailures.Kind.TOKENIZER_ERROR, "the chunk prefix of " + pre.length() + " chars does not fit "
          + maxTokens + " tokens alone");
    }
    int room = window - pre.length();
    int limit = Math.min(Math.min(sentenceEnd, text.length()), room + 1);
    TokenBudget.Prefixes candidates = budget.prefixes(pre + text.substring(0, Math.min(limit, room)));
    int lastFit = 0;
    int firstWordEnd = -1;
    for (int end = nextWordEnd(text, 0, limit); end > 0; end = nextWordEnd(text, end, limit)) {
      if (firstWordEnd < 0) {
        firstWordEnd = end;
      }
      if (end > room || !candidates.fits(pre.length() + end)) {
        break;
      }
      lastFit = end;
    }
    if (lastFit > 0) {
      return lastFit;
    }
    return splitInsideWord(text, pre, firstWordEnd < 0 ? limit : firstWordEnd, room);
  }

  /** No whole word fits: the longest prefix of the first word (capped at the window) that fits, cut inside the word. */
  private int splitInsideWord(String text, String pre, int wordEnd, int room) {
    // cap at the window on a code-point boundary, so a split never falls inside a surrogate pair
    String word = text.substring(0, Utf16.floor(text, Math.max(0, Math.min(wordEnd, room))));
    int preTokens = pre.isEmpty() ? 0 : budget.count(pre);
    int end = preTokens < maxTokens ? budget.prefixEndWithin(word, maxTokens - preTokens) : 0;
    end = Math.max(0, Math.min(end, word.length()));
    end = Utf16.floor(word, end);
    while (end > 0 && !budget.fits(pre + word.substring(0, end))) {
      end = word.offsetByCodePoints(end, -1);
    }
    if (end == 0 && !word.isEmpty()) {
      end = budget.checkedPrefixEnd(pre, word);
    }
    if (end == 0) {
      throw budget.failure(PackFailures.Kind.TOKENIZER_ERROR, "not even one code point fits " + maxTokens + " tokens");
    }
    cutInsideWord = true;
    return end;
  }

  /** End of the word starting at {@code from} (its non-whitespace run plus trailing whitespace), capped at limit; -1 at limit. */
  private static int nextWordEnd(String text, int from, int limit) {
    if (from >= limit) {
      return -1;
    }
    int i = from;
    while (i < limit && Character.isWhitespace(text.charAt(i))) {
      i++;                                            // leading whitespace joins the first word
    }
    while (i < limit && !Character.isWhitespace(text.charAt(i))) {
      i++;
    }
    while (i < limit && Character.isWhitespace(text.charAt(i))) {
      i++;
    }
    return i;
  }

  // ------------------------------------------------------------------ overlap

  /** Stream boundaries inside the chunk {@code ov + pending[0, cut)}, relative to the chunk, ascending. */
  private int[] chunkBoundaries(String ov, int cut) {
    int[] result = new int[16];
    int count = 0;
    if (ov.isEmpty() ? pendingStartIsBoundary : overlapStartIsBoundary) {
      result[count++] = 0;
    }
    if (!ov.isEmpty()) {
      for (int b : overlapBoundaries) {
        result = grow(result, count);
        result[count++] = b;
      }
      if (pendingStartIsBoundary) {
        result = grow(result, count);
        result[count++] = ov.length();
      }
    }
    for (int b : scanBoundaries) {
      if (b >= cut) {
        break;
      }
      result = grow(result, count);
      result[count++] = ov.length() + b;
    }
    return Arrays.copyOf(result, count);
  }

  /** Page starts inside the chunk {@code ov + pending[0, cut)} as {offsets[], pages[]}, the first at offset 0. */
  private int[][] chunkMarks(String ov, int cut) {
    int[] offsets = new int[overlapMarkOffsets.length + marks.size()];
    int[] pages = new int[offsets.length];
    int count = 0;
    for (int i = 0; i < overlapMarkOffsets.length && !ov.isEmpty(); i++) {
      offsets[count] = overlapMarkOffsets[i];
      pages[count++] = overlapMarkPages[i];
    }
    for (int[] mark : marks) {
      if (mark[0] < cut) {
        offsets[count] = ov.length() + mark[0];
        pages[count++] = mark[1];
      }
    }
    return new int[][] {Arrays.copyOf(offsets, count), Arrays.copyOf(pages, count)};
  }

  /**
   * The overlap: the longest suffix starting at a recorded stream boundary with at most {@code overlapTokens} tokens and
   * {@code maxWords / 2} words; else the longest whole-word suffix meeting the same limits; always at most
   * {@code window / 2} chars. None qualifying: no overlap.
   */
  private void chooseOverlap(String chunk, int[] boundaries, int[][] chunkMarks, boolean startsMidWord) {
    int n = chunk.length();
    int minStart = Math.max(0, n - window / 2);
    TokenBudget.Suffixes suffixes = budget.suffixes(chunk, minStart);
    int chosen = boundaryOverlapStart(suffixes, boundaries, minStart, n);
    boolean aligned = chosen >= 0;
    if (!aligned) {
      chosen = wordOverlapStart(suffixes, chunk, minStart, startsMidWord);
    }
    if (chosen < 0) {
      clearOverlap();
      return;
    }
    overlap = chunk.substring(chosen);
    overlapStartIsBoundary = aligned;
    int count = 0;
    int[] inner = new int[boundaries.length];
    for (int b : boundaries) {
      if (b > chosen && b < n) {
        inner[count++] = b - chosen;
      }
    }
    overlapBoundaries = Arrays.copyOf(inner, count);
    int[] offsets = chunkMarks[0];
    int[] pages = chunkMarks[1];
    int first = 0;
    while (first + 1 < offsets.length && offsets[first + 1] <= chosen) {
      first++;
    }
    overlapMarkOffsets = new int[offsets.length - first];
    overlapMarkPages = new int[offsets.length - first];
    for (int i = first; i < offsets.length; i++) {
      overlapMarkOffsets[i - first] = i == first ? 0 : offsets[i] - chosen;
      overlapMarkPages[i - first] = pages[i];
    }
  }

  /** The first recorded stream boundary in {@code [minStart, n)} that qualifies as an overlap start; -1 if none. */
  private int boundaryOverlapStart(TokenBudget.Suffixes suffixes, int[] boundaries, int minStart, int n) {
    for (int b : boundaries) {
      if (b >= minStart && b < n && qualifiesAsOverlap(suffixes, b)) {
        return b;
      }
    }
    return -1;
  }

  /**
   * The longest qualifying whole-word suffix start from {@code minStart} on; -1 if none. Longest first: every whole-word start
   * from the char cap on is scanned (BPE counts are not monotone, so a tokenizer seed could skip a longer qualifying suffix);
   * bounded by window / 2 starts.
   */
  private int wordOverlapStart(TokenBudget.Suffixes suffixes, String chunk, int minStart, boolean startsMidWord) {
    for (int p = minStart; p < chunk.length(); p++) {
      boolean wordStart = !Character.isWhitespace(chunk.charAt(p))
          && (p == 0 ? !startsMidWord : Character.isWhitespace(chunk.charAt(p - 1)));
      if (wordStart && qualifiesAsOverlap(suffixes, p)) {
        return p;
      }
    }
    return -1;
  }

  private boolean qualifiesAsOverlap(TokenBudget.Suffixes suffixes, int start) {
    return suffixes.words(start) <= maxWords / 2 && suffixes.count(start) <= overlapTokens;
  }

  private void clearOverlap() {
    overlap = "";
    overlapStartIsBoundary = false;
    overlapBoundaries = NONE;
    overlapMarkOffsets = NONE;
    overlapMarkPages = NONE;
  }

  // ------------------------------------------------------------------ pages

  private int pageAt(int offset) {
    int page = lastPage;
    for (int[] mark : marks) {
      if (mark[0] > offset) {
        break;
      }
      page = mark[1];
    }
    return page;
  }

  private void consume(int cut) {
    pendingStartsWithOverlapJoin = false;
    int nextPage = cut < pending.length() ? pageAt(cut) : lastPage;
    pending.delete(0, cut);
    Iterator<int[]> it = marks.iterator();
    while (it.hasNext()) {
      int[] mark = it.next();
      if (mark[0] <= cut) {
        it.remove();
      } else {
        mark[0] -= cut;
      }
    }
    if (pending.length() > 0 && (marks.isEmpty() || marks.peekFirst()[0] > 0)) {
      marks.addFirst(new int[] {0, nextPage});
    }
  }

  private static int[] grow(int[] array, int count) {
    return count < array.length ? array : Arrays.copyOf(array, array.length * 2);
  }

  /** Declared-boundary mode: the {@link Boundary} ordinal declared at {@code offset} of the pending text (0 for none). */
  private int declaredAt(int offset) {
    if (offset == pending.length()) {
      return endBoundary;
    }
    for (int[] mark : marks) {
      if (mark[0] == offset) {
        return mark.length > 2 ? mark[2] : 0;
      }
      if (mark[0] > offset) {
        break;
      }
    }
    return 0;
  }

  /** The part of {@link BreakIterator} the packer uses, over the scanned prefix of the pending text. */
  private interface SentenceCursor {
    void setText(CharSequence text);

    int first();

    int next();

    int last();

    int previous();
  }

  /** The PDF mode: the locale's sentence instance. */
  private static final class BreakIteratorCursor implements SentenceCursor {
    private final BreakIterator sentences;

    private BreakIteratorCursor(BreakIterator sentences) {
      this.sentences = sentences;
    }

    @Override
    public void setText(CharSequence text) {
      sentences.setText(text.toString());
    }

    @Override
    public int first() {
      return sentences.first();
    }

    @Override
    public int next() {
      return sentences.next();
    }

    @Override
    public int last() {
      return sentences.last();
    }

    @Override
    public int previous() {
      return sentences.previous();
    }
  }

  /**
   * The declared-boundary mode, with a {@link BreakIterator}'s contract over the scanned text: 0, the declared boundaries
   * inside it, and its end (which, as for a {@link BreakIterator}, is returned even when no boundary was declared there; the
   * cut's finished rule decides whether it is usable).
   */
  private final class DeclaredCursor implements SentenceCursor {
    private int[] boundaries = NONE;
    private int position;

    @Override
    public void setText(CharSequence text) {
      int len = text.length();
      int[] found = new int[marks.size() + 2];
      int count = 0;
      found[count++] = 0;
      for (int[] mark : marks) {
        if (mark[0] > 0 && mark[0] < len && mark.length > 2 && mark[2] != 0) {
          found[count++] = mark[0];
        }
      }
      if (len > 0) {
        found[count++] = len;
      }
      boundaries = Arrays.copyOf(found, count);
      position = 0;
    }

    @Override
    public int first() {
      position = 0;
      return boundaries[0];
    }

    @Override
    public int next() {
      if (position + 1 >= boundaries.length) {
        position = boundaries.length;
        return BreakIterator.DONE;
      }
      return boundaries[++position];
    }

    @Override
    public int last() {
      position = boundaries.length - 1;
      return boundaries[position];
    }

    @Override
    public int previous() {
      if (position <= 0) {
        position = 0;
        return BreakIterator.DONE;
      }
      return boundaries[--position];
    }
  }
}
