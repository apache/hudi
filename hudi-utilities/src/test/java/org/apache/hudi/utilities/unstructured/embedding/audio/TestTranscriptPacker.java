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

package org.apache.hudi.utilities.unstructured.embedding.audio;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The DEFAULT transcript chunker ({@link AudioConfig#DEFAULT_CHUNKER_CLASS}, transcript-pack-v2) over the RECORDED whisper
 * fixture ({@link RecordedUtterances}) and over built utterances: every chunk at or below the limit under openai-bpe and
 * word-count, its token count the tokenizer's count of its text, every chunk starting with its speaker label, the PDF overlap
 * between adjacent chunks, start_ms of the first utterance whose text (overlap included) is in the chunk and end_ms the latest
 * end of those utterances,
 * word and code-point splits, speaker turns as lines, normalisation, utterances pulled only while it needs text, and closing.
 */
class TestTranscriptPacker {

  private static final String LABEL = SpeakerLabels.line(SpeakerLabels.FILE_SPEAKER, "");
  private static final char NBSP = (char) 0xA0;
  private static final char ZERO_WIDTH_SPACE = (char) 0x200B;

  static Stream<Arguments> budgets() {
    return Stream.of(
        Arguments.of(new OpenAiBpeTokenizer(), 256, -1),
        Arguments.of(new OpenAiBpeTokenizer(), 64, 32),
        Arguments.of(new OpenAiBpeTokenizer(), 16, -1),
        Arguments.of(new WordCountTokenizer(), 256, -1),
        Arguments.of(new WordCountTokenizer(), 48, 24),
        Arguments.of(new WordCountTokenizer(), 16, 8));
  }

  @Test
  void theDefaultChunkerIsTheTranscriptPacker() {
    TranscriptChunker chunker = AudioConfig.from(new TypedProperties()).newChunker();
    assertEquals("transcript-pack-v2", chunker.logicVersion());
  }

  /** The recorded fixture: limits, labels, coverage of the stream with overlap, and the chunk times. */
  @ParameterizedTest
  @MethodSource("budgets")
  void recordedWhisperUtterancesPackUnderTheLimitWithOverlapLabelsAndTimes(Tokenizer tokenizer, int maxTokens,
                                                                                 int overlapTokens) {
    List<TranscriptChunker.SegmentUtterance> utterances = RecordedUtterances.all();
    assertEquals(41, utterances.size(), "the recorded fixture");
    List<TranscriptChunk> chunks = pack(utterances, 7, tokenizer, maxTokens, overlapTokens);
    Stream0 stream = new Stream0(utterances);

    assertTrue(chunks.size() > 1, "the fixture makes several chunks: " + chunks.size());
    int prevEnd = 0;
    int overlaps = 0;
    for (int i = 0; i < chunks.size(); i++) {
      TranscriptChunk chunk = chunks.get(i);
      assertEquals(7 + i, chunk.ordinal(), "dense ordinals from firstOrdinal");
      assertTrue(chunk.text().startsWith(LABEL), "every chunk starts with its label: " + chunk.text());
      assertEquals(tokenizer.countTokens(chunk.text()), chunk.tokenCount(), "token_count is the count of the text, label included");
      assertTrue(chunk.tokenCount() <= maxTokens, chunk.tokenCount() + " > " + maxTokens);
      assertEquals(Collections.singletonList(SpeakerLabels.FILE_SPEAKER), chunk.speakers());
      int[] located = stream.locate(chunk.text(), prevEnd);
      int start = located[0];
      int end = located[1];
      assertTrue(start <= prevEnd, "chunk " + i + " continues the stream: no gap, no reordering");
      assertTrue(end > prevEnd, "chunk " + i + " has new text");
      if (i == 0) {
        assertEquals(0, start);
      } else if (start < prevEnd) {
        overlaps++;
        assertTrue(chunks.get(i - 1).text().endsWith(stream.text.substring(start, prevEnd)), "the overlap is the previous suffix");
      }
      TranscriptSegment first = stream.utteranceAt(start);
      assertEquals(first.getStartMs(), chunk.startMs(), "start_ms of the first utterance with text in chunk " + i);
      assertEquals(stream.latestEndIn(start, end), chunk.endMs(), "end_ms: the latest end of the utterances in chunk " + i);
      assertEquals(stream.segmentAt(start), chunk.firstSegment());
      assertEquals(stream.segmentAt(end - 1), chunk.lastSegment());
      prevEnd = end;
    }
    assertEquals(stream.text.length(), prevEnd, "the chunks cover the whole transcript");
    assertTrue(overlaps > 0, "adjacent chunks overlap (the stub chunker never does)");
  }

  /**
   * Utterance ends are not non-decreasing: [0, 10000] then [2000, 4000] packed into one chunk ends at 10000, the latest end of
   * its utterances, not at the last utterance's 4000.
   */
  @Test
  void chunkEndsAtTheLatestEndOfItsUtterances() {
    List<TranscriptChunk> chunks = pack(Arrays.asList(utterance(0, "A long statement.", 0, 10000),
        utterance(0, "An aside.", 2000, 4000)), 0, new WordCountTokenizer(), 512, 0);
    assertEquals(1, chunks.size(), chunks.toString());
    assertEquals(LABEL + "A long statement. An aside.", chunks.get(0).text());
    assertEquals(0, chunks.get(0).startMs());
    assertEquals(10000, chunks.get(0).endMs());
  }

  @Test
  void utteranceLongerThanTheLimitIsSplitAtWordsEachPieceLabelledAndTimedByItsUtterance() {
    WordCountTokenizer words = new WordCountTokenizer();
    String longText = String.join(" ", Collections.nCopies(60, "word")) + ".";
    List<TranscriptChunk> chunks = pack(Arrays.asList(utterance(0, "Short start.", 0, 900),
        utterance(1, longText, 1000, 9000)), 0, words, 16, 0);
    assertTrue(chunks.size() >= 5, chunks.toString());
    StringBuilder bodies = new StringBuilder();
    for (int i = 0; i < chunks.size(); i++) {
      TranscriptChunk chunk = chunks.get(i);
      assertTrue(chunk.text().startsWith(LABEL), chunk.text());
      assertTrue(chunk.tokenCount() <= 16 && chunk.tokenCount() == words.countTokens(chunk.text()));
      bodies.append(chunk.text().substring(LABEL.length()));
      if (i > 0) {
        assertEquals(1000, chunk.startMs(), "a split piece carries its utterance's start (no per-word times)");
        assertEquals(9000, chunk.endMs());
      }
      assertFalse(chunk.text().substring(LABEL.length()).contains(LABEL.trim()), "the label is once per chunk here");
    }
    assertEquals("Short start. " + longText, bodies.toString(), "the pieces are the utterances, split at words");
  }

  @Test
  void wordLongerThanTheLimitIsSplitAtCodePoints() {
    OpenAiBpeTokenizer bpe = new OpenAiBpeTokenizer();
    StringBuilder word = new StringBuilder();
    for (int i = 0; i < 400; i++) {
      word.append((char) ('a' + (i * 7) % 26)).append(i % 5 == 0 ? new String(Character.toChars(0x20000)) : "");
    }
    List<TranscriptChunk> chunks = pack(Collections.singletonList(utterance(0, word.toString(), 0, 5000)), 0, bpe, 16, 0);
    assertTrue(chunks.size() > 2, chunks.toString());
    StringBuilder joined = new StringBuilder();
    for (TranscriptChunk chunk : chunks) {
      assertTrue(chunk.text().startsWith(LABEL), chunk.text());
      assertTrue(chunk.tokenCount() <= 16, chunk.toString());
      assertEquals(bpe.countTokens(chunk.text()), chunk.tokenCount());
      String body = chunk.text().substring(LABEL.length());
      assertFalse(body.isEmpty(), "no label-only chunk");
      assertFalse(Character.isLowSurrogate(body.charAt(0)), "never split inside a surrogate pair");
      joined.append(body);
    }
    assertEquals(new TextNormalizer(0).normalize(word.toString()).text(), joined.toString());
  }

  @Test
  void eachSpeakerTurnIsOneLineAndTurnsSplitAsParagraphs() {
    WordCountTokenizer words = new WordCountTokenizer();
    List<TranscriptChunker.SegmentUtterance> utterances = Arrays.asList(
        utterance(0, "spk_0", "Hello and welcome to the show.", 0, 1000),
        utterance(0, "spk_0", "Today we talk about data lakes.", 1000, 2000),
        utterance(1, "spk_1", "Thanks.", 3000, 3500),
        utterance(1, "spk_1", "Glad to be here with you all.", 3500, 4000),
        utterance(1, "spk_0", "Let us begin.", 4000, 5000));
    List<TranscriptChunk> chunks = pack(utterances, 0, words, 256, -1);
    assertEquals(1, chunks.size());
    TranscriptChunk chunk = chunks.get(0);
    assertEquals("[spk_0] Hello and welcome to the show. Today we talk about data lakes.\n[spk_1] Thanks. Glad to be here with you "
        + "all.\n[spk_0] Let us begin.", chunk.text());
    assertEquals(Arrays.asList("spk_0", "spk_1"), chunk.speakers(), "first appearance order");
    assertEquals(0, chunk.startMs());
    assertEquals(5000, chunk.endMs());
    assertEquals(0, chunk.firstSegment());
    assertEquals(1, chunk.lastSegment());

    // 16 words: "Thanks. " would still fit (15 words) but the cut prefers the turn end before it (a paragraph, 13 words)
    List<TranscriptChunk> small = pack(utterances, 0, words, 16, 0);
    assertEquals(Arrays.asList("[spk_0] Hello and welcome to the show. Today we talk about data lakes.\n",
        "[spk_1] Thanks. Glad to be here with you all.\n[spk_0] Let us begin."), texts(small));
    assertEquals(Collections.singletonList("spk_0"), small.get(0).speakers());
    assertEquals(Arrays.asList("spk_1", "spk_0"), small.get(1).speakers());
    assertEquals(3000, small.get(1).startMs());
    assertEquals(1, small.get(1).firstSegment());
  }

  /**
   * Several speakers, overlap and split utterances: a chunk that starts inside ANY speaker's turn (an overlap start, a cut between
   * two utterances of one turn, a piece of a split utterance) starts with THAT turn's label, and its times and speakers are the
   * utterances its text covers.
   */
  @ParameterizedTest
  @MethodSource("budgets")
  void chunksStartingInsideAnySpeakersTurnCarryThatSpeakersLabel(Tokenizer tokenizer, int maxTokens, int overlapTokens) {
    String[] labels = {"spk_0", "spk_1", "spk_2"};
    List<TranscriptChunker.SegmentUtterance> utterances = new ArrayList<>();
    Random random = new Random(maxTokens * 31 + overlapTokens);
    long at = 0;
    for (int i = 0; i < 120; i++) {
      // turns of 1-4 utterances; some utterances longer than the smallest limit, so they are split at words
      String label = labels[(i / (1 + i % 4)) % labels.length];
      int words = 3 + random.nextInt(i % 9 == 0 ? 40 : 12);
      StringBuilder text = new StringBuilder();
      for (int w = 0; w < words; w++) {
        text.append(w == 0 ? "" : " ").append("w").append(i).append('x').append(w);
      }
      utterances.add(utterance(i / 5, label, text.append('.').toString(), at, at + 900));
      at += 1000;
    }
    List<TranscriptChunk> chunks = pack(utterances, 0, tokenizer, maxTokens, overlapTokens);
    Stream0 stream = new Stream0(utterances);
    int prevEnd = 0;
    int insideOtherTurns = 0;
    for (int i = 0; i < chunks.size(); i++) {
      TranscriptChunk chunk = chunks.get(i);
      assertTrue(chunk.tokenCount() <= maxTokens && chunk.tokenCount() == tokenizer.countTokens(chunk.text()), chunk.toString());
      int[] located = stream.locate(chunk.text(), prevEnd);
      int start = located[0];
      int end = located[1];
      assertTrue(start <= prevEnd && end > prevEnd, "chunk " + i + " continues the stream");
      TranscriptSegment first = stream.utteranceAt(start);
      String label = SpeakerLabels.line(first.getSpeakerLabel(), "");
      assertTrue(chunk.text().startsWith(label), "chunk " + i + " starts with " + label + ": " + chunk.text());
      if (located[2] == 1 && !first.getSpeakerLabel().equals(labels[0])) {
        insideOtherTurns++;
      }
      assertEquals(first.getStartMs(), chunk.startMs(), "chunk " + i);
      assertEquals(stream.latestEndIn(start, end), chunk.endMs(), "chunk " + i);
      assertEquals(stream.speakersIn(start, end), chunk.speakers(), "chunk " + i);
      prevEnd = end;
    }
    assertEquals(stream.text.length(), prevEnd, "the chunks cover the whole transcript");
    assertTrue(insideOtherTurns > 0, "some chunk starts inside a turn of a speaker other than the first");
  }

  /**
   * A label is a generic id: one with whitespace, a bracket or a control char would let an overlap start inside it and break the
   * one-line turn, so the video fails (its FAILED row); generic ids pass.
   */
  @Test
  void labelThatIsNotAGenericIdFailsTheVideo() {
    WordCountTokenizer words = new WordCountTokenizer();
    for (String label : Arrays.asList("spk 0", "Dr. Who", "spk\n0", "spk\t0", "a]b", "[x", "spk" + NBSP + "0")) {
      List<TranscriptChunker.SegmentUtterance> labelled = Collections.singletonList(utterance(3, label, "Some words.", 0, 10));
      TranscriptPackingException e = assertThrows(TranscriptPackingException.class,
          () -> pack(labelled, 0, words, 256, -1), label);
      assertTrue(e.getMessage().contains("speaker label") && e.getMessage().contains("segment 3"), e.getMessage());
    }
    for (String label : Arrays.asList("speaker-1", "spk_0", "SPEAKER_01", "s2", "voice.a")) {
      assertEquals(1, pack(Collections.singletonList(utterance(0, label, "Some words.", 0, 10)), 0, words, 256, -1).size(), label);
    }
  }

  /**
   * No overlap is carried past the end of one video's input. One chunker packs two videos one after the other: video 1's
   * last chunk ends with its last utterance, and video 2's first chunk starts at video 2's first utterance, holds no text of
   * video 1 and is timed by video 2 alone.
   */
  @Test
  void nothingCarriesFromOneVideosInputIntoTheNext() {
    WordCountTokenizer words = new WordCountTokenizer();
    TranscriptChunker chunker = chunker();
    List<TranscriptChunker.SegmentUtterance> first = new ArrayList<>();
    List<TranscriptChunker.SegmentUtterance> second = new ArrayList<>();
    for (int i = 0; i < 30; i++) {
      first.add(utterance(i / 5, "First video utterance " + i + " with a few words.", i * 1000L, i * 1000L + 900));
      second.add(utterance(i / 5, "Second video utterance " + i + " with a few words.", 500 + i * 1000L, 500 + i * 1000L + 900));
    }
    TypedProperties props = props(16, 8);
    List<TranscriptChunk> video1 = new ArrayList<>();
    try (ClosableIterator<TranscriptChunk> chunks = chunker.chunks(first.iterator(), 0, words, props)) {
      chunks.forEachRemaining(video1::add);
    }
    List<TranscriptChunk> video2 = new ArrayList<>();
    try (ClosableIterator<TranscriptChunk> chunks = chunker.chunks(second.iterator(), 0, words, props)) {
      chunks.forEachRemaining(video2::add);
    }
    TranscriptChunk last = video1.get(video1.size() - 1);
    assertTrue(last.text().endsWith("utterance 29 with a few words."), last.text());
    assertEquals(29_900, last.endMs());
    TranscriptChunk next = video2.get(0);
    assertTrue(next.text().startsWith(LABEL + "Second video utterance 0 "), next.text());
    assertFalse(next.text().contains("First"), "no text of video 1: " + next.text());
    assertEquals(500, next.startMs());
    assertEquals(0, next.ordinal(), "ordinals restart per video");
    for (TranscriptChunk chunk : video2) {
      assertFalse(chunk.text().contains("First"), chunk.text());
    }
  }

  @Test
  void utterancesAreNormalisedAndEmptyOnesWriteNothing() {
    WordCountTokenizer words = new WordCountTokenizer();
    List<TranscriptChunk> chunks = pack(Arrays.asList(
        utterance(0, "  " + NBSP + " ", 0, 100),
        utterance(0, " " + fullwidth("Full") + ZERO_WIDTH_SPACE + "width" + NBSP + "text\r\n\r\nacross lines ", 200, 900),
        utterance(1, "\t", 1000, 1100)), 0, words, 256, -1);
    assertEquals(1, chunks.size());
    // a zero-width space between two letters is a word boundary (TextNormalizer, review 3095-1), so the words stay apart
    assertEquals(LABEL + "Full width text across lines", chunks.get(0).text());
    assertEquals(200, chunks.get(0).startMs(), "an empty utterance lends no time");
    assertEquals(900, chunks.get(0).endMs());
    assertTrue(pack(Collections.singletonList(utterance(0, " ", 0, 10)), 0, words, 256, -1).isEmpty());
    assertTrue(pack(Collections.emptyList(), 0, words, 256, -1).isEmpty());
  }

  @Test
  void pullsUtterancesOnlyWhileItNeedsText() {
    WordCountTokenizer words = new WordCountTokenizer();
    CountingUtterances input = new CountingUtterances(100_000);
    TypedProperties props = props(256, -1);
    try (ClosableIterator<TranscriptChunk> chunks = chunker().chunks(input, 0, words, props)) {
      assertTrue(chunks.hasNext());
      chunks.next();
      // the pull rule stops once the pending text reaches a window (8192 chars) and its last sentence starts beyond it: at most
      // a window plus two utterances, plus the one read ahead; at least a window's worth
      int window = EmbeddingTableConfig.DEFAULT_CHUNK_WINDOW_MAX_CHARS;
      int shortest = "Utterance number 0 says a few more words.".length() + 1;
      int longest = ("Utterance number " + 99_999 + " says a few more words.").length() + 1;
      assertTrue(input.pulled <= (window + 2 * longest) / shortest + 2, "pulled " + input.pulled + " utterances for the first chunk");
      assertTrue(input.pulled >= window / longest, "pulled " + input.pulled + ": a window's worth before the first cut");
      int seen = 1;
      while (chunks.hasNext() && seen < 200) {
        chunks.next();
        seen++;
      }
      assertTrue(input.pulled < 100_000, "lazy: " + input.pulled);
    }
  }

  @Test
  void closingEndsTheChunksAndIsIdempotent() {
    WordCountTokenizer words = new WordCountTokenizer();
    ClosableIterator<TranscriptChunk> chunks = chunker().chunks(new CountingUtterances(1000), 0, words, props(16, 0));
    assertTrue(chunks.hasNext());
    chunks.close();
    chunks.close();
    assertFalse(chunks.hasNext());
  }

  @Test
  void tokenizerFailureIsAContentFailureOfTheVideo() {
    Tokenizer failing = new WordCountTokenizer() {
      @Override
      public int countTokens(String text) {
        throw new IllegalArgumentException("tokenizer down");
      }
    };
    List<TranscriptChunker.SegmentUtterance> one = Collections.singletonList(utterance(0, "words here.", 0, 10));
    TranscriptPackingException e = assertThrows(TranscriptPackingException.class, () -> pack(one, 0, failing, 256, -1));
    assertTrue(e.getMessage().startsWith("transcript packing TOKENIZER_ERROR: tokenizer "), e.getMessage());
    assertTrue(e.getCause() instanceof IllegalArgumentException && "tokenizer down".equals(e.getCause().getMessage()),
        String.valueOf(e.getCause()));
  }

  // ------------------------------------------------------------------ helpers

  /** {@code ascii} in fullwidth forms, which NFKC folds back. */
  private static String fullwidth(String ascii) {
    StringBuilder out = new StringBuilder();
    for (char c : ascii.toCharArray()) {
      out.append((char) (c + 0xFEE0));
    }
    return out.toString();
  }

  private static List<String> texts(List<TranscriptChunk> chunks) {
    List<String> texts = new ArrayList<>();
    chunks.forEach(chunk -> texts.add(chunk.text()));
    return texts;
  }

  private static TranscriptChunker chunker() {
    return AudioConfig.from(new TypedProperties()).newChunker();
  }

  private static TypedProperties props(int maxTokens, int overlapTokens) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), String.valueOf(maxTokens));
    if (overlapTokens >= 0) {
      props.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), String.valueOf(overlapTokens));
    }
    return props;
  }

  static List<TranscriptChunk> pack(List<TranscriptChunker.SegmentUtterance> utterances, long firstOrdinal, Tokenizer tokenizer,
                                    int maxTokens, int overlapTokens) {
    Iterator<TranscriptChunker.SegmentUtterance> input = utterances.iterator();
    List<TranscriptChunk> chunks = new ArrayList<>();
    try (ClosableIterator<TranscriptChunk> packed = chunker().chunks(input, firstOrdinal, tokenizer, props(maxTokens,
        overlapTokens))) {
      packed.forEachRemaining(chunks::add);
    }
    assertFalse(input.hasNext(), "every utterance is packed");
    return chunks;
  }

  private static TranscriptChunker.SegmentUtterance utterance(long segment, String text, long startMs, long endMs) {
    return utterance(segment, SpeakerLabels.FILE_SPEAKER, text, startMs, endMs);
  }

  private static TranscriptChunker.SegmentUtterance utterance(long segment, String label, String text, long startMs, long endMs) {
    return RecordedUtterances.of(segment, new TranscriptSegment(label, text, startMs, endMs, null));
  }

  /**
   * The stream the packer cuts: a turn starts with {@code [label] }, utterances of one turn join with a space, turns with a new
   * line; each utterance owns its label (when it starts a turn), its normalised text and the separator after it.
   */
  private static final class Stream0 {
    private final String text;
    private final List<int[]> ranges = new ArrayList<>();
    private final List<TranscriptChunker.SegmentUtterance> utterances;

    private Stream0(List<TranscriptChunker.SegmentUtterance> utterances) {
      this.utterances = utterances;
      TextNormalizer normalizer = new TextNormalizer(0);
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < utterances.size(); i++) {
        TranscriptSegment u = utterances.get(i).utterance();
        int from = sb.length();
        if (i == 0 || !utterances.get(i - 1).utterance().getSpeakerLabel().equals(u.getSpeakerLabel())) {
          sb.append(SpeakerLabels.line(u.getSpeakerLabel(), ""));
        }
        sb.append(normalizer.normalize(u.getText()).text());
        if (i + 1 < utterances.size()) {
          sb.append(utterances.get(i + 1).utterance().getSpeakerLabel().equals(u.getSpeakerLabel()) ? " " : "\n");
        }
        ranges.add(new int[] {from, sb.length()});
      }
      this.text = sb.toString();
    }

    /**
     * {start, end, prefixed} of the chunk text in the stream at or before {@code prevEnd}: the whole text when the chunk starts at
     * a turn's label, else the text after the label the packer put before it (prefixed = 1).
     */
    private int[] locate(String chunkText, int prevEnd) {
      int start = text.lastIndexOf(chunkText, prevEnd);
      if (start >= 0 && start + chunkText.length() > prevEnd) {
        return new int[] {start, start + chunkText.length(), 0};
      }
      String body = chunkText.substring(chunkText.indexOf("] ") + 2);
      start = text.lastIndexOf(body, prevEnd);
      assertTrue(start >= 0, "chunk text not in the stream: " + chunkText);
      return new int[] {start, start + body.length(), 1};
    }

    private int index(int offset) {
      for (int i = 0; i < ranges.size(); i++) {
        if (offset < ranges.get(i)[1]) {
          return i;
        }
      }
      throw new AssertionError("offset " + offset + " beyond the stream");
    }

    private TranscriptSegment utteranceAt(int offset) {
      return utterances.get(index(offset)).utterance();
    }

    private long segmentAt(int offset) {
      return utterances.get(index(offset)).segmentIndex();
    }

    /** The latest end of the utterances with text in [start, end). */
    private long latestEndIn(int start, int end) {
      long latest = Long.MIN_VALUE;
      for (int i = index(start); i <= index(end - 1); i++) {
        latest = Math.max(latest, utterances.get(i).utterance().getEndMs());
      }
      return latest;
    }

    /** Distinct labels of the utterances with text in {@code [start, end)}, in order of first appearance. */
    private List<String> speakersIn(int start, int end) {
      LinkedHashSet<String> labels = new LinkedHashSet<>();
      for (int i = index(start); i <= index(end - 1); i++) {
        labels.add(utterances.get(i).utterance().getSpeakerLabel());
      }
      return new ArrayList<>(labels);
    }
  }

  /** {@code count} distinct one-sentence utterances, counting how many were pulled. */
  private static final class CountingUtterances implements Iterator<TranscriptChunker.SegmentUtterance> {
    private final int count;
    private int pulled;

    private CountingUtterances(int count) {
      this.count = count;
    }

    @Override
    public boolean hasNext() {
      return pulled < count;
    }

    @Override
    public TranscriptChunker.SegmentUtterance next() {
      int i = pulled++;
      return utterance(i / 10, "Utterance number " + i + " says a few more words.", i * 1000L, i * 1000L + 900);
    }
  }
}
