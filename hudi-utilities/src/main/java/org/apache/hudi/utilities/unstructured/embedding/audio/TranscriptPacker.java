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
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfChunkerConfig;
import org.apache.hudi.utilities.unstructured.embedding.text.ChunkPacker;
import org.apache.hudi.utilities.unstructured.embedding.text.PackFailures;
import org.apache.hudi.utilities.unstructured.embedding.text.PackedChunk;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenBudget;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The transcript chunker, logic {@value #LOGIC_VERSION}: the PDF chunker's packing rules over the speech of
 * one video, through the shared {@link ChunkPacker} in its declared-boundary mode, under the table's {@link TokenBudget}
 * ({@link EmbeddingTableConfig#CHUNK_MAX_TOKENS}, default 256; {@link EmbeddingTableConfig#CHUNK_OVERLAP_TOKENS}, default
 * min(128, max / 2); the PDF chunker's word and window limits).
 *
 * <ul>
 *   <li>Text: each utterance normalised by the PDF {@link TextNormalizer} (NFKC, whitespace, invisibles; a line break inside an
 *       utterance becomes a space); an utterance empty after it is skipped, one longer than the PDF page char cap fails the
 *       video. A speaker turn (a run of utterances with one label) is ONE line {@code [<label>] <utterance> <utterance> ...};
 *       turns are separated by {@code \n}.</li>
 *   <li>Boundaries: an utterance end is a sentence boundary and the end of a turn a paragraph boundary; an utterance that does
 *       not fit alone is split at words, and a word that does not fit at code points, as the PDF chunker splits a sentence.</li>
 *   <li>Speaker: EVERY chunk starts with the {@code [<label>] } of the utterance its first char belongs to; a chunk that starts
 *       inside a turn (an overlap, a split utterance, a cut between utterances of one turn) gets it as a chunk prefix, counted
 *       in the chunk's token count. No cross-speaker rule: a chunk may hold several turns, one line each.
 *       Labels are generic ids ({@link SpeakerLabels}): a label with whitespace, a control char or a bracket fails the video.</li>
 *   <li>Times: {@code start_ms} of the first utterance any of whose text, overlap included, is in the chunk, {@code end_ms} the
 *       latest end of those utterances (ends, unlike starts, need not be in order); a split piece carries its utterance's span
 *       (no per-word times). Speakers in order of first
 *       appearance; first and last segment of those utterances.</li>
 *   <li>The end of the utterances ends the last chunk: no overlap is carried past a video's input.</li>
 *   <li>A chunk is the packer's slice: one cut after an utterance keeps that utterance's separator (a space, or the new line
 *       before the next turn) at its end, as a PDF chunk keeps its trailing spaces.</li>
 *   <li>Bounded: it pulls one utterance at a time and only while the packer needs text; it holds the packer's pending
 *       text and overlap plus one utterance read ahead (its separator depends on the next label), and per utterance still in
 *       them its label, span and segment.</li>
 *   <li>A turn's label cut off alone (a word too long to fit beside it follows) is not written: the next chunk carries the
 *       same label as its prefix.</li>
 *   <li>Failures: a tokenizer or packing failure is a {@link TranscriptPackingException}, a content failure of the video.</li>
 * </ul>
 */
public class TranscriptPacker implements TranscriptChunker {

  /** The packing logic; part of the audio logic line, so of every audio table's preprocessing style. */
  public static final String LOGIC_VERSION = "transcript-pack-v2";

  private static final PackFailures FAILURES = (kind, message, cause) ->
      new TranscriptPackingException("transcript packing " + kind + ": " + message, cause);

  private final PdfChunkerConfig.Cache config = new PdfChunkerConfig.Cache();

  @Override
  public String logicVersion() {
    return LOGIC_VERSION;
  }

  @Override
  public ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal, Tokenizer tokenizer,
                                                  TypedProperties props) {
    return new Packing(utterances, firstOrdinal, config.get(props, tokenizer));
  }

  /** What a chunk needs of one utterance whose text is still pending or in the overlap. */
  private static final class Held {
    private final int mark;
    private final String label;
    private final long startMs;
    private final long endMs;
    private final long segment;

    private Held(int mark, String label, long startMs, long endMs, long segment) {
      this.mark = mark;
      this.label = label;
      this.startMs = startMs;
      this.endMs = endMs;
      this.segment = segment;
    }
  }

  /** The chunks of one video. */
  static final class Packing implements ClosableIterator<TranscriptChunk> {
    private final Iterator<SegmentUtterance> utterances;
    private final ChunkPacker packer;
    private final TextNormalizer normalizer;
    /** Utterances whose text is pending, in the overlap or read ahead, by ascending mark. */
    private final ArrayDeque<Held> held = new ArrayDeque<>();
    private long ordinal;
    private int nextMark;
    /** The utterance read ahead and its text (with its turn's label when it starts a turn), not yet in the packer. */
    private Held ahead;
    private String aheadText;
    private boolean inputDone;
    private boolean closed;
    private TranscriptChunk next;

    Packing(Iterator<SegmentUtterance> utterances, long firstOrdinal, PdfChunkerConfig config) {
      this.utterances = utterances;
      this.ordinal = firstOrdinal;
      this.normalizer = new TextNormalizer(config.maxPageChars());
      TokenBudget budget = new TokenBudget(config.tokenizer(), config.maxTokens(), config.maxWords(), config.windowChars(),
          FAILURES);
      this.packer = ChunkPacker.withDeclaredBoundaries(budget, config.overlapTokens(), this::prefixFor);
    }

    @Override
    public boolean hasNext() {
      if (closed) {
        return false;
      }
      if (next == null) {
        next = pack();
      }
      return next != null;
    }

    @Override
    public TranscriptChunk next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      TranscriptChunk chunk = next;
      next = null;
      return chunk;
    }

    @Override
    public void close() {
      closed = true;
      next = null;
      held.clear();
      ahead = null;
      aheadText = null;
    }

    /** Chars the packing holds: the packer's pending text and overlap plus the utterance read ahead. */
    int retainedChars() {
      return packer.retainedChars() + (aheadText == null ? 0 : aheadText.length());
    }

    /** Utterances whose label and span are held. */
    int heldUtterances() {
      return held.size();
    }

    ChunkPacker packer() {
      return packer;
    }

    private TranscriptChunk pack() {
      while (true) {
        if (!inputDone && packer.needsMoreText()) {
          pull();
        } else {
          PackedChunk cut = packer.cut(inputDone);
          if (cut == null) {
            return null;
          }
          if (!labelOnly(cut)) {
            TranscriptChunk chunk = toChunk(cut);
            dropReleased();
            return chunk;
          }
          // a turn's label cut off alone (the word after it does not fit beside it): no content, and the next chunk starts
          // with the same label as its prefix, so nothing is lost by not writing it
          dropReleased();
        }
      }
    }

    /** Reads one utterance; the one read before it enters the packer, now that its separator is known. */
    private void pull() {
      if (!utterances.hasNext()) {
        if (ahead != null) {
          packer.appendSegment(aheadText, ahead.mark, ChunkPacker.Boundary.PARAGRAPH);
          ahead = null;
          aheadText = null;
        }
        inputDone = true;
        return;
      }
      SegmentUtterance source = utterances.next();
      TranscriptSegment utterance = source.utterance();
      String text = normalize(utterance.getText(), source.segmentIndex());
      if (text.isEmpty()) {
        return;
      }
      String label = checkedLabel(utterance.getSpeakerLabel(), source.segmentIndex());
      boolean sameTurn = ahead != null && ahead.label.equals(label);
      if (ahead != null) {
        packer.appendSegment(aheadText + (sameTurn ? " " : "\n"), ahead.mark,
            sameTurn ? ChunkPacker.Boundary.SENTENCE : ChunkPacker.Boundary.PARAGRAPH);
      }
      ahead = new Held(nextMark++, label, utterance.getStartMs(), utterance.getEndMs(), source.segmentIndex());
      aheadText = sameTurn ? text : SpeakerLabels.line(label, text);
      held.addLast(ahead);
    }

    /**
     * A label is a generic id ({@link SpeakerLabels}): no whitespace, no space char, no control char and no bracket, so an overlap
     * never starts inside it and a turn stays one line.
     */
    private static String checkedLabel(String label, long segment) {
      int i = 0;
      while (i < label.length()) {
        int cp = label.codePointAt(i);
        if (Character.isWhitespace(cp) || Character.isSpaceChar(cp) || Character.isISOControl(cp) || cp == '[' || cp == ']') {
          throw new TranscriptPackingException("speaker label '" + label + "' of segment " + segment + " is not a generic id "
              + "(no whitespace, control char or bracket)");
        }
        i += Character.charCount(cp);
      }
      return label;
    }

    private String normalize(String raw, long segment) {
      TextNormalizer.Result result = normalizer.normalize(raw);
      if (result.truncated()) {
        throw new TranscriptPackingException("an utterance of segment " + segment + " is longer than the page char cap ("
            + EmbeddingTableConfig.PDF_MAX_PAGE_CHARS.key() + ") after normalisation");
      }
      return result.text().replace("\n\n", " ");
    }

    /** The prefix of a chunk starting at {@code start}: its first utterance's label, unless the chunk starts with it. */
    private String prefixFor(int firstMark, String start) {
      String labelled = SpeakerLabels.line(heldAt(firstMark).label, "");
      return start.startsWith(labelled) ? "" : labelled;
    }

    private boolean labelOnly(PackedChunk cut) {
      return cut.firstMark() == cut.lastMark() && cut.text().equals(SpeakerLabels.line(heldAt(cut.firstMark()).label, ""));
    }

    private TranscriptChunk toChunk(PackedChunk cut) {
      Held first = heldAt(cut.firstMark());
      Held last = heldAt(cut.lastMark());
      Set<String> speakers = new LinkedHashSet<>();
      // starts are non-decreasing, ends are not (an utterance may end before the one before it): the chunk ends where the
      // latest of its utterances ends
      long endMs = last.endMs;
      for (Held utterance : held) {
        if (utterance.mark > last.mark) {
          break;
        }
        if (utterance.mark >= first.mark) {
          speakers.add(utterance.label);
          endMs = Math.max(endMs, utterance.endMs);
        }
      }
      return new TranscriptChunkValue(ordinal++, cut.text(), cut.tokenCount(),
          new TranscriptChunkValue.Range(first.startMs, endMs), new ArrayList<>(speakers), new TranscriptChunkValue.Range(first.segment, last.segment));
    }

    /** Drops the utterances no later chunk can carry: below the packer's first held mark (the read-ahead one stays). */
    private void dropReleased() {
      int keepFrom = packer.firstHeldMark();
      if (keepFrom < 0) {
        keepFrom = ahead != null ? ahead.mark : nextMark;
      }
      while (!held.isEmpty() && held.peekFirst().mark < keepFrom) {
        held.removeFirst();
      }
    }

    private Held heldAt(int mark) {
      for (Held utterance : held) {
        if (utterance.mark == mark) {
          return utterance;
        }
      }
      throw new TranscriptPackingException("the packer named utterance " + mark + ", which is no longer held");
    }
  }
}
