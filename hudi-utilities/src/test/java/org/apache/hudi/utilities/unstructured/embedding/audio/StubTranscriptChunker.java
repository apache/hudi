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
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * TEST DOUBLE (no default names it; the defaults are the real implementations, {@link AudioConfig}).
 *
 * <p>DUMMY {@link TranscriptChunker}, logic {@value #LOGIC_VERSION}: greedy packing of {@code [speaker] utterance} lines (one
 * line per utterance, whitespace collapsed) into chunks of at most the table's {@link EmbeddingTableConfig#CHUNK_MAX_TOKENS}
 * under the table's tokenizer, with NO overlap between chunks ({@link TranscriptPacker}, the default, applies the PDF chunker's
 * packing and overlap rules). A line that does not fit into an empty chunk is split at words, each piece keeping its speaker
 * prefix; a single word that does not fit alone fails the video (its quarantine entry). The end of the utterances ends the last chunk.
 *
 * <p>Bounded: it pulls one utterance at a time and holds at most one chunk's text plus the one line that did not fit.
 */
public class StubTranscriptChunker implements TranscriptChunker {

  /** The stub's packing logic; part of the audio logic line, so of every audio table's preprocessing style. */
  public static final String LOGIC_VERSION = "transcript-stub-pack-v0";

  @Override
  public String logicVersion() {
    return LOGIC_VERSION;
  }

  @Override
  public ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal,
                                                  Tokenizer tokenizer, TypedProperties props) {
    return new Packer(utterances, firstOrdinal, tokenizer, EmbeddingTableConfig.from(props).chunkMaxTokens());
  }

  /** The part of one utterance not yet packed. */
  private static final class Piece {
    private final long segment;
    private final TranscriptSegment utterance;
    private String text;

    private Piece(SegmentUtterance source) {
      this.segment = source.segmentIndex();
      this.utterance = source.utterance();
      this.text = utterance.getText().trim().replaceAll("\\s+", " ");
    }
  }

  private static final class Packer implements ClosableIterator<TranscriptChunk> {
    private final Iterator<SegmentUtterance> utterances;
    private final Tokenizer tokenizer;
    private final int maxTokens;
    private long ordinal;
    private Piece pending;
    private TranscriptChunk next;

    private Packer(Iterator<SegmentUtterance> utterances, long firstOrdinal, Tokenizer tokenizer, int maxTokens) {
      this.utterances = utterances;
      this.ordinal = firstOrdinal;
      this.tokenizer = tokenizer;
      this.maxTokens = maxTokens;
    }

    @Override
    public boolean hasNext() {
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

    private TranscriptChunk pack() {
      StringBuilder text = new StringBuilder();
      Set<String> speakers = new LinkedHashSet<>();
      long startMs = -1;
      long endMs = -1;
      long firstSegment = -1;
      long lastSegment = -1;
      while (true) {
        if (pending == null) {
          if (!utterances.hasNext()) {
            break;
          }
          pending = new Piece(utterances.next());
        }
        if (pending.text.isEmpty()) {
          pending = null;
          continue;
        }
        String speaker = pending.utterance.getSpeakerLabel();
        String line = SpeakerLabels.line(speaker, pending.text);
        String candidate = text.length() == 0 ? line : text + "\n" + line;
        boolean whole = tokenizer.countTokens(candidate) <= maxTokens;
        if (!whole && text.length() > 0) {
          // the line starts the next chunk
          break;
        }
        if (whole) {
          text.setLength(0);
          text.append(candidate);
        } else {
          // the line alone does not fit an empty chunk: the longest prefix of whole words that does
          String[] words = pending.text.split(" ");
          int fit = wordsThatFit(speaker, words);
          if (fit == 0) {
            throw new IllegalStateException("transcript word '" + words[0] + "' of segment " + pending.segment
                + " does not fit " + EmbeddingTableConfig.CHUNK_MAX_TOKENS.key() + " = " + maxTokens + " tokens under tokenizer "
                + tokenizer.id());
          }
          text.append(SpeakerLabels.line(speaker, String.join(" ", Arrays.copyOfRange(words, 0, fit))));
        }
        speakers.add(speaker);
        startMs = startMs < 0 ? pending.utterance.getStartMs() : startMs;
        endMs = Math.max(endMs, pending.utterance.getEndMs());
        firstSegment = firstSegment < 0 ? pending.segment : firstSegment;
        lastSegment = pending.segment;
        if (whole) {
          pending = null;
        } else {
          String[] words = pending.text.split(" ");
          pending.text = String.join(" ", Arrays.copyOfRange(words, wordsThatFit(speaker, words), words.length));
          // the chunk is full
          break;
        }
      }
      if (text.length() == 0) {
        return null;
      }
      String chunkText = text.toString();
      return new TranscriptChunkValue(ordinal++, chunkText, tokenizer.countTokens(chunkText),
          new TranscriptChunkValue.Range(startMs, endMs), new ArrayList<>(speakers), new TranscriptChunkValue.Range(firstSegment, lastSegment));
    }

    /** The most leading words of {@code words} whose {@code [speaker] ...} line fits the limit (binary search). */
    private int wordsThatFit(String speaker, String[] words) {
      int fits = 0;
      int exceeds = words.length + 1;
      while (exceeds - fits > 1) {
        int mid = (fits + exceeds) >>> 1;
        String line = SpeakerLabels.line(speaker, String.join(" ", Arrays.copyOfRange(words, 0, mid)));
        if (tokenizer.countTokens(line) <= maxTokens) {
          fits = mid;
        } else {
          exceeds = mid;
        }
      }
      return fits;
    }

    @Override
    public void close() {
      pending = null;
      next = null;
    }
  }
}
