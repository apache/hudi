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

package org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioInput;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioSegment;
import org.apache.hudi.utilities.unstructured.embedding.audio.SegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunk;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptChunker;
import org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptSegment;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.video.VideoClips;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The audio pass end to end without Spark: {@link FfmpegAudioSegmentSource} over the speech clip, a test-only transcriber (one
 * utterance per segment, from its payload) and a test-only chunker (one chunk per utterance), into transcript rows keyed as the
 * framework numbers them (the dense ordinal), in rounds of a few segments, each resumed from the
 * previous round's checkpoint (next segment, end of its last segment, next ordinal): the source's resume contract, which data
 * prep (reading a video whole) does not use but a caller that splits a video may.
 */
class TestAudioTranscriptFlow {

  /** One speech row as data prep writes it. */
  private static final class Row {
    private final long chunkId;
    private final String text;
    private final long startMs;
    private final long endMs;
    private final long firstSegment;
    private final long lastSegment;

    private Row(TranscriptChunk chunk) {
      this.chunkId = chunk.ordinal();
      this.text = chunk.text();
      this.startMs = chunk.startMs();
      this.endMs = chunk.endMs();
      this.firstSegment = chunk.firstSegment();
      this.lastSegment = chunk.lastSegment();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Row)) {
        return false;
      }
      Row other = (Row) o;
      return chunkId == other.chunkId && text.equals(other.text) && startMs == other.startMs && endMs == other.endMs
          && firstSegment == other.firstSegment && lastSegment == other.lastSegment;
    }

    @Override
    public int hashCode() {
      return Objects.hash(chunkId, text, startMs, endMs);
    }

    @Override
    public String toString() {
      return chunkId + "[" + startMs + "," + endMs + ")" + text;
    }
  }

  /** Test-only: one utterance per segment, saying how many payload bytes it read and their level. */
  static final class PayloadTranscriber implements SegmentTranscriber {
    @Override
    public String id() {
      return "test-payload/v0";
    }

    @Override
    public boolean fileScopedSpeakers() {
      return false;
    }

    @Override
    public void validate(TypedProperties props) {
      // this test transcriber reads no settings, so there is nothing to validate
    }

    @Override
    public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) throws IOException {
      long bytes = 0;
      long loud = 0;
      try (InputStream in = segment.openPayload()) {
        int lo;
        while ((lo = in.read()) >= 0) {
          int sample = (short) (lo | in.read() << 8);
          loud += Math.abs(sample) > 1000 ? 1 : 0;
          bytes += 2;
        }
      }
      assertEquals(segment.payloadBytes(), bytes);
      String text = "segment " + segment.index() + " has " + bytes + " bytes, " + (loud > 0 ? "voiced" : "quiet");
      return iterator(Collections.singletonList(new TranscriptSegment("spk_0", text, segment.startMs(), segment.endMs(), null)));
    }
  }

  /** Test-only: one chunk per utterance, numbered from the first ordinal. */
  static final class OnePerUtteranceChunker implements TranscriptChunker {
    @Override
    public String logicVersion() {
      return "test-one-per-utterance-v0";
    }

    @Override
    public ClosableIterator<TranscriptChunk> chunks(Iterator<SegmentUtterance> utterances, long firstOrdinal, Tokenizer tokenizer,
                                                    TypedProperties props) {
      List<TranscriptChunk> chunks = new ArrayList<>();
      long ordinal = firstOrdinal;
      while (utterances.hasNext()) {
        SegmentUtterance utterance = utterances.next();
        TranscriptSegment segment = utterance.utterance();
        String text = "[" + segment.getSpeakerLabel() + "] " + segment.getText();
        chunks.add(chunk(ordinal++, text, tokenizer.countTokens(text), segment.getStartMs(), segment.getEndMs(),
            utterance.segmentIndex()));
      }
      return iterator(chunks);
    }
  }

  private static TranscriptChunk chunk(long ordinal, String text, int tokens, long startMs, long endMs, long segment) {
    return new TranscriptChunk() {
      @Override
      public long ordinal() {
        return ordinal;
      }

      @Override
      public String text() {
        return text;
      }

      @Override
      public int tokenCount() {
        return tokens;
      }

      @Override
      public long startMs() {
        return startMs;
      }

      @Override
      public long endMs() {
        return endMs;
      }

      @Override
      public List<String> speakers() {
        return Collections.singletonList("spk_0");
      }

      @Override
      public long firstSegment() {
        return segment;
      }

      @Override
      public long lastSegment() {
        return segment;
      }
    };
  }

  private static <T> ClosableIterator<T> iterator(List<T> items) {
    Iterator<T> it = items.iterator();
    return new ClosableIterator<T>() {
      @Override
      public void close() {
        // an iterator over an in-memory list holds no resource to release
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public T next() {
        return it.next();
      }
    };
  }

  /** Checkpoint of one video between rounds (the source can resume from one; data prep reads a video whole). */
  private static final class Checkpoint {
    private long nextSegment;
    private long nextStartMs;
    private long nextOrdinal;
    private boolean done;
  }

  /** One round: up to {@code roundSegments} segments from the checkpoint, transcribed and chunked; advances the checkpoint. */
  private static List<Row> round(AudioInput video, Checkpoint checkpoint, int roundSegments, TypedProperties props)
      throws IOException {
    FfmpegAudioSegmentSource source = new FfmpegAudioSegmentSource();
    SegmentTranscriber transcriber = new PayloadTranscriber();
    TranscriptChunker chunker = new OnePerUtteranceChunker();
    List<TranscriptChunker.SegmentUtterance> utterances = new ArrayList<>();
    long taken = 0;
    long lastEnd = checkpoint.nextStartMs;
    try (ClosableIterator<AudioSegment> segments = source.segments(video, checkpoint.nextSegment, checkpoint.nextStartMs, props)) {
      while (taken < roundSegments && segments.hasNext()) {
        AudioSegment segment = segments.next();
        assertEquals(checkpoint.nextSegment + taken, segment.index(), "dense segment indexes");
        assertTrue(segment.startMs() >= lastEnd, "segment " + segment + " starts at or after the checkpoint");
        try (ClosableIterator<TranscriptSegment> said = transcriber.transcribe(segment, props)) {
          while (said.hasNext()) {
            TranscriptSegment utterance = said.next();
            long index = segment.index();
            utterances.add(new TranscriptChunker.SegmentUtterance() {
              @Override
              public long segmentIndex() {
                return index;
              }

              @Override
              public TranscriptSegment utterance() {
                return utterance;
              }
            });
          }
        }
        lastEnd = segment.endMs();
        taken++;
      }
      checkpoint.done = taken == 0;
    }
    List<Row> rows = new ArrayList<>();
    try (ClosableIterator<TranscriptChunk> chunks = chunker.chunks(utterances.iterator(), checkpoint.nextOrdinal,
        new WordCountTokenizer(), props)) {
      while (chunks.hasNext()) {
        rows.add(new Row(chunks.next()));
      }
    }
    checkpoint.nextSegment += taken;
    checkpoint.nextStartMs = lastEnd;
    checkpoint.nextOrdinal += rows.size();
    return rows;
  }

  @Test
  void speechClipBecomesTranscriptRowsAcrossResumedRounds() throws IOException, URISyntaxException {
    Path clip = Paths.get(Objects.requireNonNull(getClass().getResource("/video/h264-aac-speech-12s.mp4")).toURI());
    AudioInput video = AudioClips.input("fid-speech", clip, VideoClips.supplier(clip));
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.SEGMENT_MAX_MS.key(), "1500");

    Checkpoint once = new Checkpoint();
    List<Row> oneRound = round(video, once, Integer.MAX_VALUE, props);
    assertTrue(oneRound.size() >= 6, "rows " + oneRound);

    Checkpoint checkpoint = new Checkpoint();
    List<Row> rounds = new ArrayList<>();
    int roundCount = 0;
    while (!checkpoint.done) {
      rounds.addAll(round(video, checkpoint, 2, props));
      roundCount++;
    }
    assertTrue(roundCount > 3, "spread over rounds: " + roundCount);
    // the same rows, chunk ids dense from 0, text naming the speaker and the payload read
    assertEquals(oneRound, rounds);
    for (int i = 0; i < rounds.size(); i++) {
      Row row = rounds.get(i);
      assertEquals(i, row.chunkId);
      assertEquals(i, row.firstSegment);
      assertTrue(row.text.startsWith("[spk_0] segment " + i + " has " + (row.endMs - row.startMs) * 32 + " bytes, voiced"),
          row.text);
      assertTrue(row.endMs - row.startMs <= 1500);
    }
    assertEquals(rounds.size(), checkpoint.nextOrdinal);
    assertEquals(rounds.get(rounds.size() - 1).endMs, checkpoint.nextStartMs);
  }
}
