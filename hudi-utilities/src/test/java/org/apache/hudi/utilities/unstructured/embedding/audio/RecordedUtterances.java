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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The RECORDED utterance fixture of the transcript packer tests ({@value #RESOURCE}): whisper base.en's utterances of the first
 * 180 s of a public-domain reading, recorded once with whisper.cpp 1.7.1 in 30 s segments (provenance in the file's
 * header), and {@link Transcriber}, a deterministic fake transcriber that replays them, so no test needs a whisper model.
 */
public final class RecordedUtterances {

  public static final String RESOURCE = "transcript/selfish-giant-whisper-base-en.txt";
  /** Span of one recorded segment. */
  public static final long SEGMENT_MS = 30_000;

  private static List<List<TranscriptSegment>> bySegment;

  private RecordedUtterances() {
  }

  /** The recorded utterances, by recorded segment (6 segments of {@link #SEGMENT_MS}), on the recording's timeline. */
  public static synchronized List<List<TranscriptSegment>> bySegment() {
    if (bySegment == null) {
      bySegment = Collections.unmodifiableList(load());
    }
    return bySegment;
  }

  /** Every recorded utterance in time order, each with its recorded segment index. */
  public static List<TranscriptChunker.SegmentUtterance> all() {
    List<TranscriptChunker.SegmentUtterance> all = new ArrayList<>();
    List<List<TranscriptSegment>> segments = bySegment();
    for (int s = 0; s < segments.size(); s++) {
      for (TranscriptSegment utterance : segments.get(s)) {
        all.add(of(s, utterance));
      }
    }
    return all;
  }

  /** {@code utterance} tagged with segment {@code index}. */
  public static TranscriptChunker.SegmentUtterance of(long index, TranscriptSegment utterance) {
    return new TranscriptChunker.SegmentUtterance() {
      @Override
      public long segmentIndex() {
        return index;
      }

      @Override
      public TranscriptSegment utterance() {
        return utterance;
      }
    };
  }

  private static List<List<TranscriptSegment>> load() {
    List<List<TranscriptSegment>> segments = new ArrayList<>();
    try (InputStream in = RecordedUtterances.class.getResourceAsStream(RESOURCE);
         BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.startsWith("#") || line.isEmpty()) {
          continue;
        }
        String[] f = line.split("\t", 4);
        int segment = Integer.parseInt(f[0]);
        while (segments.size() <= segment) {
          segments.add(new ArrayList<>());
        }
        segments.get(segment).add(new TranscriptSegment(SpeakerLabels.FILE_SPEAKER, f[3], Long.parseLong(f[1]),
            Long.parseLong(f[2]), null));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return segments;
  }

  /**
   * Deterministic fake transcriber: segment {@code k} of any video says the recorded utterances of recorded segment
   * {@code k mod 6}, their times mapped linearly into segment {@code k}'s span (so starts stay non-decreasing and inside it).
   * Its id names the fixture, so a table it writes has its own style.
   */
  public static class Transcriber implements SegmentTranscriber {

    public static final String ID = "recorded/selfish-giant-whisper-base-en-v1";

    @Override
    public String id() {
      return ID;
    }

    @Override
    public boolean fileScopedSpeakers() {
      return true;
    }

    @Override
    public void validate(TypedProperties props) {
      // a recorded transcript needs no configuration
    }

    @Override
    public ClosableIterator<TranscriptSegment> transcribe(AudioSegment segment, TypedProperties props) {
      return ClosableIterator.wrap(utterancesOf(segment.index(), segment.startMs(), segment.endMs()).iterator());
    }

    /** What segment {@code index} spanning {@code [startMs, endMs)} says. */
    public static List<TranscriptSegment> utterancesOf(long index, long startMs, long endMs) {
      List<List<TranscriptSegment>> recorded = bySegment();
      int k = (int) (index % recorded.size());
      long origin = k * SEGMENT_MS;
      long span = endMs - startMs;
      List<TranscriptSegment> out = new ArrayList<>();
      for (TranscriptSegment u : recorded.get(k)) {
        long start = startMs + (u.getStartMs() - origin) * span / SEGMENT_MS;
        long end = startMs + (u.getEndMs() - origin) * span / SEGMENT_MS;
        out.add(new TranscriptSegment(u.getSpeakerLabel(), u.getText(), Math.min(start, endMs - 1), Math.max(Math.min(start,
            endMs - 1), Math.min(end, endMs)), null));
      }
      return out;
    }
  }
}
