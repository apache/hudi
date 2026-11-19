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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The energy gate over windows of known levels: one window = 4 samples of one constant amplitude. */
class TestEnergyVadSegmenter {

  private static final int SAMPLES = 4;
  /** -35 dBFS, the default threshold. */
  private static final double THRESHOLD = -35;
  /** About -18 dBFS: speech. */
  private static final short LOUD = 4000;
  /** About -24 dBFS: speech, but quieter than LOUD. */
  private static final short QUIET = 2000;

  /** A stream of windows, each filled with one amplitude; 0 is silence. */
  private static EnergyVadSegmenter.WindowReader reader(short[] amplitudes, int from) {
    int[] next = {from};
    return window -> {
      if (next[0] >= amplitudes.length) {
        return false;
      }
      Arrays.fill(window, amplitudes[next[0]++]);
      return true;
    };
  }

  private static List<EnergyVadSegmenter.Span> run(short[] amplitudes, int minSilence, int maxWindows, int from) {
    EnergyVadSegmenter segmenter = new EnergyVadSegmenter(reader(amplitudes, from), SAMPLES, THRESHOLD, minSilence, maxWindows, from);
    List<EnergyVadSegmenter.Span> spans = new ArrayList<>();
    EnergyVadSegmenter.Span span;
    while ((span = segmenter.next()) != null) {
      spans.add(span);
    }
    assertNull(segmenter.next(), "stays ended");
    return spans;
  }

  /** speech windows given as [from, to) pairs over {@code length} windows. */
  private static short[] layout(int length, int... ranges) {
    short[] amplitudes = new short[length];
    for (int i = 0; i < ranges.length; i += 2) {
      Arrays.fill(amplitudes, ranges[i], ranges[i + 1], LOUD);
    }
    return amplitudes;
  }

  private static EnergyVadSegmenter.Span span(long first, int windows) {
    return new EnergyVadSegmenter.Span(first, windows);
  }

  @Test
  void silenceOnlyHasNoSegment() {
    assertEquals(new ArrayList<>(), run(new short[100], 5, 50, 0));
  }

  @Test
  void speechSpansEndAtTheirLastSpeechWindow() {
    // speech 10-20 and 40-45, separated by 20 silent windows (>= 5): two segments, trailing silence trimmed
    assertEquals(Arrays.asList(span(10, 10), span(40, 5)), run(layout(60, 10, 20, 40, 45), 5, 50, 0));
  }

  @Test
  void shortSilencesAreMerged() {
    // gaps of 4 windows (< 5) are part of the segment; a gap of exactly 5 ends it
    assertEquals(Arrays.asList(span(0, 18), span(23, 2)), run(layout(40, 0, 5, 9, 12, 16, 18, 23, 25), 5, 50, 0));
  }

  @Test
  void speechAtTheEndOfTheStreamIsASegment() {
    assertEquals(Arrays.asList(span(7, 3)), run(layout(10, 7, 10), 5, 50, 0));
  }

  @Test
  void overLongSpeechIsCutAtTheQuietestWindowOfItsLastFifth() {
    // 25 windows of speech, bound 10: the last fifth of a full segment is windows 8-9; window 8 is quieter -> cut at 8
    short[] amplitudes = layout(30, 0, 25);
    amplitudes[8] = QUIET;
    amplitudes[17] = QUIET;
    List<EnergyVadSegmenter.Span> spans = run(amplitudes, 5, 10, 0);
    // first: [0, 8); second starts at 8, full at 18 -> last fifth 16-17, 17 is quieter -> [8, 17); third [17, 25)
    assertEquals(Arrays.asList(span(0, 8), span(8, 9), span(17, 8)), spans);
    for (EnergyVadSegmenter.Span s : spans) {
      assertTrue(s.windows() <= 10);
    }
  }

  @Test
  void oneWindowBoundCutsEveryWindow() {
    assertEquals(Arrays.asList(span(2, 1), span(3, 1), span(4, 1)), run(layout(8, 2, 5), 3, 1, 0));
  }

  @Test
  void segmentStartingInsideTheReadAheadIsFound() {
    // the cut leaves windows 8.. buffered; a silence among them then speech: the replay must find the speech after the silence
    short[] amplitudes = layout(40, 0, 10, 12, 15);
    amplitudes[8] = 1;
    // bound 10: [0,10) full -> last fifth 8-9, window 8 is near silent (below the threshold) -> cut at 8
    // replay from 8: 8 silent, 9 speech -> segment from 9: 9, gap 10-11 (2 < 5), 12-14 -> [9, 15)
    assertEquals(Arrays.asList(span(0, 8), span(9, 6)), run(amplitudes, 5, 10, 0));
  }

  @Test
  void payloadIsTheSegmentsSamplesInLittleEndian() {
    short[] amplitudes = layout(10, 2, 4);
    amplitudes[3] = (short) 0x1234;
    EnergyVadSegmenter segmenter = new EnergyVadSegmenter(reader(amplitudes, 0), SAMPLES, THRESHOLD, 3, 50, 0);
    EnergyVadSegmenter.Span span = segmenter.next();
    assertEquals(span(2, 2), span);
    byte[] expected = new byte[2 * SAMPLES * 2];
    for (int i = 0; i < SAMPLES; i++) {
      expected[2 * i] = (byte) LOUD;
      expected[2 * i + 1] = (byte) (LOUD >> 8);
      expected[2 * SAMPLES + 2 * i] = 0x34;
      expected[2 * SAMPLES + 2 * i + 1] = 0x12;
    }
    assertArrayEquals(expected, Arrays.copyOf(segmenter.buffer(), expected.length));
  }

  /**
   * The resume rule: the segments after segment k are what a new segmenter finds starting at the end of segment k, for random
   * level sequences, silence lengths and bounds.
   */
  @Test
  void segmentsAfterAnyEndEqualAFreshRunFromThatEnd() {
    Random random = new Random(42);
    short[] levels = {0, 0, 0, 1, 100, QUIET, LOUD, LOUD, 3000};
    for (int trial = 0; trial < 300; trial++) {
      short[] amplitudes = new short[50 + random.nextInt(300)];
      short current = 0;
      for (int i = 0; i < amplitudes.length; i++) {
        if (random.nextInt(4) == 0) {
          current = levels[random.nextInt(levels.length)];
        }
        amplitudes[i] = current;
      }
      int minSilence = 1 + random.nextInt(8);
      int maxWindows = 1 + random.nextInt(40);
      List<EnergyVadSegmenter.Span> full = run(amplitudes, minSilence, maxWindows, 0);
      for (int k = 0; k < full.size(); k++) {
        EnergyVadSegmenter.Span last = full.get(k);
        int end = (int) (last.firstWindow() + last.windows());
        assertTrue(last.windows() <= maxWindows);
        assertEquals(full.subList(k + 1, full.size()), run(amplitudes, minSilence, maxWindows, end),
            "trial " + trial + " resume after segment " + k);
      }
    }
  }
}
