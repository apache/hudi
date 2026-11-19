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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The resampler tied to the absolute timeline. */
class TestAnchoredResampler {

  private static float tone(long i, int rate, double hz, double amplitude) {
    return (float) (amplitude * Math.sin(2 * Math.PI * hz * i / rate));
  }

  /** Output by absolute index for input samples [from, to) of a tone. */
  private static Map<Long, Short> run(int inRate, int outRate, long from, long to, double hz) {
    Map<Long, Short> out = new TreeMap<>();
    AnchoredResampler resampler = new AnchoredResampler(inRate, outRate, (index, sample) -> {
      Short previous = out.put(index, sample);
      assertEquals(null, previous, "output " + index + " emitted twice");
    });
    resampler.restartAt(from);
    for (long i = from; i < to; i++) {
      resampler.push(tone(i, inRate, hz, 0.5));
    }
    resampler.finish();
    return out;
  }

  @Test
  void equalRatesCopyTheSamples() {
    Map<Long, Short> out = run(16000, 16000, 0, 100, 440);
    assertEquals(100, out.size());
    for (long i = 0; i < 100; i++) {
      assertEquals((short) Math.round(tone(i, 16000, 440, 0.5) * 32768.0), out.get(i));
    }
  }

  @ParameterizedTest
  @CsvSource({"44100,16000", "48000,16000", "22050,16000", "8000,16000", "11025,16000", "16000,8000"})
  void toneKeepsItsLevelAndFrequency(int inRate, int outRate) {
    long seconds = 2;
    Map<Long, Short> out = run(inRate, outRate, 0, seconds * inRate, 1000);
    // every output of the stream's span exists, in order, from 0
    assertEquals(seconds * outRate, out.size(), "outputs");
    assertEquals(0L, (long) ((TreeMap<Long, Short>) out).firstKey());
    // away from the edges: RMS of a 0.5 sine is 0.354 of full scale; 1 kHz = 2000 zero crossings per second
    double energy = 0;
    int crossings = 0;
    long from = outRate / 4;
    long to = outRate * seconds - outRate / 4;
    for (long m = from; m < to; m++) {
      double v = out.get(m) / 32768.0;
      energy += v * v;
      if (m > from && (out.get(m - 1) < 0) != (out.get(m) < 0)) {
        crossings++;
      }
    }
    double rms = Math.sqrt(energy / (to - from));
    assertTrue(Math.abs(rms - 0.5 / Math.sqrt(2)) < 0.01, "rms " + rms);
    double expectedCrossings = 2.0 * 1000 * (to - from) / outRate;
    assertTrue(Math.abs(crossings - expectedCrossings) <= 2, "crossings " + crossings + " vs " + expectedCrossings);
  }

  @ParameterizedTest
  @CsvSource({"44100,16000", "48000,16000", "22050,16000", "8000,16000"})
  void outputDoesNotDependOnWhereTheInputStarted(int inRate, int outRate) {
    Map<Long, Short> full = run(inRate, outRate, 0, inRate, 523.25);
    for (long start : new long[] {1, 441, 1000, 4097, inRate / 3}) {
      Map<Long, Short> resumed = run(inRate, outRate, start, inRate, 523.25);
      AnchoredResampler probe = new AnchoredResampler(inRate, outRate, (i, s) -> { });
      // outputs whose filter reaches back only into input at or after the start are identical
      long firstExact = Math.floorDiv((start + probe.halfWidth() + 1) * outRate, inRate) + 1;
      int compared = 0;
      for (Map.Entry<Long, Short> e : resumed.entrySet()) {
        if (e.getKey() >= firstExact) {
          assertEquals(full.get(e.getKey()), e.getValue(), "start " + start + " output " + e.getKey());
          compared++;
        }
      }
      assertTrue(compared > 1000, "compared " + compared);
    }
  }

  @Test
  void fullScaleInputIsClampedTo16Bits() {
    Map<Long, Short> out = new TreeMap<>();
    AnchoredResampler resampler = new AnchoredResampler(16000, 16000, out::put);
    resampler.push(1.5f);
    resampler.push(-1.5f);
    assertEquals(Short.MAX_VALUE, (short) out.get(0L));
    assertEquals(Short.MIN_VALUE, (short) out.get(1L));
  }
}
