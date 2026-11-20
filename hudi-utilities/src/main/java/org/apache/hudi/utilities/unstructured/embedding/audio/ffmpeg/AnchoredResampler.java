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

import java.util.Arrays;

/**
 * Converts mono samples at the stream's rate to 16-bit samples at the output rate, with every output sample tied to the ABSOLUTE
 * timeline: output sample {@code m} is the band-limited input evaluated at input position {@code m · inRate / outRate}, so its value
 * depends only on the input samples around that position and never on where the input started (FFmpeg's resampler, by contrast,
 * fixes its output phase and delay at its first input sample, so a resume after a seek would be shifted against a run from 0).
 *
 * <p>Filter: a Blackman-windowed sinc low-pass at {@value #CUTOFF} of the lower Nyquist frequency with {@value #ZERO_CROSSINGS} zero
 * crossings on each side, tabulated at {@value #TABLE_STEPS} points per input sample and linearly interpolated; equal rates copy.
 * Output is {@code round(y · 32768)} clamped to 16 bits. Input before the first sample pushed counts as silence.
 *
 * <p>Memory: the kernel table ({@code TABLE_STEPS · halfWidth} floats) and a ring of {@code 2 · halfWidth + 2} input samples.
 */
final class AnchoredResampler {

  static final double CUTOFF = 0.9;
  static final int ZERO_CROSSINGS = 8;
  static final int TABLE_STEPS = 512;

  /** Receives output samples in order of their absolute index. */
  interface Sink {
    void accept(long index, short sample);
  }

  private final long inRate;
  private final long outRate;
  private final boolean copy;
  /** Input samples on each side of an output position that the kernel reaches. */
  private final int halfWidth;
  private final float[] table;
  private final float[] ring;
  private final int ringMask;
  private final Sink sink;

  /** Absolute index of the first input sample since the last {@link #restartAt}. */
  private long firstInput;
  /** Absolute index of the next input sample. */
  private long nextInput;
  /** Absolute index of the next output sample. */
  private long nextOutput;

  AnchoredResampler(int inRate, int outRate, Sink sink) {
    if (inRate <= 0 || outRate <= 0) {
      throw new IllegalArgumentException("sample rates must be positive: " + inRate + " -> " + outRate);
    }
    this.inRate = inRate;
    this.outRate = outRate;
    this.sink = sink;
    this.copy = inRate == outRate;
    if (copy) {
      this.halfWidth = 0;
      this.table = new float[0];
      this.ring = new float[1];
      this.ringMask = 0;
    } else {
      // cutoff as a fraction of the input rate, times two: sinc(bandwidth · t) has its zeros at multiples of 1 / bandwidth
      double bandwidth = CUTOFF * Math.min(inRate, outRate) / inRate;
      this.halfWidth = (int) Math.ceil(ZERO_CROSSINGS / bandwidth);
      this.table = new float[halfWidth * TABLE_STEPS + 2];
      for (int j = 0; j < table.length; j++) {
        double t = (double) j / TABLE_STEPS;
        table[j] = t >= halfWidth ? 0f : (float) (bandwidth * sinc(bandwidth * t) * blackman(t / halfWidth));
      }
      int size = Integer.highestOneBit(2 * halfWidth + 2) << 1;
      this.ring = new float[size];
      this.ringMask = size - 1;
    }
    restartAt(0);
  }

  /**
   * Continues the input at absolute input index {@code inputIndex} with no history (input before it counts as silence); the next
   * output is the first whose position is at or after it. Outputs between the previous position and this one are never produced.
   */
  void restartAt(long inputIndex) {
    Arrays.fill(ring, 0f);
    firstInput = inputIndex;
    nextInput = inputIndex;
    nextOutput = ceilDiv(inputIndex * outRate, inRate);
  }

  /** Input samples on each side of an output position that the filter reads (0 when the rates are equal). */
  int halfWidth() {
    return halfWidth;
  }

  /** Absolute index of the next input sample {@link #push} expects. */
  long nextInput() {
    return nextInput;
  }

  /** Feeds the next input sample, in [-1, 1]; emits every output whose kernel the input now covers. */
  void push(float sample) {
    if (copy) {
      sink.accept(nextInput++, quantize(sample));
      nextOutput = nextInput;
      return;
    }
    ring[(int) (nextInput & ringMask)] = sample;
    nextInput++;
    emitReady(nextInput - 1);
  }

  /**
   * Emits the outputs that need input past the last sample pushed, taking that input as silence (the end of the stream): every
   * output whose position lies before the end of the last input sample's period, {@code m · inRate / outRate < nextInput}.
   */
  void finish() {
    if (copy) {
      return;
    }
    long last = nextInput - 1;
    while (true) {
      if (nextOutput * inRate >= nextInput * outRate) {
        // the output lies past the stream's end
        return;
      }
      sink.accept(nextOutput, quantize(evaluate(nextOutput, last)));
      nextOutput++;
    }
  }

  /** Bytes allocated by this resampler (the per-task term this class adds). */
  long allocatedBytes() {
    return (long) (table.length + ring.length) * Float.BYTES;
  }

  private void emitReady(long lastInput) {
    while (true) {
      long center = Math.floorDiv(nextOutput * inRate, outRate);
      if (center + halfWidth > lastInput) {
        return;
      }
      sink.accept(nextOutput, quantize(evaluate(nextOutput, lastInput)));
      nextOutput++;
    }
  }

  /** The band-limited input at output m's position; inputs past {@code lastInput} or before the first count as silence. */
  private double evaluate(long m, long lastInput) {
    long scaled = m * inRate;
    long center = Math.floorDiv(scaled, outRate);
    double fraction = (double) (scaled - center * outRate) / outRate;
    double sum = 0;
    // inputs before the first or past lastInput are silence: the window is clamped to the inputs there are
    long last = Math.min(center + halfWidth, lastInput);
    for (long i = Math.max(center - halfWidth + 1, firstInput); i <= last; i++) {
      double distance = Math.abs(center - i + fraction) * TABLE_STEPS;
      int j = (int) distance;
      if (j < table.length - 1) {
        double weight = table[j] + (distance - j) * (table[j + 1] - table[j]);
        sum += weight * ring[(int) (i & ringMask)];
      }
    }
    return sum;
  }

  private static short quantize(double value) {
    long scaled = Math.round(value * 32768.0);
    return (short) Math.max(Short.MIN_VALUE, Math.min(Short.MAX_VALUE, scaled));
  }

  private static double sinc(double x) {
    return x == 0 ? 1.0 : Math.sin(Math.PI * x) / (Math.PI * x);
  }

  private static double blackman(double x) {
    // x in [0, 1): the window's right half, 1 at the centre
    double phase = Math.PI * (1 + x);
    return 0.42 - 0.5 * Math.cos(phase) + 0.08 * Math.cos(2 * phase);
  }

  private static long ceilDiv(long a, long b) {
    return -Math.floorDiv(-a, b);
  }
}
