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
 * Energy-gate voice activity over fixed windows, cutting a mono 16-bit sample stream into bounded speech segments, one at a time.
 *
 * <p>A window is speech when its level, {@code 10·log10(mean(s²) / 32768²)} dBFS, is at or above the threshold. A segment starts at
 * the first speech window, absorbs silences shorter than {@code minSilenceWindows}, and ends at the end of its last speech window
 * once that many silent windows follow (trailing silence is not part of it). A segment that reaches {@code maxWindows} ends at its
 * last speech window when it is already in a silence, else it is cut at the start of its quietest window among the last
 * {@value #CUT_SEARCH_PERCENT} % (the earliest on a tie).
 *
 * <p>After a segment is taken the segmenter is in its initial state at the segment's end, and the windows it had read past that end
 * are processed again from there. So the segments after segment k are exactly those a new segmenter finds when it starts at the end
 * of segment k: a resume needs only that end, never the audio before it.
 *
 * <p>Memory: one byte buffer that grows by doubling up to {@code maxWindows} windows (the current segment plus the windows read past
 * its end) and one level per buffered window; nothing grows with the length of the stream.
 */
final class EnergyVadSegmenter {

  /** The cut of an over-long segment is searched in this last share of it. */
  static final int CUT_SEARCH_PERCENT = 20;
  private static final int INITIAL_WINDOWS = 16;
  private static final double FULL_SCALE_SQUARED = 32768.0 * 32768.0;

  /** Supplies the stream one window at a time. */
  interface WindowReader {
    /**
     * Fills {@code window} with the next window's samples (zero-padded when the stream ends inside it).
     *
     * @return false when the stream has no more samples
     */
    boolean read(short[] window);
  }

  /** A segment just cut: its first window (absolute index) and its length in windows; its bytes are the buffer's prefix. */
  static final class Span {
    private final long firstWindow;
    private final int windows;

    Span(long firstWindow, int windows) {
      this.firstWindow = firstWindow;
      this.windows = windows;
    }

    long firstWindow() {
      return firstWindow;
    }

    int windows() {
      return windows;
    }

    @Override
    public boolean equals(Object o) {
      return o instanceof Span && ((Span) o).firstWindow == firstWindow && ((Span) o).windows == windows;
    }

    @Override
    public int hashCode() {
      return Long.hashCode(firstWindow) * 31 + windows;
    }

    @Override
    public String toString() {
      return "[" + firstWindow + "+" + windows + "]";
    }
  }

  private final WindowReader reader;
  private final int windowSamples;
  private final int windowBytes;
  private final double thresholdDb;
  private final int minSilenceWindows;
  private final int maxWindows;
  private final short[] window;

  private byte[] buffer;
  private double[] levels;
  /** Absolute index of the buffer's window 0. */
  private long baseWindow;
  /** Windows held in the buffer. */
  private int buffered;
  /** Buffered windows already processed; those after it are processed again (read past the end of the last segment). */
  private int processed;
  /** True while a segment is open; it starts at buffer window 0. */
  private boolean inSegment;
  private int lastSpeech;
  /** Windows of the segment taken last; dropped from the buffer when the next segment is requested. */
  private int taken;
  private boolean ended;

  /**
   * @param firstWindow absolute index of the first window {@code reader} supplies
   */
  EnergyVadSegmenter(WindowReader reader, int windowSamples, double thresholdDb, int minSilenceWindows, int maxWindows,
                     long firstWindow) {
    this.reader = reader;
    this.windowSamples = windowSamples;
    this.windowBytes = windowSamples * FfmpegAudioConfig.BYTES_PER_SAMPLE;
    this.thresholdDb = thresholdDb;
    this.minSilenceWindows = minSilenceWindows;
    this.maxWindows = maxWindows;
    this.window = new short[windowSamples];
    int initial = Math.min(maxWindows, INITIAL_WINDOWS);
    this.buffer = new byte[initial * windowBytes];
    this.levels = new double[initial];
    this.baseWindow = firstWindow;
  }

  /** The next segment, or null at the end of the stream. Its bytes stay valid until the next call. */
  Span next() {
    dropTaken();
    while (true) {
      int w;
      if (processed < buffered) {
        w = processed++;
      } else {
        if (ended || !readWindow()) {
          ended = true;
          return inSegment ? take(lastSpeech + 1) : null;
        }
        w = processed++;
      }
      Span span = process(w);
      if (span != null) {
        return span;
      }
    }
  }

  /** The buffer whose first {@code span.windows() · windowBytes} bytes are the segment last returned, pcm_s16le. */
  byte[] buffer() {
    return buffer;
  }

  int windowBytes() {
    return windowBytes;
  }

  /** Bytes allocated by this segmenter's buffers (the per-task term this class adds). */
  long allocatedBytes() {
    return buffer.length + (long) levels.length * Double.BYTES + (long) window.length * Short.BYTES;
  }

  private Span process(int w) {
    boolean speech = levels[w] >= thresholdDb;
    if (!inSegment) {
      if (!speech) {
        if (processed == buffered) {
          // nothing buffered is pending: the silence read so far is dropped
          baseWindow += buffered;
          buffered = 0;
          processed = 0;
        }
        return null;
      }
      shiftToFront(w);
      inSegment = true;
      lastSpeech = 0;
      w = 0;
    } else if (speech) {
      lastSpeech = w;
    }
    if (w - lastSpeech >= minSilenceWindows) {
      return take(lastSpeech + 1);
    }
    if (w + 1 >= maxWindows) {
      // full: a segment already in its trailing silence ends at its last speech window, one still speaking at its quietest window
      return take(lastSpeech < w ? lastSpeech + 1 : cutPoint(w + 1));
    }
    return null;
  }

  /** Start of the quietest window in the last share of a full segment of {@code length} windows; at least 1. */
  private int cutPoint(int length) {
    int from = Math.max(1, length - Math.max(1, length * CUT_SEARCH_PERCENT / 100));
    if (from >= length) {
      return length;
    }
    int quietest = from;
    for (int i = from + 1; i < length; i++) {
      if (levels[i] < levels[quietest]) {
        quietest = i;
      }
    }
    return quietest;
  }

  private Span take(int windows) {
    inSegment = false;
    taken = windows;
    return new Span(baseWindow, windows);
  }

  /** Drops the segment taken last; the windows read past its end move to the front, to be processed again. */
  private void dropTaken() {
    if (taken > 0) {
      shiftToFront(taken);
      processed = 0;
      taken = 0;
    }
  }

  private void shiftToFront(int w) {
    if (w == 0) {
      return;
    }
    System.arraycopy(buffer, w * windowBytes, buffer, 0, (buffered - w) * windowBytes);
    System.arraycopy(levels, w, levels, 0, buffered - w);
    baseWindow += w;
    buffered -= w;
    processed -= w;
  }

  private boolean readWindow() {
    if (!reader.read(window)) {
      return false;
    }
    if (buffered == levels.length) {
      int capacity = (int) Math.min((long) levels.length * 2, Math.max(maxWindows, levels.length + 1L));
      buffer = Arrays.copyOf(buffer, Math.multiplyExact(capacity, windowBytes));
      levels = Arrays.copyOf(levels, capacity);
    }
    int offset = buffered * windowBytes;
    double energy = 0;
    for (int i = 0; i < windowSamples; i++) {
      short s = window[i];
      energy += (double) s * s;
      buffer[offset++] = (byte) s;
      buffer[offset++] = (byte) (s >> 8);
    }
    levels[buffered] = energy == 0 ? Double.NEGATIVE_INFINITY : 10 * Math.log10(energy / windowSamples / FULL_SCALE_SQUARED);
    buffered++;
    return true;
  }
}
