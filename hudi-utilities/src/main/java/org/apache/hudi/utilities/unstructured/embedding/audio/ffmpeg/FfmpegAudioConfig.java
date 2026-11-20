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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;

/**
 * The style settings of {@link FfmpegAudioSegmentSource}, read from the {@code prep.audio.*} keys of {@link AudioConfigKeys} and
 * validated: every value that cannot be honoured is refused with an {@link IllegalArgumentException} naming its key.
 *
 * <p>Speech detection works on {@value #WINDOW_MS} ms windows, so the sample rate must hold a whole number of samples per window
 * (a multiple of 100 Hz) and the segment bounds must hold at least one window.
 */
public final class FfmpegAudioConfig {

  /** Length of one voice-activity window, ms. Windows are aligned to the video's timeline: window w is [w·30, (w+1)·30) ms. */
  public static final int WINDOW_MS = 30;
  /** Bytes of one pcm_s16le mono sample. */
  public static final int BYTES_PER_SAMPLE = 2;

  static final int MIN_SAMPLE_RATE = 8000;
  static final int MAX_SAMPLE_RATE = 48000;
  /** A segment payload is one Java array: the bound stays far below the largest one. */
  static final long MAX_SEGMENT_MAX_BYTES = 256L * 1024 * 1024;
  /** Levels below this are not a meaningful threshold for 16-bit audio (its quietest non-zero window is about -96 dBFS). */
  static final int MIN_VAD_THRESHOLD_DB = -120;
  /** The longest track this source accepts at all: 7 days (keeps sample and microsecond arithmetic far from overflow). */
  static final long MAX_MAX_DURATION_MS = 7L * 24 * 60 * 60 * 1000;

  private final int sampleRate;
  private final int vadThresholdDb;
  private final long vadMinSilenceMs;
  private final long segmentMaxMs;
  private final long segmentMaxBytes;
  private final long maxDurationMs;

  private FfmpegAudioConfig(int sampleRate, int vadThresholdDb, long vadMinSilenceMs, long segmentMaxMs, long segmentMaxBytes,
                            long maxDurationMs) {
    this.sampleRate = sampleRate;
    this.vadThresholdDb = vadThresholdDb;
    this.vadMinSilenceMs = vadMinSilenceMs;
    this.segmentMaxMs = segmentMaxMs;
    this.segmentMaxBytes = segmentMaxBytes;
    this.maxDurationMs = maxDurationMs;
  }

  /**
   * @throws IllegalArgumentException naming the offending key
   */
  public static FfmpegAudioConfig from(TypedProperties props) {
    long sampleRate = parseLong(props, AudioConfigKeys.SAMPLE_RATE.key(), AudioConfigKeys.DEFAULT_SAMPLE_RATE);
    if (sampleRate < MIN_SAMPLE_RATE || sampleRate > MAX_SAMPLE_RATE || sampleRate % 100 != 0) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.SAMPLE_RATE.key() + " must be a multiple of 100 in ["
          + MIN_SAMPLE_RATE + ", " + MAX_SAMPLE_RATE + "] (a " + WINDOW_MS + " ms window is a whole number of samples), got "
          + sampleRate);
    }
    long threshold = parseLong(props, AudioConfigKeys.VAD_THRESHOLD_DB.key(), AudioConfigKeys.DEFAULT_VAD_THRESHOLD_DB);
    if (threshold < MIN_VAD_THRESHOLD_DB || threshold >= 0) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.VAD_THRESHOLD_DB.key() + " must be in [" + MIN_VAD_THRESHOLD_DB
          + ", -1] dBFS, got " + threshold);
    }
    long minSilenceMs = parseLong(props, AudioConfigKeys.VAD_MIN_SILENCE_MS.key(), AudioConfigKeys.DEFAULT_VAD_MIN_SILENCE_MS);
    if (minSilenceMs < WINDOW_MS) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.VAD_MIN_SILENCE_MS.key() + " must be at least one window ("
          + WINDOW_MS + " ms), got " + minSilenceMs);
    }
    long segmentMaxMs = parseLong(props, AudioConfigKeys.SEGMENT_MAX_MS.key(), AudioConfigKeys.DEFAULT_SEGMENT_MAX_MS);
    if (segmentMaxMs < WINDOW_MS) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.SEGMENT_MAX_MS.key() + " must be at least one window ("
          + WINDOW_MS + " ms), got " + segmentMaxMs);
    }
    long windowBytes = sampleRate * WINDOW_MS / 1000 * BYTES_PER_SAMPLE;
    long segmentMaxBytes = parseLong(props, AudioConfigKeys.SEGMENT_MAX_BYTES.key(), AudioConfigKeys.DEFAULT_SEGMENT_MAX_BYTES);
    if (segmentMaxBytes < windowBytes || segmentMaxBytes > MAX_SEGMENT_MAX_BYTES) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.SEGMENT_MAX_BYTES.key() + " must be in [" + windowBytes
          + " (one " + WINDOW_MS + " ms window at " + sampleRate + " Hz), " + MAX_SEGMENT_MAX_BYTES + "], got " + segmentMaxBytes);
    }
    long maxDurationMs = parseLong(props, AudioConfigKeys.MAX_DURATION_MS.key(), AudioConfigKeys.DEFAULT_MAX_DURATION_MS);
    if (maxDurationMs <= 0 || maxDurationMs > MAX_MAX_DURATION_MS) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.MAX_DURATION_MS.key() + " must be in [1, " + MAX_MAX_DURATION_MS
          + "] (7 days), got " + maxDurationMs);
    }
    return new FfmpegAudioConfig((int) sampleRate, (int) threshold, minSilenceMs, segmentMaxMs, segmentMaxBytes, maxDurationMs);
  }

  private static long parseLong(TypedProperties props, String key, long defaultValue) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), null);
    if (value == null || value.trim().isEmpty()) {
      return defaultValue;
    }
    try {
      return Long.parseLong(value.trim());
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + key + " must be an integer, got " + value);
    }
  }

  /** Output sample rate, Hz. */
  public int getSampleRate() {
    return sampleRate;
  }

  /** Samples of one {@value #WINDOW_MS} ms window at the output rate. */
  public int getWindowSamples() {
    return sampleRate * WINDOW_MS / 1000;
  }

  /** A window at or above this level (dBFS) is speech. */
  public int getVadThresholdDb() {
    return vadThresholdDb;
  }

  /** Silent windows after the last speech window that end a segment: {@code ceil(vad.min.silence.ms / 30)}. */
  public int getMinSilenceWindows() {
    return (int) Math.min(Integer.MAX_VALUE, 1 + (vadMinSilenceMs - 1) / WINDOW_MS);
  }

  /** The most windows one segment holds: the smaller of the time bound and the byte bound, at least one. */
  public int getMaxSegmentWindows() {
    long byTime = segmentMaxMs / WINDOW_MS;
    long byBytes = segmentMaxBytes / ((long) getWindowSamples() * BYTES_PER_SAMPLE);
    return (int) Math.max(1, Math.min(Integer.MAX_VALUE, Math.min(byTime, byBytes)));
  }

  public long getMaxDurationMs() {
    return maxDurationMs;
  }
}
