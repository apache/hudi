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

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;

/**
 * Audio configuration: runtime implementation keys ({@link #RUNTIME_PREFIX}) and output-changing style values
 * ({@link #STYLE_PREFIX}). The audio prep logic version persists the resolved implementations' logic, and each style value is
 * pinned on the table at its effective value.
 * Implementations fold output-changing runtime settings into their logicVersion/id; model locations and thread counts
 * do not become style by raw value. Disabled audio does not instantiate its implementations.
 */
@ConfigClassProperty(name = "Embedding Audio Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations controlling audio preparation, speech segmentation and transcription implementations.")
public final class AudioConfigKeys {

  private static final String SINCE_VERSION = "1.3.0";

  /** Prefix of the non-style audio keys. */
  public static final String RUNTIME_PREFIX = EmbeddingTableConfig.PREFIX + "audio.";

  /** Prefix of the audio style keys. */
  public static final String STYLE_PREFIX = EmbeddingTableConfig.PREP_PREFIX + "audio.";

  /**
   * Whether video prep also writes the video's speech rows; default {@link #DEFAULT_ENABLED} (audio off). Not a style key: when on,
   * the resolved audio logic line is.
   */
  public static final ConfigProperty<Boolean> ENABLED = ConfigProperty
      .key(RUNTIME_PREFIX + "enabled")
      .defaultValue(false)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Whether video prep also writes the video's speech rows; default false (audio off). Not a style key: when "
          + "on, the resolved audio logic line is.");
  public static final boolean DEFAULT_ENABLED = false;

  /**
   * {@link AudioSegmentSource} implementation (public no-arg constructor); unset, the default
   * {@link AudioConfig#DEFAULT_SEGMENT_SOURCE_CLASS} (FFmpeg). Not a style key.
   */
  public static final ConfigProperty<String> SEGMENT_SOURCE_CLASS = ConfigProperty
      .key(RUNTIME_PREFIX + "segment.source.class")
      .defaultValue("org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("AudioSegmentSource implementation (public no-arg constructor); unset, the default "
          + "org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource (FFmpeg). Not a "
          + "style key.");

  /**
   * {@link SegmentTranscriber} implementation (public no-arg constructor); unset, the default
   * {@link AudioConfig#DEFAULT_TRANSCRIBER_CLASS} (whisper). Not a style key.
   */
  public static final ConfigProperty<String> TRANSCRIBER_CLASS = ConfigProperty
      .key(RUNTIME_PREFIX + "transcriber.class")
      .defaultValue("org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("SegmentTranscriber implementation (public no-arg constructor); unset, the default "
          + "org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber (whisper). Not a "
          + "style key.");

  /**
   * {@link TranscriptChunker} implementation (public no-arg constructor); unset, the default
   * {@link AudioConfig#DEFAULT_CHUNKER_CLASS} (the transcript packer). Not a style key.
   */
  public static final ConfigProperty<String> CHUNKER_CLASS = ConfigProperty
      .key(RUNTIME_PREFIX + "chunker.class")
      .defaultValue("org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptPacker")
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("TranscriptChunker implementation (public no-arg constructor); unset, the default "
          + "org.apache.hudi.utilities.unstructured.embedding.audio.TranscriptPacker (the transcript packer). Not a "
          + "style key.");

  /** Sample rate the audio is resampled to, Hz; default {@link #DEFAULT_SAMPLE_RATE} (the common speech-model rate). */
  public static final ConfigProperty<Integer> SAMPLE_RATE = ConfigProperty
      .key(STYLE_PREFIX + "sample.rate")
      .defaultValue(16000)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Sample rate the audio is resampled to, Hz; default 16000 (the common speech-model rate).");
  public static final int DEFAULT_SAMPLE_RATE = 16000;

  /** Speech detection: energy threshold in dBFS below which a window is silence; default {@link #DEFAULT_VAD_THRESHOLD_DB}. */
  public static final ConfigProperty<Integer> VAD_THRESHOLD_DB = ConfigProperty
      .key(STYLE_PREFIX + "vad.threshold.db")
      .defaultValue(-35)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Speech detection: energy threshold in dBFS below which a window is silence; default -35.");
  /** Placeholder default, not yet measured. */
  public static final int DEFAULT_VAD_THRESHOLD_DB = -35;

  /** Speech detection: silence that ends a speech span, ms; default {@link #DEFAULT_VAD_MIN_SILENCE_MS}. */
  public static final ConfigProperty<Long> VAD_MIN_SILENCE_MS = ConfigProperty
      .key(STYLE_PREFIX + "vad.min.silence.ms")
      .defaultValue(500L)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Speech detection: silence that ends a speech span, ms; default 500.");
  /** Placeholder default, not yet measured. */
  public static final long DEFAULT_VAD_MIN_SILENCE_MS = 500L;

  /** Longest segment, ms; a longer speech span is cut; default {@link #DEFAULT_SEGMENT_MAX_MS}. */
  public static final ConfigProperty<Long> SEGMENT_MAX_MS = ConfigProperty
      .key(STYLE_PREFIX + "segment.max.ms")
      .defaultValue(30000L)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Longest segment, ms; a longer speech span is cut; default 30000.");
  /** Placeholder default 30 s (not yet measured; bounded by the chosen engine's per-request limit). */
  public static final long DEFAULT_SEGMENT_MAX_MS = 30_000L;

  /** Largest segment payload, bytes; default {@link #DEFAULT_SEGMENT_MAX_BYTES} (30 s of 16 kHz mono 16-bit = 960,000 B). */
  public static final ConfigProperty<Long> SEGMENT_MAX_BYTES = ConfigProperty
      .key(STYLE_PREFIX + "segment.max.bytes")
      .defaultValue(1048576L)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Largest segment payload, bytes; default 1048576 (30 s of 16 kHz mono 16-bit = 960,000 B).");
  public static final long DEFAULT_SEGMENT_MAX_BYTES = 1024L * 1024L;

  /** Longest audio track accepted, ms; a longer one fails its video before any decode; default {@link #DEFAULT_MAX_DURATION_MS}. */
  public static final ConfigProperty<Long> MAX_DURATION_MS = ConfigProperty
      .key(STYLE_PREFIX + "max.duration.ms")
      .defaultValue(14400000L)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Longest audio track accepted, ms; a longer one fails its video before any decode; default 14400000.");
  public static final long DEFAULT_MAX_DURATION_MS = 4L * 60 * 60 * 1000;

  private AudioConfigKeys() {
  }
}
