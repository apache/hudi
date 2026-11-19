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

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;

/**
 * The resolved audio configuration ({@link AudioConfigKeys}) of one embedding table: on/off, the three implementations of the
 * audio seam, their bounds, and the audio logic line that joins the preprocessing style.
 *
 * <p>Registration contract of an implementation (a real segment source, transcriber or chunker registers here by
 * configuration alone, without editing data prep or the default implementations):
 * <ul>
 *   <li>it is named by ONE class key ({@link AudioConfigKeys#SEGMENT_SOURCE_CLASS}, {@link AudioConfigKeys#TRANSCRIBER_CLASS},
 *       {@link AudioConfigKeys#CHUNKER_CLASS}) and has a public no-arg constructor; unset, the key resolves to the real
 *       implementation ({@link #DEFAULT_SEGMENT_SOURCE_CLASS}: FFmpeg, {@link #DEFAULT_TRANSCRIBER_CLASS}: whisper,
 *       {@link #DEFAULT_CHUNKER_CLASS}: {@link TranscriptPacker}), so enabling audio never embeds stub text: the whisper
 *       transcriber needs its model ({@code ...transcriber.whisper.model.dir} for the default named model), and
 *       {@link #validate()} refuses a job without it at start, naming the key. The stubs are test doubles only;</li>
 *   <li>a segment source's or transcriber's own settings (a model file, a language) live under {@link #SEGMENT_SOURCE_PREFIX}
 *       or {@link #TRANSCRIBER_PREFIX}: NOT style keys by raw value; the implementation folds every setting that changes its
 *       output into {@link AudioSegmentSource#logicVersion()} or {@link SegmentTranscriber#id()}, which the {@link #logicLine}
 *       records in the style. A change is not detected; use a new table for a different preprocessing style. A chunker's
 *       settings are the table's style keys (below);</li>
 *   <li>table-wide segmentation settings stay {@link AudioConfigKeys#STYLE_PREFIX} keys, recorded by value;</li>
 *   <li>a segment source's or transcriber's {@code validate(props)} runs on the driver before any Spark work and throws
 *       {@link IllegalArgumentException} naming the key; it is also the call that hands an instance its configuration:
 *       every source and transcriber instance (driver or task) is validated before its {@code logicVersion()} /
 *       {@code id()} is read or it is used, so a version built from the implementation's own keys is safe. A
 *       {@link TranscriptChunker} has no {@code validate}: its {@code logicVersion()} is read without the configuration,
 *       so its own {@link #CHUNKER_PREFIX} keys cannot reach the version; a chunker whose output depends on settings
 *       reads the table's {@code chunk.*} style keys, which are recorded by value;</li>
 *   <li>lifecycle: instances are cheap to construct (the driver builds throwaway ones for {@link #validate} and
 *       {@link #logicLine}; acquire an engine or model lazily, on first use). Data prep's video preprocessor builds ONE instance of
 *       each per Spark task, uses it for the task's videos one after another (never concurrently), and, when it implements
 *       {@link AutoCloseable}, closes it exactly once at the end of the task (success, failure or completion); every
 *       throwaway driver instance is closed too.</li>
 * </ul>
 */
public final class AudioConfig implements Serializable {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(AudioConfig.class);

  /** Prefix of the segment source implementation's own (non-style) keys (beside its class key); never {@code .class}. */
  public static final String SEGMENT_SOURCE_PREFIX = AudioConfigKeys.RUNTIME_PREFIX + "segment.source.";
  /** Prefix of the transcriber implementation's own (non-style) keys, e.g. its model; never {@code .class}. */
  public static final String TRANSCRIBER_PREFIX = AudioConfigKeys.RUNTIME_PREFIX + "transcriber.";
  /** Prefix of the chunker implementation's own (non-style) keys; never {@code .class}. */
  public static final String CHUNKER_PREFIX = AudioConfigKeys.RUNTIME_PREFIX + "chunker.";

  /**
   * The segment source when {@link AudioConfigKeys#SEGMENT_SOURCE_CLASS} is unset: the FFmpeg source, by name, so nothing loads
   * FFmpeg (not shipped by Hudi; the job adds its jars) unless a video's speech is prepared.
   */
  public static final String DEFAULT_SEGMENT_SOURCE_CLASS =
      "org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource";
  /** The transcriber when {@link AudioConfigKeys#TRANSCRIBER_CLASS} is unset: whisper, by name. */
  public static final String DEFAULT_TRANSCRIBER_CLASS =
      "org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber";
  /** The chunker when {@link AudioConfigKeys#CHUNKER_CLASS} is unset: the transcript packer. */
  public static final String DEFAULT_CHUNKER_CLASS = TranscriptPacker.class.getName();

  /** Key of the segment source's logic version in the {@link #logicStyle} object. */
  public static final String STYLE_SOURCE = "source";
  /** Key of the transcriber's id in the {@link #logicStyle} object. */
  public static final String STYLE_ASR = "asr";
  /** Key of the transcript chunker's logic version in the {@link #logicStyle} object. */
  public static final String STYLE_PACK = "pack";
  /** Key of the tokenizer's id in the {@link #logicStyle} object. */
  public static final String STYLE_TOKENIZER = "tokenizer";
  /** Key of the speech-row logic version in the {@link #logicStyle} object. */
  public static final String STYLE_SPEECH = "speech";

  private final TypedProperties props;

  private AudioConfig(TypedProperties props) {
    this.props = props;
  }

  public static AudioConfig from(TypedProperties props) {
    return new AudioConfig(props);
  }

  /**
   * {@link AudioConfigKeys#ENABLED}; default {@link AudioConfigKeys#DEFAULT_ENABLED}.
   *
   * @throws IllegalArgumentException if the value is neither {@code true} nor {@code false}
   */
  public boolean enabled() {
    String value = ConfigUtils.getStringWithAltKeys(props, AudioConfigKeys.ENABLED, String.valueOf(AudioConfigKeys.DEFAULT_ENABLED)).trim();
    if (!"true".equalsIgnoreCase(value) && !"false".equalsIgnoreCase(value)) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.ENABLED.key() + " must be true or false, got " + value);
    }
    return "true".equalsIgnoreCase(value);
  }

  /** {@link AudioConfigKeys#SEGMENT_MAX_MS}, positive. */
  long segmentMaxMs() {
    return positive(AudioConfigKeys.SEGMENT_MAX_MS.key(), AudioConfigKeys.DEFAULT_SEGMENT_MAX_MS);
  }

  /** {@link AudioConfigKeys#MAX_DURATION_MS}, positive. */
  long maxDurationMs() {
    return positive(AudioConfigKeys.MAX_DURATION_MS.key(), AudioConfigKeys.DEFAULT_MAX_DURATION_MS);
  }

  /** {@link AudioConfigKeys#SAMPLE_RATE}, positive. */
  int sampleRate() {
    long value = positive(AudioConfigKeys.SAMPLE_RATE.key(), AudioConfigKeys.DEFAULT_SAMPLE_RATE);
    if (value > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Config " + AudioConfigKeys.SAMPLE_RATE.key() + " must be at most " + Integer.MAX_VALUE
          + " but is " + value);
    }
    return (int) value;
  }

  /** A new instance of the configured {@link AudioConfigKeys#SEGMENT_SOURCE_CLASS}. */
  public AudioSegmentSource newSegmentSource() {
    return load(AudioConfigKeys.SEGMENT_SOURCE_CLASS.key(), DEFAULT_SEGMENT_SOURCE_CLASS, AudioSegmentSource.class);
  }

  /** A new instance of the configured {@link AudioConfigKeys#TRANSCRIBER_CLASS}. */
  public SegmentTranscriber newTranscriber() {
    return load(AudioConfigKeys.TRANSCRIBER_CLASS.key(), DEFAULT_TRANSCRIBER_CLASS, SegmentTranscriber.class);
  }

  /** A new instance of the configured {@link AudioConfigKeys#CHUNKER_CLASS}. */
  public TranscriptChunker newChunker() {
    return load(AudioConfigKeys.CHUNKER_CLASS.key(), DEFAULT_CHUNKER_CLASS, TranscriptChunker.class);
  }

  /**
   * The audio logic line ({@link AudioRows#AUDIO_LOGIC_FORMAT}) of the configured implementations under {@code tokenizer}:
   * one line of the preprocessing style when audio is enabled, and aux {@link AudioRows#AUX_AUDIO_LOGIC} of every audio row.
   * The source and the transcriber are validated first: an implementation may build its version from its own keys, which it
   * reads in {@code validate(props)}.
   */
  public String logicLine(Tokenizer tokenizer) {
    Map<String, String> style = logicStyle(tokenizer);
    return String.format(AudioRows.AUDIO_LOGIC_FORMAT, style.get(STYLE_SOURCE), style.get(STYLE_ASR), style.get(STYLE_PACK),
        style.get(STYLE_TOKENIZER));
  }

  /**
   * The audio logic of the configured implementations under {@code tokenizer}: {@value #STYLE_SOURCE},
   * {@value #STYLE_ASR}, {@value #STYLE_PACK}, {@value #STYLE_TOKENIZER} and {@value #STYLE_SPEECH}, the values of
   * {@link #logicLine(Tokenizer)}. The source and the transcriber are validated first, as there.
   */
  public Map<String, String> logicStyle(Tokenizer tokenizer) {
    AudioSegmentSource source = null;
    SegmentTranscriber transcriber = null;
    TranscriptChunker chunker = null;
    try {
      source = newSegmentSource();
      transcriber = newTranscriber();
      chunker = newChunker();
      source.validate(props);
      transcriber.validate(props);
      Map<String, String> style = new TreeMap<>();
      style.put(STYLE_SOURCE, source.logicVersion());
      style.put(STYLE_ASR, transcriber.id());
      style.put(STYLE_PACK, chunker.logicVersion());
      style.put(STYLE_TOKENIZER, tokenizer.id());
      style.put(STYLE_SPEECH, AudioRows.SPEECH_LOGIC_VERSION);
      return style;
    } finally {
      closeQuietly(source);
      closeQuietly(transcriber);
      closeQuietly(chunker);
    }
  }

  /**
   * The audio logic line of these implementations, already validated (their versions may depend on the keys they read in
   * {@code validate(props)}), under {@code tokenizer}.
   */
  public static String logicLine(AudioSegmentSource source, SegmentTranscriber transcriber, TranscriptChunker chunker,
                                 Tokenizer tokenizer) {
    return String.format(AudioRows.AUDIO_LOGIC_FORMAT, source.logicVersion(), transcriber.id(), chunker.logicVersion(),
        tokenizer.id());
  }

  /** {@link #logicLine(Tokenizer)} under the table's configured tokenizer. */
  public String logicLine() {
    return logicLine(TokenizerFactory.create(props));
  }

  /**
   * Checks the whole audio configuration on the driver: every bound parses, the three classes load, and the source's and
   * transcriber's own {@code validate}.
   *
   * @throws IllegalArgumentException naming the offending key
   */
  public void validate() {
    segmentMaxMs();
    maxDurationMs();
    sampleRate();
    AudioSegmentSource source = null;
    SegmentTranscriber transcriber = null;
    TranscriptChunker chunker = null;
    try {
      source = newSegmentSource();
      transcriber = newTranscriber();
      chunker = newChunker();
      source.validate(props);
      transcriber.validate(props);
    } finally {
      closeQuietly(source);
      closeQuietly(transcriber);
      closeQuietly(chunker);
    }
  }

  /**
   * Closes {@code implementation} when it is {@link AutoCloseable} (an implementation holding a native engine context); a
   * failing close is logged, never thrown, so it cannot hide the outcome it follows.
   */
  public static void closeQuietly(Object implementation) {
    if (implementation instanceof AutoCloseable) {
      try {
        ((AutoCloseable) implementation).close();
      } catch (Exception | LinkageError e) {
        LOG.warn("Closing audio implementation {} failed", implementation.getClass().getName(), e);
      }
    }
  }

  private long positive(String key, long defaultValue) {
    String raw = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), String.valueOf(defaultValue)).trim();
    long value;
    try {
      value = Long.parseLong(raw);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Config " + key + " must be an integer, got " + raw, e);
    }
    if (value <= 0) {
      throw new IllegalArgumentException("Config " + key + " must be positive but is " + value);
    }
    return value;
  }

  private <T> T load(String key, String defaultClass, Class<T> type) {
    String configured = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), "").trim();
    String className = configured.isEmpty() ? defaultClass : configured;
    Class<?> implementation;
    try {
      implementation = ReflectionUtils.getClass(className);
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Config " + key + "=" + className + " cannot be loaded", e);
    }
    if (!type.isAssignableFrom(implementation)) {
      throw new IllegalArgumentException("Config " + key + "=" + className + " is not a " + type.getName());
    }
    try {
      return type.cast(ReflectionUtils.loadClass(className));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException("Config " + key + "=" + className
          + " cannot be instantiated through a public no-arg constructor", e);
    }
  }
}
