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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import org.apache.hudi.common.config.ConfigClassProperty;
import org.apache.hudi.common.config.ConfigGroups;
import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * The resolved settings of {@link WhisperSegmentTranscriber}. Its keys live under the transcriber's implementation-owned prefix
 * {@code hoodie.unstructured.embedding.audio.transcriber.whisper.}; they are not style keys: the settings that change the text
 * (model, language, task) are folded into {@link #id()}, which the audio logic line records as {@code asr=<id>}, so changing one
 * of them is a new preprocessing style. {@link #threadCount()} only changes speed (whisper.cpp's greedy decoding gives the same
 * transcript for any thread count), so it stays out of the id.
 */
@ConfigClassProperty(name = "Whisper Transcription Configs",
    groupName = ConfigGroups.Names.HUDI_STREAMER,
    subGroupName = ConfigGroups.SubGroupNames.DELTA_STREAMER_SOURCE,
    description = "Configurations selecting the whisper.cpp model, language, task and transcription threads.")
public final class WhisperConfig implements Serializable {

  private static final String SINCE_VERSION = "1.3.0";

  private static final long serialVersionUID = 1L;

  /** Prefix of every whisper key. */
  public static final String PREFIX = AudioConfigKeys.RUNTIME_PREFIX + "transcriber.whisper.";

  /** base.en: the smaller native footprint of the two benchmarked models (about 0.38 GB of whisper.cpp memory against 0.82 GB). */
  public static final String DEFAULT_MODEL = "base.en";
  /**
   * The ggml model: a name such as {@code base.en} or {@code small.en}, resolved to {@code <model.dir>/ggml-<name>.bin}, or an
   * absolute path to a {@code .bin} file; default {@link #DEFAULT_MODEL}. Models whose name ends {@code .en} are English-only.
   */
  public static final ConfigProperty<String> MODEL = ConfigProperty
      .key(PREFIX + "model")
      .defaultValue(DEFAULT_MODEL)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("The ggml model: a name such as base.en or small.en, resolved to <model.dir>/ggml-<name>.bin, or an absolute "
          + "path to a .bin file; default base.en. Models whose name ends .en are English-only.");

  /** Directory holding {@code ggml-<name>.bin} for a named model; it must exist on every executor. */
  public static final ConfigProperty<String> MODEL_DIR = ConfigProperty
      .key(PREFIX + "model.dir")
      .defaultValue("")
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Directory holding ggml-<name>.bin for a named model; it must exist on every executor. required for named "
          + "model; not required for absolute model path.");

  public static final String DEFAULT_LANGUAGE = "en";
  /** Spoken language: one of whisper's language codes, or {@code auto} to detect it (multilingual models only); default {@code en}. */
  public static final ConfigProperty<String> LANGUAGE = ConfigProperty
      .key(PREFIX + "language")
      .defaultValue(DEFAULT_LANGUAGE)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Spoken language: one of whisper's language codes, or auto to detect it (multilingual models only); default "
          + "en.");

  public static final String TASK_TRANSCRIBE = "transcribe";
  /** {@code transcribe} (text in the spoken language) or {@code translate} (English text; multilingual models only). */
  public static final ConfigProperty<String> TASK = ConfigProperty
      .key(PREFIX + "task")
      .defaultValue(TASK_TRANSCRIBE)
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("transcribe (text in the spoken language) or translate (English text; multilingual models only).");
  public static final String TASK_TRANSLATE = "translate";

  /** Threads of one transcription; default 1, one per Spark task core. */
  public static final ConfigProperty<Integer> THREADS = ConfigProperty
      .key(PREFIX + "threads")
      .defaultValue(1)
      .markAdvanced()
      .sinceVersion(SINCE_VERSION)
      .withDocumentation("Threads of one transcription; default 1, one per Spark task core.");
  public static final int DEFAULT_THREADS = 1;

  /** whisper.cpp version inside the whisper-jni binding this class is built against. */
  public static final String ENGINE_VERSION = "1.7.1";

  /** The sample rate whisper.cpp takes; the audio pass must resample to it. */
  public static final int WHISPER_SAMPLE_RATE = 16000;

  /**
   * Upper bound accepted for {@code ...prep.audio.segment.max.bytes}: 64 MiB, about 35 minutes of 16 kHz mono 16-bit audio. Whisper
   * decodes 30 s windows; a larger bound is a misconfiguration and would make one call hold up to 128 MiB of float samples.
   */
  public static final long MAX_SEGMENT_BYTES = 64L * 1024 * 1024;

  /** Language codes whisper.cpp 1.7.1 knows (its g_lang table; evidence: the native library's string table). */
  private static final Set<String> LANGUAGES = new HashSet<>(Arrays.asList(
      "en", "zh", "de", "es", "ru", "ko", "fr", "ja", "pt", "tr", "pl", "ca", "nl", "ar", "sv", "it", "id", "hi", "fi", "vi",
      "he", "uk", "el", "ms", "cs", "ro", "da", "hu", "ta", "no", "th", "ur", "hr", "bg", "lt", "la", "mi", "ml", "cy", "sk",
      "te", "fa", "lv", "bn", "sr", "az", "sl", "kn", "et", "mk", "br", "eu", "is", "hy", "ne", "mn", "bs", "kk", "sq", "sw",
      "gl", "mr", "pa", "si", "km", "sn", "yo", "so", "af", "oc", "ka", "be", "tg", "sd", "gu", "am", "yi", "lo", "uz", "fo",
      "ht", "ps", "tk", "nn", "mt", "sa", "lb", "my", "bo", "tl", "mg", "as", "tt", "haw", "ln", "ha", "ba", "jw", "su", "yue"));
  private static final String AUTO_LANGUAGE = "auto";

  private static final Pattern MODEL_NAME = Pattern.compile("[A-Za-z0-9._-]+");
  private static final String MODEL_FILE_PREFIX = "ggml-";
  private static final String MODEL_FILE_SUFFIX = ".bin";

  private final String modelName;
  private final String modelPath;
  private final String languageCode;
  private final String taskName;
  private final int threadCount;
  private final long segmentMaxBytes;

  private WhisperConfig(String modelName, String modelPath, String language, String task, int threads, long segmentMaxBytes) {
    this.modelName = modelName;
    this.modelPath = modelPath;
    this.languageCode = language;
    this.taskName = task;
    this.threadCount = threads;
    this.segmentMaxBytes = segmentMaxBytes;
  }

  /**
   * Resolves and checks the whisper keys of {@code props}. Checks syntax only: whether the model file exists is known only on the
   * executor that loads it.
   *
   * @throws IllegalArgumentException naming the offending key
   */
  public static WhisperConfig from(TypedProperties props) {
    int sampleRate = intValue(props, AudioConfigKeys.SAMPLE_RATE.key(), AudioConfigKeys.DEFAULT_SAMPLE_RATE);
    require(sampleRate == WHISPER_SAMPLE_RATE, AudioConfigKeys.SAMPLE_RATE.key(),
        "must be " + WHISPER_SAMPLE_RATE + " for whisper, was " + sampleRate);
    long segmentMaxBytes = longValue(props, AudioConfigKeys.SEGMENT_MAX_BYTES.key(), AudioConfigKeys.DEFAULT_SEGMENT_MAX_BYTES);
    require(segmentMaxBytes > 0 && segmentMaxBytes <= MAX_SEGMENT_BYTES, AudioConfigKeys.SEGMENT_MAX_BYTES.key(),
        "must be in [1, " + MAX_SEGMENT_BYTES + "] for whisper, was " + segmentMaxBytes);

    String model = ConfigUtils.getStringWithAltKeys(props, MODEL, DEFAULT_MODEL).trim();
    String modelName;
    String modelPath;
    if (model.startsWith("/")) {
      require(model.endsWith(MODEL_FILE_SUFFIX), MODEL.key(), "an absolute model path must name a " + MODEL_FILE_SUFFIX + " file: " + model);
      String file = Paths.get(model).getFileName().toString();
      String base = file.substring(0, file.length() - MODEL_FILE_SUFFIX.length());
      modelName = base.startsWith(MODEL_FILE_PREFIX) ? base.substring(MODEL_FILE_PREFIX.length()) : base;
      require(MODEL_NAME.matcher(modelName).matches(), MODEL.key(), "the model file name gives no model name: " + model);
      modelPath = model;
    } else {
      require(MODEL_NAME.matcher(model).matches(), MODEL.key(),
          "must be a model name like base.en (letters, digits, '.', '_', '-') or an absolute .bin path: " + model);
      String dir = ConfigUtils.getStringWithAltKeys(props, MODEL_DIR, "").trim();
      require(!dir.isEmpty(), MODEL_DIR.key(), "is required for the named model " + model);
      modelName = model;
      modelPath = Paths.get(dir, MODEL_FILE_PREFIX + model + MODEL_FILE_SUFFIX).toString();
    }

    String language = ConfigUtils.getStringWithAltKeys(props, LANGUAGE, DEFAULT_LANGUAGE).trim();
    require(AUTO_LANGUAGE.equals(language) || LANGUAGES.contains(language), LANGUAGE.key(),
        "must be auto or a language code whisper knows (e.g. en, de, zh): " + language);
    String task = ConfigUtils.getStringWithAltKeys(props, TASK, TASK_TRANSCRIBE).trim();
    require(TASK_TRANSCRIBE.equals(task) || TASK_TRANSLATE.equals(task), TASK.key(),
        "must be " + TASK_TRANSCRIBE + " or " + TASK_TRANSLATE + ": " + task);
    if (modelName.endsWith(".en")) {
      require(DEFAULT_LANGUAGE.equals(language), LANGUAGE.key(), "the English-only model " + modelName + " takes language en: " + language);
      require(TASK_TRANSCRIBE.equals(task), TASK.key(), "the English-only model " + modelName + " only transcribes: " + task);
    }
    int threads = intValue(props, THREADS.key(), DEFAULT_THREADS);
    require(threads >= 1, THREADS.key(), "must be at least 1, was " + threads);
    return new WhisperConfig(modelName, modelPath, language, task, threads, segmentMaxBytes);
  }

  /** Engine, model, language and task: {@code whisper-cpp-<version>-<model>/<language>/<task>}. */
  public String id() {
    return "whisper-cpp-" + ENGINE_VERSION + "-" + modelName + "/" + languageCode + "/" + taskName;
  }

  public String modelName() {
    return modelName;
  }

  public Path modelPath() {
    return Paths.get(modelPath);
  }

  public String languageCode() {
    return languageCode;
  }

  public String taskName() {
    return taskName;
  }

  public boolean translate() {
    return TASK_TRANSLATE.equals(taskName);
  }

  public int threadCount() {
    return threadCount;
  }

  /** Largest segment payload accepted ({@link AudioConfigKeys#SEGMENT_MAX_BYTES}). */
  public long segmentMaxBytes() {
    return segmentMaxBytes;
  }

  private static void require(boolean condition, String key, String message) {
    if (!condition) {
      throw new IllegalArgumentException(key + " " + message);
    }
  }

  private static int intValue(TypedProperties props, String key, int defaultValue) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), String.valueOf(defaultValue)).trim();
    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer: " + value, e);
    }
  }

  private static long longValue(TypedProperties props, String key, long defaultValue) {
    String value = ConfigUtils.getStringWithAltKeys(props, ConfigProperty.key(key).noDefaultValue(), String.valueOf(defaultValue)).trim();
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(key + " must be an integer: " + value, e);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof WhisperConfig)) {
      return false;
    }
    WhisperConfig that = (WhisperConfig) o;
    return threadCount == that.threadCount && segmentMaxBytes == that.segmentMaxBytes && modelName.equals(that.modelName)
        && modelPath.equals(that.modelPath) && languageCode.equals(that.languageCode) && taskName.equals(that.taskName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(modelName, modelPath, languageCode, taskName, threadCount, segmentMaxBytes);
  }

  @Override
  public String toString() {
    return "WhisperConfig{" + id() + ", modelPath=" + modelPath + ", threads=" + threadCount + ", segmentMaxBytes=" + segmentMaxBytes + "}";
  }
}
