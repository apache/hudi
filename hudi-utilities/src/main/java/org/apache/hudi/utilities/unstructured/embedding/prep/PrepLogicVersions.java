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

package org.apache.hudi.utilities.unstructured.embedding.prep;

import org.apache.hudi.common.config.ConfigProperty;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TokenizerFactory;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The preparation contract persisted at table creation, as plain {@value #PREFIX}* table properties, each compared key by key
 * when a job starts:
 * <ul>
 *   <li>{@code <kind>.logic.version} for each source kind (text, image, video, audio): the hand-bumped logic constants of the
 *       code that prepares that kind, readable (text, by default:
 *       {@code prep-logic-v3;jtokkit=jtokkit-bpe-encoding-v1;normalizer=text-normalizer-v2;source-version-check=mtime-size-v1}), among them one per
 *       dependency use (the jtokkit encoding, JCodec decoding, whisper-jni transcription, the FFmpeg keyframe and audio
 *       logic); PDF text extraction is pinned in the PDF preprocessor's versioned name. Every row is stamped with its kind's
 *       logic version;</li>
 *   <li>one property per output setting ({@link #OUTPUT_SETTINGS}, the effective chunk overlap and whether audio is enabled)
 *       at its effective value, defaults included, named after the setting ({@code hoodie.unstructured.chunk.max.tokens} is
 *       stored as {@code hoodie.table.embedding.prep.chunk.max.tokens});</li>
 *   <li>{@code extension.<ext>}: the preprocessor class and versioned name each file extension is prepared with;</li>
 *   <li>{@code tokenizer.id} and {@code tokenizer.class}: the resolved tokenizer;</li>
 *   <li>{@code java.specification.version}: the JDK feature release, which decides NFKC normalization, sentence boundaries
 *       and AWT/ImageIO decoding, resizing and JPEG encoding. JDK patch and vendor changes are not pinned.</li>
 * </ul>
 * A setting a table does not store (one added after the table was created) compares as its default, and an extension a table
 * does not store compares as its default mapping, so a new setting at its default keeps existing tables. A logic version, the
 * tokenizer or the JDK release a table does not store is refused, never inferred or upgraded in place. A job is checked only
 * on the kinds it prepares ({@link EmbeddingTableConfig#PREP_FILE_EXTENSIONS}; all of them by default; video speech is
 * prepared with the audio contract). A mismatch refuses the job before anything is prepared, naming each key with the job's
 * and the table's value. The dependencies' jar versions are not part of the contract: a dependency upgrade that changes prep
 * output must bump its logic constant, which the golden prep tests prove. Performance and implementation-class keys are
 * excluded, and a key under the prep prefixes that no prep setting declares is refused.
 */
public final class PrepLogicVersions {

  private static final String TEXT = "text";
  private static final String IMAGE = "image";
  private static final String VIDEO = "video";
  private static final String AUDIO = "audio";
  private static final List<String> KINDS = Arrays.asList(TEXT, IMAGE, VIDEO, AUDIO);
  /** The contract's format, the first entry of every logic version; bump when the contract's composition changes. */
  static final String FORMAT = "prep-logic-v3";
  /** The prefix of every table property of the contract. */
  static final String PREFIX = "hoodie.table.embedding.prep.";
  private static final String EXTENSION_PREFIX = PREFIX + "extension.";
  private static final String LOGIC_VERSION_SUFFIX = ".logic.version";
  static final String TOKENIZER_ID = PREFIX + "tokenizer.id";
  static final String TOKENIZER_CLASS = PREFIX + "tokenizer.class";
  static final String JAVA_SPECIFICATION_VERSION = PREFIX + "java.specification.version";
  /** The pinned value of an output setting that has no default and is not configured. */
  static final String UNSET = "<unset>";
  /** What a table that stores no such key, or a job that maps no such extension, is compared as when there is no default. */
  private static final String NONE = "none";
  /** The configuration classes whose keys under {@link #PREP_PREFIXES} are the recognized embedding prep settings. */
  private static final Class<?>[] PREP_CONFIG_CLASSES = {EmbeddingTableConfig.class, AudioConfigKeys.class};
  /** Prefixes whose keys are prep settings: a user-set key under one of them must be a recognized one. */
  private static final String[] PREP_PREFIXES = {EmbeddingTableConfig.CHUNK_PREFIX, EmbeddingTableConfig.PDF_PREFIX,
      EmbeddingTableConfig.PREP_PREFIX};
  /** Output-changing settings, pinned at their effective values; the overlap and audio.enabled are pinned besides. */
  static final ConfigProperty<?>[] OUTPUT_SETTINGS = {
      EmbeddingTableConfig.CHUNK_MAX_TOKENS, EmbeddingTableConfig.CHUNK_MAX_WORDS,
      EmbeddingTableConfig.CHUNK_WINDOW_MAX_CHARS, EmbeddingTableConfig.CHUNK_SENTENCE_LOCALE,
      EmbeddingTableConfig.CHUNK_NORMALIZE_DEHYPHENATE, EmbeddingTableConfig.TOKENIZER_TYPE,
      EmbeddingTableConfig.TOKENIZER_ENCODING, EmbeddingTableConfig.PDF_MAX_PAGES, EmbeddingTableConfig.PDF_MAX_PAGE_CHARS,
      EmbeddingTableConfig.PDF_MAX_PAGE_GLYPHS, EmbeddingTableConfig.PDF_MAX_DECODED_STREAM_BYTES,
      EmbeddingTableConfig.PDF_MAX_PAGE_FONT_BYTES, EmbeddingTableConfig.PDF_MAX_GRAPHICS_DEPTH,
      EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS, EmbeddingTableConfig.PREP_IMAGE_HEADER_MAX_BYTES,
      EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS, EmbeddingTableConfig.PREP_VIDEO_DEDUP_METHOD,
      EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE, EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS,
      AudioConfigKeys.ENABLED, AudioConfigKeys.SAMPLE_RATE, AudioConfigKeys.VAD_THRESHOLD_DB, AudioConfigKeys.VAD_MIN_SILENCE_MS,
      AudioConfigKeys.SEGMENT_MAX_MS, AudioConfigKeys.SEGMENT_MAX_BYTES, AudioConfigKeys.MAX_DURATION_MS
  };
  /**
   * Recognized prep settings that do not decide prep output and are not pinned: performance and placement keys,
   * implementation-class keys whose resolved identity is pinned instead (the extension mapping, the tokenizer), and the input
   * size caps, which only refuse a file and never change the rows of a file they accept, so a cap can
   * be added to or changed on an existing table. Raising a cap does not re-prepare a file it already refused: prep reads a file
   * again only when its source row changes, so the refusal stays until the source object changes.
   */
  static final ConfigProperty<?>[] NON_OUTPUT_SETTINGS = {
      EmbeddingTableConfig.PDF_MAX_INPUT_BYTES, EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES, EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES,
      EmbeddingTableConfig.TOKENIZER_CLASS, EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS,
      EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES, EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES,
      EmbeddingTableConfig.BALANCE_STRATEGY_CLASS, EmbeddingTableConfig.BALANCE_NUM_PARTITIONS,
      EmbeddingTableConfig.BALANCE_MIN_PARTITIONS, EmbeddingTableConfig.BALANCE_DEFAULT_MAX_BYTES_PER_TASK,
      EmbeddingTableConfig.PREP_SOURCE_RETRY_MAX_ATTEMPTS, EmbeddingTableConfig.PREP_SOURCE_RETRY_BACKOFF_MS,
      EmbeddingTableConfig.PREP_FILE_EXTENSIONS
  };
  /** Every key (and alternative) the {@link #PREP_CONFIG_CLASSES} declare under {@link #PREP_PREFIXES}, templates aside. */
  static final Set<String> DECLARED_PREP_KEYS = declaredPrepKeys();
  /** The job settings stored as table properties: {@link #OUTPUT_SETTINGS} and the chunk overlap, by table property. */
  private static final Map<String, String> SETTING_BY_PROPERTY = settingsByProperty();

  /** The logic version of each kind. */
  private final Map<String, String> versions;
  /** Every table property of the contract and its value, sorted by key. */
  private final Map<String, String> pins;
  /** The kinds whose output each table property decides. */
  private final Map<String, Set<String>> kindsByProperty;
  /** The kinds this job prepares. */
  private final Set<String> preparedKinds;

  private PrepLogicVersions(Map<String, String> versions, Map<String, String> pins, Map<String, Set<String>> kindsByProperty,
                            Set<String> preparedKinds) {
    this.versions = versions;
    this.pins = pins;
    this.kindsByProperty = kindsByProperty;
    this.preparedKinds = preparedKinds;
  }

  /** Resolves the contract without opening source files or loading optional native libraries. */
  public static PrepLogicVersions from(TypedProperties props) {
    return from(props, System.getProperty("java.specification.version"));
  }

  /** {@link #from(TypedProperties)} on a JDK of feature release {@code javaSpecificationVersion}. */
  static PrepLogicVersions from(TypedProperties props, String javaSpecificationVersion) {
    rejectUnrecognizedPrepKeys(props);
    PreprocessorRegistry registry = new PreprocessorRegistry(props);
    try {
      Tokenizer tokenizer = TokenizerFactory.create(props);
      Map<String, String> versions = new LinkedHashMap<>();
      Map<String, String> pins = new TreeMap<>();
      Map<String, Set<String>> kinds = new TreeMap<>();
      effectiveSettings(props).forEach((setting, value) -> put(pins, kinds, settingProperty(setting), value, kindsOf(setting)));
      for (Map.Entry<String, String> entry : registry.classNamesByExtension().entrySet()) {
        FilePreprocessor processor = registry.forExtension(entry.getKey()).get();
        put(pins, kinds, EXTENSION_PREFIX + entry.getKey(), entry.getValue() + ";" + processor.name(),
            Collections.singleton(kindOf(processor.sourceKind())));
      }
      Set<String> textAndAudio = new LinkedHashSet<>(Arrays.asList(TEXT, AUDIO));
      put(pins, kinds, TOKENIZER_ID, tokenizer.id(), textAndAudio);
      put(pins, kinds, TOKENIZER_CLASS, tokenizer.getClass().getName(), textAndAudio);
      put(pins, kinds, JAVA_SPECIFICATION_VERSION, javaSpecificationVersion, new LinkedHashSet<>(KINDS));
      for (String kind : KINDS) {
        String version = logicVersion(kind, tokenizer, props);
        versions.put(kind, version);
        put(pins, kinds, logicVersionProperty(kind), version, Collections.singleton(kind));
      }
      return new PrepLogicVersions(versions, pins, kinds, preparedKinds(props, registry));
    } finally {
      registry.close();
    }
  }

  private static void put(Map<String, String> pins, Map<String, Set<String>> kinds, String property, String value,
                          Set<String> propertyKinds) {
    if (pins.put(property, value) != null) {
      throw new IllegalStateException("Two embedding prep pins share the table property " + property);
    }
    kinds.put(property, Collections.unmodifiableSet(propertyKinds));
  }

  /**
   * {@code kind}'s logic version: {@link #FORMAT} and the hand-bumped logic constants that decide its output, by name. Text:
   * PDF text extraction is pinned through the extension mapping, in the PDF preprocessor's versioned name.
   */
  private static String logicVersion(String kind, Tokenizer tokenizer, TypedProperties props) {
    Map<String, String> logic = new TreeMap<>();
    logic.put("source-version-check", "mtime-size-v1");
    if (kind.equals(TEXT) || kind.equals(AUDIO)) {
      logic.put("normalizer", TextNormalizer.LOGIC_VERSION);
      if (tokenizer instanceof OpenAiBpeTokenizer) {
        logic.put("jtokkit", OpenAiBpeTokenizer.ENCODING_LOGIC_VERSION);
      }
    }
    if (kind.equals(VIDEO)) {
      logic.put("ffmpeg", FfmpegKeyframeVideoFrameSource.LOGIC_VERSION);
      logic.put("jcodec", JcodecKeyframeVideoFrameSource.LOGIC_VERSION);
      logic.put("jcodec.decode", JcodecKeyframeVideoFrameSource.DECODE_LOGIC_VERSION);
    } else if (kind.equals(AUDIO)) {
      putAudioLogic(tokenizer, props, logic);
    }
    StringBuilder version = new StringBuilder(FORMAT);
    logic.forEach((name, value) -> version.append(';').append(name).append('=').append(value));
    return version.toString();
  }

  /** The audio implementations' logic when audio is enabled; whether it is, and the tokenizer, are pinned on their own. */
  private static void putAudioLogic(Tokenizer tokenizer, TypedProperties props, Map<String, String> logic) {
    AudioConfig audio = AudioConfig.from(props);
    if (!audio.enabled()) {
      return;
    }
    Map<String, String> style = new TreeMap<>(audio.logicStyle(tokenizer));
    style.remove(AudioConfig.STYLE_TOKENIZER);
    logic.putAll(style);
    if (ConfigUtils.getStringWithAltKeys(props, AudioConfigKeys.TRANSCRIBER_CLASS,
        AudioConfig.DEFAULT_TRANSCRIBER_CLASS).trim().equals(AudioConfig.DEFAULT_TRANSCRIBER_CLASS)) {
      logic.put("whisper.jni", WhisperSegmentTranscriber.ENGINE_LOGIC_VERSION);
      logic.put("whisper.adapter", WhisperSegmentTranscriber.IMPLEMENTATION_VERSION);
    }
  }

  /** The kinds a job prepares: those of its listed file extensions (video with its speech, audio), or all of them. */
  private static Set<String> preparedKinds(TypedProperties props, PreprocessorRegistry registry) {
    Option<List<String>> extensions = EmbeddingTableConfig.from(props).prepFileExtensions();
    if (!extensions.isPresent()) {
      return Collections.unmodifiableSet(new LinkedHashSet<>(KINDS));
    }
    Set<String> kinds = new LinkedHashSet<>();
    for (String extension : extensions.get()) {
      Option<FilePreprocessor> processor = registry.forExtension(extension);
      if (!processor.isPresent()) {
        // a listed extension without a preprocessor is refused when the job is validated; check every kind meanwhile
        return Collections.unmodifiableSet(new LinkedHashSet<>(KINDS));
      }
      String kind = kindOf(processor.get().sourceKind());
      kinds.add(kind);
      if (kind.equals(VIDEO)) {
        kinds.add(AUDIO);
      }
    }
    return Collections.unmodifiableSet(kinds);
  }

  /** Every table property of the contract and its value, written when the table is created. */
  public Map<String, String> tableProperties() {
    return Collections.unmodifiableMap(pins);
  }

  /** Adds the contract to a table's creation pins; an existing table is checked key by key ({@link #checkTable}). */
  public void requireTableProperties(UnstructuredTableProperties.Builder settings) {
    settings.requireTableProperties(pins, this::check);
  }

  /** Carries the driver-validated contract into each executor without changing the caller's job settings. */
  public TypedProperties executionProperties(TypedProperties props) {
    TypedProperties execution = TypedProperties.copy(props);
    pins.forEach(execution::setProperty);
    return execution;
  }

  /** Refuses executor configuration or code drift before any file is read. Standalone iterators may omit execution pins. */
  public void checkExecution(TypedProperties props) {
    if (pins.keySet().stream().noneMatch(props::containsKey)) {
      return;
    }
    Set<String> properties = new TreeSet<>(pins.keySet());
    props.stringPropertyNames().stream().filter(key -> key.startsWith(PREFIX)).forEach(properties::add);
    List<String> drift = new ArrayList<>();
    for (String property : properties) {
      String actual = pins.getOrDefault(property, NONE);
      String expected = props.getProperty(property);
      if (!actual.equals(expected)) {
        drift.add(property + "=" + actual + " (the driver pinned " + (expected == null ? NONE : expected) + ")");
      }
    }
    if (!drift.isEmpty()) {
      throw new IllegalArgumentException("Embedding prep executor resolves " + String.join(", ", drift)
          + ", which does not match the driver/table contract; nothing was prepared");
    }
  }

  /** Compares the contract with the table's key by key ({@link #check}) before a direct transformer invocation prepares any rows. */
  public void checkTable(String basePath, HoodieTableConfig table) {
    check("Embedding prep", basePath, table.getProps());
  }

  /**
   * Compares the contract with a table's, key by key, on the kinds this job prepares. A setting the table does not store
   * compares as its default, an extension as its default mapping; any other key the table does not store is refused, and so
   * is a key the table stores that this build does not pin (written by a later build: this build cannot honour it).
   */
  private void check(String sourceName, String basePath, Properties table) {
    // the defaults of settings the table does not store follow the settings it stores (the overlap follows the max tokens);
    // computed only when a setting this job checks is missing, so a key of a kind the job does not prepare is never parsed
    boolean needsDefaults = SETTING_BY_PROPERTY.keySet().stream()
        .anyMatch(property -> table.getProperty(property) == null && decidesPreparedKinds(property));
    Map<String, String> storedSettings = needsDefaults ? storedSettings(table) : Collections.emptyMap();
    Set<String> properties = new TreeSet<>(pins.keySet());
    // a key this build does not pin (one a later build added, or an extension this job does not map) is compared too
    table.stringPropertyNames().stream().filter(key -> key.startsWith(PREFIX)).forEach(properties::add);
    List<String> mismatches = new ArrayList<>();
    for (String property : properties.stream().filter(this::decidesPreparedKinds).collect(Collectors.toList())) {
      String current = pins.getOrDefault(property, NONE);
      String stored = storedValue(property, table, storedSettings);
      if (!current.equals(stored)) {
        String setting = SETTING_BY_PROPERTY.get(property);
        mismatches.add(property + "=" + current + " (" + (setting == null ? "" : "set by " + setting + "; ")
            + "the table stores " + stored + ")");
      }
    }
    if (!mismatches.isEmpty()) {
      throw new IllegalArgumentException(sourceName + " requires the table's " + String.join(", ", mismatches)
          + ": the table at " + basePath + " was prepared differently; write this job to a new table");
    }
  }

  /** Whether {@code property} decides the output of a kind this job prepares; a key this build does not pin always does. */
  private boolean decidesPreparedKinds(String property) {
    return !Collections.disjoint(kindsByProperty.getOrDefault(property, preparedKinds), preparedKinds);
  }

  /**
   * The table's value of {@code property}: what it stores, else a setting's default ({@code storedSettings}) or an extension's
   * default mapping, else {@value #NONE}.
   */
  private static String storedValue(String property, Properties table, Map<String, String> storedSettings) {
    String stored = table.getProperty(property);
    if (stored == null && SETTING_BY_PROPERTY.containsKey(property)) {
      stored = storedSettings.get(property);
    } else if (stored == null && property.startsWith(EXTENSION_PREFIX)) {
      stored = defaultMapping(property.substring(EXTENSION_PREFIX.length()));
    }
    return stored == null ? NONE : stored;
  }

  /** The effective settings of a table's stored ones, by table property: a setting the table does not store is its default. */
  private static Map<String, String> storedSettings(Properties table) {
    TypedProperties stored = new TypedProperties();
    SETTING_BY_PROPERTY.forEach((property, setting) -> {
      String value = table.getProperty(property);
      if (value != null && !UNSET.equals(value)) {
        stored.setProperty(setting, value);
      }
    });
    Map<String, String> result = new TreeMap<>();
    effectiveSettings(stored).forEach((setting, value) -> result.put(settingProperty(setting), value));
    return result;
  }

  /** The default preprocessor mapping of {@code extension}, as stored, or {@value #NONE}. */
  private static String defaultMapping(String extension) {
    PreprocessorRegistry defaults = new PreprocessorRegistry(new TypedProperties());
    try {
      return defaults.forExtension(extension).map(processor -> defaults.classNamesByExtension().get(extension) + ";"
          + processor.name()).orElse(NONE);
    } finally {
      defaults.close();
    }
  }

  /** The table-pinned version for the source kind; video speech uses the audio contract. */
  public String forRow(SourceKind kind, String modality) {
    return versions.get(kind == SourceKind.VIDEO && TEXT.equals(modality) ? AUDIO : kindOf(kind));
  }

  /** The expected version of text, image, video or audio preparation. */
  public String version(String kind) {
    if (!versions.containsKey(kind)) {
      throw new IllegalArgumentException("Unknown prep source kind " + kind);
    }
    return versions.get(kind);
  }

  static String logicVersionProperty(String kind) {
    return PREFIX + kind + LOGIC_VERSION_SUFFIX;
  }

  /**
   * The table property a job setting is stored as: {@value #PREFIX} and the setting's name below its family's prefix
   * ({@code hoodie.unstructured.chunk.max.tokens}: {@code chunk.max.tokens}; {@code hoodie.unstructured.embedding.prep.video.max.pixels}:
   * {@code video.max.pixels}; {@code hoodie.unstructured.embedding.audio.enabled}: {@code audio.enabled}).
   */
  static String settingProperty(String setting) {
    for (String prefix : new String[] {EmbeddingTableConfig.PREP_PREFIX, EmbeddingTableConfig.PREFIX,
        EmbeddingTableConfig.UNSTRUCTURED_PREFIX}) {
      if (setting.startsWith(prefix)) {
        return PREFIX + setting.substring(prefix.length());
      }
    }
    throw new IllegalStateException("Embedding prep setting " + setting + " is outside " + EmbeddingTableConfig.UNSTRUCTURED_PREFIX);
  }

  private static Map<String, String> settingsByProperty() {
    Map<String, String> result = new TreeMap<>();
    Stream.concat(Arrays.stream(OUTPUT_SETTINGS), Stream.of(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS)).map(ConfigProperty::key)
        .forEach(setting -> {
          if (result.put(settingProperty(setting), setting) != null) {
            throw new IllegalStateException("Two embedding prep settings share the table property " + settingProperty(setting));
          }
        });
    return Collections.unmodifiableMap(result);
  }

  private static String kindOf(SourceKind kind) {
    switch (kind) {
      case DOCUMENT:
        return TEXT;
      case IMAGE:
        return IMAGE;
      case VIDEO:
        return VIDEO;
      default:
        throw new UnsupportedOperationException("Unknown prep source kind " + kind);
    }
  }

  /** The effective value of each stored setting, by setting key. */
  private static Map<String, String> effectiveSettings(TypedProperties props) {
    Map<String, String> settings = new TreeMap<>();
    for (ConfigProperty<?> key : OUTPUT_SETTINGS) {
      // A key without a default is pinned as "<unset>" when not configured.
      String unset = key.hasDefaultValue() ? String.valueOf(key.defaultValue()) : UNSET;
      settings.put(key.key(), ConfigUtils.getStringWithAltKeys(props, key, unset).trim());
    }
    int maxTokens = Integer.parseInt(settings.get(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()));
    settings.put(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), ConfigUtils.getStringWithAltKeys(props,
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS, String.valueOf(Math.min(EmbeddingTableConfig.DEFAULT_CHUNK_OVERLAP_TOKENS, maxTokens / 2))).trim());
    settings.put(AudioConfigKeys.ENABLED.key(), String.valueOf(AudioConfig.from(props).enabled()));
    return settings;
  }

  /**
   * Refuses, at job start, a user-set key under the prep prefixes that no prep setting declares (a misspelt or retired key):
   * it would otherwise be ignored while the job runs with another setting than the user meant.
   */
  private static void rejectUnrecognizedPrepKeys(TypedProperties props) {
    for (String key : new TreeSet<>(props.stringPropertyNames())) {
      if (Arrays.stream(PREP_PREFIXES).anyMatch(key::startsWith) && !recognizedPrepKey(key)) {
        throw new IllegalArgumentException("Config " + key + " is not a recognized embedding prep setting; remove it or"
            + " correct its name. Nothing was prepared");
      }
    }
  }

  /** A declared prep key, or a member of a declared key family: {@code preprocessor.<ext>.class}, {@code balance.<group>.max.bytes.per.task}. */
  static boolean recognizedPrepKey(String key) {
    return DECLARED_PREP_KEYS.contains(key) || EmbeddingTableConfig.isPreprocessorClassKey(key)
        || (key.startsWith(EmbeddingTableConfig.BALANCE_PREFIX) && key.endsWith(EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX)
        && key.length() > EmbeddingTableConfig.BALANCE_PREFIX.length() + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX.length());
  }

  private static Set<String> declaredPrepKeys() {
    Set<String> keys = new HashSet<>();
    for (Class<?> config : PREP_CONFIG_CLASSES) {
      for (Field field : config.getFields()) {
        if (Modifier.isStatic(field.getModifiers()) && ConfigProperty.class.isAssignableFrom(field.getType())) {
          ConfigProperty<?> property;
          try {
            property = (ConfigProperty<?>) field.get(null);
          } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read " + config.getName() + "." + field.getName(), e);
          }
          Stream.concat(Stream.of(property.key()), property.getAlternatives().stream())
              .filter(key -> Arrays.stream(PREP_PREFIXES).anyMatch(key::startsWith) && !key.contains("<"))
              .forEach(keys::add);
        }
      }
    }
    return Collections.unmodifiableSet(keys);
  }

  /** The kinds whose output a stored setting decides. */
  private static Set<String> kindsOf(String setting) {
    return KINDS.stream().filter(kind -> applies(kind, setting)).collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private static boolean applies(String kind, String key) {
    if (key.startsWith(EmbeddingTableConfig.CHUNK_PREFIX)) {
      return kind.equals(TEXT) || kind.equals(AUDIO);
    }
    if (key.startsWith(EmbeddingTableConfig.PDF_PREFIX)) {
      return kind.equals(TEXT);
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "image.")) {
      return kind.equals(IMAGE);
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "video.")) {
      return kind.equals(VIDEO);
    }
    if (key.startsWith(EmbeddingTableConfig.PREP_PREFIX + "audio.") || key.startsWith(AudioConfigKeys.RUNTIME_PREFIX)) {
      return kind.equals(AUDIO);
    }
    return true;
  }
}
