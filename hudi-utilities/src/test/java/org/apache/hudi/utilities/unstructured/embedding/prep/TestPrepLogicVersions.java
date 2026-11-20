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
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceConfig;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredSourceContract;
import org.apache.hudi.utilities.sources.unstructured.UnstructuredTableProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableChecks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.AudioConfigKeys;
import org.apache.hudi.utilities.unstructured.embedding.audio.ffmpeg.FfmpegAudioSegmentSource;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperConfig;
import org.apache.hudi.utilities.unstructured.embedding.audio.whisper.WhisperSegmentTranscriber;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfBoxPageSource;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFilePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.text.OpenAiBpeTokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.TextNormalizer;
import org.apache.hudi.utilities.unstructured.embedding.video.FfmpegKeyframeVideoFrameSource;
import org.apache.hudi.utilities.unstructured.embedding.video.JcodecKeyframeVideoFrameSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestPrepLogicVersions {
  @TempDir
  Path temp;

  /** A JDK upgrade that keeps prep output identical keeps every pin: the JDK's identity is not part of it. */
  @Test
  void jdkIdentityDoesNotChangeThePins() {
    PrepLogicVersions before = PrepLogicVersions.from(new TypedProperties());
    String runtime = System.getProperty("java.runtime.version");
    String vendor = System.getProperty("java.vendor");
    try {
      System.setProperty("java.runtime.version", "99.0.1+7-another-build");
      System.setProperty("java.vendor", "Another JDK Vendor");
      PrepLogicVersions after = PrepLogicVersions.from(new TypedProperties());
      for (String kind : new String[] {"text", "image", "video", "audio"}) {
        assertEquals(before.version(kind), after.version(kind), kind);
      }
    } finally {
      System.setProperty("java.runtime.version", runtime);
      System.setProperty("java.vendor", vendor);
    }
  }

  /** Another jar version of a prep dependency keeps the pin while that dependency's logic constant is unchanged. */
  @Test
  void libraryJarVersionsDoNotChangeThePins() throws Exception {
    TypedProperties audio = new TypedProperties();
    audio.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    audio.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    String[][] cases = {
        {"com/knuddels/jtokkit/Encodings.class", "com.knuddels/jtokkit", "text"},
        {"com/knuddels/jtokkit/Encodings.class", "com.knuddels/jtokkit", "audio"},
        {"org/jcodec/api/FrameGrab.class", "org.jcodec/jcodec", "video"},
        {"org/jcodec/scale/AWTUtil.class", "org.jcodec/jcodec-javase", "video"},
        {"io/github/givimad/whisperjni/WhisperJNI.class", "io.github.givimad/whisper-jni", "audio"},
        {"org/apache/pdfbox/util/Version.class", "org.apache.pdfbox/pdfbox", "text"}};
    for (String[] c : cases) {
      assertEquals(bundledIdentity(c[0], c[1], "1", c[2], audio), bundledIdentity(c[0], c[1], "2", c[2], audio),
          c[1] + " in " + c[2]);
    }
  }

  /** Each dependency use is pinned by its hand-bumped logic constant, by name, in its kinds' logic versions. */
  @Test
  void everyDependencyLogicConstantIsNamedInItsKindsLogicVersion() {
    TypedProperties audio = new TypedProperties();
    audio.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    audio.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    PrepLogicVersions versions = PrepLogicVersions.from(audio);
    String[][] constants = {
        {"text", "jtokkit", OpenAiBpeTokenizer.ENCODING_LOGIC_VERSION},
        {"audio", "jtokkit", OpenAiBpeTokenizer.ENCODING_LOGIC_VERSION},
        {"text", "normalizer", TextNormalizer.LOGIC_VERSION},
        {"audio", "normalizer", TextNormalizer.LOGIC_VERSION},
        {"video", "jcodec.decode", JcodecKeyframeVideoFrameSource.DECODE_LOGIC_VERSION},
        {"video", "jcodec", JcodecKeyframeVideoFrameSource.LOGIC_VERSION},
        {"video", "ffmpeg", FfmpegKeyframeVideoFrameSource.LOGIC_VERSION},
        {"audio", "whisper.jni", WhisperSegmentTranscriber.ENGINE_LOGIC_VERSION},
        {"audio", AudioConfig.STYLE_SOURCE, FfmpegAudioSegmentSource.LOGIC_VERSION}};
    for (String[] c : constants) {
      assertTrue(Arrays.asList(versions.version(c[0]).split(";")).contains(c[1] + "=" + c[2]), c[0] + ": " + versions.version(c[0]));
    }
    // PDF text extraction is pinned in the PDF preprocessor's versioned name, the pdf extension's mapping
    assertEquals(PdfFilePreprocessor.class.getName() + ";" + PdfFilePreprocessor.NAME_PREFIX + PdfFilePreprocessor.IMPLEMENTATION_VERSION
        + ";" + PdfBoxPageSource.LOGIC_VERSION, versions.tableProperties().get(PIN + "extension.pdf"));
    assertEquals("pdf-text-extraction-v1", PdfBoxPageSource.LOGIC_VERSION);
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      String version = versions.version(kind);
      assertTrue(version.startsWith("prep-logic-v3;") && version.contains(";source-version-check=mtime-size-v1"), version);
      assertFalse(version.contains("sha256"), version);
      assertFalse(version.contains(System.getProperty("java.runtime.version")), version);
      assertEquals(version, versions.tableProperties().get(PIN + kind + ".logic.version"), kind);
    }
    assertTrue(versions.tableProperties().values().stream().noneMatch(value -> value.contains(System.getProperty("java.runtime.version"))),
        versions.tableProperties().toString());
  }

  @Test
  void oldPdfJarVersionNamespaceIsRefusedWithoutRelabeling() {
    TypedProperties props = job(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf");
    PrepLogicVersions current = PrepLogicVersions.from(props);
    Properties pins = tablePins(props);
    String key = PIN + "extension.pdf";
    String prefix = PdfFilePreprocessor.class.getName() + ";" + PdfFilePreprocessor.NAME_PREFIX
        + PdfFilePreprocessor.IMPLEMENTATION_VERSION + ";";
    assertEquals(prefix + PdfBoxPageSource.LOGIC_VERSION, pins.getProperty(key));
    assertDoesNotThrow(() -> current.checkTable(BASE, table(pins)));
    String legacy = prefix + "pdfbox=" + org.apache.pdfbox.util.Version.getVersion();
    pins.setProperty(key, legacy);
    HoodieTableConfig stored = table(pins);

    String refused = assertThrows(IllegalArgumentException.class, () -> current.checkTable(BASE, stored)).getMessage();

    assertTrue(refused.contains(key), refused);
    assertTrue(refused.contains(legacy), refused);
    assertEquals(legacy, stored.getProps().getProperty(key), "validation never relabels the stored identity");
    assertEquals(legacy, pins.getProperty(key));
  }

  /** The tokenizer's resolved class is pinned on its own: a custom class with the default tokenizer's id is a changed pin. */
  @Test
  void tokenizerClassIsPinned() {
    TypedProperties custom = new TypedProperties();
    custom.setProperty(EmbeddingTableConfig.TOKENIZER_CLASS.key(), RenamedBpeTokenizer.class.getName());
    PrepLogicVersions defaults = PrepLogicVersions.from(new TypedProperties());
    PrepLogicVersions renamed = PrepLogicVersions.from(custom);
    assertEquals(new OpenAiBpeTokenizer().id(), new RenamedBpeTokenizer().id(), "same id, another class");
    assertEquals(OpenAiBpeTokenizer.class.getName(), defaults.tableProperties().get(PIN + "tokenizer.class"));
    assertEquals(RenamedBpeTokenizer.class.getName(), renamed.tableProperties().get(PIN + "tokenizer.class"));
    assertEquals(defaults.tableProperties().get(PIN + "tokenizer.id"), renamed.tableProperties().get(PIN + "tokenizer.id"));
    String message = refusal(custom, tablePins(new TypedProperties()));
    assertTrue(message.contains(PIN + "tokenizer.class=" + RenamedBpeTokenizer.class.getName() + " (the table stores "
        + OpenAiBpeTokenizer.class.getName() + ")"), message);
    // a job that prepares neither text nor video speech does not tokenize
    assertDoesNotThrow(() -> PrepLogicVersions.from(job(EmbeddingTableConfig.TOKENIZER_CLASS.key(), RenamedBpeTokenizer.class.getName(),
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "png")).checkTable(BASE, table(tablePins(new TypedProperties()))));
    TypedProperties explicit = new TypedProperties();
    explicit.setProperty(EmbeddingTableConfig.TOKENIZER_CLASS.key(), OpenAiBpeTokenizer.class.getName());
    assertEquals(defaults.tableProperties(), PrepLogicVersions.from(explicit).tableProperties(), "the default class, named");
  }

  /** The refusal fires where a job starts: building the source contract of an embedding job. */
  @Test
  void unknownPrepKeyIsRefusedByTheSourceContract() {
    TypedProperties props = new TypedProperties();
    props.setProperty(UnstructuredSourceConfig.MODE.key(), "embedding");
    props.setProperty(UnstructuredSourceConfig.CLOUD.key(), "s3");
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    assertDoesNotThrow(() -> new UnstructuredSourceContract(props));
    props.setProperty(EmbeddingTableConfig.PREP_PREFIX + "foo", "1");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> new UnstructuredSourceContract(props));
    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.PREP_PREFIX + "foo is not a recognized embedding prep setting"),
        refused.getMessage());
  }

  /** Every prep key the config classes declare is either pinned as an output setting or classified as not deciding output. */
  @Test
  void everyDeclaredPrepKeyIsClassified() {
    Set<String> classified = new TreeSet<>();
    for (ConfigProperty<?> key : PrepLogicVersions.OUTPUT_SETTINGS) {
      classified.add(key.key());
    }
    classified.add(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key());
    for (ConfigProperty<?> key : PrepLogicVersions.NON_OUTPUT_SETTINGS) {
      classified.add(key.key());
    }
    classified.remove(AudioConfigKeys.ENABLED.key());   // pinned, outside the prep prefixes
    assertEquals(classified, new TreeSet<>(PrepLogicVersions.DECLARED_PREP_KEYS),
        "a new prep key must be added to OUTPUT_SETTINGS or NON_OUTPUT_SETTINGS");
    assertTrue(PrepLogicVersions.recognizedPrepKey(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "mov"
        + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX));
    assertTrue(PrepLogicVersions.recognizedPrepKey(EmbeddingTableConfig.BALANCE_PREFIX + "video"
        + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX));
    assertFalse(PrepLogicVersions.recognizedPrepKey(EmbeddingTableConfig.BALANCE_PREFIX + "foo"));
    assertFalse(PrepLogicVersions.recognizedPrepKey(EmbeddingTableConfig.PREP_PREFIX + "source.retry.foo"));
  }

  /** A key under the prep prefixes that no prep setting declares is refused at job start, naming the key. */
  @Test
  void unknownPrepKeysAreRefusedNamingTheKey() {
    for (String key : new String[] {EmbeddingTableConfig.PREP_PREFIX + "foo", EmbeddingTableConfig.CHUNK_PREFIX + "max.tokenz",
        EmbeddingTableConfig.PREP_PREFIX + "video.keyframe.interval", EmbeddingTableConfig.PDF_PREFIX + "max.pagez"}) {
      TypedProperties props = new TypedProperties();
      props.setProperty(key, "1");
      IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> PrepLogicVersions.from(props), key);
      assertTrue(refused.getMessage().contains(key + " is not a recognized embedding prep setting"), refused.getMessage());
    }
  }

  /** A changed output setting still refuses an existing table, with the refusal that names a new table. */
  @Test
  void changedOutputSettingRefusesAnExistingTable() throws Exception {
    TypedProperties props = new TypedProperties();
    UnstructuredTableProperties settings = UnstructuredSourceContract.embeddingTablePins("Embedding prep", props);
    String base = temp.resolve("existing").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(settings.tableProperties())
        .setTableVersion(9).setTableName("embedding").setTableType(HoodieTableType.COPY_ON_WRITE)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), base);
    EmbeddingTableChecks.checkTableVersion(base, HoodieTestUtils.getDefaultStorageConf(), EmbeddingTableConfig.from(props));
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "500");
    EmbeddingTableConfig changed = EmbeddingTableConfig.from(props);
    StorageConfiguration<?> storage = HoodieTestUtils.getDefaultStorageConf();
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> EmbeddingTableChecks.checkTableVersion(base, storage, changed));
    assertTrue(refused.getMessage().contains("hoodie.table.embedding.prep.video.keyframe.interval.ms=500"), refused.getMessage());
    assertTrue(refused.getMessage().endsWith("write this job to a new table"), refused.getMessage());
  }

  /** A real shaded-JAR resource lookup: one stable outer version, independently versioned dependency metadata. */
  private String bundledIdentity(String resource, String coordinate, String version, String kind, TypedProperties props)
      throws Exception {
    Path jar = Files.createTempFile(temp, "bundle-", ".jar");
    Manifest manifest = new Manifest();
    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
    manifest.getMainAttributes().put(Attributes.Name.IMPLEMENTATION_VERSION, "same-hudi-snapshot");
    try (JarOutputStream out = new JarOutputStream(Files.newOutputStream(jar), manifest)) {
      out.putNextEntry(new JarEntry(resource));
      try (InputStream original = PrepLogicVersions.class.getClassLoader().getResourceAsStream(resource)) {
        out.write(original.readAllBytes());
      }
      out.closeEntry();
      out.putNextEntry(new JarEntry("META-INF/maven/" + coordinate + "/pom.properties"));
      out.write(("version=" + version + "\n").getBytes(StandardCharsets.UTF_8));
      out.closeEntry();
    }
    try (ComposerLoader loader = new ComposerLoader(new URL[] {jar.toUri().toURL()}) {
      @Override
      public URL getResource(String name) {
        return resource.equals(name) ? findResource(name) : super.getResource(name);
      }
    }) {
      Class<?> composer = loader.loadClass(PrepLogicVersions.class.getName());
      Object versions = composer.getMethod("from", TypedProperties.class).invoke(null, props);
      return (String) composer.getMethod("version", String.class).invoke(versions, kind);
    }
  }

  /** Loads its own copy of {@link PrepLogicVersions}, so the composer's class-loader lookups go through this loader. */
  private static class ComposerLoader extends URLClassLoader {
    private final ClassLoader classes;

    ComposerLoader(URL[] urls) {
      super(urls, PrepLogicVersions.class.getClassLoader());
      this.classes = PrepLogicVersions.class.getClassLoader();
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (!PrepLogicVersions.class.getName().equals(name) && !name.startsWith(PrepLogicVersions.class.getName() + "$")) {
        return super.loadClass(name, resolve);
      }
      synchronized (getClassLoadingLock(name)) {
        Class<?> loaded = findLoadedClass(name);
        if (loaded == null) {
          try (InputStream bytes = classes.getResourceAsStream(name.replace('.', '/') + ".class")) {
            byte[] code = bytes.readAllBytes();
            loaded = defineClass(name, code, 0, code.length);
          } catch (IOException e) {
            throw new ClassNotFoundException(name, e);
          }
        }
        if (resolve) {
          resolveClass(loaded);
        }
        return loaded;
      }
    }
  }

  /**
   * A deployment adds FFmpeg's jars to the executors only: a driver without them must pin the version an executor with them
   * computes, so the executor's check lets the round run.
   */
  @Test
  void driverWithoutFfmpegPinsWhatAnExecutorWithFfmpegChecks() throws Exception {
    TypedProperties props = new TypedProperties();
    props.setProperty(AudioConfigKeys.ENABLED.key(), "true");
    props.setProperty(WhisperConfig.MODEL_DIR.key(), "/models");
    PrepLogicVersions executor = PrepLogicVersions.from(props);
    assertNotNull(PrepLogicVersions.class.getClassLoader().getResource("org/bytedeco/javacv/FFmpegFrameGrabber.class"),
        "the executor side has FFmpeg");
    try (ComposerLoader driverLoader = new ComposerLoader(new URL[0]) {
      @Override
      public URL getResource(String name) {
        return name.startsWith("org/bytedeco/") ? null : super.getResource(name);
      }
    }) {
      assertNull(driverLoader.getResource("org/bytedeco/javacv/FFmpegFrameGrabber.class"), "the driver side has no FFmpeg");
      Class<?> composer = driverLoader.loadClass(PrepLogicVersions.class.getName());
      assertNotEquals(PrepLogicVersions.class, composer, "the driver's composer is its own copy");
      Object driver = composer.getMethod("from", TypedProperties.class).invoke(null, props);
      for (String kind : new String[] {"video", "audio"}) {
        assertEquals(executor.version(kind), composer.getMethod("version", String.class).invoke(driver, kind), kind);
      }
      TypedProperties execution = (TypedProperties) composer.getMethod("executionProperties", TypedProperties.class)
          .invoke(driver, props);
      assertDoesNotThrow(() -> executor.checkExecution(execution));
    }
  }

  @Test
  void implicitAndExplicitDefaultsShareAContract() {
    TypedProperties implicit = new TypedProperties();
    TypedProperties explicit = new TypedProperties();
    explicit.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "256");
    explicit.setProperty(EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "128");
    explicit.setProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key(), "10000");
    explicit.setProperty(AudioConfigKeys.ENABLED.key(), "false");
    explicit.setProperty(EmbeddingTableConfig.PREP_VIDEO_PREPROCESSOR_CLASS.key(), VideoFramePreprocessor.class.getName());
    assertEquals(PrepLogicVersions.from(implicit).tableProperties(), PrepLogicVersions.from(explicit).tableProperties());
  }

  /**
   * The input size caps are not part of the stored contract: they only refuse a file and never change the rows of a file they
   * accept, so a table created uncapped accepts a job that adds or changes a cap.
   */
  @Test
  void inputSizeCapsAreNotPinned() {
    Map<String, String> uncapped = PrepLogicVersions.from(new TypedProperties()).tableProperties();
    for (String cap : new String[] {EmbeddingTableConfig.PDF_MAX_INPUT_BYTES.key(), EmbeddingTableConfig.PREP_VIDEO_MAX_BYTES.key(),
        EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key()}) {
      TypedProperties capped = job(cap, "536870912");
      assertEquals(uncapped, PrepLogicVersions.from(capped).tableProperties(), cap);
      assertDoesNotThrow(() -> PrepLogicVersions.from(capped).checkTable(BASE, table(tablePins(new TypedProperties()))), cap);
    }
    assertTrue(uncapped.keySet().stream().noneMatch(key -> key.endsWith(".max.input.bytes") || key.endsWith("video.max.bytes")
        || key.endsWith("image.max.bytes")), uncapped.toString());
  }

  /** A setting change is a change of its own key only, and is checked only for the kinds whose output it decides. */
  @Test
  void outputChangesInvalidateOnlyTheirSourceKinds() {
    PrepLogicVersions before = PrepLogicVersions.from(new TypedProperties());
    PrepLogicVersions after = PrepLogicVersions.from(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128",
        EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "64"));
    assertEquals(changed(changed(before.tableProperties(), PIN + "chunk.max.tokens", "128"), PIN + "chunk.overlap.tokens", "64"),
        after.tableProperties());
    for (String kind : new String[] {"text", "image", "video", "audio"}) {
      assertEquals(before.version(kind), after.version(kind), "settings are not part of the logic versions: " + kind);
    }
    Properties pins = tablePins(new TypedProperties());
    assertDoesNotThrow(() -> PrepLogicVersions.from(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128",
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "png,jpg")).checkTable(BASE, table(pins)), "images are not chunked");
    assertTrue(refusal(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128", EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4"),
        pins).contains(PIN + "chunk.max.tokens=128"), "video speech is chunked");
    assertDoesNotThrow(() -> PrepLogicVersions.from(job(EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "2",
        EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf")).checkTable(BASE, table(pins)));
    assertTrue(refusal(job(EmbeddingTableConfig.PREP_VIDEO_DEDUP_MAX_DISTANCE.key(), "2"), pins)
        .contains(PIN + "video.dedup.max.distance=2"));
  }

  @Test
  void performanceSettingsDoNotChangeRowsOrTheirVersions() {
    TypedProperties original = new TypedProperties();
    TypedProperties changed = new TypedProperties();
    changed.setProperty(EmbeddingTableConfig.PREP_PREFETCH_MAX_BYTES.key(), "0");
    changed.setProperty(EmbeddingTableConfig.BALANCE_NUM_PARTITIONS.key(), "4");
    changed.setProperty(EmbeddingTableConfig.BALANCE_MIN_PARTITIONS.key(), "5");
    changed.setProperty(AudioConfigKeys.RUNTIME_PREFIX + "transcriber.whisper.threads", "2");
    changed.setProperty(AudioConfigKeys.RUNTIME_PREFIX + "transcriber.whisper.model.dir", "/models/another-location");
    changed.setProperty(EmbeddingTableConfig.BALANCE_PREFIX + "video" + EmbeddingTableConfig.BALANCE_MAX_BYTES_PER_TASK_SUFFIX, "1024");
    changed.setProperty(EmbeddingTableConfig.PREP_SOURCE_RETRY_MAX_ATTEMPTS.key(), "7");
    changed.setProperty(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key(), "1048576");
    changed.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf,mp4");
    assertEquals(PrepLogicVersions.from(original).tableProperties(), PrepLogicVersions.from(changed).tableProperties());
  }

  @Test
  void persistedPinsRefuseChangedLogicAndMissingKeys() throws Exception {
    TypedProperties props = new TypedProperties();
    UnstructuredTableProperties settings = UnstructuredSourceContract.embeddingTablePins("Embedding prep", props);
    Properties pins = settings.tableProperties();
    String base = temp.resolve("table").toUri().toString();
    HoodieTableMetaClient meta = HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(pins)
        .setTableVersion(9).setTableName("embedding").setTableType(HoodieTableType.COPY_ON_WRITE)
        .initTable(HoodieTestUtils.getDefaultStorageConf(), base);
    for (String key : pins.stringPropertyNames()) {
      assertEquals(pins.getProperty(key), meta.getTableConfig().getProps().getProperty(key), key);
    }
    EmbeddingTableChecks.checkTableVersion(base, HoodieTestUtils.getDefaultStorageConf(), EmbeddingTableConfig.from(props));
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128");
    StorageConfiguration<?> storage = HoodieTestUtils.getDefaultStorageConf();
    EmbeddingTableConfig changed = EmbeddingTableConfig.from(props);
    assertThrows(IllegalArgumentException.class, () -> EmbeddingTableChecks.checkTableVersion(base, storage, changed));
    // the exact pins stored, the prep contract missing: the key-by-key check refuses, naming the missing logic versions
    HoodieTableConfig missing = new HoodieTableConfig();
    for (String key : pins.stringPropertyNames()) {
      if (!key.startsWith(PIN)) {
        missing.getProps().setProperty(key, pins.getProperty(key));
      }
    }
    UnstructuredTableProperties required = settings;
    String message = assertThrows(IllegalArgumentException.class, () -> required.checkTable(base, missing)).getMessage();
    assertTrue(message.startsWith("Embedding prep requires the table's "), message);
    assertTrue(message.contains(PIN + "text.logic.version=" + PrepLogicVersions.from(new TypedProperties()).version("text")
        + " (the table stores none)"), message);
  }

  private static final String PIN = "hoodie.table.embedding.prep.";
  private static final String BASE = "file:/tables/embedding";

  /** The prep pins a table created by {@code job} stores. */
  private static Properties tablePins(TypedProperties job) {
    UnstructuredTableProperties.Builder settings = UnstructuredTableProperties.builder("Embedding prep");
    PrepLogicVersions.from(job).requireTableProperties(settings);
    return settings.build().tableProperties();
  }

  private static HoodieTableConfig table(Properties pins) {
    HoodieTableConfig table = new HoodieTableConfig();
    table.getProps().putAll(pins);
    return table;
  }

  private static TypedProperties job(String... keyValues) {
    TypedProperties job = new TypedProperties();
    for (int i = 0; i < keyValues.length; i += 2) {
      job.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return job;
  }

  private static String refusal(TypedProperties job, Properties pins) {
    PrepLogicVersions versions = PrepLogicVersions.from(job);
    HoodieTableConfig table = table(pins);
    String message = assertThrows(IllegalArgumentException.class, () -> versions.checkTable(BASE, table)).getMessage();
    assertTrue(message.endsWith("; write this job to a new table"), message);
    return message;
  }

  /**
   * An output setting an older table does not store compares as its default, so adding a new output setting at today's
   * default keeps every existing table; set to another value, it still refuses.
   */
  @Test
  void settingMissingFromAnOlderTableComparesAsItsDefault() {
    Properties pins = tablePins(new TypedProperties());
    assertEquals("256", pins.getProperty(PIN + "chunk.max.tokens"));
    assertEquals("128", pins.getProperty(PIN + "chunk.overlap.tokens"));
    assertEquals(String.valueOf(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.defaultValue()),
        pins.getProperty(PIN + "video.keyframe.interval.ms"));
    assertEquals(String.valueOf(AudioConfigKeys.SAMPLE_RATE.defaultValue()), pins.getProperty(PIN + "audio.sample.rate"));
    assertEquals("false", pins.getProperty(PIN + "audio.enabled"));
    int settings = 0;
    for (String key : pins.stringPropertyNames()) {
      if (key.endsWith(".logic.version") || key.startsWith(PIN + "extension.") || key.startsWith(PIN + "tokenizer.")
          || key.equals(PIN + "java.specification.version")) {
        continue;
      }
      settings++;
      Properties older = new Properties();
      older.putAll(pins);
      older.remove(key);
      assertDoesNotThrow(() -> PrepLogicVersions.from(new TypedProperties()).checkTable(BASE, table(older)), key);
    }
    assertEquals(PrepLogicVersions.OUTPUT_SETTINGS.length + 1, settings, "every output setting and the overlap is stored");
    // the overlap's default follows the max tokens the table stores
    Properties small = tablePins(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100"));
    assertEquals("50", small.getProperty(PIN + "chunk.overlap.tokens"));
    small.remove(PIN + "chunk.overlap.tokens");
    assertDoesNotThrow(() -> PrepLogicVersions.from(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "100"))
        .checkTable(BASE, table(small)));
    Properties older = new Properties();
    older.putAll(pins);
    older.remove(PIN + "chunk.max.tokens");
    String message = refusal(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128"), older);
    assertTrue(message.contains(PIN + "chunk.max.tokens=128 (set by " + EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()
        + "; the table stores 256)"), message);
  }

  /** A changed output setting refuses, naming each changed key with the job's and the table's value. */
  @Test
  void changedSettingRefusalNamesEachKeyWithBothValues() {
    Properties pins = tablePins(new TypedProperties());
    String message = refusal(job(EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key(), "500"), pins);
    assertTrue(message.startsWith("Embedding prep requires the table's " + PIN + "video.keyframe.interval.ms=500 (set by "
        + EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.key() + "; the table stores "
        + EmbeddingTableConfig.PREP_VIDEO_KEYFRAME_INTERVAL_MS.defaultValue() + "): the table at " + BASE), message);
    assertFalse(message.contains("sha256"), message);
    message = refusal(job(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "128", EmbeddingTableConfig.PDF_MAX_PAGES.key(), "7"), pins);
    assertTrue(message.contains(PIN + "chunk.max.tokens=128 (set by " + EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()
        + "; the table stores 256)"), message);
    assertTrue(message.contains(PIN + "chunk.overlap.tokens=64"), "the derived overlap changes with it: " + message);
    assertTrue(message.contains(PIN + "pdf.max.pages=7"), message);
    assertFalse(message.contains("logic.version"), "no logic changed: " + message);
  }

  /** java.specification.version is pinned: a JDK patch or vendor change keeps the table, a feature release refuses it. */
  @Test
  void jdkFeatureReleaseRefusesWhilePatchAndVendorDoNot() {
    Properties pins = tablePins(new TypedProperties());
    String specification = System.getProperty("java.specification.version");
    assertEquals(specification, pins.getProperty(PIN + "java.specification.version"));
    String runtime = System.getProperty("java.runtime.version");
    String vendor = System.getProperty("java.vendor");
    String vmVersion = System.getProperty("java.vm.version");
    try {
      System.setProperty("java.runtime.version", specification + ".0.99+7-another-build");
      System.setProperty("java.vm.version", specification + ".0.99+7");
      System.setProperty("java.vendor", "Another JDK Vendor");
      assertDoesNotThrow(() -> PrepLogicVersions.from(new TypedProperties()).checkTable(BASE, table(pins)));
    } finally {
      System.setProperty("java.runtime.version", runtime);
      System.setProperty("java.vm.version", vmVersion);
      System.setProperty("java.vendor", vendor);
    }
    PrepLogicVersions nextRelease = PrepLogicVersions.from(new TypedProperties(), "99");
    HoodieTableConfig table = table(pins);
    String message = assertThrows(IllegalArgumentException.class, () -> nextRelease.checkTable(BASE, table)).getMessage();
    assertTrue(message.contains(PIN + "java.specification.version=99 (the table stores " + specification + ")"), message);
    assertTrue(message.endsWith("; write this job to a new table"), message);
  }

  /**
   * Each kind's logic version is its hand-bumped logic constants, readable; a table written before a bump of one kind's
   * constant is refused only by a job that prepares that kind, naming that kind's key.
   */
  @Test
  void bumpedKindLogicRefusesOnlyJobsPreparingThatKind() {
    PrepLogicVersions versions = PrepLogicVersions.from(new TypedProperties());
    assertTrue(versions.version("video").startsWith("prep-logic-v3;"), versions.version("video"));
    assertTrue(versions.version("video").contains("jcodec=" + JcodecKeyframeVideoFrameSource.LOGIC_VERSION), versions.version("video"));
    assertTrue(versions.version("video").contains("ffmpeg=" + FfmpegKeyframeVideoFrameSource.LOGIC_VERSION), versions.version("video"));
    assertTrue(versions.version("text").contains("normalizer=" + TextNormalizer.LOGIC_VERSION), versions.version("text"));
    assertFalse(versions.version("text").contains("jcodec"), versions.version("text"));
    Properties pins = tablePins(new TypedProperties());
    pins.setProperty(PIN + "video.logic.version", versions.version("video").replace(JcodecKeyframeVideoFrameSource.LOGIC_VERSION,
        "video-keyframe-h264mp4-v1"));
    TypedProperties documents = job(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "pdf");
    assertDoesNotThrow(() -> PrepLogicVersions.from(documents).checkTable(BASE, table(pins)), "a job preparing no video");
    String message = refusal(new TypedProperties(), pins);
    assertTrue(message.contains(PIN + "video.logic.version=" + versions.version("video") + " (the table stores "), message);
    assertTrue(message.contains("video-keyframe-h264mp4-v1"), message);
    assertFalse(message.contains(PIN + "text.logic.version"), message);
    pins.setProperty(PIN + "text.logic.version", "prep-logic-v3;normalizer=text-normalizer-v1");
    message = refusal(documents, pins);
    assertTrue(message.contains(PIN + "text.logic.version="), message);
    assertFalse(message.contains(PIN + "video.logic.version"), message);
    // a video job prepares video speech with the audio contract: an audio logic change refuses it, not a document job
    Properties audio = tablePins(new TypedProperties());
    audio.setProperty(PIN + "audio.logic.version", "prep-logic-v3;speech=older");
    assertDoesNotThrow(() -> PrepLogicVersions.from(documents).checkTable(BASE, table(audio)));
    assertTrue(refusal(job(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "mp4"), audio).contains(PIN + "audio.logic.version="));
  }

  /** A table without a kind's logic version (or from an earlier dev build) is refused, never inferred. */
  @Test
  void missingOrEarlierFormatLogicVersionIsRefused() {
    Properties pins = tablePins(new TypedProperties());
    pins.remove(PIN + "image.logic.version");
    assertTrue(refusal(new TypedProperties(), pins).contains(PIN + "image.logic.version="
        + PrepLogicVersions.from(new TypedProperties()).version("image") + " (the table stores none)"));
    Properties v2 = tablePins(new TypedProperties());
    v2.setProperty(PIN + "text.logic.version", "prep-logic-v2;sha256=" + String.join("", java.util.Collections.nCopies(64, "0")));
    assertTrue(refusal(new TypedProperties(), v2).contains("the table stores prep-logic-v2;sha256="));
  }

  /** Every stored setting has its own table property: no two settings, and no setting and another pin, share one. */
  @Test
  void everyStoredSettingHasItsOwnTableProperty() {
    Map<String, String> pins = PrepLogicVersions.from(new TypedProperties()).tableProperties();
    Set<String> properties = new TreeSet<>();
    for (ConfigProperty<?> setting : PrepLogicVersions.OUTPUT_SETTINGS) {
      String property = PrepLogicVersions.settingProperty(setting.key());
      assertTrue(property.startsWith(PIN), property);
      assertTrue(pins.containsKey(property), property);
      assertTrue(properties.add(property), property);
    }
    assertEquals(PIN + "chunk.max.tokens", PrepLogicVersions.settingProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key()));
    assertEquals(PIN + "pdf.max.pages", PrepLogicVersions.settingProperty(EmbeddingTableConfig.PDF_MAX_PAGES.key()));
    assertEquals(PIN + "image.max.pixels", PrepLogicVersions.settingProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key()));
    assertEquals(PIN + "audio.sample.rate", PrepLogicVersions.settingProperty(AudioConfigKeys.SAMPLE_RATE.key()));
    assertEquals(PIN + "audio.enabled", PrepLogicVersions.settingProperty(AudioConfigKeys.ENABLED.key()));
  }

  /**
   * A key the table stores that this build does not pin (a setting or kind a later build added) is refused, so an older
   * build cannot prepare rows without it; on an executor, so is a key only the driver's build pins.
   */
  @Test
  void keyFromALaterBuildIsRefused() {
    Properties pins = tablePins(new TypedProperties());
    pins.setProperty(PIN + "chunk.later.setting", "7");
    String message = refusal(new TypedProperties(), pins);
    assertTrue(message.contains(PIN + "chunk.later.setting=none (the table stores 7)"), message);
    pins.remove(PIN + "chunk.later.setting");
    pins.setProperty(PIN + "extension.later", "com.example.LaterPreprocessor;later/1");
    assertTrue(refusal(new TypedProperties(), pins).contains(PIN + "extension.later=none (the table stores com.example.LaterPreprocessor;later/1)"));
    PrepLogicVersions versions = PrepLogicVersions.from(new TypedProperties());
    TypedProperties execution = versions.executionProperties(new TypedProperties());
    assertDoesNotThrow(() -> versions.checkExecution(execution));
    execution.setProperty(PIN + "chunk.later.setting", "7");
    IllegalArgumentException drift = assertThrows(IllegalArgumentException.class, () -> versions.checkExecution(execution));
    assertTrue(drift.getMessage().contains(PIN + "chunk.later.setting=none (the driver pinned 7)"), drift.getMessage());
  }

  /** A job checks only the stored settings of kinds it prepares: a malformed value of another kind's setting is not read. */
  @Test
  void settingsOfKindsTheJobDoesNotPrepareAreNotRead() {
    Properties pins = tablePins(new TypedProperties());
    pins.setProperty(PIN + "chunk.max.tokens", "not-a-number");
    pins.setProperty(PIN + "audio.enabled", "maybe");
    pins.remove(PIN + "video.max.pixels");
    TypedProperties images = job(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "png");
    assertDoesNotThrow(() -> PrepLogicVersions.from(images).checkTable(BASE, table(pins)));
  }

  private static Map<String, String> changed(Map<String, String> pins, String key, String value) {
    Map<String, String> result = new TreeMap<>(pins);
    assertNotNull(result.put(key, value), key);
    return result;
  }
}
