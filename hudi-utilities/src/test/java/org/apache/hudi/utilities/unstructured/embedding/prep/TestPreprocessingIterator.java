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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.blob.BlobTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.input.PayloadDestination;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.CountingObjectFileSystem;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlob;
import org.apache.hudi.utilities.unstructured.embedding.source.SeekableBlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PreprocessingIterator}: one embedding row per chunk the extension's preprocessor yields, keyed by the blob version and
 * a per-file chunk id, with every model column left NULL.
 */
public class TestPreprocessingIterator {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType BLOB_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, true),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, true),
      DataTypes.createStructField("team", DataTypes.StringType, true)});
  /** The modification time every source row of these tests carries. */
  private static final long MODIFICATION_TIME = 1234L;

  /** Splits a file's UTF-8 text on {@code |}: one text chunk per part, its index in aux. */
  public static class PipeSplitPreprocessor implements FilePreprocessor {
    static final AtomicInteger OPENED = new AtomicInteger();
    static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public String name() {
      return "pipe-split-v1";
    }

    @Override
    public SourceKind sourceKind() {
      return SourceKind.DOCUMENT;
    }

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
        throws IOException {
      OPENED.incrementAndGet();
      String text;
      try (InputStream in = bytes.open()) {
        text = new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      List<Chunk> chunks = new ArrayList<>();
      String[] parts = text.split("\\|");
      for (int i = 0; i < parts.length; i++) {
        Map<String, String> aux = new HashMap<>();
        aux.put("part", String.valueOf(i));
        chunks.add(Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT).text(parts[i]).tokenCount(tokenizer.countTokens(parts[i]))
            .aux(aux).build());
      }
      Iterator<Chunk> it = chunks.iterator();
      return new ClosableIterator<Chunk>() {
        @Override
        public void close() {
          CLOSED.incrementAndGet();
        }

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public Chunk next() {
          return it.next();
        }
      };
    }
  }

  /** Emits one derived payload to exercise the iterator's input-store contract. */
  public static class DerivedPayloadPreprocessor extends PipeSplitPreprocessor {
    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
      Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE)
          .payload(() -> new ByteArrayInputStream(new byte[] {1, 2, 3}),
              props.getLong("test.payload.size", 3), props.getString("test.payload.type", "image/jpeg")).build();
      Iterator<Chunk> chunks = Collections.singletonList(chunk).iterator();
      return new ClosableIterator<Chunk>() {
        @Override
        public void close() {
          // nothing to release: the chunk is held in memory
        }

        @Override
        public boolean hasNext() {
          return chunks.hasNext();
        }

        @Override
        public Chunk next() {
          return chunks.next();
        }
      };
    }
  }

  /** Records the existing decoder cleanup policy that catches source-close exceptions. */
  public static class CloseIgnoringPreprocessor extends PipeSplitPreprocessor {
    static final AtomicInteger CLOSE_FAILURES = new AtomicInteger();

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) throws IOException {
      SeekableBlob input = ((SeekableBlobBytesSupplier) bytes).openSeekable();
      byte[] value = new byte[1];
      input.read(value, 0, 1);
      Iterator<Chunk> chunks = Collections.singletonList(Chunk.builder(EmbeddingTableSchema.MODALITY_TEXT)
          .text(new String(value, StandardCharsets.UTF_8)).build()).iterator();
      return new ClosableIterator<Chunk>() {
        @Override
        public void close() {
          try {
            String changed = props.getString("test.change.at.close", null);
            if (changed != null) {
              Files.setLastModifiedTime(java.nio.file.Paths.get(changed), FileTime.fromMillis(MODIFICATION_TIME + 2000));
            }
          } catch (IOException e) {
            throw new java.io.UncheckedIOException(e);
          } finally {
            CloseStatusFileSystem.failStatus = props.getBoolean("test.status.failure.at.close", false);
            try {
              input.close();
            } catch (Exception failure) {
              CLOSE_FAILURES.incrementAndGet();
              if (props.getBoolean("test.propagate.close.failure", false)) {
                throw failure instanceof RuntimeException ? (RuntimeException) failure : new java.io.UncheckedIOException((IOException) failure);
              }
            }
          }
        }

        @Override
        public boolean hasNext() {
          return chunks.hasNext();
        }

        @Override
        public Chunk next() {
          return chunks.next();
        }
      };
    }
  }

  @Test
  void sourceChangeSwallowedByDecoderCloseStillFailsTheFile() throws IOException {
    Path file = Files.write(temp.resolve("closed-source.txt"), new byte[] {'a'});
    Files.setLastModifiedTime(file, FileTime.fromMillis(MODIFICATION_TIME));
    Row row = outOfLineRow("source", file.toUri().toString());
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        CloseIgnoringPreprocessor.class.getName());
    CloseIgnoringPreprocessor.CLOSE_FAILURES.set(0);
    try (PreprocessingIterator unchanged = iterator(props, row)) {
      assertEquals(1, drain(unchanged).size());
      assertTrue(unchanged.getSideOutputs().isEmpty());
      assertEquals(0, CloseIgnoringPreprocessor.CLOSE_FAILURES.get());
    }
    props.setProperty("test.change.at.close", file.toString());
    try (PreprocessingIterator changed = iterator(props, row)) {
      assertEquals(1, drain(changed).size(), "the established partial-file policy keeps already emitted chunks");
      assertEquals(1, CloseIgnoringPreprocessor.CLOSE_FAILURES.get(), "the source guard detected the change at close");
      assertEquals(1, changed.getSideOutputs().size(), "the decoder cannot discard a confirmed source change");
      assertTrue(changed.getSideOutputs().get(0).getError().contains("changed"));
    }
    assertEquals(1L, filesOk.value(), "only the unchanged control is successful");
    assertEquals(1L, filesFailed.value());
  }

  public static class CloseStatusFileSystem extends CountingObjectFileSystem {
    static boolean failStatus;

    @Override
    public org.apache.hadoop.fs.FileStatus getFileStatus(org.apache.hadoop.fs.Path path) throws IOException {
      if (failStatus) {
        throw new IOException("post-read status unavailable");
      }
      return super.getFileStatus(path);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sourceStatusFailureSwallowedByDecoderCloseStillFailsTheRound(boolean propagate) throws IOException {
    Configuration conf = new Configuration();
    CountingObjectFileSystem.installCounting(conf, temp.resolve("buckets"));
    conf.set("fs.countfs.impl", CloseStatusFileSystem.class.getName());
    Path local = temp.resolve("buckets/bucket/source.txt");
    Files.createDirectories(local.getParent());
    Files.write(local, new byte[] {'a'});
    Files.setLastModifiedTime(local, FileTime.fromMillis(MODIFICATION_TIME));
    Row row = outOfLineRow("source", "countfs://bucket/source.txt");
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        CloseIgnoringPreprocessor.class.getName());
    props.setProperty("test.status.failure.at.close", "true");
    props.setProperty("test.propagate.close.failure", String.valueOf(propagate));
    CloseStatusFileSystem.failStatus = false;
    CloseIgnoringPreprocessor.CLOSE_FAILURES.set(0);
    try (PreprocessingIterator iterator = PreprocessingIterator.builder(Collections.singletonList(row).iterator(), EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(conf)))
        .registry(new PreprocessorRegistry(props)).tokenizer(new WordCountTokenizer())
        .counters(new PrepCounters(filesOk, filesFailed, chunks, new LongAccumulator(), new LongAccumulator())).build()) {
      SourceReadException failure = assertThrows(SourceReadException.class, () -> drain(iterator));
      assertFalse(failure.getMessage().contains("Self-suppression"), failure.getMessage());
      assertEquals(1, CloseIgnoringPreprocessor.CLOSE_FAILURES.get());
      assertEquals(0L, filesOk.value());
      assertEquals(0L, filesFailed.value(), "an unavailable status is not a confirmed content change");
    } finally {
      CloseStatusFileSystem.failStatus = false;
    }
  }

  @Test
  void derivedPayloadWithoutDestinationKeepsCustomContentType() {
    TypedProperties props = derivedPayloadProps();
    props.setProperty("test.payload.type", "application/octet-stream");
    try (PreprocessingIterator iterator = iterator(props, blobRow("sha", "txt", "input"))) {
      List<Row> rows = drain(iterator);
      assertEquals(1, rows.size());
      Row payload = (Row) column(rows.get(0), EmbeddingTableSchema.build(EmbeddingTableConfig.from(props)),
          EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(INLINE, payload.getString(0));
      assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) payload.get(1));
      assertTrue(iterator.getSideOutputs().isEmpty());
    }
  }

  @Test
  void derivedPayloadWithoutDestinationUsesSharedSizeFailure() {
    TypedProperties props = derivedPayloadProps();
    props.setProperty("test.payload.size", "4");
    try (PreprocessingIterator iterator = iterator(props, blobRow("sha", "txt", "input"))) {
      assertTrue(drain(iterator).isEmpty());
      assertTrue(iterator.getSideOutputs().get(0).getError().contains("Payload of 3 bytes, but its chunk declares 4"));
    }
  }

  @Test
  void validDerivedPayloadWithoutDestinationKeepsBytes() {
    TypedProperties props = derivedPayloadProps();
    try (PreprocessingIterator iterator = iterator(props, blobRow("sha", "txt", "input"))) {
      List<Row> rows = drain(iterator);
      assertEquals(1, rows.size());
      Row payload = (Row) column(rows.get(0), EmbeddingTableSchema.build(EmbeddingTableConfig.from(props)),
          EmbeddingTableSchema.PREPARED_IMAGE);
      assertEquals(INLINE, payload.getString(0));
      assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) payload.get(1));
      assertNull(payload.get(2));
      assertTrue(iterator.getSideOutputs().isEmpty());
    }
  }

  private TypedProperties derivedPayloadProps() {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        DerivedPayloadPreprocessor.class.getName());
    return props;
  }

  public static class CyclicFailurePreprocessor extends PipeSplitPreprocessor {
    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
      IllegalStateException first = new IllegalStateException("first");
      IOException second = new IOException("second", first);
      first.initCause(second);
      throw first;
    }
  }

  @TempDir
  Path temp;

  private String embeddingTable;
  private LongAccumulator filesOk;
  private LongAccumulator filesFailed;
  private LongAccumulator chunks;

  @BeforeEach
  void reset() throws IOException {
    // an existing (empty) embedding table
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.MERGE_ON_READ).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS)).initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
    PipeSplitPreprocessor.OPENED.set(0);
    PipeSplitPreprocessor.CLOSED.set(0);
    filesOk = new LongAccumulator();
    filesFailed = new LongAccumulator();
    chunks = new LongAccumulator();
  }

  private TypedProperties props(String... models) {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        PipeSplitPreprocessor.class.getName());
    props.setProperty(EmbeddingTableConfig.MODELS.key(), String.join(",", models));
    for (String model : models) {
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + model + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
      props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    }
    return props;
  }

  private static Row blobRow(String sha, String extension, String text) {
    byte[] bytes = text.getBytes(StandardCharsets.UTF_8);
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, MODIFICATION_TIME, blob, "s3a://bucket/dir/a.txt", "a.txt", "blue"}, BLOB_ROW);
  }

  private PreprocessingIterator iterator(TypedProperties props, Row... rows) {
    return PreprocessingIterator.builder(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())))
        .registry(new PreprocessorRegistry(props))
        .tokenizer(new WordCountTokenizer())
        .counters(new PrepCounters(filesOk, filesFailed, chunks, new LongAccumulator(), new LongAccumulator()))
        .build();
  }

  /** The value of {@code name} in an output row (a schemaless RowFactory row): looked up through the table layout. */
  private static Object column(Row row, StructType layout, String name) {
    return row.get(layout.fieldIndex(name));
  }

  private static List<Row> drain(PreprocessingIterator iterator) {
    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }
    return rows;
  }

  @Test
  void causeCycleIsFormattedAsAFileFailureWithoutHanging() {
    assertEquals(1, drain(iterator(props(), blobRow("control", "txt", "alpha"))).size());
    TypedProperties failing = props();
    failing.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "txt" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        CyclicFailurePreprocessor.class.getName());
    assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
      try (PreprocessingIterator iterator = iterator(failing, blobRow("failed", "txt", "alpha"))) {
        assertTrue(drain(iterator).isEmpty());
        assertEquals(1, iterator.getSideOutputs().size());
        assertTrue(iterator.getSideOutputs().get(0).getError().contains("root cause IOException: second"));
      }
    });
  }

  @Test
  void executorMustMatchDriverPinsBeforeOpeningAFile() {
    TypedProperties matching = PrepLogicVersions.from(props()).executionProperties(props());
    assertEquals(1, drain(iterator(matching, blobRow("control", "txt", "alpha"))).size());
    int opened = PipeSplitPreprocessor.OPENED.get();
    TypedProperties changed = TypedProperties.copy(matching);
    changed.setProperty("hoodie.table.embedding.prep.text.logic.version", "different-driver-runtime");
    Row changedRow = blobRow("refused", "txt", "beta");
    IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> iterator(changed, changedRow));
    assertTrue(refused.getMessage().contains("hoodie.table.embedding.prep.text.logic.version="), refused.getMessage());
    TypedProperties setting = TypedProperties.copy(matching);
    setting.setProperty("hoodie.table.embedding.prep.chunk.max.tokens", "7");
    Row settingRow = blobRow("setting", "txt", "delta");
    assertThrows(IllegalArgumentException.class, () -> iterator(setting, settingRow));
    TypedProperties incomplete = TypedProperties.copy(matching);
    incomplete.remove("hoodie.table.embedding.prep.audio.logic.version");
    Row incompleteRow = blobRow("missing", "txt", "gamma");
    assertThrows(IllegalArgumentException.class, () -> iterator(incomplete, incompleteRow));
    assertEquals(opened, PipeSplitPreprocessor.OPENED.get(), "the refused executors opened no source file");
  }

  @Test
  void textRowsCarryOnePrepLogicVersion() {
    List<Row> rows = drain(iterator(props(), blobRow("sha-a", "txt", "alpha beta")));
    assertEquals(1, rows.size());
    Map<String, String> aux = rows.get(0).getJavaMap(10);
    assertTrue(aux.containsKey("prep_logic_version"));
    assertFalse(aux.containsKey("input_logic"));
    assertFalse(aux.containsKey("video_logic"));
    assertFalse(aux.containsKey("audio_logic"));
  }

  @Test
  void rowsCarryTheFiveKeyColumnsAndTheSourceColumns() {
    List<Row> rows = drain(iterator(props("m1", "m2"), blobRow("sha-a", "txt", "alpha beta|gamma")));

    assertEquals(2, rows.size());
    Row first = rows.get(0);
    assertEquals(14, first.length());
    assertEquals("sha-a", first.get(0));
    assertEquals(1234L, first.get(1));
    assertEquals(16L, first.get(2));
    assertEquals("text", first.get(3));
    assertEquals(0L, first.get(4));
    assertEquals("s3a://bucket/dir/a.txt", first.get(5));
    assertEquals("a.txt", first.get(6));
    assertEquals("txt", first.get(7));
    assertEquals("alpha beta", first.get(8));
    assertEquals(2, first.get(9));
    assertEquals("0", ((scala.collection.Map<String, String>) first.get(10)).apply("part"));
    assertNull(first.get(11), "a text row has no prepared_image");
    assertNull(first.get(12), "model columns stay NULL until compute");
    assertNull(first.get(13));

    Row second = rows.get(1);
    assertEquals("sha-a", second.get(0));
    assertEquals(1234L, second.get(1));
    assertEquals(16L, second.get(2));
    assertEquals("text", second.get(3));
    assertEquals(1L, second.get(4));
    assertEquals("gamma", second.get(8));
    assertEquals(1, second.get(9));
  }

  @Test
  void userPartitionColumnsAreCopiedFromTheSourceRow() {
    TypedProperties props = props("m1");
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), "team,missing");
    StructType schema = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props));
    List<Row> rows = drain(iterator(props, blobRow("sha-a", "txt", "alpha|beta")));

    assertEquals(2, rows.size());
    for (Row row : rows) {
      assertEquals("blue", row.get(schema.fieldIndex("team")));
      assertNull(row.get(schema.fieldIndex("missing")));
      assertNull(row.get(schema.fieldIndex("emb_m1")));
    }
  }

  @Test
  void chunkIdsRestartAtZeroForEveryFile() {
    List<Row> rows = drain(iterator(props("m1"),
        blobRow("sha-a", "txt", "a|b|c"), blobRow("sha-b", "TXT", "d|e")));

    assertEquals(5, rows.size());
    List<Object> ids = new ArrayList<>();
    StructType schema = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props("m1")));
    for (Row row : rows) {
      ids.add(column(row, schema, EmbeddingTableSchema.SOURCE_PATH_SHA) + "#" + column(row, schema, EmbeddingTableSchema.CHUNK_ID));
    }
    assertEquals(Arrays.asList("sha-a#0", "sha-a#1", "sha-a#2", "sha-b#0", "sha-b#1"), ids);
    assertEquals(2L, filesOk.value());
    assertEquals(5L, chunks.value());
  }

  /**
   * The embedding job reads an object only by its absolute path: an OUT_OF_LINE source row with a relative reference (the blob
   * table's managed form, which SQL read_blob resolves against that table's base) is a reference error of that file alone, and
   * the files beside it are prepared.
   */
  @Test
  void overwrittenObjectFailsPrepInsteadOfUsingItsPreviousVersionKey() throws IOException {
    Path file = temp.resolve("source.txt");
    Files.write(file, new byte[] {'a'});
    Files.setLastModifiedTime(file, FileTime.fromMillis(MODIFICATION_TIME));
    Row sourceRow = outOfLineRow("source", file.toUri().toString());
    StringBuilder observed = new StringBuilder("source_path_sha\tsource_modification_time\tsource_file_size\tmodality\tchunk_id\ttext\tstage\terror_class\n");
    try (PreprocessingIterator control = iterator(props(), sourceRow)) {
      List<Row> rows = drain(control);
      assertEquals(1, rows.size());
      assertEquals("a", rows.get(0).getString(8));
      Row row = rows.get(0);
      observed.append(row.getString(0)).append('\t').append(row.getLong(1)).append('\t').append(row.getLong(2))
          .append('\t').append(row.getString(3)).append('\t').append(row.getLong(4)).append('\t').append(row.getString(8)).append("\t\t\n");
      assertTrue(control.getSideOutputs().isEmpty());
    }
    Files.write(file, new byte[] {'b'});
    Files.setLastModifiedTime(file, FileTime.fromMillis(MODIFICATION_TIME + 1000));
    try (PreprocessingIterator changed = iterator(props(), sourceRow)) {
      assertEquals(Collections.emptyList(), drain(changed), "new bytes must not use the old source version key");
      assertEquals(1, changed.getSideOutputs().size());
      assertEquals(PrepSideOutput.STAGE_PREP, changed.getSideOutputs().get(0).getStage());
      assertTrue(changed.getSideOutputs().get(0).getError().contains("changed"));
      PrepSideOutput failure = changed.getSideOutputs().get(0);
      observed.append(failure.getSourcePathSha()).append('\t').append(failure.getSourceModificationTime()).append('\t')
          .append(failure.getSourceFileSize()).append("\t\t\t\t").append(failure.getStage()).append('\t')
          .append(failure.getError().split(":", 2)[0]).append('\n');
    }
    try (InputStream golden = getClass().getResourceAsStream("/unstructured/embedding/golden/source-version-guard.txt")) {
      assertNotNull(golden);
      assertEquals(new String(golden.readAllBytes(), StandardCharsets.UTF_8), observed.toString());
    }
  }

  @Test
  void relativeOutOfLineReferenceFailsTheFileOnly() {
    PreprocessingIterator iterator = iterator(props("m1"), outOfLineRow("sha-rel", ".hoodie/.blob/txt/a.bin"),
        blobRow("sha-b", "txt", "d|e"));
    List<Row> rows = drain(iterator);

    assertEquals(2, rows.size(), "the file beside the failed one is prepared");
    for (Row row : rows) {
      assertEquals("sha-b", row.get(0));
    }
    assertEquals(1L, filesOk.value());
    assertEquals(1, iterator.getSideOutputs().size(), iterator.getSideOutputs().toString());
    PrepSideOutput failure = iterator.getSideOutputs().get(0);
    assertEquals("sha-rel", failure.getSourcePathSha());
    assertEquals(PrepSideOutput.STAGE_PREP, failure.getStage());
    assertTrue(failure.getError().contains("relative external_path"), failure.getError());
  }

  /** A source row with no blob at all. A mapped extension fails the file as gone since announced. */
  private static Row goneRow(String sha, String extension) {
    return new GenericRowWithSchema(new Object[] {sha, extension, 5L, MODIFICATION_TIME, null,
        "s3a://bucket/dir/a." + extension, "a." + extension, "blue"}, BLOB_ROW);
  }

  @Test
  void goneObjectWithAPreprocessorIsAFailedFileNamedGoneSinceAnnounced() {
    PreprocessingIterator iterator = iterator(props("m1"), goneRow("sha-a", "txt"), blobRow("sha-b", "txt", "beta"));
    List<Row> rows = drain(iterator);
    assertEquals(1, rows.size(), "only the other file's chunk");
    assertEquals("sha-b", column(rows.get(0), EmbeddingTableSchema.build(EmbeddingTableConfig.from(props("m1"))), EmbeddingTableSchema.SOURCE_PATH_SHA));
    assertEquals(1, iterator.getSideOutputs().size());
    PrepSideOutput gone = iterator.getSideOutputs().get(0);
    assertEquals("sha-a", gone.getSourcePathSha());
    assertEquals(PrepSideOutput.STAGE_PREP, gone.getStage());
    assertEquals(BlobTableSchema.GONE_SINCE_ANNOUNCED, gone.getError());
    assertEquals(MODIFICATION_TIME, gone.getSourceModificationTime());
    assertEquals(5L, gone.getSourceFileSize());
    assertEquals(EmbeddingTableSchema.MODALITY_TEXT, gone.getModality(), "the preprocessor's marker modality");
    assertEquals(1L, filesOk.value());
    assertEquals(1L, filesFailed.value(), "a gone file is counted as failed");
  }

  /** A gone object no preprocessor would have read is what it always was: an empty "no preprocessor" output, not a failure. */
  @Test
  void goneObjectWithoutAPreprocessorStaysANoPreprocessorOutput() {
    PreprocessingIterator iterator = iterator(props("m1"), goneRow("sha-a", "bin"));
    assertEquals(0, drain(iterator).size());
    assertEquals(1, iterator.getSideOutputs().size());
    PrepSideOutput none = iterator.getSideOutputs().get(0);
    assertEquals(PrepSideOutput.STAGE_EMPTY, none.getStage());
    assertEquals(PrepSideOutput.NO_PREPROCESSOR, none.getError());
  }

  /** A source row whose blob is an OUT_OF_LINE managed reference at {@code externalPath}, no range. */
  private static Row outOfLineRow(String sha, String externalPath) {
    StructType reference = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {externalPath, null, null, true}, reference)}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, "txt", 1L, MODIFICATION_TIME, blob, "s3a://bucket/dir/a.txt", "a.txt", "blue"}, BLOB_ROW);
  }

  @Test
  void carriesTheSourceFileColumns() {
    StructType schema = DataTypes.createStructType(new StructField[] {
        DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
        DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
        DataTypes.createStructField(BlobColumns.BLOB, BLOB, false)});
    byte[] bytes = "one|two".getBytes(StandardCharsets.UTF_8);
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    Row row = new GenericRowWithSchema(new Object[] {"sha-a", MODIFICATION_TIME, "s3a://bucket/dir/a.txt", "a.txt", "txt",
        (long) bytes.length, blob}, schema);

    List<Row> rows = drain(iterator(props("m1"), row));

    assertEquals(2, rows.size());
    StructType layout = EmbeddingTableSchema.build(EmbeddingTableConfig.from(props("m1")));
    for (Row out : rows) {
      assertEquals("s3a://bucket/dir/a.txt", column(out, layout, EmbeddingTableSchema.SOURCE_FILE_PATH), "source_file_path");
      assertEquals("a.txt", column(out, layout, EmbeddingTableSchema.SOURCE_FILE_NAME), "source_file_name");
      assertEquals("txt", column(out, layout, EmbeddingTableSchema.SOURCE_FILE_EXTENSION), "source_file_extension");
    }
    assertEquals(1L, column(rows.get(1), layout, EmbeddingTableSchema.CHUNK_ID));
  }

  @Test
  void closesEveryFilesChunksOnce() {
    PreprocessingIterator iterator = iterator(props("m1"), blobRow("sha-a", "txt", "a|b"), blobRow("sha-b", "txt", "c"));
    drain(iterator);
    iterator.close();

    assertEquals(2, PipeSplitPreprocessor.OPENED.get());
    assertEquals(2, PipeSplitPreprocessor.CLOSED.get());
  }

  @Test
  void closeMidFileClosesTheOpenFileAndEndsTheIteration() {
    PreprocessingIterator iterator = iterator(props("m1"), blobRow("sha-a", "txt", "a|b"), blobRow("sha-b", "txt", "c"));
    assertTrue(iterator.hasNext());
    iterator.next();
    iterator.close();

    assertEquals(1, PipeSplitPreprocessor.OPENED.get(), "the second file is never opened");
    assertEquals(1, PipeSplitPreprocessor.CLOSED.get());
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  /** The Error the open file's close throws in {@link #closeKeepsClosingAfterAnErrorAndSuppressesTheLaterFailure}. */
  static final class FileCloseError extends Error {
    FileCloseError() {
      super("the open file's close failed");
    }
  }

  /** Two derived image payloads per file, stored by the input store; closing a file's chunks throws a {@link FileCloseError}. */
  public static class ErrorOnCloseImagePreprocessor implements FilePreprocessor {
    static final AtomicInteger CLOSED = new AtomicInteger();

    @Override
    public String name() {
      return "error-on-close-image-v1";
    }

    @Override
    public SourceKind sourceKind() {
      return SourceKind.IMAGE;
    }

    @Override
    public boolean storesInputs() {
      return true;
    }

    @Override
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props) {
      byte[] payload = {1, 2, 3, 4};
      Iterator<Chunk> chunks = Arrays.asList(
          Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).payload(() -> new ByteArrayInputStream(payload), payload.length, "image/jpeg")
              .build(),
          Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).payload(() -> new ByteArrayInputStream(payload), payload.length, "image/jpeg")
              .build()).iterator();
      return new ClosableIterator<Chunk>() {
        @Override
        public void close() {
          CLOSED.incrementAndGet();
          throw new FileCloseError();
        }

        @Override
        public boolean hasNext() {
          return chunks.hasNext();
        }

        @Override
        public Chunk next() {
          return chunks.next();
        }
      };
    }
  }

  /**
   * Close closes every part whatever an earlier one threw: the open file's close throws an Error and the registry's close a
   * RuntimeException; the Error is thrown, the RuntimeException suppressed on it, and the input store is still closed (its
   * background write threads end, which only its close makes them do).
   */
  @Test
  void closeKeepsClosingAfterAnErrorAndSuppressesTheLaterFailure() throws Exception {
    TypedProperties props = props("m1");
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + "img" + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        ErrorOnCloseImagePreprocessor.class.getName());
    // every payload is written out of line, in the background; no timeline server is needed for its markers
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), "1");
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    ErrorOnCloseImagePreprocessor.CLOSED.set(0);
    IllegalStateException registryFailure = new IllegalStateException("the registry's close failed");
    PreprocessorRegistry registry = new PreprocessorRegistry(props) {
      @Override
      public void close() {
        super.close();
        throw registryFailure;
      }
    };
    Set<Thread> before = writeThreads();
    PreprocessingIterator iterator = PreprocessingIterator.builder(Collections.singletonList(blobRow("sha-a", "img", "x")).iterator(),
            EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())))
        .registry(registry)
        .tokenizer(new WordCountTokenizer())
        .counters(new PrepCounters(filesOk, filesFailed, chunks, new LongAccumulator(), new LongAccumulator()))
        .destination(new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable, props))
        .build();
    assertTrue(iterator.hasNext());
    iterator.next();
    Set<Thread> started = writeThreads();
    started.removeAll(before);
    assertFalse(started.isEmpty(), "the input store wrote the first payload in the background");

    FileCloseError thrown = assertThrows(FileCloseError.class, iterator::close);
    assertEquals(1, ErrorOnCloseImagePreprocessor.CLOSED.get(), "the open file was closed");
    assertEquals(1, thrown.getSuppressed().length, Arrays.toString(thrown.getSuppressed()));
    assertSame(registryFailure, thrown.getSuppressed()[0], "the registry's failure is suppressed on the Error");
    for (Thread thread : started) {
      thread.join(TimeUnit.SECONDS.toMillis(10));
      assertFalse(thread.isAlive(), "the input store was closed after the Error: its write thread " + thread.getName() + " ended");
    }
  }

  /** The live threads of every out-of-line write pool. */
  private static Set<Thread> writeThreads() {
    return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive)
        .filter(thread -> thread.getName().startsWith("hudi-out-of-line-write-")).collect(Collectors.toCollection(HashSet::new));
  }

  @Test
  void anEmptyBatchYieldsNoRow() {
    PreprocessingIterator iterator = iterator(props("m1"));

    assertFalse(iterator.hasNext());
    assertEquals(0L, filesOk.value());
    assertEquals(Collections.emptyList(), drain(iterator));
  }
}
