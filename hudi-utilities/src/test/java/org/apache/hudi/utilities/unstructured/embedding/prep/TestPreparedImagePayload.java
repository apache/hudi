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
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingInputConfig;
import org.apache.hudi.utilities.unstructured.embedding.input.EmbeddingPayloadWriteException;
import org.apache.hudi.utilities.unstructured.embedding.input.PayloadDestination;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
import org.apache.hudi.utilities.unstructured.embedding.source.PrepPlatformException;
import org.apache.hudi.utilities.unstructured.embedding.text.Tokenizer;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.storage.ManagedBlobPaths;
import org.apache.hudi.utilities.unstructured.storage.OutOfLineWriterTestExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The media input contract of {@link PreprocessingIterator}: a chunk's payload becomes the row's {@code prepared_image}. A source
 * payload stores the source row's own blob value, unchanged; a derived payload is stored inline up to the input store's inline
 * threshold and, with a payload destination, out of line under the embedding table above it; a derived payload whose bytes
 * differ from the declared size, or (without a destination) one above the threshold, fails that chunk alone; a payload file
 * that cannot be written fails the round.
 */
@ExtendWith(OutOfLineWriterTestExtension.class)
class TestPreparedImagePayload {

  private static final StructType BLOB = (StructType) BlobType.dataType();
  private static final StructType REFERENCE = (StructType) BLOB.apply(HoodieSchema.Blob.EXTERNAL_REFERENCE).dataType();
  private static final StructType SOURCE_ROW = DataTypes.createStructType(new StructField[] {
      DataTypes.createStructField(BlobColumns.SOURCE_PATH_SHA, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_EXTENSION, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_SIZE, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false),
      DataTypes.createStructField(BlobColumns.BLOB, BLOB, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_PATH, DataTypes.StringType, false),
      DataTypes.createStructField(BlobColumns.SOURCE_FILE_NAME, DataTypes.StringType, false)});
  private static final byte[] DERIVED = "derived-frame".getBytes(StandardCharsets.UTF_8);

  /** One image chunk per file whose payload is the file's own source bytes. */
  public static class SourcePayloadPreprocessor implements FilePreprocessor {
    @Override
    public String name() {
      return "source-payload-v1";
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
      Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).sourcePayload(bytes, file.getFileSize(), "image/png").build();
      return ClosableIterator.wrap(Collections.singletonList(chunk).iterator());
    }
  }

  /** One image chunk per file of {@link #DERIVED}, at its exact size. */
  public static class OneDerivedPayloadPreprocessor implements FilePreprocessor {
    @Override
    public String name() {
      return "one-derived-payload-v1";
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
      Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE)
          .payload(() -> new ByteArrayInputStream(DERIVED), DERIVED.length, "image/jpeg").build();
      return ClosableIterator.wrap(Collections.singletonList(chunk).iterator());
    }
  }

  /** Two image chunks per file of {@link #DERIVED}: chunk 0 at its exact size, chunk 1 declared at the size the file's text names. */
  public static class DerivedPayloadPreprocessor implements FilePreprocessor {
    @Override
    public String name() {
      return "derived-payload-v1";
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
    public ClosableIterator<Chunk> open(BlobBytesSupplier bytes, FileMeta file, Tokenizer tokenizer, TypedProperties props)
        throws IOException {
      long declared;
      try (java.io.InputStream in = bytes.open()) {
        declared = Long.parseLong(new String(in.readAllBytes(), StandardCharsets.UTF_8).trim());
      }
      List<Chunk> chunks = new ArrayList<>();
      chunks.add(Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE)
          .payload(() -> new ByteArrayInputStream(DERIVED), DERIVED.length, "image/jpeg").build());
      chunks.add(Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE)
          .payload(() -> new ByteArrayInputStream(DERIVED), declared, "image/jpeg").build());
      return ClosableIterator.wrap(chunks.iterator());
    }
  }

  /** Stores inputs; its derived payload's read fails in storage under the reader's own IOException. */
  public static class StorageFailingPayloadPreprocessor implements FilePreprocessor {
    @Override
    public String name() {
      return "storage-failing-payload-v1";
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
      Chunk chunk = Chunk.builder(EmbeddingTableSchema.MODALITY_IMAGE).payload(() -> {
        throw new IOException("frame read failed", new SourceReadException("Failed to read the blob source",
            new IOException("connection reset")));
      }, 4, "image/jpeg").build();
      return ClosableIterator.wrap(Collections.singletonList(chunk).iterator());
    }
  }

  @TempDir
  Path temp;

  private String embeddingTable;

  @BeforeEach
  void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    preprocessor(props, "png", SourcePayloadPreprocessor.class);
    preprocessor(props, "frm", DerivedPayloadPreprocessor.class);
    preprocessor(props, "srf", StorageFailingPayloadPreprocessor.class);
    return props;
  }

  private static void preprocessor(TypedProperties props, String extension, Class<?> preprocessor) {
    props.setProperty(EmbeddingTableConfig.PREPROCESSOR_CLASS_PREFIX + extension + EmbeddingTableConfig.PREPROCESSOR_CLASS_SUFFIX,
        preprocessor.getName());
  }

  private static Row inlineRow(String sha, String extension, byte[] bytes) {
    Row blob = new GenericRowWithSchema(new Object[] {INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, 1L, blob, "s3://bucket/" + sha + "." + extension, sha + "." + extension}, SOURCE_ROW);
  }

  private static Row referenceRow(String sha, String path, long size) {
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {path, null, null, false}, REFERENCE)}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, "png", size, 1L, blob, "s3://bucket/" + sha + ".png", sha + ".png"}, SOURCE_ROW);
  }

  private PreprocessingIterator iterator(TypedProperties props, Row... rows) {
    return PreprocessingIterator.builder(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())))
        .registry(new PreprocessorRegistry(props))
        .tokenizer(new WordCountTokenizer())
        .counters(counters())
        .build();
  }

  /** {@link #iterator} with the input store writing out-of-line payloads under {@code table}. */
  private PreprocessingIterator storingIterator(TypedProperties props, String table, Row... rows) {
    return PreprocessingIterator.builder(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())))
        .registry(new PreprocessorRegistry(props))
        .tokenizer(new WordCountTokenizer())
        .counters(counters())
        .destination(new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), table, props))
        .build();
  }

  /** The payload files under the embedding table's blob folder, relative to its base, the checksum files left out. */
  private List<String> payloadFiles() throws IOException {
    Path blob = temp.resolve("embedding").resolve(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob");
    if (!Files.exists(blob)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.walk(blob)) {
      return files.filter(Files::isRegularFile).filter(file -> !file.getFileName().toString().endsWith(".crc"))
          .map(file -> temp.resolve("embedding").relativize(file).toString()).sorted().collect(Collectors.toList());
    }
  }

  private static PrepCounters counters() {
    return new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator());
  }

  private static List<Row> drain(PreprocessingIterator iterator) {
    List<Row> rows = new ArrayList<>();
    while (iterator.hasNext()) {
      rows.add(iterator.next());
    }
    return rows;
  }

  private static Row preparedImage(Row row) {
    return (Row) row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.PREPARED_IMAGE));
  }

  @Test
  void sourcePayloadStoresTheSourceRowsInlineBlobUnchanged() {
    byte[] bytes = {1, 2, 3, 4};
    Row source = inlineRow("sha-inline", "png", bytes);
    List<Row> rows = drain(iterator(props(), source));

    assertEquals(1, rows.size());
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, rows.get(0).get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.MODALITY)));
    Row prepared = preparedImage(rows.get(0));
    assertSame(source.getAs(BlobColumns.BLOB), prepared, "the source row's own blob value");
    assertEquals(INLINE, prepared.getString(0));
    assertArrayEquals(bytes, (byte[]) prepared.get(1));
  }

  @Test
  void sourcePayloadStoresTheReferenceToTheObjectWithoutReadingIt() {
    // the object does not exist: a source payload is never read by the framework
    String path = temp.resolve("absent.png").toUri().toString();
    Row source = referenceRow("sha-ref", path, 5_000_000L);
    PreprocessingIterator iterator = iterator(props(), source);
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size(), iterator.getSideOutputs().toString());
    Row prepared = preparedImage(rows.get(0));
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, prepared.getString(0));
    Row reference = prepared.getStruct(2);
    assertEquals(path, reference.getString(0));
    assertNull(reference.get(1));
    assertNull(reference.get(2));
    assertEquals(false, reference.get(3), "an unmanaged reference to the source object");
  }

  @Test
  void derivedPayloadIsStoredInlineAndWithoutADestinationAnOversizedOneFailsAlone() {
    TypedProperties props = props();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length));
    // chunk 0 is exactly at the threshold; chunk 1 declares one byte more
    byte[] declared = String.valueOf(DERIVED.length + 1).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = iterator(props, inlineRow("sha-frm", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size(), "the chunk at the threshold is stored");
    Row prepared = preparedImage(rows.get(0));
    assertEquals(INLINE, prepared.getString(0));
    assertArrayEquals(DERIVED, (byte[]) prepared.get(1));
    assertEquals(1, iterator.getSideOutputs().size(), iterator.getSideOutputs().toString());
    assertEquals(Long.valueOf(1L), iterator.getSideOutputs().get(0).getChunkId(), "chunk 1 fails alone");
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("above the inline threshold " + EmbeddingInputConfig.INLINE_MAX_BYTES.key() + "=" + DERIVED.length
        + ", and this prep has no payload destination"), error);
  }

  /**
   * A background payload write that fails is thrown where the iterator reports its end, and the iterator is closed there: the
   * task's write threads end although nobody calls {@link PreprocessingIterator#close} (outside Spark nothing else would).
   */
  @Test
  void failedWriteAtTheEndClosesTheIterator() throws Exception {
    TypedProperties props = props();
    preprocessor(props, "one", OneDerivedPayloadPreprocessor.class);
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    // the write fails at creating its file, whatever timeline server an earlier test left configured
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    // the blob folder is a file, so the payload file cannot be created
    Path blobFolder = temp.resolve("embedding").resolve(ManagedBlobPaths.FOLDER);
    Files.createDirectories(blobFolder.getParent());
    Files.write(blobFolder, new byte[0]);
    Set<Thread> before = writeThreads();
    PreprocessingIterator iterator = storingIterator(props, embeddingTable, inlineRow("sha-fail", "one", new byte[] {1}));

    assertTrue(iterator.hasNext());
    iterator.next();
    assertThrows(EmbeddingPayloadWriteException.class, iterator::hasNext);
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    Set<Thread> left = writeThreads();
    left.removeAll(before);
    while (!left.isEmpty() && System.nanoTime() < deadline) {
      for (Thread thread : left) {
        thread.join(100);
      }
      left = writeThreads();
      left.removeAll(before);
    }
    assertEquals(Collections.emptySet(), left, "the task's write threads ended");
  }

  /**
   * When the barrier at the end of input fails and the close that follows fails too, the barrier's failure is the one thrown,
   * with the close's suppressed on it: the task's failure names its write, not the cleanup after it.
   */
  @Test
  void failedWriteAtTheEndIsThrownWithTheCloseFailureSuppressedOnIt() throws Exception {
    TypedProperties props = props();
    preprocessor(props, "one", OneDerivedPayloadPreprocessor.class);
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    props.setProperty(HoodieWriteConfig.BLOB_MARKERS_ENABLE.key(), "false");
    // the blob folder is a file, so the payload file cannot be created
    Path blobFolder = temp.resolve("embedding").resolve(ManagedBlobPaths.FOLDER);
    Files.createDirectories(blobFolder.getParent());
    Files.write(blobFolder, new byte[0]);
    IllegalStateException closeFailure = new IllegalStateException("the registry's close failed");
    PreprocessorRegistry registry = new PreprocessorRegistry(props) {
      @Override
      public void close() {
        super.close();
        throw closeFailure;
      }
    };
    PreprocessingIterator iterator = PreprocessingIterator.builder(
            Collections.singletonList(inlineRow("sha-fail", "one", new byte[] {1})).iterator(), EmbeddingTableConfig.from(props), "001")
        .bytesSource(new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())))
        .registry(registry)
        .tokenizer(new WordCountTokenizer())
        .counters(counters())
        .destination(new PayloadDestination(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable, props))
        .build();

    assertTrue(iterator.hasNext());
    iterator.next();
    EmbeddingPayloadWriteException thrown = assertThrows(EmbeddingPayloadWriteException.class, iterator::hasNext);
    assertEquals(1, thrown.getSuppressed().length, Arrays.toString(thrown.getSuppressed()));
    assertSame(closeFailure, thrown.getSuppressed()[0], "the close's failure is suppressed on the write's");
  }

  /** The live threads of every out-of-line write pool. */
  private static Set<Thread> writeThreads() {
    return Thread.getAllStackTraces().keySet().stream().filter(Thread::isAlive)
        .filter(thread -> thread.getName().startsWith("hudi-out-of-line-write-")).collect(Collectors.toCollection(HashSet::new));
  }

  @Test
  void derivedPayloadAboveTheThresholdIsWrittenOutOfLineUnderTheEmbeddingTable() throws IOException {
    TypedProperties props = props();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    byte[] declared = String.valueOf(DERIVED.length).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = storingIterator(props, embeddingTable, inlineRow("sha-ool", "frm", declared));
    List<Row> rows = drain(iterator);

    assertTrue(iterator.getSideOutputs().isEmpty(), iterator.getSideOutputs().toString());
    assertEquals(2, rows.size());
    List<String> references = new ArrayList<>();
    for (Row row : rows) {
      Row prepared = preparedImage(row);
      assertEquals(HoodieSchema.Blob.OUT_OF_LINE, prepared.getString(0));
      assertNull(prepared.get(1), "the row holds the reference only");
      Row reference = prepared.getStruct(2);
      String path = reference.getString(0);
      assertTrue(path.matches("\\.hoodie/\\.blob/[0-9a-f-]{36}-0_\\d+-\\d+-\\d+_001\\.bin"), path);
      assertEquals(0L, reference.getLong(1));
      assertEquals((long) DERIVED.length, reference.getLong(2));
      assertEquals(true, reference.get(3), "a managed file of the embedding table");
      assertArrayEquals(DERIVED, Files.readAllBytes(temp.resolve("embedding").resolve(path)));
      references.add(path);
    }
    references.sort(null);
    assertEquals(references, payloadFiles(), "one payload file per row, nothing else");
  }

  @Test
  void derivedPayloadOfAnotherSizeAboveTheThresholdFailsAloneAndLeavesNoFile() throws IOException {
    TypedProperties props = props();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    // chunk 1 declares three bytes more than its stream has
    byte[] declared = String.valueOf(DERIVED.length + 3).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = storingIterator(props, embeddingTable, inlineRow("sha-ool-short", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size(), "chunk 0 is stored");
    assertEquals(1, iterator.getSideOutputs().size(), iterator.getSideOutputs().toString());
    assertEquals(Long.valueOf(1L), iterator.getSideOutputs().get(0).getChunkId(), "chunk 1 fails alone");
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("could not be stored") && error.contains("declares " + (DERIVED.length + 3)), error);
    assertEquals(Collections.singletonList(preparedImage(rows.get(0)).getStruct(2).getString(0)), payloadFiles(),
        "the short payload's partial file is removed");
  }

  @Test
  void payloadFileThatCannotBeWrittenFailsTheRound() throws IOException {
    TypedProperties props = props();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    // the blob folder is a file: no payload file can be created under it
    Files.write(temp.resolve("embedding").resolve(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob"), new byte[] {0});
    byte[] declared = String.valueOf(DERIVED.length).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = storingIterator(props, embeddingTable, inlineRow("sha-ool-fail", "frm", declared));

    RuntimeException failure = assertThrows(RuntimeException.class, () -> drain(iterator));
    assertTrue(PrepFailures.roundFailure(failure) instanceof EmbeddingPayloadWriteException, String.valueOf(failure));
    // both chunks are written on the task's write pool at once: the round fails with whichever failure comes first
    assertTrue(failure.getMessage().matches("(?s).*modality=image chunk_id=[01] .*"), failure.getMessage());
    assertTrue(iterator.getSideOutputs().isEmpty(), "never a quarantine entry: " + iterator.getSideOutputs());
    assertFalse(Files.isDirectory(temp.resolve("embedding").resolve(HoodieTableMetaClient.METAFOLDER_NAME + "/.blob")));
  }

  @Test
  void derivedPayloadShorterThanDeclaredFailsAlone() {
    byte[] declared = String.valueOf(DERIVED.length + 3).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-short", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size());
    assertArrayEquals(DERIVED, (byte[]) preparedImage(rows.get(0)).get(1));
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("Payload of " + DERIVED.length + " bytes, but its chunk declares " + (DERIVED.length + 3)), error);
  }

  @Test
  void derivedPayloadLongerThanDeclaredFailsAlone() {
    byte[] declared = String.valueOf(DERIVED.length - 1).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-long", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size());
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("Payload of more than " + (DERIVED.length - 1) + " bytes, but its chunk declares " + (DERIVED.length - 1)), error);
  }

  @Test
  void derivedPayloadReadFailingInStorageFailsTheRound() {
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-srf", "srf", "x".getBytes(StandardCharsets.UTF_8)));

    assertThrows(SourceReadException.class, () -> drain(iterator));
    assertTrue(iterator.getSideOutputs().isEmpty(), "never a quarantine entry: " + iterator.getSideOutputs());
  }

  @Test
  void textRowsHaveNoPreparedImage() {
    TypedProperties props = props();
    preprocessor(props, "txt", TestPreprocessingIterator.PipeSplitPreprocessor.class);
    List<Row> rows = drain(iterator(props, inlineRow("sha-t", "txt", "a|b".getBytes(StandardCharsets.UTF_8))));

    assertEquals(2, rows.size());
    rows.forEach(row -> assertNull(preparedImage(row)));
  }

  /**
   * Inside a Spark task whose heap share puts the input store's ceiling below its threshold, the store refuses before the first
   * payload: the job's configuration, so the round fails; the file is never quarantined and no payload file is written. (A
   * writer the store cannot build is a payload write failure, a round failure too: TestPreparedInputsWriterSetup.)
   */
  @Test
  void inputStoreRefusalInATaskFailsTheRound() throws IOException {
    TypedProperties props = props();
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_BYTES.key(), String.valueOf(DERIVED.length - 1));
    // a fraction so small that any task heap share gives a ceiling below the threshold
    props.setProperty(EmbeddingInputConfig.INLINE_MAX_HEAP_FRACTION.key(), "0.000000001");
    byte[] declared = String.valueOf(DERIVED.length).getBytes(StandardCharsets.UTF_8);
    SparkEnv priorEnv = SparkEnv.get();
    SparkEnv env = mock(SparkEnv.class);
    when(env.conf()).thenReturn(new SparkConf(false).set("spark.master", "local[4]").set("spark.testing.reservedMemory", "0"));
    SparkEnv.set(env);
    TaskContext$.MODULE$.setTaskContext(mock(TaskContext.class));
    try {
      PreprocessingIterator iterator = storingIterator(props, embeddingTable, inlineRow("sha-ceiling", "frm", declared));
      RuntimeException failure = assertThrows(RuntimeException.class, () -> drain(iterator));
      RuntimeException round = PrepFailures.roundFailure(failure);
      assertTrue(round instanceof PrepPlatformException, String.valueOf(failure));
      assertTrue(round.getMessage().contains(EmbeddingInputConfig.INLINE_MAX_BYTES.key()), round.getMessage());
      assertTrue(iterator.getSideOutputs().isEmpty(), "never a quarantine entry: " + iterator.getSideOutputs());
      assertEquals(Collections.emptyList(), payloadFiles());
    } finally {
      TaskContext$.MODULE$.unset();
      SparkEnv.set(priorEnv);
    }
  }
}
