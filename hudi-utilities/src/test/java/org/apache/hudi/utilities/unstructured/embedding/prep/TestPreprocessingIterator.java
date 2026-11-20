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
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.source.FileMeta;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

  @TempDir
  Path temp;

  private String embeddingTable;
  private LongAccumulator filesOk;
  private LongAccumulator filesFailed;
  private LongAccumulator chunks;

  @BeforeEach
  public void reset() throws IOException {
    // an existing (empty) embedding table
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.MERGE_ON_READ).setTableName("embedding")
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
    return new PreprocessingIterator(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props),
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(),
        new PrepCounters(filesOk, filesFailed, chunks, new LongAccumulator(), new LongAccumulator()),
        null);
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
  public void rowsCarryTheFiveKeyColumnsAndTheSourceColumns() {
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
  public void userPartitionColumnsAreCopiedFromTheSourceRow() {
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
  public void chunkIdsRestartAtZeroForEveryFile() {
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
  public void relativeOutOfLineReferenceFailsTheFileOnly() {
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
  public void goneObjectWithAPreprocessorIsAFailedFileNamedGoneSinceAnnounced() {
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
  public void goneObjectWithoutAPreprocessorStaysANoPreprocessorOutput() {
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
  public void carriesTheSourceFileColumns() {
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
  public void closesEveryFilesChunksOnce() {
    PreprocessingIterator iterator = iterator(props("m1"), blobRow("sha-a", "txt", "a|b"), blobRow("sha-b", "txt", "c"));
    drain(iterator);
    iterator.close();

    assertEquals(2, PipeSplitPreprocessor.OPENED.get());
    assertEquals(2, PipeSplitPreprocessor.CLOSED.get());
  }

  @Test
  public void closeMidFileClosesTheOpenFileAndEndsTheIteration() {
    PreprocessingIterator iterator = iterator(props("m1"), blobRow("sha-a", "txt", "a|b"), blobRow("sha-b", "txt", "c"));
    assertTrue(iterator.hasNext());
    iterator.next();
    iterator.close();

    assertEquals(1, PipeSplitPreprocessor.OPENED.get(), "the second file is never opened");
    assertEquals(1, PipeSplitPreprocessor.CLOSED.get());
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);
  }

  @Test
  public void anEmptyBatchYieldsNoRow() {
    PreprocessingIterator iterator = iterator(props("m1"));

    assertFalse(iterator.hasNext());
    assertEquals(0L, filesOk.value());
    assertEquals(Collections.emptyList(), drain(iterator));
  }
}
