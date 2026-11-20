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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.apache.hudi.common.schema.HoodieSchema.Blob.INLINE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The media input contract of {@link PreprocessingIterator}: a chunk's payload becomes the row's {@code prepared_image}. A source
 * payload stores the source row's own blob value, unchanged; a derived payload is stored inline up to the source's inline
 * threshold; a derived payload whose bytes differ from the declared size fails that chunk alone; one whose read fails in storage
 * fails the round.
 */
public class TestPreparedImagePayload {

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
  public void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
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
    return new PreprocessingIterator(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props),
        new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), new PreprocessorRegistry(props),
        new WordCountTokenizer(), counters(), null);
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
  public void sourcePayloadStoresTheSourceRowsInlineBlobUnchanged() {
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
  public void sourcePayloadStoresTheReferenceToTheObjectWithoutReadingIt() {
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
  public void derivedPayloadShorterThanDeclaredFailsAlone() {
    byte[] declared = String.valueOf(DERIVED.length + 3).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-short", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size());
    assertArrayEquals(DERIVED, (byte[]) preparedImage(rows.get(0)).get(1));
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("ended after " + DERIVED.length + " of its declared " + (DERIVED.length + 3)), error);
  }

  @Test
  public void derivedPayloadLongerThanDeclaredFailsAlone() {
    byte[] declared = String.valueOf(DERIVED.length - 1).getBytes(StandardCharsets.UTF_8);
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-long", "frm", declared));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size());
    String error = iterator.getSideOutputs().get(0).getError();
    assertTrue(error.contains("longer than its declared " + (DERIVED.length - 1)), error);
  }

  @Test
  public void derivedPayloadReadFailingInStorageFailsTheRound() {
    PreprocessingIterator iterator = iterator(props(), inlineRow("sha-srf", "srf", "x".getBytes(StandardCharsets.UTF_8)));

    assertThrows(SourceReadException.class, () -> drain(iterator));
    assertTrue(iterator.getSideOutputs().isEmpty(), "never a quarantine entry: " + iterator.getSideOutputs());
  }

  @Test
  public void textRowsHaveNoPreparedImage() {
    TypedProperties props = props();
    preprocessor(props, "txt", TestPreprocessingIterator.PipeSplitPreprocessor.class);
    List<Row> rows = drain(iterator(props, inlineRow("sha-t", "txt", "a|b".getBytes(StandardCharsets.UTF_8))));

    assertEquals(2, rows.size());
    rows.forEach(row -> assertNull(preparedImage(row)));
  }

}
