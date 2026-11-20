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
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.utilities.sources.blob.BucketFolderFileSystem;
import org.apache.hudi.utilities.sources.blob.SourceOpenHooks;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.UnstructuredTableTestProperties;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobColumns;
import org.apache.hudi.utilities.unstructured.embedding.text.WordCountTokenizer;
import org.apache.hudi.utilities.unstructured.retry.TransientRetry;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Image files through the prep framework with the default registry: each yields exactly one image row whose
 * {@code prepared_image} is the source row's own blob value (inline bytes, or the reference to the object, whose header is read
 * by random access); every refusal is a quarantine entry of its file alone; a storage failure that is not the file's own fails the
 * round after the source's open retries.
 */
public class TestImageInputPrepRows {

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

  @TempDir
  Path temp;

  private String embeddingTable;

  @BeforeEach
  public void createTable() throws IOException {
    embeddingTable = temp.resolve("embedding").toUri().toString();
    HoodieTableMetaClient.newTableBuilder().setTableType(HoodieTableType.COPY_ON_WRITE).setTableName("embedding")
        .setRecordKeyFields(String.join(",", EmbeddingTableSchema.KEY_COLUMNS))
        .setAdditionalProperties(UnstructuredTableTestProperties.layoutOne())
        .initTable(HadoopFSUtils.getStorageConf(new Configuration()), embeddingTable);
  }

  @AfterEach
  public void clearHooks() {
    SourceOpenHooks.clear();
  }

  @Test
  public void eachImageIsOneRowStoringTheSourceBlobValue() throws IOException {
    byte[] png = ImageFixtures.image("png", 97, 41);
    // libtiff layout: the directory after the pixel data, read by random access over the object
    byte[] tiff = ImageFixtures.singleStripTiff(400, 300);
    String tiffPath = writeSource(temp.resolve("scan.tif"), tiff).toUri().toString();
    Row inline = inlineRow("sha-png", "png", png);
    Row reference = referenceRow("sha-tif", "tif", tiffPath, tiff.length);

    PreprocessingIterator iterator = iterator(props(), new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())),
        inline, reference);
    List<Row> rows = drain(iterator);

    assertEquals(2, rows.size(), iterator.getSideOutputs().toString());
    assertTrue(iterator.getSideOutputs().isEmpty(), iterator.getSideOutputs().toString());
    assertImageRow(rows.get(0), "sha-png", 97, 41, "png");
    assertSame(inline.getAs(BlobColumns.BLOB), preparedImage(rows.get(0)), "the inline source blob, as it came");
    assertImageRow(rows.get(1), "sha-tif", 400, 300, "tiff");
    Row prepared = preparedImage(rows.get(1));
    assertSame(reference.getAs(BlobColumns.BLOB), prepared, "the reference to the object, as the source row carries it");
    assertEquals(tiffPath, prepared.getStruct(2).getString(0));
  }

  /** Each refusal fails its own file (a quarantine entry, modality image) and the good file between them is still one row. */
  @Test
  public void everyRefusalIsAQuarantineEntryOfItsFile() throws IOException {
    TypedProperties props = props();
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key(), "10000");
    props.setProperty(EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key(), "200000");
    String missing = temp.resolve("gone.png").toUri().toString();
    Row[] rows = {
        inlineRow("sha-garbage", "png", ImageFixtures.garbage()),
        inlineRow("sha-truncated", "png", ImageFixtures.truncatedPng()),
        inlineRow("sha-webp", "png", ImageFixtures.webpHeader(64, 48)),
        inlineRow("sha-pixels", "png", ImageFixtures.image("png", 200, 100)),
        inlineRow("sha-good", "jpg", ImageFixtures.image("jpeg", 20, 10)),
        referenceRow("sha-bytes", "png", missing, 300000),
        referenceRow("sha-missing", "png", missing, 1000)};
    PreprocessingIterator iterator = iterator(props, new BlobBytesSource(HadoopFSUtils.getStorageConf(new Configuration())), rows);
    List<Row> out = drain(iterator);

    assertEquals(1, out.size());
    assertImageRow(out.get(0), "sha-good", 20, 10, "jpeg");
    Map<String, String> errors = new HashMap<>();
    for (PrepSideOutput entry : iterator.getSideOutputs()) {
      assertEquals(PrepSideOutput.STAGE_PREP, entry.getStage(), entry.toString());
      assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, entry.getModality(), entry.toString());
      errors.put(entry.getSourcePathSha(), entry.getError());
    }
    assertEquals(6, errors.size(), errors.toString());
    assertContains(errors.get("sha-garbage"), "no image reader recognises the content");
    assertContains(errors.get("sha-truncated"), "corrupt png image header");
    assertContains(errors.get("sha-webp"), "image webp 64x48 is not stored");
    assertContains(errors.get("sha-pixels"), "declares 20000 pixels, above " + EmbeddingTableConfig.PREP_IMAGE_MAX_PIXELS.key());
    assertContains(errors.get("sha-bytes"), "image of 300000 bytes is above " + EmbeddingTableConfig.PREP_IMAGE_MAX_BYTES.key());
    assertContains(errors.get("sha-missing"), "gone.png");
  }

  /** A transient failure to open an image's object is retried in the round; the row is written once it opens. */
  @Test
  public void transientOpenFailureIsRetried() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Path local = bucketObject("retried.png", png);
    AtomicInteger attempts = new AtomicInteger();
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      if (attempts.incrementAndGet() == 1) {
        throw new IOException("injected 503 Slow Down");
      }
    });
    PreprocessingIterator iterator = iterator(props(), bucketSource(new TransientRetry(2, 0L)),
        referenceRow("sha-retried", "png", bucketPath("retried.png"), png.length));
    List<Row> rows = drain(iterator);

    assertEquals(1, rows.size(), iterator.getSideOutputs().toString());
    assertImageRow(rows.get(0), "sha-retried", 30, 20, "png");
    assertEquals(2, attempts.get());
  }

  /** A storage failure that outlasts the retries is no quarantine entry: the round fails, so a later run reads the file. */
  @Test
  public void persistentStorageFailureFailsTheRound() throws IOException {
    byte[] png = ImageFixtures.image("png", 30, 20);
    Path local = bucketObject("throttled.png", png);
    SourceOpenHooks.beforeOpen(local.toAbsolutePath().toString(), () -> {
      throw new IOException("injected 503 Slow Down");
    });
    PreprocessingIterator iterator = iterator(props(), bucketSource(new TransientRetry(2, 0L)),
        referenceRow("sha-throttled", "png", bucketPath("throttled.png"), png.length));
    RuntimeException failed = assertThrows(RuntimeException.class, () -> drain(iterator));
    assertNotNull(PrepFailures.roundFailure(failed), failed.toString());
    assertTrue(iterator.getSideOutputs().isEmpty(), iterator.getSideOutputs().toString());
  }

  private static void assertImageRow(Row row, String sha, int width, int height, String format) {
    assertEquals(sha, row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.SOURCE_PATH_SHA)));
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.MODALITY)));
    assertNull(row.get(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)));
    Map<String, String> expectedAux = new HashMap<>();
    expectedAux.put(ImageInputPreprocessor.AUX_WIDTH, String.valueOf(width));
    expectedAux.put(ImageInputPreprocessor.AUX_HEIGHT, String.valueOf(height));
    expectedAux.put(ImageInputPreprocessor.AUX_FORMAT, format);
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_WIDTH, String.valueOf(width));
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_HEIGHT, String.valueOf(height));
    expectedAux.put(EmbeddingTableSchema.AUX_INPUT_LOGIC, ImageInputPreprocessor.INPUT_LOGIC);
    assertEquals(expectedAux, row.getJavaMap(EmbeddingTableSchema.BASE_COLUMNS.indexOf(EmbeddingTableSchema.AUX)));
  }

  private static void assertContains(String text, String expected) {
    assertNotNull(text);
    assertTrue(text.contains(expected), text);
  }

  private TypedProperties props() {
    TypedProperties props = new TypedProperties();
    props.setProperty(HoodieWriteConfig.BASE_PATH.key(), embeddingTable);
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_ONLY);
    return props;
  }

  private BlobBytesSource bucketSource(TransientRetry retry) {
    Configuration conf = new Configuration();
    BucketFolderFileSystem.install(conf, temp.resolve("buckets"));
    return new BlobBytesSource(HadoopFSUtils.getStorageConf(conf), retry);
  }

  private Path bucketObject(String key, byte[] bytes) throws IOException {
    Path file = BucketFolderFileSystem.file(temp.resolve("buckets"), "bucket", key);
    Files.createDirectories(file.getParent());
    return writeSource(file, bytes);
  }

  private static Path writeSource(Path file, byte[] bytes) throws IOException {
    Files.write(file, bytes);
    Files.setLastModifiedTime(file, FileTime.fromMillis(1L));
    return file;
  }

  private static String bucketPath(String key) {
    return BucketFolderFileSystem.SCHEME + "://bucket/" + key;
  }

  private static Row inlineRow(String sha, String extension, byte[] bytes) {
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.INLINE, bytes, null}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, (long) bytes.length, 1L, blob, "s3://bucket/" + sha + "." + extension, sha + "." + extension}, SOURCE_ROW);
  }

  private static Row referenceRow(String sha, String extension, String path, long size) {
    Row blob = new GenericRowWithSchema(new Object[] {HoodieSchema.Blob.OUT_OF_LINE, null,
        new GenericRowWithSchema(new Object[] {path, null, null, false}, REFERENCE)}, BLOB);
    return new GenericRowWithSchema(new Object[] {sha, extension, size, 1L, blob, "s3://bucket/" + sha + "." + extension, sha + "." + extension}, SOURCE_ROW);
  }

  private PreprocessingIterator iterator(TypedProperties props, BlobBytesSource source, Row... rows) {
    PrepCounters counters = new PrepCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
    return new PreprocessingIterator(Arrays.asList(rows).iterator(), EmbeddingTableConfig.from(props), source,
        new PreprocessorRegistry(props), new WordCountTokenizer(), counters, null);
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
}
