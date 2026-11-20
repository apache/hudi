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

package org.apache.hudi.utilities.unstructured.embedding.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.config.HoodieErrorTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.image.ImageFixtures;
import org.apache.hudi.utilities.unstructured.embedding.image.ImagePreparer;
import org.apache.hudi.utilities.unstructured.embedding.prep.ImageInputPreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSource;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Image files through one real embedding round of the single writer, on both table types and in both modes: each image is one
 * {@code image} row in the round's one commit, whose {@code prepared_image} is the source row's own blob value (inline bytes for a
 * small image, the reference to the object for one above the inline threshold); under {@code prep_and_compute} a multimodal client
 * embeds the resized image in the same commit, and under {@code prep_only} the table has no vector column but keeps the prepared
 * images. A file that is not an image is quarantined at prep; a text-only provider refuses image rows into the error table.
 */
@Tag("functional")
class TestImageEmbeddingRound extends EmbeddingRoundTestBase {

  /** Objects up to this size are inline in their source row; the large image is above it. */
  private static final long IMAGE_INLINE_MAX_BYTES = 1024;

  private final FixtureFile small = object("images/small.png", ImageFixtures.image("png", 8, 6));
  private final FixtureFile large = object("images/large.jpg", ImageFixtures.image("jpeg", 320, 200));
  private final FixtureFile notAnImage = object("images/not-an-image.png", ImageFixtures.garbage());

  private TypedProperties imageProps(String mode, String provider) {
    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      return props(table(), mode, IMAGE_INLINE_MAX_BYTES);
    }
    String model = EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL;
    TypedProperties props = props(table(), mode, IMAGE_INLINE_MAX_BYTES, model + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, provider,
        model + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, DummyMultimodalEmbeddingClient.PROVIDER.equals(provider) ? "text,image" : "text");
    // the provider decides the client: the base's scripted client class is removed
    props.remove(model + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX);
    return props;
  }

  @ParameterizedTest(name = "{0}, {1}")
  @CsvSource({"COPY_ON_WRITE, prep_only", "COPY_ON_WRITE, prep_and_compute",
      "MERGE_ON_READ, prep_only", "MERGE_ON_READ, prep_and_compute"})
  void imageFilesBecomeOneImageRowEachInOneCommit(HoodieTableType tableType, String mode) throws Exception {
    assertTrue(small.bytes.length <= IMAGE_INLINE_MAX_BYTES, "small.png is inline: " + small.bytes.length);
    assertTrue(large.bytes.length > IMAGE_INLINE_MAX_BYTES, "large.jpg is above the inline threshold: " + large.bytes.length);
    announce(small, large, notAnImage);
    TypedProperties props = imageProps(mode, DummyMultimodalEmbeddingClient.PROVIDER);

    Round round = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());

    assertEquals(1, round.getCompleted().size(), round.toString());
    assertEquals(1, EmbeddingStreamers.commits(table(), storage()).size());
    assertEquals(tableType, EmbeddingStreamers.metaClient(table(), storage()).getTableType());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
    assertEquals(2, round.getRowsWritten());
    // the file that is not an image: one quarantine entry at prep, no row
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(notAnImage.sha, error.get("source_path_sha"));
    assertEquals("prep", error.get("stage"));
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, error.get("modality"));

    Map<String, Row> rows = rows(table());
    assertEquals(new TreeSet<>(Arrays.asList(key(large, "image", 0), key(small, "image", 0))), rows.keySet());
    Row smallRow = rows.get(key(small, "image", 0));
    Row largeRow = rows.get(key(large, "image", 0));
    assertImageRow(smallRow, small, 8, 6, "png");
    assertImageRow(largeRow, large, 320, 200, "jpeg");

    assertStoredForms(smallRow, largeRow);

    if (EmbeddingTableConfig.MODE_PREP_ONLY.equals(mode)) {
      for (Row row : rows.values()) {
        assertFalse(Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN),
            "prep_only writes no model column: " + row.schema().simpleString());
      }
    } else {
      ImagePreparer preparer = new ImagePreparer(EmbeddingTableConfig.from(props).imagePreprocessing());
      for (FixtureFile file : Arrays.asList(small, large)) {
        float[] expected = DummyMultimodalEmbeddingClient.vectorForImage(
            preparer.prepare(ImageFixtures.source(file.bytes)).getEncoded(), EmbeddingStreamers.MODEL_DIMENSIONS);
        assertArrayEquals(expected, vector(rows.get(key(file, "image", 0))), file.sourcePath);
      }
    }

    // the same versions announced again: the same keys and values, no new rows
    List<String> before = describeImages(rows);
    Version smallVersion = versionOf(small);
    Version largeVersion = versionOf(large);
    announce(small, large);
    touch(small, Instant.ofEpochMilli(smallVersion.modificationTime));
    touch(large, Instant.ofEpochMilli(largeVersion.modificationTime));
    Round rerun = EmbeddingStreamers.round(spark(), props, cfg -> cfg.tableType = tableType.name());
    assertEquals(1, rerun.getCompleted().size(), rerun.toString());
    assertEquals(before, describeImages(rows(table())));
  }

  /** The small image is stored inline, the large one as an unmanaged reference to its source; both read back as the source. */
  private void assertStoredForms(Row smallRow, Row largeRow) throws Exception {
    Row smallImage = smallRow.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
    assertEquals(HoodieSchema.Blob.INLINE, smallImage.getAs(HoodieSchema.Blob.TYPE));
    assertArrayEquals(small.bytes, (byte[]) smallImage.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD));
    Row largeImage = largeRow.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, largeImage.getAs(HoodieSchema.Blob.TYPE));
    Row reference = largeImage.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
    assertEquals(large.sourcePath, reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_PATH));
    assertEquals(Boolean.FALSE, reference.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE_IS_MANAGED),
        "an unmanaged reference to the source object: the embedding table stores no copy");
    // both stored forms read back as the source's bytes
    assertArrayEquals(small.bytes, read(smallImage));
    assertArrayEquals(large.bytes, read(largeImage));
  }

  @Test
  void textOnlyDeclarationRefusesImageRowsAndEmbedsTextRows() throws Exception {
    FixtureFile text = text("notes", "alpha", "beta");
    announce(small, text);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyEmbeddingClient.PROVIDER);

    Round round = round(props);

    assertEquals(1, round.getCompleted().size(), round.toString());
    Map<String, Row> rows = rows(table());
    // the refused image row is written with its prepared image and no vector; its error goes to the error table
    Row image = rows.get(key(small, "image", 0));
    assertNull(vector(image));
    assertArrayEquals(small.bytes, read(image.getAs(EmbeddingTableSchema.PREPARED_IMAGE)));
    for (long chunk = 0; chunk < 2; chunk++) {
      Row row = rows.get(key(text, "text", chunk));
      assertArrayEquals(DummyEmbeddingClient.vectorFor(text.sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row));
    }
    assertEquals(1, round.getErrors().size(), round.toString());
    Map<String, Object> error = round.getErrors().get(0);
    assertEquals(small.sha, error.get("source_path_sha"));
    assertTrue(String.valueOf(error.get("error")).contains("EMBED-COMPUTE-IMAGE-NOT-SUPPORTED"), error.toString());
    assertTrue(String.valueOf(error.get("error")).contains(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL
        + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX + "=text does not declare image"), error.toString());
  }

  /** An image-only declaration meets text rows: they get no call, a NULL vector and an error record; the image is embedded. */
  @Test
  void imageOnlyDeclarationRefusesTextRowsAndEmbedsImageRows() throws Exception {
    FixtureFile text = text("notes", "alpha", "beta");
    announce(small, text);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyMultimodalEmbeddingClient.PROVIDER);
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX,
        "image");

    Round round = round(props);

    assertEquals(1, round.getCompleted().size(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(EmbeddingStreamers.MODEL_DIMENSIONS, vector(rows.get(key(small, "image", 0))).length,
        "the image row is embedded at the model's one length");
    for (long chunk = 0; chunk < 2; chunk++) {
      assertNull(vector(rows.get(key(text, "text", chunk))), "chunk " + chunk);
    }
    assertEquals(2, round.getErrors().size(), round.toString());
    for (Map<String, Object> error : round.getErrors()) {
      assertEquals(text.sha, error.get("source_path_sha"));
      assertEquals("EMBED-COMPUTE-TEXT-NOT-SUPPORTED model=" + EmbeddingStreamers.MODEL + " provider="
          + DummyMultimodalEmbeddingClient.PROVIDER + ": " + EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL
          + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX + "=image does not declare text, so this row is not sent to the model",
          String.valueOf(error.get("error")));
    }
  }

  /**
   * With the error table off, content of a modality the model does not declare is refused at start: nothing is prepared or
   * written, rather than a round failing on every sync once a batch holds such a row.
   */
  @Test
  void textOnlyDeclarationWithImageContentIsRefusedAtStartWhenTheErrorTableIsOff() throws Exception {
    FixtureFile text = text("notes", "alpha", "beta");
    announce(small, text);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyEmbeddingClient.PROVIDER);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false");

    Exception failure = assertThrows(Exception.class, () -> round(props));

    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains("prepares image rows (from "), causes);
    assertTrue(causes.contains(EmbeddingTableConfig.MODEL_PREFIX + EmbeddingStreamers.MODEL
        + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX + "=text does not declare, and the error table is off ("
        + HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key() + ")"), causes);
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "nothing committed");
  }

  /** The same job with only the text extension prepared starts and embeds its text rows. */
  @Test
  void textOnlyDeclarationWithTextContentOnlyStartsWhenTheErrorTableIsOff() throws Exception {
    FixtureFile text = text("notes", "alpha", "beta");
    announce(text);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyEmbeddingClient.PROVIDER);
    props.setProperty(HoodieErrorTableConfig.ERROR_TABLE_ENABLED.key(), "false");
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION);

    Round round = round(props);

    assertEquals(1, round.getCompleted().size(), round.toString());
    Map<String, Row> rows = rows(table());
    for (long chunk = 0; chunk < 2; chunk++) {
      assertArrayEquals(DummyEmbeddingClient.vectorFor(text.sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS),
          vector(rows.get(key(text, "text", chunk))));
    }
  }

  /**
   * Two images of different files whose headers pass prep but whose bodies are cut short fail compute alike in ONE client call:
   * each is a row error naming its own row, both rows are written with their prepared image and no vector, and the round commits
   * (never the whole-call rejection of a provider that refuses the request).
   */
  @Test
  void imagesOfTwoFilesFailingComputeAlikeAreRowErrorsAndTheRoundCommits() throws Exception {
    byte[] first = ImageFixtures.image("png", 40, 30);
    byte[] second = ImageFixtures.image("png", 30, 40);
    FixtureFile cutA = object("images/cut-a.png", Arrays.copyOf(first, first.length / 2));
    FixtureFile cutB = object("images/cut-b.png", Arrays.copyOf(second, second.length / 2));
    announce(cutA, cutB);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyMultimodalEmbeddingClient.PROVIDER);
    // one compute task, so both inputs are in one client call
    props.setProperty(EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "1");

    Round round = round(props);

    assertEquals(1, round.getCompleted().size(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(new TreeSet<>(Arrays.asList(key(cutA, "image", 0), key(cutB, "image", 0))), rows.keySet());
    for (FixtureFile file : Arrays.asList(cutA, cutB)) {
      Row row = rows.get(key(file, "image", 0));
      assertNull(vector(row), file.sourcePath);
      assertArrayEquals(file.bytes, read(row.getAs(EmbeddingTableSchema.PREPARED_IMAGE)));
    }
    assertEquals(2, round.getErrors().size(), round.toString());
    for (Map<String, Object> error : round.getErrors()) {
      String text = String.valueOf(error.get("error"));
      assertTrue(text.contains("source_path_sha=" + error.get("source_path_sha")), "the error names its own row: " + error);
    }
  }

  /** A bad compute image setting is refused at round start, before prep writes anything. */
  @Test
  void badComputeImageSettingIsRefusedBeforeAnyWrite() throws Exception {
    announce(small);
    TypedProperties props = imageProps(EmbeddingTableConfig.MODE_PREP_AND_COMPUTE, DummyMultimodalEmbeddingClient.PROVIDER);
    props.setProperty(EmbeddingTableConfig.COMPUTE_IMAGE_ENCODING.key(), "webp");

    Exception failure = assertThrows(Exception.class, () -> round(props));

    assertTrue(EmbeddingStreamers.causes(failure).contains(EmbeddingTableConfig.COMPUTE_IMAGE_ENCODING.key()),
        EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty());
  }

  private void assertImageRow(Row row, FixtureFile file, int width, int height, String format) {
    assertEquals(file.sha, row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA));
    assertEquals(EmbeddingTableSchema.MODALITY_IMAGE, row.getAs(EmbeddingTableSchema.MODALITY));
    assertEquals(0L, (long) row.getAs(EmbeddingTableSchema.CHUNK_ID));
    assertEquals(file.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
    assertEquals(file.extension, row.getAs(EmbeddingTableSchema.SOURCE_FILE_EXTENSION));
    assertEquals((long) file.bytes.length, (long) row.getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE));
    assertNull(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertNull(row.getAs(EmbeddingTableSchema.CHUNK_TOKEN_COUNT));
    scala.collection.Map<String, String> aux = row.getAs(EmbeddingTableSchema.AUX);
    assertEquals(String.valueOf(width), aux.apply(ImageInputPreprocessor.AUX_WIDTH));
    assertEquals(String.valueOf(height), aux.apply(ImageInputPreprocessor.AUX_HEIGHT));
    assertEquals(format, aux.apply(ImageInputPreprocessor.AUX_FORMAT));
    assertEquals(prepLogicVersion("image"), aux.apply(EmbeddingTableSchema.AUX_PREP_LOGIC_VERSION));
  }

  /** Each row as text, with the inline bytes of its prepared image by value. */
  private static List<String> describeImages(Map<String, Row> rows) {
    List<String> out = new ArrayList<>();
    for (Map.Entry<String, Row> entry : rows.entrySet()) {
      Row row = entry.getValue();
      Row image = row.getAs(EmbeddingTableSchema.PREPARED_IMAGE);
      byte[] inline = image == null ? null : image.getAs(HoodieSchema.Blob.INLINE_DATA_FIELD);
      Row reference = image == null ? null : image.getAs(HoodieSchema.Blob.EXTERNAL_REFERENCE);
      float[] vector = Arrays.asList(row.schema().fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN) ? vector(row) : null;
      out.add(entry.getKey() + " aux=" + row.getAs(EmbeddingTableSchema.AUX) + " inline=" + Arrays.toString(inline)
          + " reference=" + reference + " vector=" + Arrays.toString(vector));
    }
    return out;
  }

  private byte[] read(Row preparedImage) throws IOException {
    try (InputStream in = new BlobBytesSource(storage()).open(preparedImage)) {
      return in.readAllBytes();
    }
  }
}
