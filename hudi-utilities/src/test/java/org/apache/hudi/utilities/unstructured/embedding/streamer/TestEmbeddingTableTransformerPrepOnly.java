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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV1;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieValidationException;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.prep.TextPipePreprocessor;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link EmbeddingTableTransformer} refuses the instant-less {@code apply}: it needs the commit started before the transformer.
 */
@Tag("functional")
public class TestEmbeddingTableTransformerPrepOnly extends EmbeddingRoundTestBase {

  private TypedProperties prepOnly() {
    return props(table(), EmbeddingTableConfig.MODE_PREP_ONLY,
        EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "16", EmbeddingTableConfig.CHUNK_OVERLAP_TOKENS.key(), "0");
  }

  @Test
  public void oneCommitHoldsEveryPreparedRowWithoutAModelColumnAndTheStockCheckpoint() throws Exception {
    FixtureFile a = text("a", "alpha", "beta", "gamma");
    FixtureFile b = text("b", "delta");
    FixtureFile pdf = object("c.pdf", PdfFixtures.text(
        "Alpha beta gamma delta epsilon zeta. Eta theta iota kappa lambda mu. Nu xi omicron pi rho sigma.",
        "Tau upsilon phi chi psi omega. Alpha beta gamma delta epsilon zeta. Eta theta iota kappa lambda mu."));
    announce(a, b, pdf);

    Round round = round(prepOnly());

    HoodieInstant commit = round.getCommit().get();
    assertTrue(round.getErrors().isEmpty(), round.toString());
    Map<String, Row> rows = rows(table());
    List<Row> pdfRows = rows.values().stream().filter(row -> pdf.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)))
        .sorted((x, y) -> Long.compare(x.getAs(EmbeddingTableSchema.CHUNK_ID), y.getAs(EmbeddingTableSchema.CHUNK_ID)))
        .collect(Collectors.toList());
    assertTrue(pdfRows.size() >= 2, "a 36-word PDF at 16 tokens per chunk is more than one chunk: " + pdfRows);
    assertEquals(4 + pdfRows.size(), rows.size(), rows.keySet().toString());
    assertEquals(rows.size(), round.getRowsWritten());
    for (FixtureFile file : Arrays.asList(a, b, pdf)) {
      long chunks = rows.values().stream().filter(row -> file.sha.equals(row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA))).count();
      for (long chunk = 0; chunk < chunks; chunk++) {
        Row row = rows.get(key(file, EmbeddingTableSchema.MODALITY_TEXT, chunk));
        assertEquals(file.sha, row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA));
        assertEquals(versionOf(file).modificationTime, row.<Long>getAs(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME));
        assertEquals(versionOf(file).size, row.<Long>getAs(EmbeddingTableSchema.SOURCE_FILE_SIZE));
        assertEquals("text", row.getAs(EmbeddingTableSchema.MODALITY));
        assertEquals(chunk, row.<Long>getAs(EmbeddingTableSchema.CHUNK_ID));
        assertEquals(file.sourcePath, row.getAs(EmbeddingTableSchema.SOURCE_FILE_PATH));
        assertEquals(file.fileName(), row.getAs(EmbeddingTableSchema.SOURCE_FILE_NAME));
        assertEquals(file.extension, row.getAs(EmbeddingTableSchema.SOURCE_FILE_EXTENSION));
      }
    }
    assertEquals("beta", rows.get(key(a, EmbeddingTableSchema.MODALITY_TEXT, 1)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    String pdfText = pdfRows.stream().map(row -> row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK)).collect(Collectors.joining(" "));
    for (String word : Arrays.asList("Alpha", "sigma", "Tau", "omega")) {
      assertTrue(pdfText.contains(word), word + " in " + pdfText);
    }

    // no model column (no VECTOR column at all), no preprocessor column
    StructType schema = EmbeddingTableSchema.writerReadSchema(table(), storage()).get();
    assertFalse(Arrays.asList(schema.fieldNames()).contains(EmbeddingStreamers.MODEL_COLUMN), Arrays.toString(schema.fieldNames()));
    assertFalse(Arrays.stream(schema.fields()).anyMatch(field -> field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)
        && field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD).startsWith("VECTOR(")), schema.treeString());
    // the prepared_image BLOB column is there in prep_only too
    assertTrue(Arrays.asList(schema.fieldNames()).contains(EmbeddingTableSchema.PREPARED_IMAGE), schema.treeString());
    assertFalse(Arrays.asList(schema.fieldNames()).contains("preprocessor"), Arrays.toString(schema.fieldNames()));

    // the stock S3 events checkpoint under the V1 key, and no other checkpoint key
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), commit));
    assertEquals(Collections.singletonList(StreamerCheckpointV1.STREAMER_CHECKPOINT_KEY_V1),
        EmbeddingStreamers.checkpointKeys(table(), storage(), commit));
  }

  @Test
  public void roundWithoutNewEventsCommitsNothingAndTheNextEventsCommitIsReadOnce() throws Exception {
    FixtureFile a = text("a", "alpha", "beta");
    announce(a);
    assertTrue(round(prepOnly()).getCommit().isPresent());

    Round idle = round(prepOnly());
    assertFalse(idle.getCommit().isPresent(), idle.toString());

    FixtureFile c = text("c", "gamma", "delta", "epsilon");
    announce(c);
    Round next = round(prepOnly());
    assertEquals(3, next.getRowsWritten(), "only the new events commit's object is read: " + next);
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), next.getCommit().get()));
    assertEquals(5, rows(table()).size());
    assertEquals(2, TextPipePreprocessor.OPENED.get(), "each file is prepared once");
  }

  @Test
  public void anOverwriteAddsVersionRowsAndKeepsTheOld() throws Exception {
    FixtureFile a = text("a", "alpha", "beta", "gamma");
    announce(a);
    touch(a, Instant.parse("2026-01-02T10:00:00Z"));
    Version old = versionOf(a);
    round(prepOnly());

    FixtureFile overwritten = new FixtureFile(a.sourcePath, "alpha2|beta2".getBytes(StandardCharsets.UTF_8));
    announce(overwritten);
    touch(a, Instant.parse("2026-01-02T10:00:01Z"));
    Round round = round(prepOnly());

    assertEquals(2, round.getRowsWritten(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals("alpha2", rows.get(key(a, EmbeddingTableSchema.MODALITY_TEXT, 0)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals("beta2", rows.get(key(a, EmbeddingTableSchema.MODALITY_TEXT, 1)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals("alpha", rows.get(key(a, old, "text", 0)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals("beta", rows.get(key(a, old, "text", 1)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals("gamma", rows.get(key(a, old, "text", 2)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertEquals(5, rows.size());
  }

  @Test
  public void rowsOfExtensionsOutsideThePrepAllowListAreDroppedWithNoErrorRecord() throws Exception {
    FixtureFile pdf = object("c.pdf", PdfFixtures.text("Omicron pi rho sigma."));
    announce(text("a", "alpha", "beta"), pdf);
    TypedProperties props = prepOnly();
    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), "PDF");

    Round round = round(props);

    assertTrue(round.getErrors().isEmpty(), round.toString());
    Map<String, Row> rows = rows(table());
    assertEquals(Collections.singleton(pdf.sha), rows.values().stream()
        .map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)).collect(Collectors.toSet()), rows.keySet().toString());
    assertEquals(0, TextPipePreprocessor.OPENED.get(), "the text object is not opened");
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()),
        "the checkpoint moves past the dropped rows");
  }

  @Test
  public void failedFileIsAnErrorRecordAndTheOtherFilesAreCommitted() throws Exception {
    FixtureFile good = text("good", "alpha", "beta");
    FixtureFile broken = text("broken", TextPipePreprocessor.BROKEN);
    announce(good, broken);

    Round round = round(prepOnly());

    assertEquals(2, round.getRowsWritten(), round.toString());
    assertTrue(rows(table()).keySet().stream().allMatch(key -> key.startsWith("source_path_sha:" + good.sha + ",")), rows(table()).keySet().toString());
    assertEquals(1, round.getErrors().size(), round.toString());
    assertEquals(broken.sha, round.getErrors().get(0).get("source_path_sha"));
    assertEquals("prep", round.getErrors().get(0).get("stage"));
    assertTrue(String.valueOf(round.getErrors().get(0).get("error")).contains("TEST_BROKEN"), round.toString());
  }

  @Test
  public void theTransformerNeedsTheInstantStartedBeforeIt() {
    assertThrows(HoodieException.class, () -> new EmbeddingTableTransformer().apply(jsc(), spark(), null, prepOnly()));
  }

  /** Streamer shutdown retires the final round while the caller keeps its own cache and Spark context. */
  @Test
  void streamerCloseReleasesItsCachesAndKeepsCallerState() throws Exception {
    announce(text("a", "alpha", "beta"));
    Dataset<Row> caller = spark().range(3).toDF().cache();
    try {
      caller.count();
      int callerCaches = spark().sparkContext().getPersistentRDDs().size();
      Round round = EmbeddingStreamers.round(spark(), prepOnly(), cfg -> { }, false);
      assertTrue(round.getCommit().isPresent(), round.toString());
      assertEquals(callerCaches, spark().sparkContext().getPersistentRDDs().size(), "the streamer retired only its round caches");
      assertEquals(3, caller.count(), "the caller's cache and Spark context remain usable");
      EmbeddingTableTransformer.releaseRound(table());
      assertEquals(callerCaches, spark().sparkContext().getPersistentRDDs().size(), "explicit retirement after close is harmless");
    } finally {
      caller.unpersist();
    }
  }

  /** Every row the default PDF preprocessor prepares holds a positive token count no greater than chunk.max.tokens. */
  @Test
  public void defaultPdfPreprocessorRowsStayWithinChunkMaxTokens() throws Exception {
    FixtureFile pdf = object("c.pdf", PdfFixtures.text(
        "Alpha beta gamma delta epsilon zeta. Eta theta iota kappa lambda mu. Nu xi omicron pi rho sigma.",
        "Tau upsilon phi chi psi omega. Alpha beta gamma delta epsilon zeta. Eta theta iota kappa lambda mu."));
    announce(pdf);
    Round round = round(prepOnly());
    assertTrue(round.getErrors().isEmpty(), round.toString());
    Map<String, Row> rows = rows(table());
    assertTrue(rows.size() >= 2, "a 36-word PDF at 16 tokens per chunk is more than one chunk: " + rows.keySet());
    for (Row row : rows.values()) {
      Integer tokens = row.getAs(EmbeddingTableSchema.CHUNK_TOKEN_COUNT);
      assertTrue(tokens != null && tokens > 0 && tokens <= 16, "token count within chunk.max.tokens: " + row);
    }
  }

  @Test
  public void prepOnlyJobWithAComputeSettingIsRefusedAndWritesNothing() throws Exception {
    announce(text("a", "alpha"));
    TypedProperties props = prepOnly();
    props.setProperty(EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key(), "8");

    Exception failure = assertThrows(Exception.class, () -> round(props));
    IllegalArgumentException refused = EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure)));

    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.COMPUTE_BATCH_SIZE.key()), refused.getMessage());
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty());
  }

  /**
   * A job that preps video (no allow-list: every mapped extension, the video ones included) whose executor heap cannot hold the
   * frame decodes is refused by the start checks before any prep work, and writes nothing; the same job with an allow-list
   * without video is not checked.
   */
  @Test
  public void videoJobWhoseHeapCannotHoldItsFramesIsRefusedBeforeAnyRound() throws Exception {
    announce(text("a", "alpha"));
    TypedProperties props = prepOnly();
    // this JVM is the executor in local mode: a fixed term of its whole heap leaves no room for any frame
    props.setProperty(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key(), String.valueOf(Runtime.getRuntime().maxMemory()));

    Exception failure = assertThrows(Exception.class, () -> round(props));
    HoodieValidationException refused = EmbeddingStreamers.causeOf(failure, HoodieValidationException.class)
        .orElseThrow(() -> new AssertionError(EmbeddingStreamers.causes(failure)));

    assertTrue(refused.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_MAX_PIXELS.key())
        && refused.getMessage().contains(EmbeddingTableConfig.PREP_VIDEO_HEAP_FIXED_BYTES.key()), refused.getMessage());
    // without an allow-list the refusal also names it: a job without videos lists its extensions there
    assertTrue(refused.getMessage().contains("list its extensions in " + EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key()),
        refused.getMessage());
    assertEquals(0, TextPipePreprocessor.OPENED.get(), "no prep work ran");
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty());

    props.setProperty(EmbeddingTableConfig.PREP_FILE_EXTENSIONS.key(), TEXT_EXTENSION);
    Round round = round(props);
    assertTrue(round.getCommit().isPresent(), round.toString());
  }
}
