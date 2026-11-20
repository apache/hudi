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
import org.apache.hudi.exception.HoodieException;
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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
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

}
