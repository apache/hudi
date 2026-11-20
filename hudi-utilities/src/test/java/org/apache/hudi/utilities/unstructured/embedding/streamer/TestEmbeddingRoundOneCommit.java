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

import org.apache.hudi.utilities.unstructured.embedding.EmbeddingRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.FixtureFile;
import org.apache.hudi.utilities.unstructured.embedding.compute.DummyEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.prep.pdf.PdfFixtures;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The embedding job in {@code prep_and_compute} mode: one commit holds every row the round prepares, each with its derived
 * data and its embedding; no committed row waits for compute.
 */
@Tag("functional")
class TestEmbeddingRoundOneCommit extends EmbeddingRoundTestBase {

  @Test
  void oneCommitHoldsEveryRowWithItsEmbedding() throws Exception {
    FixtureFile a = text("a", "alpha", "beta", "gamma");
    FixtureFile b = text("b", "delta");
    FixtureFile pdf = object("c.pdf", PdfFixtures.text("Omicron pi rho sigma.", "Tau upsilon phi chi."));
    announce(a, b, pdf);

    Round round = round(props(table(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE,
        EmbeddingTableConfig.COMPUTE_PARALLELISM.key(), "3"));

    assertTrue(round.getCommit().isPresent(), round.toString());
    assertEquals(1, round.getCompleted().size(), "one commit: " + round);
    Map<String, Row> rows = rows(table());
    assertTrue(rows.size() >= 5, rows.keySet().toString());
    assertEquals(rows.size(), round.getRowsWritten());
    for (Row row : rows.values()) {
      String sha = row.getAs(EmbeddingTableSchema.SOURCE_PATH_SHA);
      long chunk = row.getAs(EmbeddingTableSchema.CHUNK_ID);
      assertArrayEquals(DummyEmbeddingClient.vectorFor(sha, chunk, EmbeddingStreamers.MODEL_DIMENSIONS), vector(row),
          "every committed row carries its embedding: " + row);
      assertFalse(row.<String>getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK).isEmpty(), "derived data kept: " + row);
    }
    assertEquals("beta", rows.get(key(a, EmbeddingTableSchema.MODALITY_TEXT, 1)).getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
    assertTrue(ScriptedEmbeddingClient.CALLS.get() > 0);
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), round.getCommit().get()));
  }

  @Test
  void prepAndComputeDerivesTheSameColumnsAsPrepOnly() throws Exception {
    announce(text("a", "alpha", "beta"), text("b", "gamma", "delta", "epsilon"));

    round(props(table("prep_only"), EmbeddingTableConfig.MODE_PREP_ONLY));
    round(props(table("prep_and_compute"), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE));

    Map<String, Row> prepOnly = rows(table("prep_only"));
    Map<String, Row> computed = rows(table("prep_and_compute"));
    assertEquals(prepOnly.keySet(), computed.keySet());
    for (String key : prepOnly.keySet()) {
      for (String column : Arrays.asList(EmbeddingTableSchema.SOURCE_PATH_SHA, EmbeddingTableSchema.CHUNK_ID,
          EmbeddingTableSchema.MODALITY, EmbeddingTableSchema.SOURCE_FILE_PATH, EmbeddingTableSchema.SOURCE_FILE_NAME,
          EmbeddingTableSchema.SOURCE_FILE_EXTENSION, EmbeddingTableSchema.PREPARED_TEXT_CHUNK, EmbeddingTableSchema.CHUNK_TOKEN_COUNT)) {
        assertEquals((Object) prepOnly.get(key).getAs(column), computed.get(key).getAs(column), key + " " + column);
      }
    }
  }
}
