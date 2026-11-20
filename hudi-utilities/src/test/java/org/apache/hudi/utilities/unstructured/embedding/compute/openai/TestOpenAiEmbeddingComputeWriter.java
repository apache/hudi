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

package org.apache.hudi.utilities.unstructured.embedding.compute.openai;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.utilities.unstructured.embedding.ComputeRoundCounters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.embeddings;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.json;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.recorded;
import static org.apache.hudi.utilities.unstructured.embedding.compute.openai.StubEmbeddingServer.vectorFor;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider writer assertions on the single prep_and_compute writer over the unstructured data source on S3 and the stock checkpoint. */
@Tag("functional")
class TestOpenAiEmbeddingComputeWriter extends OpenAiRoundTestBase {

  @Test
  void testOpenAiModelFillsVectorsInOneCommitAndKeepsPreparedColumns() throws Exception {
    announce(pdf("a"), pdf("b"));
    server.otherwise(PATH, embeddings(DIM));
    ComputeRoundCounters.Counted report = ComputeRoundCounters.round(spark(), providerProps(table()));
    assertEquals(1, report.round.getCompleted().size());
    assertEquals(6, report.round.getRowsWritten());
    assertEquals(6, report.counters.rowsEmbedded());
    assertEquals(0, report.counters.rowsRetried() + report.counters.rowsPermanent());
    assertTrue(report.round.getErrors().isEmpty());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), report.round.getCommit().get()));
    Map<String, Row> rows = rows(table());
    assertEquals(6, rows.size());
    for (Row row : rows.values()) {
      String text = row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
      assertNotNull(text);
      assertArrayEquals(vectorFor(text, DIM), vector(row));
      assertNotNull(row.getAs(EmbeddingTableSchema.AUX));
    }
    for (StubEmbeddingServer.Recorded request : server.posts()) {
      assertEquals("Bearer " + KEY, request.header("Authorization"));
      assertEquals(DIM, request.json().get("dimensions").asInt());
      assertEquals("text-embedding-3-small", request.json().get("model").asText());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {401, 429, 503})
  void providerFailureKeepsCommitAndStockCheckpointThenRecovers(int status) throws Exception {
    server.otherwise(PATH, embeddings(DIM));
    announce(pdf("first"));
    TypedProperties props = providerProps(table());
    Round first = round(props);
    List<HoodieInstant> before = EmbeddingStreamers.commits(table(), storage());
    String checkpoint = EmbeddingStreamers.checkpoint(table(), storage(), first.getCommit().get());
    announce(pdf("second"));
    server.otherwise(PATH, request -> recorded(status, "error-" + status + (status == 429 ? "-quota" : "") + ".json"));
    Exception failure = assertThrows(Exception.class, () -> round(props));
    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains(status == 503 ? "EMBED-COMPUTE-PROVIDER-UNAVAILABLE" : "EmbeddingClientConfigException"), causes);
    assertFalse(causes.contains(KEY.substring(0, 8)), causes);
    assertEquals(before, EmbeddingStreamers.commits(table(), storage()), "no commit completed");
    assertEquals(checkpoint, EmbeddingStreamers.checkpoint(table(), storage(), before.get(before.size() - 1)));
    assertEquals(3, rows(table()).size(), "failed round data is invisible");
    assertTrue(EmbeddingStreamers.payloads(EmbeddingStreamers.errorTablePath(props)).isEmpty());
    server.otherwise(PATH, embeddings(DIM));
    Round recovered = round(props);
    assertEquals(1, recovered.getCompleted().size());
    assertEquals(3, recovered.getRowsWritten());
    assertEquals(6, rows(table()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), recovered.getCommit().get()));
    for (Row row : rows(table()).values()) {
      assertArrayEquals(vectorFor(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK), DIM), vector(row));
    }
  }

  @Test
  void anInputRejectionKeepsItsPreparedRowAndAddsAnErrorBesideSuccessfulVectors() throws Exception {
    announce(pdf("a"), pdf("b"));
    server.otherwise(PATH, request -> request.inputs().stream().anyMatch(text -> text.startsWith("Hudi"))
        ? json(400, "{\"error\":{\"message\":\"bad document chunk\",\"type\":\"invalid_request_error\",\"param\":\"input\"}}")
        : embeddings(DIM).apply(request));
    Round report = round(providerProps(table()));
    assertEquals(1, report.getCompleted().size());
    assertEquals(6, report.getRowsWritten());
    assertEquals(2, report.getErrors().size(), report.toString());
    for (Row row : rows(table()).values()) {
      String text = row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK);
      assertNotNull(text);
      assertNotNull(row.getAs(EmbeddingTableSchema.AUX));
      if (text.startsWith("Hudi")) {
        assertNull(vector(row));
        Map<String, Object> error = report.getErrors().stream()
            .filter(item -> EmbeddingTableSchema.KEY_COLUMNS.stream().allMatch(column ->
                String.valueOf(row.<Object>getAs(column)).equals(String.valueOf(item.get(column))))).findFirst().get();
        assertEquals(EmbeddingStreamers.MODEL, error.get("model"));
        assertTrue(String.valueOf(error.get("error")).contains("bad document chunk"));
      } else {
        assertArrayEquals(vectorFor(text, DIM), vector(row));
      }
    }
  }

  @Test
  void testDriverRejectsAnInvalidModelBeforeSparkWork() throws Exception {
    announce(pdf("invalid-model"));
    TypedProperties props = providerProps(table(), PREFIX + EmbeddingTableConfig.MODEL_NAME_SUFFIX, "text-embedding-ada-002");
    Exception failure = assertThrows(Exception.class, () -> round(props));
    assertTrue(EmbeddingStreamers.causeOf(failure, IllegalArgumentException.class).isPresent(), EmbeddingStreamers.causes(failure));
    assertTrue(EmbeddingStreamers.causes(failure).contains(EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX));
    assertTrue(EmbeddingStreamers.commits(table(), storage()).isEmpty(), "no completed commit");
    assertEquals(0, server.requests().size(), "the driver refuses the model before a provider call");
  }

  @Test
  void testKeyNeverInTableFilesOrConfig() throws Exception {
    announce(pdf("key-check"));
    server.otherwise(PATH, embeddings(DIM));
    TypedProperties props = providerProps(table());
    assertTrue(round(props).getCommit().isPresent());
    String prefix = KEY.substring(0, 8);
    Path local = basePath().startsWith("file:") ? Paths.get(URI.create(basePath())) : Paths.get(basePath());
    List<Path> files;
    try (Stream<Path> paths = Files.walk(local)) {
      files = paths.filter(Files::isRegularFile).collect(Collectors.toList());
    }
    assertTrue(files.size() > 10, "the scan includes the actual tables");
    for (Path file : files) {
      assertFalse(new String(Files.readAllBytes(file), StandardCharsets.ISO_8859_1).contains(prefix), file.toString());
    }
    assertEquals(0, spark().read().format("hudi").load(table()).toJSON().toDF("json").filter(functions.col("json").contains(prefix)).count());
    assertEquals(0, spark().read().format("hudi").load(objects().eventsPath()).toJSON().toDF("json").filter(functions.col("json").contains(prefix)).count());
    assertEquals(1, spark().createDataset(Arrays.asList("safe", "token " + KEY), Encoders.STRING()).toDF("json").filter(functions.col("json").contains(prefix)).count(),
        "the scan detects a key when present");
    assertFalse(props.toString().contains(prefix));
    for (HoodieInstant commit : EmbeddingStreamers.commits(table(), storage())) {
      assertFalse(EmbeddingStreamers.metadata(table(), storage(), commit).getExtraMetadata().toString().contains(prefix));
    }
  }
}
