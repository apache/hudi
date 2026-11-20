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

package org.apache.hudi.utilities.unstructured.embedding.compute.jina;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.utilities.unstructured.embedding.ComputeRoundCounters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.RestProviderRoundTestBase;
import org.apache.hudi.utilities.unstructured.embedding.compute.ScriptedHttpStub;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider writer assertions on the single prep_and_compute commit over the unstructured data source on S3 and the stock checkpoint. */
@Tag("functional")
public class TestJinaEmbeddingComputeWriter extends RestProviderRoundTestBase {

  private static final int DIM = 256;
  private static final String FAKE_KEY = "fake-rest-provider-key";
  private ScriptedHttpStub server;
  @TempDir
  Path secrets;
  private Path keyFile;

  @BeforeEach
  public void startEndpoint() throws Exception {
    server = new ScriptedHttpStub();
    keyFile = secrets.resolve("fake.key");
    Files.writeString(keyFile, FAKE_KEY);

  }

  @AfterEach
  public void stopEndpoint() {
    server.close();

  }

  private TypedProperties providerProps() {
    TypedProperties props = providerProps("jina", "jina-embeddings-v3", DIM, server.url() + "/v1");
    props.setProperty(PREFIX + ".credential.file", keyFile.toString());
    props.setProperty(PREFIX + ".bedrock.region", "us-east-1");
    return props;
  }

  @Test
  public void testModelFillsVectors() throws Exception {
    announce(pdf("a"), pdf("b"));
    server.otherwise(JinaStubReplies.embeddings(DIM));
    ComputeRoundCounters.Counted report = ComputeRoundCounters.round(spark(), providerProps());
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
      assertNotNull(row.getAs(EmbeddingTableSchema.AUX));
      assertArrayEquals(ScriptedHttpStub.vectorFor(text, DIM), vector(row));
    }
    for (ScriptedHttpStub.Recorded request : server.requests()) {
      assertEquals("Bearer " + FAKE_KEY, request.header("Authorization"));
      assertEquals(DIM, request.json().get("dimensions").asInt());
      assertEquals("jina-embeddings-v3", request.json().get("model").asText());
    }
  }

  @Test
  public void testRejectedInputIsQuarantinedAndItsBatchMatesEmbed() throws Exception {
    announce(pdf("a"), pdf("b"));
    server.otherwise(request -> request.body.contains("Hudi") ? JinaStubReplies.error(400, "INPUT_TOKEN_LIMIT_EXCEEDED", "bad document chunk") : JinaStubReplies.embeddings(DIM).apply(request));
    Round report = round(providerProps());
    assertEquals(1, report.getCompleted().size());
    assertEquals(6, report.getRowsWritten());
    assertEquals(2, report.getErrors().size());
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
        assertArrayEquals(ScriptedHttpStub.vectorFor(text, DIM), vector(row));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {403, 503})
  public void testRejectedConfigurationFailsTheSyncWithNothingCommitted(int status) throws Exception {
    server.otherwise(JinaStubReplies.embeddings(DIM));
    announce(pdf("first"));
    TypedProperties props = providerProps();
    Round first = round(props);
    List<HoodieInstant> before = EmbeddingStreamers.commits(table(), storage());
    String checkpoint = EmbeddingStreamers.checkpoint(table(), storage(), first.getCommit().get());
    announce(pdf("second"));
    server.otherwise(request -> JinaStubReplies.error(status, status == 403 ? "AUTHZ_INSUFFICIENT_BALANCE" : "SERVER_ERROR", "unavailable"));
    Exception failure = assertThrows(Exception.class, () -> round(props));
    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains(status == 503 ? "EMBED-COMPUTE-PROVIDER-UNAVAILABLE" : "EmbeddingClientConfigException"), causes);
    assertFalse(causes.contains(FAKE_KEY), causes);
    assertEquals(before, EmbeddingStreamers.commits(table(), storage()));
    assertEquals(checkpoint, EmbeddingStreamers.checkpoint(table(), storage(), before.get(before.size() - 1)));
    assertEquals(3, rows(table()).size());
    assertTrue(EmbeddingStreamers.payloads(EmbeddingStreamers.errorTablePath(props)).isEmpty());
    server.otherwise(JinaStubReplies.embeddings(DIM));
    Round recovered = round(props);
    assertEquals(1, recovered.getCompleted().size());
    assertEquals(3, recovered.getRowsWritten());
    assertEquals(6, rows(table()).size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), recovered.getCommit().get()));
    for (Row row : rows(table()).values()) {
      assertArrayEquals(ScriptedHttpStub.vectorFor(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK), DIM), vector(row));
    }
  }

}
