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

package org.apache.hudi.utilities.unstructured.embedding.compute.vertex;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.utilities.unstructured.embedding.ComputeRoundCounters;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingStreamers.Round;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.RestProviderRoundTestBase;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Token-refresh writer assertions on the single writer over the unstructured data source on S3 and the stock checkpoint. */
@Tag("functional")
class TestVertexTokenRefreshSync extends RestProviderRoundTestBase {

  private static final int DIM = 128;
  private static final int TOKEN_REQUESTS = 2;
  private VertexStubServer stub;
  private GoogleTokenStubServer tokens;
  @TempDir
  Path secrets;
  private Path tokenFile;

  @BeforeEach
  void startEndpoints() throws Exception {
    stub = new VertexStubServer();
    tokens = new GoogleTokenStubServer();
    tokenFile = secrets.resolve("token");
    Files.writeString(tokenFile, "fake-file-token-1");
  }

  @AfterEach
  void stopEndpoints() {
    stub.close();
    tokens.close();
  }

  private TypedProperties providerProps() {
    TypedProperties props = providerProps(VertexMultimodalEmbeddingClient.PROVIDER, "multimodalembedding@001", DIM, stub.endpoint());
    props.setProperty(PREFIX + VertexClientConfig.CREDENTIAL_FILE_SUFFIX, tokenFile.toString());
    return props;
  }

  @ParameterizedTest
  @ValueSource(strings = {"token", "google"})
  void syncLongerThanTheTokenLifetimeCompletes(String source) throws Exception {
    TypedProperties props = providerProps();
    AtomicInteger generation = new AtomicInteger(1);
    if ("google".equals(source)) {
      props.setProperty(PREFIX + VertexClientConfig.CREDENTIAL_SOURCE_SUFFIX, "google");
      props.setProperty(PREFIX + VertexClientConfig.CREDENTIAL_FILE_SUFFIX,
          tokens.writeServiceAccountKey(secrets.resolve("sa.json")).toString());
      stub.expireTokensAfter(TOKEN_REQUESTS, expired -> { });
    } else {
      stub.expireTokensAfter(TOKEN_REQUESTS, expired -> writeToken("fake-file-token-" + generation.incrementAndGet()));
    }
    announce(pdf("one"), pdf("two"), pdf("three"));
    ComputeRoundCounters.Counted report = ComputeRoundCounters.round(spark(), props);
    assertEquals(9, report.round.getRowsWritten());
    assertEquals(9, report.counters.rowsEmbedded());
    assertEquals(0, report.counters.rowsRetried() + report.counters.rowsPermanent());
    assertTrue(report.round.getErrors().isEmpty());
    assertEquals(1, report.round.getCompleted().size());
    assertEquals(latestEventsCheckpoint(), EmbeddingStreamers.checkpoint(table(), storage(), report.round.getCommit().get()));
    assertEquals(9, rows(table()).size());
    assertVectors();
    assertEquals(9, stub.requests().stream().filter(request -> request.status == 200).count());
    assertTrue(stub.requests().stream().anyMatch(request -> request.status == 401));
    assertTrue(stub.requests().stream().map(request -> request.authorization).distinct().count() >= 5);
    if ("google".equals(source)) {
      assertTrue(tokens.issued() >= 5);
    } else {
      assertTrue(generation.get() >= 5);
    }
  }

  @Test
  void rejectedRefreshFailsTheRound() throws Exception {
    TypedProperties props = providerProps();
    props.setProperty(PREFIX + VertexClientConfig.CREDENTIAL_SOURCE_SUFFIX, "google");
    props.setProperty(PREFIX + VertexClientConfig.CREDENTIAL_FILE_SUFFIX,
        tokens.writeServiceAccountKey(secrets.resolve("sa.json")).toString());
    announce(pdf("baseline"));
    Round first = round(props);
    List<HoodieInstant> before = EmbeddingStreamers.commits(table(), storage());
    String checkpoint = EmbeddingStreamers.checkpoint(table(), storage(), first.getCommit().get());
    stub.expireTokensAfter(TOKEN_REQUESTS, expired -> tokens.enqueue(400, "{\"error\":\"invalid_grant\"}"));
    announce(pdf("one"), pdf("two"), pdf("three"));
    Exception failure = assertThrows(Exception.class, () -> round(props));
    String causes = EmbeddingStreamers.causes(failure);
    assertTrue(causes.contains("Cannot refresh the Vertex access token"), causes);
    assertTrue(causes.contains("EmbeddingClientConfigException"), causes);
    assertEquals(before, EmbeddingStreamers.commits(table(), storage()));
    assertEquals(checkpoint, EmbeddingStreamers.checkpoint(table(), storage(), before.get(before.size() - 1)));
    assertEquals(3, rows(table()).size());
    assertTrue(EmbeddingStreamers.payloads(EmbeddingStreamers.errorTablePath(props)).isEmpty());
    stub.expireTokensAfter(0, expired -> { });
    Round recovered = round(props);
    assertEquals(1, recovered.getCompleted().size());
    assertEquals(9, recovered.getRowsWritten());
    assertEquals(12, rows(table()).size());
    assertVectors();
  }

  private void assertVectors() throws Exception {
    JsonNode recorded = VertexStubServer.MAPPER.readTree(VertexStubServer.resource("predict-text-response.json"))
        .path("predictions").path(0).path("textEmbedding");
    float[] expected = new float[DIM];
    for (int i = 0; i < DIM; i++) {
      expected[i] = (float) recorded.get(i).asDouble();
    }
    for (Row row : rows(table()).values()) {
      assertNotNull(row.getAs(EmbeddingTableSchema.PREPARED_TEXT_CHUNK));
      assertNotNull(row.getAs(EmbeddingTableSchema.AUX));
      assertArrayEquals(expected, vector(row));
    }
  }

  private void writeToken(String value) {
    try {
      Files.writeString(tokenFile, value);
    } catch (java.io.IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
