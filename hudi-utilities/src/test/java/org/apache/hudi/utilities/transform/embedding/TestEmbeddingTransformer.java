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

package org.apache.hudi.utilities.transform.embedding;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.utilities.config.EmbeddingTransformerConfig;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import com.sun.net.httpserver.HttpServer;
import org.apache.spark.SparkException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Embedding transformer against a stub OpenAI-compatible server: record-level
 * batching, deterministic vectors, null vectors for text-less rows, VECTOR
 * metadata on both apply() and transformedSchema(), 429-retry, and hard failure
 * on persistent errors.
 */
public class TestEmbeddingTransformer extends UtilitiesTestBase {

  private static final int DIM = 2;
  private static final Pattern INPUT_PATTERN = Pattern.compile("\"input\":\\[(.*?)\\]");

  private static HttpServer server;
  private static final AtomicInteger REQUEST_COUNT = new AtomicInteger();
  private static final AtomicInteger REMAINING_FAILURES = new AtomicInteger();
  private static volatile int failureStatus = 500;

  @BeforeAll
  public static void setupAll() throws Exception {
    initTestServices();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/v1/embeddings", exchange -> {
      REQUEST_COUNT.incrementAndGet();
      if (REMAINING_FAILURES.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        exchange.getResponseHeaders().add("Retry-After", "1");
        exchange.sendResponseHeaders(failureStatus, -1);
        exchange.close();
        return;
      }
      String body = new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8);
      // deterministic stub: vector = [textLength, 0.5] per input, in request order
      StringBuilder data = new StringBuilder("{\"data\":[");
      Matcher matcher = INPUT_PATTERN.matcher(body);
      assertTrue(matcher.find());
      String[] inputs = matcher.group(1).split("\",\"");
      for (int i = 0; i < inputs.length; i++) {
        String text = inputs[i].replaceAll("^\"|\"$", "");
        data.append(i > 0 ? "," : "")
            .append("{\"index\":").append(i)
            .append(",\"embedding\":[").append(text.length()).append(".0,0.5]}");
      }
      data.append("]}");
      byte[] response = data.toString().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response);
      }
    });
    server.start();
  }

  @AfterAll
  public static void teardownAll() {
    if (server != null) {
      server.stop(0);
    }
  }

  private static byte[] readAll(java.io.InputStream in) throws IOException {
    java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
    byte[] chunk = new byte[4096];
    int n;
    while ((n = in.read(chunk)) > 0) {
      buffer.write(chunk, 0, n);
    }
    return buffer.toByteArray();
  }

  private TypedProperties props(int batchSize) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTransformerConfig.ENDPOINT_URL.key(),
        "http://localhost:" + server.getAddress().getPort() + "/v1/embeddings");
    props.setProperty(EmbeddingTransformerConfig.MODEL.key(), "stub-model");
    props.setProperty(EmbeddingTransformerConfig.DIMENSION.key(), String.valueOf(DIM));
    props.setProperty(EmbeddingTransformerConfig.BATCH_SIZE.key(), String.valueOf(batchSize));
    return props;
  }

  private Dataset<Row> sourceDataset(String... texts) {
    StructType schema = new StructType(new StructField[] {
        DataTypes.createStructField("path", DataTypes.StringType, false),
        DataTypes.createStructField("extracted_text", DataTypes.StringType, true)});
    List<Row> rows = new java.util.ArrayList<>();
    for (int i = 0; i < texts.length; i++) {
      rows.add(RowFactory.create("file-" + i, texts[i]));
    }
    return sparkSession.createDataFrame(rows, schema);
  }

  @Test
  public void testBatchingVectorsAndNullForEmptyText() {
    REQUEST_COUNT.set(0);
    // 5 rows in one partition, 4 with text, batch size 2 -> rows buffer as
    // [2 texts][2 texts incl. the empty-text row][1 text] = 3 API calls
    Dataset<Row> input = sourceDataset("alpha", "bete", "", "gamma7", "epsilon90").coalesce(1);
    Dataset<Row> output = new EmbeddingTransformer().apply(jsc, sparkSession, input, props(2));

    List<Row> rows = output.collectAsList();
    assertEquals(5, rows.size());
    int vectorIndex = output.schema().fieldIndex("embedding");
    assertEquals(3, REQUEST_COUNT.get());

    for (Row row : rows) {
      String text = row.getString(1);
      if (text == null || text.isEmpty()) {
        assertNull(row.get(vectorIndex)); // text-less rows are never sent to the API
      } else {
        List<Float> vector = row.getList(vectorIndex);
        assertEquals(DIM, vector.size());
        assertEquals((float) text.length(), vector.get(0)); // deterministic stub value
      }
    }
  }

  @Test
  public void testVectorMetadataOnApplyAndTransformedSchema() {
    Dataset<Row> input = sourceDataset("one");
    EmbeddingTransformer transformer = new EmbeddingTransformer();

    StructField applied = new StructType(
        transformer.apply(jsc, sparkSession, input, props(16)).schema().fields())
        .apply("embedding");
    assertEquals("VECTOR(" + DIM + ")",
        applied.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));

    StructField declared = transformer
        .transformedSchema(jsc, sparkSession, input.schema(), props(16)).apply("embedding");
    assertEquals("VECTOR(" + DIM + ")",
        declared.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD));
  }

  @Test
  public void testRetryOn429ThenSuccessAndFailFastOnClientError() {
    // one 429 (with Retry-After) followed by success -> transform completes
    REMAINING_FAILURES.set(1);
    failureStatus = 429;
    Dataset<Row> ok = new EmbeddingTransformer()
        .apply(jsc, sparkSession, sourceDataset("retryable").coalesce(1), props(16));
    assertEquals(1, ok.collectAsList().size());

    // non-retriable client error -> the batch fails loudly and immediately
    // (no silent null vectors, no pointless backoff)
    REMAINING_FAILURES.set(Integer.MAX_VALUE);
    failureStatus = 400;
    Dataset<Row> failing = new EmbeddingTransformer()
        .apply(jsc, sparkSession, sourceDataset("doomed").coalesce(1), props(16));
    assertThrows(SparkException.class, failing::collectAsList);
    REMAINING_FAILURES.set(0);
  }
}
