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

package org.apache.hudi.utilities.streamer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.utilities.config.EmbeddingTransformerConfig;
import org.apache.hudi.utilities.config.UnstructuredFileSourceConfig;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamer;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerTestBase;
import org.apache.hudi.utilities.sources.UnstructuredFileDFSSource;
import org.apache.hudi.utilities.transform.embedding.EmbeddingTransformer;

import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Streamer-level round trip for the unstructured file source on a PARQUET COW table:
 * one sync ingesting a small (INLINE) and a large (OUT_OF_LINE) file, blob struct and
 * parse columns verified through the Hudi read path, then a second sync after modifying
 * the small file, verifying upsert-by-path keeps the table current with the source dir.
 */
public class TestUnstructuredIngestE2E extends HoodieDeltaStreamerTestBase {

  private static final String PROPS_FILE = "test-unstructured-source.properties";
  private static final int INLINE_THRESHOLD = 1024;
  private static final int DIM = 2;
  private static final Pattern INPUT_PATTERN = Pattern.compile("\"input\":\\[(.*?)\\]");

  private static HttpServer embeddingStub;

  @BeforeAll
  public static void startEmbeddingStub() throws IOException {
    embeddingStub = HttpServer.create(new InetSocketAddress(0), 0);
    embeddingStub.createContext("/v1/embeddings", exchange -> {
      java.io.ByteArrayOutputStream buffer = new java.io.ByteArrayOutputStream();
      byte[] chunk = new byte[4096];
      int n;
      while ((n = exchange.getRequestBody().read(chunk)) > 0) {
        buffer.write(chunk, 0, n);
      }
      String body = new String(buffer.toByteArray(), StandardCharsets.UTF_8);
      Matcher matcher = INPUT_PATTERN.matcher(body);
      StringBuilder data = new StringBuilder("{\"data\":[");
      if (matcher.find()) {
        String[] inputs = matcher.group(1).split("\",\"");
        for (int i = 0; i < inputs.length; i++) {
          // deterministic: first component = input text length
          data.append(i > 0 ? "," : "").append("{\"index\":").append(i)
              .append(",\"embedding\":[").append(inputs[i].replaceAll("^\"|\"$", "").length())
              .append(".0,0.5]}");
        }
      }
      data.append("]}");
      byte[] response = data.toString().getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(200, response.length);
      try (OutputStream out = exchange.getResponseBody()) {
        out.write(response);
      }
    });
    embeddingStub.start();
  }

  @AfterAll
  public static void stopEmbeddingStub() {
    if (embeddingStub != null) {
      embeddingStub.stop(0);
    }
  }

  private void writeSourceFile(String dir, String name, byte[] content) throws IOException {
    try (FSDataOutputStream out = fs.create(new Path(dir, name), true)) {
      out.write(content);
    }
  }

  @Test
  public void testIngestAndUpsertBlobTable() throws Exception {
    String sourceRoot = basePath + "/unstructured-input";
    String tableBasePath = basePath + "/unstructured_table";
    writeSourceFile(sourceRoot, "small.txt", "hudi lakehouse smoke document".getBytes(StandardCharsets.UTF_8));
    byte[] big = new byte[INLINE_THRESHOLD * 4];
    Arrays.fill(big, (byte) 'b');
    writeSourceFile(sourceRoot, "big.txt", big);

    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.streamer.source.dfs.root", sourceRoot);
    props.setProperty(UnstructuredFileSourceConfig.BLOB_INLINE_MAX_BYTES.key(), String.valueOf(INLINE_THRESHOLD));
    props.setProperty("hoodie.datasource.write.recordkey.field", "path");
    props.setProperty("hoodie.datasource.write.partitionpath.field", "extension");
    props.setProperty(EmbeddingTransformerConfig.ENDPOINT_URL.key(),
        "http://localhost:" + embeddingStub.getAddress().getPort() + "/v1/embeddings");
    props.setProperty(EmbeddingTransformerConfig.MODEL.key(), "stub-model");
    props.setProperty(EmbeddingTransformerConfig.DIMENSION.key(), String.valueOf(DIM));
    Helpers.savePropsToDFS(props, storage, basePath + "/" + PROPS_FILE);

    HoodieDeltaStreamer.Config cfg = TestHelpers.makeConfig(tableBasePath, WriteOperationType.UPSERT,
        UnstructuredFileDFSSource.class.getName(),
        Collections.singletonList(EmbeddingTransformer.class.getName()), PROPS_FILE, false, false,
        100_000_000, false, null, "COPY_ON_WRITE", "modification_time", null);
    new HoodieDeltaStreamer(cfg, jsc).sync();

    Dataset<Row> table = sparkSession.read().format("hudi").load(tableBasePath);
    List<Row> rows = table.collectAsList();
    assertEquals(2, rows.size());

    Row small = rows.stream().filter(r -> r.getString(table.schema().fieldIndex("file_name"))
        .equals("small.txt")).findFirst().get();
    Row smallBlob = small.getStruct(table.schema().fieldIndex("content"));
    assertEquals(HoodieSchema.Blob.INLINE, smallBlob.getString(0));
    assertEquals("hudi lakehouse smoke document",
        new String((byte[]) smallBlob.get(1), StandardCharsets.UTF_8));
    assertEquals("SUCCESS", small.getString(table.schema().fieldIndex("parse_status")));
    assertTrue(small.getString(table.schema().fieldIndex("extracted_text")).contains("smoke"));

    Row bigRow = rows.stream().filter(r -> r.getString(table.schema().fieldIndex("file_name"))
        .equals("big.txt")).findFirst().get();
    Row bigBlob = bigRow.getStruct(table.schema().fieldIndex("content"));
    assertEquals(HoodieSchema.Blob.OUT_OF_LINE, bigBlob.getString(0));
    assertNull(bigBlob.get(1));
    assertTrue(bigBlob.getStruct(2).getString(0).endsWith("big.txt"));

    // embeddings populated by the transformer via the stub API (value = text length)
    int embeddingIndex = table.schema().fieldIndex("embedding");
    List<Float> smallVector = small.getList(embeddingIndex);
    assertEquals(DIM, smallVector.size());
    assertEquals((float) "hudi lakehouse smoke document".length(), smallVector.get(0));

    // the committed table schema carries the vector logical type end to end
    HoodieTableMetaClient metaClient = HoodieTestUtils.createMetaClient(storage, tableBasePath);
    Schema tableSchema = new TableSchemaResolver(metaClient).getTableSchema(false).toAvroSchema();
    Schema embeddingField = tableSchema.getField("embedding").schema();
    Schema embeddingType = embeddingField.getType() == Schema.Type.UNION
        ? embeddingField.getTypes().stream().filter(t -> t.getType() != Schema.Type.NULL).findFirst().get()
        : embeddingField;
    assertNotNull(embeddingType.getLogicalType(), "embedding column lost the vector logical type");
    assertTrue(embeddingType.getLogicalType().getName().toLowerCase().contains("vector"));

    // Second sync after the source file changes: upsert-by-path refreshes the row in place.
    writeSourceFile(sourceRoot, "small.txt",
        "hudi lakehouse refreshed document".getBytes(StandardCharsets.UTF_8));
    new HoodieDeltaStreamer(cfg, jsc).sync();

    Dataset<Row> refreshed = sparkSession.read().format("hudi").load(tableBasePath);
    assertEquals(2, refreshed.count());
    Row refreshedSmall = refreshed.collectAsList().stream()
        .filter(r -> r.getString(refreshed.schema().fieldIndex("file_name")).equals("small.txt"))
        .findFirst().get();
    assertTrue(refreshedSmall.getString(refreshed.schema().fieldIndex("extracted_text"))
        .contains("refreshed"));
    // the embedding refreshed with the text (stub vector tracks text length)
    assertEquals((float) "hudi lakehouse refreshed document".length(),
        refreshedSmall.<Float>getList(refreshed.schema().fieldIndex("embedding")).get(0));
  }
}
