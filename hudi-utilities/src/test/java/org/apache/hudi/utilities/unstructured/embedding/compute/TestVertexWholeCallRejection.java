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

package org.apache.hudi.utilities.unstructured.embedding.compute;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;
import org.apache.hudi.utilities.unstructured.embedding.compute.vertex.VertexMultimodalEmbeddingClient;
import org.apache.hudi.utilities.unstructured.embedding.compute.vertex.VertexStubServer;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A real Vertex client must leave uniform cross-file rejections to the compute whole-call guard. */
class TestVertexWholeCallRejection {
  private static final String ERROR = "{\"error\":{\"code\":400,\"message\":\"invalid input\"}}";
  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.preparedImageField())
      .add("emb_mm", DataTypes.createArrayType(DataTypes.FloatType), true)
      .add(ComputeRound.ERROR_WORKING_COLUMN, DataTypes.StringType, true);
  @TempDir
  Path tempDir;

  @Test
  void uniformCrossFileRejectionsRefuseTheCall() throws Exception {
    try (VertexStubServer stub = new VertexStubServer()) {
      ComputeCounters counters = counters();
      List<Row> rows = rejected(stub, counters, row("a", 0), row("b", 0));
      assertEquals(1, rows.size(), "one refusal marker ends the task without quarantining the inputs");
      assertTrue(rows.get(0).getString(8).startsWith(ComputeRound.REFUSED_PREFIX + "EMBED-COMPUTE-REJECTED-AS-A-WHOLE"));
      assertEquals(1, counters.getCallsRefused().value());
      assertEquals(0, counters.getRowsPermanent().value());
      assertEquals(2, stub.requests().size());
    }
  }

  @Test
  void sameFileRejectionsKeepTheirInputOutcomes() throws Exception {
    try (VertexStubServer stub = new VertexStubServer()) {
      ComputeCounters counters = counters();
      List<Row> rows = rejected(stub, counters, row("a", 0), row("a", 1));
      assertEquals(2, rows.size());
      for (Row row : rows) {
        assertNull(row.get(7));
        assertEquals("Vertex rejected the input with HTTP 400: " + ERROR, row.getString(8));
      }
      assertEquals(0, counters.getCallsRefused().value());
      assertEquals(2, counters.getRowsPermanent().value());
    }
  }

  private List<Row> rejected(VertexStubServer stub, ComputeCounters counters, Row... inputs) throws Exception {
    Path credential = tempDir.resolve("token");
    Files.write(credential, "recorded-test-token".getBytes(StandardCharsets.UTF_8));
    String prefix = EmbeddingTableConfig.MODEL_PREFIX + "mm";
    TypedProperties props = new TypedProperties();
    props.setProperty(prefix + ".endpoint", stub.endpoint());
    props.setProperty(prefix + ".credential.file", credential.toString());
    props.setProperty(EmbeddingTableConfig.CHUNK_MAX_TOKENS.key(), "32");
    EmbeddingModelSpec model = new EmbeddingModelSpec("mm", "emb_mm", 128, null,
        VertexMultimodalEmbeddingClient.PROVIDER, "multimodalembedding@001", Collections.singleton("text"));
    for (Row ignored : inputs) {
      stub.enqueueStatus(400, ERROR);
    }
    List<Row> rows = new ArrayList<>();
    try (MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(Arrays.asList(inputs).iterator(), model,
        props, SCHEMA, 8, counters)) {
      iterator.forEachRemaining(rows::add);
    }
    return rows;
  }

  private static Row row(String source, long chunk) {
    return RowFactory.create(source, 101L, 17L, "text", chunk, "a cat", null, null, null);
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
  }
}
