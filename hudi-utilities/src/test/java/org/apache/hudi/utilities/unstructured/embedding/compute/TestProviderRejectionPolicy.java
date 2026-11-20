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

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.util.LongAccumulator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider-level request rejection policy and pacing reach the micro-batch iterator without dropping prepared row values. */
class TestProviderRejectionPolicy {

  private static final String POLICY = "test.rejection.policy";
  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.preparedImageField())
      .add("emb_m", DataTypes.createArrayType(DataTypes.FloatType), true)
      .add(ComputeRound.ERROR_WORKING_COLUMN, DataTypes.StringType, true);

  public static class RejectionClient implements EmbeddingClient {
    private EmbeddingModelSpec model;
    private String policy;
    private int calls;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      model = spec;
      policy = props.getString(POLICY);
    }

    @Override
    public EmbeddingModelSpec spec() {
      return model;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      calls++;
      if ("throw".equals(policy)) {
        throw new EmbeddingClientConfigException("bad deployment");
      }
      if ("retry".equals(policy) && calls == 1) {
        throw new IllegalStateException("transient batch failure");
      }
      return inputs.stream().map(input -> "retry".equals(policy) || "ok".equals(input.getText())
          ? EmbeddingResult.ok(new float[] {1, 2, 3, 4}) : EmbeddingResult.permanent("provider rejected input"))
          .collect(Collectors.toList());
    }

    @Override
    public boolean answersEachInputAlone() {
      return "independent".equals(policy);
    }

    @Override
    public EmbeddingCallStats lastCallStats() {
      return new EmbeddingCallStats(1, 0, 0, -1, 7, 7);
    }
  }

  @Test
  void equalPermanentRejectionsAcrossFilesEndTheTaskWithARefusalMarker() {
    ComputeCounters counters = counters();
    List<Row> result = drain("uniform", counters, row("a", 0, "bad"), row("b", 0, "bad"));
    assertEquals(1, result.size());
    String failure = result.get(0).getString(8);
    assertTrue(failure.startsWith(ComputeRound.REFUSED_PREFIX));
    assertEquals(1, counters.getCallsRefused().value());
    assertTrue(failure.contains("EMBED-COMPUTE-REJECTED-AS-A-WHOLE"));
    assertTrue(failure.contains("source_path_sha=a"), failure);
    assertTrue(failure.contains("source_modification_time=101"), failure);
    assertTrue(failure.contains("source_file_size=17"), failure);
    assertTrue(failure.contains("modality=text"), failure);
    assertTrue(failure.contains("chunk_id=0"), failure);
  }

  @Test
  void equalPermanentRejectionsOfOneFileRemainRowErrors() {
    List<Row> rows = drain("uniform", counters(), row("a", 0, "bad"), row("a", 1, "bad"));
    assertEquals(2, rows.size());
    for (Row row : rows) {
      assertEquals("bad", row.getString(5));
      assertNull(row.get(7));
      assertEquals("provider rejected input", row.getString(8));
    }
  }

  @Test
  void anIndependentInputClientKeepsCrossFileRejectionsAsRowErrors() {
    List<Row> rows = drain("independent", counters(), row("a", 0, "bad"), row("b", 0, "bad"));
    assertEquals(2, rows.size());
    rows.forEach(row -> assertEquals("provider rejected input", row.getString(8)));
  }

  @Test
  void successfulNeighbourKeepsOneBadInputFromFailingTheRound() {
    List<Row> rows = drain("uniform", counters(), row("a", 0, "bad"), row("b", 0, "ok"));
    assertEquals("bad", rows.get(0).getString(5));
    assertNull(rows.get(0).get(7));
    assertEquals("provider rejected input", rows.get(0).getString(8));
    assertEquals("ok", rows.get(1).getString(5));
    assertNotNull(rows.get(1).get(7));
    assertNull(rows.get(1).get(8));
  }

  @Test
  void rateWaitIncludesThrowingBatchAndItsSuccessfulSingleInputRetries() {
    ComputeCounters counters = counters();
    List<Row> rows = drain("retry", counters, row("a", 0, "bad"), row("b", 0, "bad"));
    assertEquals(2, rows.size());
    rows.forEach(row -> assertNotNull(row.get(7)));
    assertEquals(21, counters.getRateLimitWaitMillis().value());
    assertEquals(2, counters.getRowsRetried().value());
  }

  @Test
  void rateWaitIncludesAConfigurationException() {
    ComputeCounters counters = counters();
    List<Row> result = drain("throw", counters, row("a", 0, "bad"));
    assertEquals(1, result.size());
    assertTrue(result.get(0).getString(8).startsWith(ComputeRound.REFUSED_PREFIX));
    assertEquals(1, counters.getCallsRefused().value());
    assertEquals(7, counters.getRateLimitWaitMillis().value());
  }

  private static Row row(String source, long chunk, String text) {
    return RowFactory.create(source, 101L, 17L, "text", chunk, text, null, null, null);
  }

  private static ComputeCounters counters() {
    return new ComputeCounters(new LongAccumulator(), new LongAccumulator(), new LongAccumulator(),
        new LongAccumulator(), new LongAccumulator());
  }

  private static List<Row> drain(String policy, ComputeCounters counters, Row... rows) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "4");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, RejectionClient.class.getName());
    props.setProperty(EmbeddingTableConfig.COMPUTE_RETRY_BACKOFF_MS.key(), "0");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "dummy");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m" + EmbeddingTableConfig.MODEL_MODALITIES_SUFFIX, "text");
    props.setProperty(POLICY, policy);
    EmbeddingModelSpec model = EmbeddingTableConfig.from(props).model("m");
    List<Row> out = new ArrayList<>();
    try (MicroBatchEmbeddingIterator iterator = new MicroBatchEmbeddingIterator(Arrays.asList(rows).iterator(), model,
        props, SCHEMA, 8, counters)) {
      iterator.forEachRemaining(out::add);
    }
    return out;
  }
}
