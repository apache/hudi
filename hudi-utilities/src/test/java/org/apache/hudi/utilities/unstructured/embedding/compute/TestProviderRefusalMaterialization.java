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
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingModelSpec;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableConfig;
import org.apache.hudi.utilities.unstructured.embedding.EmbeddingTableSchema;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.BlobType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider refusals reach the driver after materialization and release the round's persisted output. */
@Tag("functional")
class TestProviderRefusalMaterialization extends SparkClientFunctionalTestHarness {
  private static final StructType SCHEMA = new StructType()
      .add(EmbeddingTableSchema.SOURCE_PATH_SHA, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.SOURCE_MODIFICATION_TIME, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.SOURCE_FILE_SIZE, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.MODALITY, DataTypes.StringType, false)
      .add(EmbeddingTableSchema.CHUNK_ID, DataTypes.LongType, false)
      .add(EmbeddingTableSchema.PREPARED_TEXT_CHUNK, DataTypes.StringType, true)
      .add(EmbeddingTableSchema.PREPARED_IMAGE, BlobType.dataType(), true)
      .add("emb_m1", DataTypes.createArrayType(DataTypes.FloatType, false), true);

  @Test
  void materializationRefusesOnTheDriverAndReleasesItsCache() {
    TypedProperties props = props(RefusingClient.class);
    ComputeCounters counters = ComputeCounters.register(jsc().sc());
    int before = spark().sparkContext().getPersistentRDDs().size();

    assertThrows(EmbeddingClientConfigException.class, () -> embed(props, counters));

    assertEquals(1L, counters.getCallsRefused().value());
    assertEquals(before, spark().sparkContext().getPersistentRDDs().size());
  }

  @Test
  void acceptedMaterializationKeepsEveryRowAndItsVector() {
    TypedProperties props = props(AcceptingClient.class);
    ComputeCounters counters = ComputeCounters.register(jsc().sc());
    Dataset<Row> output = embed(props, counters);
    try {
      List<Row> rows = output.orderBy(EmbeddingTableSchema.SOURCE_PATH_SHA).collectAsList();
      assertEquals(Arrays.asList("first", "second"), rows.stream()
          .map(row -> row.<String>getAs(EmbeddingTableSchema.SOURCE_PATH_SHA)).collect(Collectors.toList()));
      for (Row row : rows) {
        assertEquals(Arrays.asList(1.0f, 2.0f), row.getList(row.fieldIndex("emb_m1")));
      }
      assertEquals(2L, counters.getRowsEmbedded().value());
      assertEquals(0L, counters.getCallsRefused().value());
    } finally {
      output.unpersist(false);
    }
  }

  @Test
  void refusalMarkersAreExcludedFromRowErrorRouting() {
    StructType working = SCHEMA.add("working_error", DataTypes.StringType, true);
    Dataset<Row> embedded = spark().createDataFrame(Arrays.asList(
        RowFactory.create("refused", 1L, 2L, "text", 0L, "chunk", null, null, ComputeRound.REFUSED_PREFIX + "refused"),
        RowFactory.create("invalid", 1L, 2L, "text", 0L, "chunk", null, null, "invalid input")), working);
    StructType output = SCHEMA.add(ComputeRound.CORRUPT_RECORD, DataTypes.StringType, true);
    EmbeddingModelSpec model = EmbeddingTableConfig.from(props(AcceptingClient.class)).activeModel();
    List<Row> errors = ComputeRound.errorRows(embedded, output, model).collectAsList();
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).<String>getAs(ComputeRound.CORRUPT_RECORD).contains("invalid input"));
  }

  @ParameterizedTest
  @ValueSource(strings = {ComputeRound.REFUSED_PREFIX + "ordinary row error", "\0ordinary row error"})
  void arbitraryRowMessagesRemainErrorEvents(String message) {
    TypedProperties properties = props(RowErrorClient.class);
    Dataset<Row> input = spark().createDataFrame(Arrays.asList(
        RowFactory.create("first", 1L, 2L, "text", 0L, message, null, null)), SCHEMA);
    EmbeddingModelSpec model = EmbeddingTableConfig.from(properties).activeModel();
    ComputeCounters counters = ComputeCounters.register(jsc().sc());
    Dataset<Row> embedded = ComputeRound.embed(spark(), input.javaRDD(), SCHEMA, model, properties,
        new ComputeRound.Resources(null, counters, StorageLevel.MEMORY_AND_DISK()));
    try {
      List<Row> errors = ComputeRound.errorRows(embedded,
          SCHEMA.add(ComputeRound.CORRUPT_RECORD, DataTypes.StringType, true), model).collectAsList();
      assertEquals(1, errors.size());
      assertEquals(message, new com.fasterxml.jackson.databind.ObjectMapper()
          .readTree(errors.get(0).<String>getAs(ComputeRound.CORRUPT_RECORD)).path("error").asText());
      assertEquals(0, counters.getCallsRefused().value());
      assertEquals(1, counters.getRowsPermanent().value());
    } catch (java.io.IOException failure) {
      throw new AssertionError(failure);
    } finally {
      embedded.unpersist(false);
    }
  }

  public static class RowErrorClient extends AcceptingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      return java.util.Collections.singletonList(EmbeddingResult.permanent(inputs.get(0).getText()));
    }
  }

  private Dataset<Row> embed(TypedProperties props, ComputeCounters counters) {
    Dataset<Row> input = spark().createDataFrame(Arrays.asList(
        RowFactory.create("first", 1L, 2L, "text", 0L, "first chunk", null, null),
        RowFactory.create("second", 1L, 2L, "text", 0L, "second chunk", null, null)), SCHEMA).coalesce(1);
    EmbeddingModelSpec model = EmbeddingTableConfig.from(props).activeModel();
    return ComputeRound.embed(spark(), input.javaRDD(), SCHEMA, model, props,
        new ComputeRound.Resources(null, counters, StorageLevel.MEMORY_AND_DISK()));
  }

  private static TypedProperties props(Class<?> client) {
    TypedProperties props = new TypedProperties();
    props.setProperty(EmbeddingTableConfig.MODE.key(), EmbeddingTableConfig.MODE_PREP_AND_COMPUTE);
    props.setProperty(EmbeddingTableConfig.MODELS.key(), "m1");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_DIMENSIONS_SUFFIX, "2");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_PROVIDER_SUFFIX, "test");
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1" + EmbeddingTableConfig.MODEL_CLIENT_CLASS_SUFFIX, client.getName());
    props.setProperty(EmbeddingTableConfig.MODEL_PREFIX + "m1.modalities", "text");
    return props;
  }

  public static class AcceptingClient implements EmbeddingClient {
    private EmbeddingModelSpec model;

    @Override
    public void init(EmbeddingModelSpec spec, TypedProperties props) {
      model = spec;
    }

    @Override
    public EmbeddingModelSpec spec() {
      return model;
    }

    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      List<EmbeddingResult> results = new ArrayList<>();
      for (EmbeddingInput input : inputs) {
        results.add(EmbeddingResult.ok(new float[] {1.0f, 2.0f}));
      }
      return results;
    }
  }

  public static class RefusingClient extends AcceptingClient {
    @Override
    public List<EmbeddingResult> embed(List<EmbeddingInput> inputs) {
      throw new EmbeddingClientConfigException("configured service refuses this request");
    }
  }
}
