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

package org.apache.hudi.utilities.transform.debezium;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.debezium.DebeziumConstants;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.config.DebeziumTransformerConfig;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.hudi.config.HoodieErrorTableConfig.ERROR_TABLE_ENABLED;
import static org.apache.hudi.utilities.streamer.BaseErrorTableWriter.ERROR_TABLE_CURRUPT_RECORD_COL_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests the database-agnostic flattening behavior of {@link AbstractDebeziumTransformer}.
 */
class TestAbstractDebeziumTransformer extends DebeziumTransformerTestBase {

  // One insert (op=c), one update (op=u), one delete (op=d). before/after differ so the test can
  // prove which image is selected. The delete carries values only in `before` (after is null).
  private static final String INSERT_EVENT =
      "{\"op\":\"c\",\"ts_ms\":1700000000500,"
          + "\"before\":null,"
          + "\"after\":{\"id\":1,\"name\":\"Alice\",\"email\":\"alice@x.com\"},"
          + "\"source\":{\"name\":\"pgdb\",\"ts_ms\":1700000000000,\"schema\":\"public\"}}";
  private static final String UPDATE_EVENT =
      "{\"op\":\"u\",\"ts_ms\":1700000001500,"
          + "\"before\":{\"id\":2,\"name\":\"Bob\",\"email\":\"old@x.com\"},"
          + "\"after\":{\"id\":2,\"name\":\"Bob\",\"email\":\"new@x.com\"},"
          + "\"source\":{\"name\":\"pgdb\",\"ts_ms\":1700000001000,\"schema\":\"public\"}}";
  private static final String DELETE_EVENT =
      "{\"op\":\"d\",\"ts_ms\":1700000002500,"
          + "\"before\":{\"id\":3,\"name\":\"Carol\",\"email\":\"carol@x.com\"},"
          + "\"after\":null,"
          + "\"source\":{\"name\":\"pgdb\",\"ts_ms\":1700000002000,\"schema\":\"public\"}}";

  @Test
  void testAfterUsedForInsertUpdate_beforeUsedForDelete() {
    Dataset<Row> input = jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT);
    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, input, new TypedProperties());

    List<String> columns = Arrays.asList(result.columns());
    assertTrue(columns.containsAll(Arrays.asList("id", "name", "email")), "data columns lifted to root");
    assertFalse(columns.contains(DebeziumConstants.INCOMING_AFTER_FIELD), "after dropped");
    assertFalse(columns.contains(DebeziumConstants.INCOMING_BEFORE_FIELD), "before dropped");

    List<Row> rows = result.orderBy("id").collectAsList();
    assertEquals(3, rows.size());

    // insert -> taken from `after`
    assertEquals("c", rows.get(0).getAs(DebeziumConstants.FLATTENED_OP_COL_NAME));
    assertEquals("Alice", rows.get(0).getAs("name"));
    assertEquals("alice@x.com", rows.get(0).getAs("email"));

    // update -> taken from `after` (the NEW email, not the before/old one)
    assertEquals("u", rows.get(1).getAs(DebeziumConstants.FLATTENED_OP_COL_NAME));
    assertEquals("new@x.com", rows.get(1).getAs("email"));

    // delete -> taken from `before` (after is null, but we keep the key/row)
    assertEquals("d", rows.get(2).getAs(DebeziumConstants.FLATTENED_OP_COL_NAME));
    assertEquals("Carol", rows.get(2).getAs("name"));
    assertEquals("carol@x.com", rows.get(2).getAs("email"));
  }

  @Test
  void testDefaultMetadataColumnsAtRootWhenFlat() {
    Dataset<Row> input = jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT);
    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, input, new TypedProperties());

    List<String> columns = Arrays.asList(result.columns());
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_OP_COL_NAME));
    assertTrue(columns.contains(DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME));
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_SHARD_NAME));
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_TS_COL_NAME));
    assertFalse(columns.contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "no nested struct when flat");

    // shard name comes from source.name
    assertEquals("pgdb", result.orderBy("id").collectAsList().get(0).getAs(DebeziumConstants.FLATTENED_SHARD_NAME));
  }

  @Test
  void testNestedModeGroupsMetadataButKeepsOpAndLsnAtRoot() {
    // Supply an LSN-named metadata column so the LSN-to-root branch is exercised.
    Column lsnColumn = new Column(DebeziumConstants.INCOMING_SOURCE_NAME_FIELD).alias(DebeziumConstants.FLATTENED_LSN_COL_NAME);
    TestableDebeziumTransformer transformer =
        new TestableDebeziumTransformer(Collections.singletonList(lsnColumn), Option.empty(), false);

    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "true");

    Dataset<Row> result = transformer.apply(jsc, spark, jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT), props);
    List<String> columns = Arrays.asList(result.columns());

    assertTrue(columns.contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD), "metadata grouped under a struct");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_OP_COL_NAME), "op stays at root");
    assertTrue(columns.contains(DebeziumConstants.FLATTENED_LSN_COL_NAME), "LSN stays at root");
    assertFalse(columns.contains(DebeziumConstants.FLATTENED_SHARD_NAME), "shard moved into the nested struct");

    Row metadata = result.first().getAs(DebeziumConstants.DEBEZIUM_METADATA_FIELD);
    List<String> nested = Arrays.asList(metadata.schema().fieldNames());
    assertTrue(nested.contains(DebeziumConstants.FLATTENED_SHARD_NAME));
    assertTrue(nested.contains(DebeziumConstants.FLATTENED_SCHEMA_NAME), "source.schema present -> nested schema col added");
    assertFalse(nested.contains(DebeziumConstants.FLATTENED_LSN_COL_NAME), "LSN is at root, not nested");
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testNestedDefaultIsUsedWhenPropertyAbsent(boolean subclassDefault) {
    TestableDebeziumTransformer transformer =
        new TestableDebeziumTransformer(Collections.emptyList(), Option.empty(), subclassDefault);

    // property NOT set -> subclass default decides
    Dataset<Row> result = transformer.apply(jsc, spark, jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT), new TypedProperties());
    assertEquals(subclassDefault, Arrays.asList(result.columns()).contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD));
  }

  @Test
  void testExplicitPropertyOverridesSubclassDefault() {
    // subclass defaults to nested=true, but an explicit false must win
    TestableDebeziumTransformer transformer =
        new TestableDebeziumTransformer(Collections.emptyList(), Option.empty(), true);
    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.ENABLE_NESTED_FIELDS.key(), "false");

    Dataset<Row> result = transformer.apply(jsc, spark, jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT), props);
    assertFalse(Arrays.asList(result.columns()).contains(DebeziumConstants.DEBEZIUM_METADATA_FIELD));
  }

  @Test
  void testPostProcessingHookIsApplied() {
    Function<Dataset<Row>, Dataset<Row>> addColumn =
        ds -> ds.withColumn("processed", org.apache.spark.sql.functions.lit(true));
    TestableDebeziumTransformer transformer =
        new TestableDebeziumTransformer(Collections.emptyList(), Option.of(addColumn), false);

    Dataset<Row> result = transformer.apply(jsc, spark, jsonToDataset(INSERT_EVENT, UPDATE_EVENT, DELETE_EVENT), new TypedProperties());
    assertTrue(Arrays.asList(result.columns()).contains("processed"));
    assertTrue(result.collectAsList().stream().allMatch(r -> Boolean.TRUE.equals(r.getAs("processed"))));
  }

  @Test
  void testEmptyDatasetIsReturnedUnchanged() {
    Dataset<Row> empty = spark.emptyDataFrame();
    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, empty, new TypedProperties());
    assertEquals(0, result.columns().length);
  }

  @Test
  void testSchemaAsNullableFalsePreservesNonNullableSourceFields() {
    // "id" is declared non-nullable in the source row schema; "name" is nullable.
    StructType rowDataSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.LongType, false, Metadata.empty()),
        DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty())));
    StructType sourceSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty()),
        DataTypes.createStructField("ts_ms", DataTypes.LongType, true, Metadata.empty())));
    StructType envelopeSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("op", DataTypes.StringType, true, Metadata.empty()),
        DataTypes.createStructField("ts_ms", DataTypes.LongType, true, Metadata.empty()),
        DataTypes.createStructField("before", rowDataSchema, true, Metadata.empty()),
        DataTypes.createStructField("after", rowDataSchema, true, Metadata.empty()),
        DataTypes.createStructField("source", sourceSchema, true, Metadata.empty())));

    Row afterRow = RowFactory.create(1L, "Alice");
    Row sourceRow = RowFactory.create("pgdb", 1700000000000L);
    Row envelopeRow = RowFactory.create("c", 1700000000500L, null, afterRow, sourceRow);
    Dataset<Row> input = spark.createDataFrame(Collections.singletonList(envelopeRow), envelopeSchema);

    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.SCHEMA_AS_NULLABLE.key(), "false");

    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, input, props);

    assertFalse(result.schema().apply("id").nullable(), "id was non-nullable in the source schema");
    assertTrue(result.schema().apply("name").nullable(), "name was nullable in the source schema");
    long id = result.first().<Long>getAs("id");
    assertEquals(1L, id);
    assertEquals("Alice", result.first().getAs("name"));
  }

  @Test
  void testSchemaAsNullableFalseMakesMetadataColumnsNullableEvenIfSourceDeclaredNonNullable() {
    // "op" is declared non-nullable at the envelope level, but it is a Debezium metadata column
    // (not a __data-derived business column), so it must NOT be treated as a "known non-nullable
    // source column" -- it should end up nullable in the output regardless of its declared
    // nullability in the raw envelope schema.
    StructType rowDataSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.LongType, false, Metadata.empty()),
        DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty())));
    StructType sourceSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("name", DataTypes.StringType, true, Metadata.empty()),
        DataTypes.createStructField("ts_ms", DataTypes.LongType, true, Metadata.empty())));
    StructType envelopeSchema = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("op", DataTypes.StringType, false, Metadata.empty()),
        DataTypes.createStructField("ts_ms", DataTypes.LongType, true, Metadata.empty()),
        DataTypes.createStructField("before", rowDataSchema, true, Metadata.empty()),
        DataTypes.createStructField("after", rowDataSchema, true, Metadata.empty()),
        DataTypes.createStructField("source", sourceSchema, true, Metadata.empty())));

    Row afterRow = RowFactory.create(1L, "Alice");
    Row sourceRow = RowFactory.create("pgdb", 1700000000000L);
    Row envelopeRow = RowFactory.create("c", 1700000000500L, null, afterRow, sourceRow);
    Dataset<Row> input = spark.createDataFrame(Collections.singletonList(envelopeRow), envelopeSchema);

    TypedProperties props = new TypedProperties();
    props.setProperty(DebeziumTransformerConfig.SCHEMA_AS_NULLABLE.key(), "false");

    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, input, props);

    assertTrue(result.schema().apply(DebeziumConstants.FLATTENED_OP_COL_NAME).nullable(),
        "op is a Debezium metadata column, not a known non-nullable source column, so it must be nullable");
    assertFalse(result.schema().apply("id").nullable(), "id remains non-nullable as a known source column");
  }

  @Test
  void testErrorTableEnabledAddsNullCorruptRecordWhenMissing() {
    // Include UPDATE_EVENT alongside so Spark's JSON inference sees a non-null `before`
    // example and types the column as a struct rather than (incorrectly) as a string.
    TypedProperties props = new TypedProperties();
    props.setProperty(ERROR_TABLE_ENABLED.key(), "true");

    Dataset<Row> result = new TestableDebeziumTransformer()
        .apply(jsc, spark, jsonToDataset(INSERT_EVENT, UPDATE_EVENT), props);

    assertTrue(Arrays.asList(result.columns()).contains(ERROR_TABLE_CURRUPT_RECORD_COL_NAME),
        "_corrupt_record column added when error table is enabled");
    Row insertRow = result.where(new Column(DebeziumConstants.FLATTENED_OP_COL_NAME).equalTo("c")).first();
    assertNull(insertRow.getAs(ERROR_TABLE_CURRUPT_RECORD_COL_NAME), "no corrupt value was present on input");
  }

  @Test
  void testErrorTableEnabledPreservesExistingCorruptRecordValue() {
    Dataset<Row> input = jsonToDataset(INSERT_EVENT, UPDATE_EVENT)
        .withColumn(ERROR_TABLE_CURRUPT_RECORD_COL_NAME, functions.lit("malformed-row"));
    TypedProperties props = new TypedProperties();
    props.setProperty(ERROR_TABLE_ENABLED.key(), "true");

    Dataset<Row> result = new TestableDebeziumTransformer().apply(jsc, spark, input, props);

    Row insertRow = result.where(new Column(DebeziumConstants.FLATTENED_OP_COL_NAME).equalTo("c")).first();
    assertEquals("malformed-row", insertRow.getAs(ERROR_TABLE_CURRUPT_RECORD_COL_NAME),
        "existing corrupt-record value is preserved, not overwritten");
  }

  /** Minimal concrete subclass to exercise the otherwise database-agnostic base. */
  private static class TestableDebeziumTransformer extends AbstractDebeziumTransformer {
    TestableDebeziumTransformer() {
      super(Collections.emptyList(), Option.empty());
    }

    TestableDebeziumTransformer(List<Column> typeSpecificMetadataColumns,
                                Option<Function<Dataset<Row>, Dataset<Row>>> postProcessingOption,
                                boolean nestedFieldsEnabledByDefault) {
      super(typeSpecificMetadataColumns, postProcessingOption, nestedFieldsEnabledByDefault);
    }
  }
}
