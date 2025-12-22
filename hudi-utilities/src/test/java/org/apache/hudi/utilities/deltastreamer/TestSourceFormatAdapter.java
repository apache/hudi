/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.checkpoint.Checkpoint;
import org.apache.hudi.common.table.checkpoint.StreamerCheckpointV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.config.HoodieStreamerConfig;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.RowSource;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.streamer.SourceFormatAdapter;
import org.apache.hudi.utilities.testutils.SanitizationTestUtils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.hudi.testutils.HoodieClientTestUtils.getSparkConfForTest;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSourceFormatAdapter {

  protected static SparkSession spark;
  protected static JavaSparkContext jsc;
  private static final String DUMMY_CHECKPOINT = "dummy_checkpoint";
  private TestRowDataSource testRowDataSource;
  private TestJsonDataSource testJsonDataSource;

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .config(getSparkConfForTest(TestSourceFormatAdapter.class.getName()))
        .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  public static void shutdown() throws IOException {
    jsc.close();
    spark.close();
  }

  // Forces to initialize object before every test.
  @AfterEach
  public void teardown() {
    testRowDataSource = null;
    testJsonDataSource = null;
  }

  private void setupRowSource(Dataset<Row> ds, TypedProperties properties, SchemaProvider schemaProvider) {
    InputBatch<Dataset<Row>> batch = new InputBatch<>(Option.of(ds), DUMMY_CHECKPOINT, schemaProvider);
    testRowDataSource = new TestRowDataSource(properties, jsc, spark, schemaProvider, batch);
  }

  private void setupJsonSource(JavaRDD<String> ds, HoodieSchema schema) {
    SchemaProvider basicSchemaProvider = new BasicSchemaProvider(schema);
    InputBatch<JavaRDD<String>> batch = new InputBatch<>(Option.of(ds), DUMMY_CHECKPOINT, basicSchemaProvider);
    testJsonDataSource = new TestJsonDataSource(new TypedProperties(), jsc, spark, basicSchemaProvider, batch);
  }

  private InputBatch<Dataset<Row>> fetchRowData(JavaRDD<String> rdd, StructType unsanitizedSchema, SchemaProvider schemaProvider) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES.key(), true);
    typedProperties.put(HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.key(), "__");
    setupRowSource(spark.read().schema(unsanitizedSchema).json(rdd), typedProperties, schemaProvider);
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(testRowDataSource, Option.empty(), Option.of(typedProperties));
    return sourceFormatAdapter.fetchNewDataInRowFormat(Option.of(new StreamerCheckpointV2(DUMMY_CHECKPOINT)), 10L);
  }

  private InputBatch<Dataset<Row>> fetchJsonData(JavaRDD<String> rdd, StructType sanitizedSchema) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(HoodieStreamerConfig.SANITIZE_SCHEMA_FIELD_NAMES.key(), true);
    typedProperties.put(HoodieStreamerConfig.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK.key(), "__");
    setupJsonSource(rdd, HoodieSchema.fromAvroSchema(SchemaConverters.toAvroType(sanitizedSchema, false, "record", "")));
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(testJsonDataSource, Option.empty(), Option.of(typedProperties));
    return sourceFormatAdapter.fetchNewDataInRowFormat(Option.of(new StreamerCheckpointV2(DUMMY_CHECKPOINT)), 10L);
  }

  private void verifySanitization(InputBatch<Dataset<Row>> inputBatch, String sanitizedDataFile, StructType sanitizedSchema) {
    JavaRDD<String> expectedRDD = jsc.textFile(sanitizedDataFile);
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertEquals(2, ds.collectAsList().size());
    assertEquals(sanitizedSchema, ds.schema());
    if (inputBatch.getSchemaProvider() instanceof RowBasedSchemaProvider) {
      assertEquals(HoodieSchema.fromAvroSchema(AvroConversionUtils.convertStructTypeToAvroSchema(sanitizedSchema,
          "hoodie_source", "hoodie.source")), inputBatch.getSchemaProvider().getSourceHoodieSchema());
    }
    assertEquals(expectedRDD.collect(), ds.toJSON().collectAsList());
  }

  @ParameterizedTest
  @MethodSource("provideDataFiles")
  public void testRowSanitization(String unsanitizedDataFile, String sanitizedDataFile, StructType unsanitizedSchema, StructType sanitizedSchema) {
    JavaRDD<String> unsanitizedRDD = jsc.textFile(unsanitizedDataFile);
    SchemaProvider schemaProvider = InputBatch.NullSchemaProvider.getInstance();
    verifySanitization(fetchRowData(unsanitizedRDD, unsanitizedSchema, schemaProvider), sanitizedDataFile, sanitizedSchema);
    verifySanitization(fetchRowData(unsanitizedRDD, unsanitizedSchema, null), sanitizedDataFile, sanitizedSchema);
  }

  @ParameterizedTest
  @MethodSource("provideDataFiles")
  public void testJsonSanitization(String unsanitizedDataFile, String sanitizedDataFile, StructType unsanitizedSchema, StructType sanitizedSchema) {
    JavaRDD<String> unsanitizedRDD = jsc.textFile(unsanitizedDataFile);
    verifySanitization(fetchJsonData(unsanitizedRDD, sanitizedSchema), sanitizedDataFile, sanitizedSchema);
  }

  public static class TestRowDataSource extends RowSource {
    private final InputBatch<Dataset<Row>> batch;
    public TestRowDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                             SchemaProvider schemaProvider, InputBatch<Dataset<Row>> batch) {
      super(props, sparkContext, sparkSession, schemaProvider);
      this.batch = batch;
    }

    @Override
    protected Pair<Option<Dataset<Row>>, Checkpoint> fetchNextBatch(Option<Checkpoint> lastCkptStr, long sourceLimit) {
      return Pair.of(batch.getBatch(), batch.getCheckpointForNextBatch());
    }
  }

  public static class TestJsonDataSource extends Source<JavaRDD<String>> {
    private final InputBatch<JavaRDD<String>> batch;

    public TestJsonDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                             SchemaProvider schemaProvider, InputBatch<JavaRDD<String>> batch) {
      super(props, sparkContext, sparkSession, schemaProvider, SourceType.JSON);
      this.batch = batch;
    }

    @Override
    protected InputBatch<JavaRDD<String>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
      return batch;
    }
  }

  public static class BasicSchemaProvider extends SchemaProvider {

    private final HoodieSchema schema;

    public BasicSchemaProvider(HoodieSchema schema) {
      this(null, null, schema);
    }

    public BasicSchemaProvider(TypedProperties props, JavaSparkContext jssc, HoodieSchema schema) {
      super(props, jssc);
      this.schema = schema;
    }

    @Override
    public HoodieSchema getSourceHoodieSchema() {
      return schema;
    }
  }

  private static Stream<Arguments> provideDataFiles() {
    return SanitizationTestUtils.provideDataFiles();
  }

}
