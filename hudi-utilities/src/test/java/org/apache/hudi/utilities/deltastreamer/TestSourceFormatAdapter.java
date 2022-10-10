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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSourceFormatAdapter {
  private static final String DUMMY_CHECKPOINT = "dummy_checkpoint";

  private static SparkSession spark;
  private static JavaSparkContext jsc;
  private TestDataSource testDataSource;

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(TestSourceFormatAdapter.class.getName())
        .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  public static void shutdown() {
    jsc.close();
    spark.close();
  }

  // Forces to initialize object before every test.
  @AfterEach
  public void teardown() {
    testDataSource = null;
  }

  private String sanitizeIfNeeded(String src, boolean shouldSanitize) {
    return shouldSanitize ? HoodieAvroUtils.sanitizeName(src, "__") : src;
  }

  private StructType getSchemaWithProperNaming() {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField("state", DataTypes.StringType, true, Metadata.empty()),
        new StructField("street", DataTypes.StringType, true, Metadata.empty()),
        new StructField("zip", DataTypes.LongType, true, Metadata.empty()),
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField("address", addressStruct, true, Metadata.empty()),
        new StructField("name", DataTypes.StringType, true, Metadata.empty()),
        new StructField("occupation", DataTypes.StringType, true, Metadata.empty()),
        new StructField("place", DataTypes.StringType, true, Metadata.empty())
    });
    return personStruct;
  }

  private StructType getSchemaWithBadAvroNamingForStructType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@_addr*$ess", shouldSanitize),
            addressStruct, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("9name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("_occu9pation", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@plac.e.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty())
    });
    return personStruct;
  }

  private StructType getSchemaWithBadAvroNamingForArrayType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@arr@", shouldSanitize),
            new ArrayType(addressStruct, true), true, Metadata.empty())
    });
    return personStruct;
  }

  private StructType getSchemaWithBadAvroNamingForMapType(boolean shouldSanitize) {
    StructType addressStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@state.", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@@stree@t@", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("8@_zip", shouldSanitize),
            DataTypes.LongType, true, Metadata.empty())
    });

    StructType personStruct = new StructType(new StructField[] {
        new StructField(sanitizeIfNeeded("@name", shouldSanitize),
            DataTypes.StringType, true, Metadata.empty()),
        new StructField(sanitizeIfNeeded("@map9", shouldSanitize),
            new MapType(DataTypes.StringType, addressStruct, true), true, Metadata.empty()),
    });
    return personStruct;
  }

  private void setupSource(Dataset<Row> ds) {
    SchemaProvider nullSchemaProvider = new InputBatch.NullSchemaProvider();
    InputBatch<Dataset<Row>> batch = new InputBatch<>(Option.of(ds), DUMMY_CHECKPOINT, nullSchemaProvider);
    testDataSource = new TestDataSource(new TypedProperties(), jsc, spark, nullSchemaProvider, batch);
  }

  private InputBatch<Dataset<Row>> fetchData(Dataset<Row> inputDs) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(SourceFormatAdapter.SourceFormatAdapterConfig.SANITIZE_AVRO_FIELD_NAMES.key(), true);
    typedProperties.put(SourceFormatAdapter.SourceFormatAdapterConfig.AVRO_FIELD_NAME_INVALID_CHAR_MASK.key(), "__");
    setupSource(inputDs);
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(testDataSource, Option.empty(), Option.of(typedProperties));
    return sourceFormatAdapter.fetchNewDataInRowFormat(Option.of(DUMMY_CHECKPOINT), 10L);
  }

  @Test
  public void nestedTypeWithProperNaming() {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization.json");
    StructType inputSchema = getSchemaWithProperNaming();
    Dataset<Row> inputDs = spark.read().schema(inputSchema).json(rdd);
    InputBatch<Dataset<Row>> inputBatch = fetchData(inputDs);
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(inputSchema.equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @Test
  public void structTypeAndBadNaming() {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_in.json");
    StructType readSchema = getSchemaWithBadAvroNamingForStructType(false);
    Dataset<Row> inputDs = spark.read().schema(readSchema).json(rdd);
    InputBatch<Dataset<Row>> inputBatch = fetchData(inputDs);
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(getSchemaWithBadAvroNamingForStructType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @Test
  public void arrayTypeAndBadNaming() {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_array_in.json");
    StructType readSchema = getSchemaWithBadAvroNamingForArrayType(false);
    Dataset<Row> inputDs = spark.read().schema(readSchema).json(rdd);
    InputBatch<Dataset<Row>> inputBatch = fetchData(inputDs);
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(getSchemaWithBadAvroNamingForArrayType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_array_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @Test
  public void mapTypeAndBadNaming() {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_map_in.json");
    StructType readSchema = getSchemaWithBadAvroNamingForMapType(false);
    Dataset<Row> inputDs = spark.read().schema(readSchema).json(rdd);
    InputBatch<Dataset<Row>> inputBatch = fetchData(inputDs);
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(getSchemaWithBadAvroNamingForMapType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_map_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  public static class TestDataSource extends Source<Dataset<Row>> {
    private final InputBatch<Dataset<Row>> batch;

    public TestDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                          SchemaProvider schemaProvider, InputBatch<Dataset<Row>> batch) {
      super(props, sparkContext, sparkSession, schemaProvider, SourceType.ROW);
      this.batch = batch;
    }

    @Override
    protected InputBatch<Dataset<Row>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
      return batch;
    }
  }
}