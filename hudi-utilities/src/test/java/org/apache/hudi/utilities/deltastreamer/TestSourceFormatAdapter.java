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
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.InputBatch;
import org.apache.hudi.utilities.sources.Source;
import org.apache.hudi.utilities.sources.helpers.SanitizationUtils;

import org.apache.avro.Schema;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSourceFormatAdapter {
  private static final String DUMMY_CHECKPOINT = "dummy_checkpoint";

  private static SparkSession spark;
  private static JavaSparkContext jsc;
  private TestRowDataSource testRowDataSource;
  private TestJsonDataSource testJsonDataSource;

  @BeforeAll
  public static void start() {
    spark = SparkSession
        .builder()
        .master("local[*]")
        .appName(TestSourceFormatAdapter.class.getName())
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
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
    testRowDataSource = null;
    testJsonDataSource = null;
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

  private void setupRowSource(Dataset<Row> ds) {
    SchemaProvider nullSchemaProvider = new InputBatch.NullSchemaProvider();
    InputBatch<Dataset<Row>> batch = new InputBatch<>(Option.of(ds), DUMMY_CHECKPOINT, nullSchemaProvider);
    testRowDataSource = new TestRowDataSource(new TypedProperties(), jsc, spark, nullSchemaProvider, batch);
  }

  private void setupJsonSource(JavaRDD<String> ds, Schema schema) {
    SchemaProvider basicSchemaProvider = new BasicSchemaProvider(schema);
    InputBatch<JavaRDD<String>> batch = new InputBatch<>(Option.of(ds), DUMMY_CHECKPOINT, basicSchemaProvider);
    testJsonDataSource = new TestJsonDataSource(new TypedProperties(), jsc, spark, basicSchemaProvider, batch);
  }

  private InputBatch<Dataset<Row>> fetchRowData(JavaRDD<String> rdd, StructType inputSchema) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(SanitizationUtils.Config.SANITIZE_SCHEMA_FIELD_NAMES, true);
    typedProperties.put(SanitizationUtils.Config.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK, "__");
    setupRowSource(spark.read().schema(inputSchema).json(rdd));
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(testRowDataSource, Option.empty(), Option.of(typedProperties));
    return sourceFormatAdapter.fetchNewDataInRowFormat(Option.of(DUMMY_CHECKPOINT), 10L);
  }

  private InputBatch<Dataset<Row>> fetchJsonData(JavaRDD<String> rdd, StructType inputSchema) {
    TypedProperties typedProperties = new TypedProperties();
    typedProperties.put(SanitizationUtils.Config.SANITIZE_SCHEMA_FIELD_NAMES, true);
    typedProperties.put(SanitizationUtils.Config.SCHEMA_FIELD_NAME_INVALID_CHAR_MASK, "__");
    setupJsonSource(rdd, SchemaConverters.toAvroType(inputSchema, false, "record", ""));
    SourceFormatAdapter sourceFormatAdapter = new SourceFormatAdapter(testJsonDataSource, Option.empty(), Option.of(typedProperties));
    return sourceFormatAdapter.fetchNewDataInRowFormat(Option.of(DUMMY_CHECKPOINT), 10L);
  }

  @ParameterizedTest
  @ValueSource(strings = {"row", "json"})
  public void nestedTypeWithProperNaming(String sourceType) {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization.json");
    StructType inputSchema = getSchemaWithProperNaming();
    InputBatch<Dataset<Row>> inputBatch;
    switch (sourceType) {
      case "row":
        inputBatch = fetchRowData(rdd, inputSchema);
        break;
      case "json":
        inputBatch = fetchJsonData(rdd, inputSchema);
        break;
      default:
        throw new HoodieException("Invalid test source");
    }
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(inputSchema.equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"row", "json"})
  public void structTypeAndBadNaming(String sourceType) {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_in.json");
    InputBatch<Dataset<Row>> inputBatch;
    switch (sourceType) {
      case "row":
        inputBatch = fetchRowData(rdd, getSchemaWithBadAvroNamingForStructType(false));
        break;
      case "json":
        inputBatch = fetchJsonData(rdd, getSchemaWithBadAvroNamingForStructType(true));
        break;
      default:
        throw new HoodieException("Invalid test source");
    }
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(getSchemaWithBadAvroNamingForStructType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"row", "json"})
  public void arrayTypeAndBadNaming(String sourceType) {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_array_in.json");
    InputBatch<Dataset<Row>> inputBatch;
    switch (sourceType) {
      case "row":
        inputBatch = fetchRowData(rdd, getSchemaWithBadAvroNamingForArrayType(false));
        break;
      case "json":
        inputBatch = fetchJsonData(rdd, getSchemaWithBadAvroNamingForArrayType(true));
        break;
      default:
        throw new HoodieException("Invalid test source");
    }
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    System.out.println(getSchemaWithBadAvroNamingForArrayType(true));
    System.out.println(ds.schema());
    assertTrue(getSchemaWithBadAvroNamingForArrayType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_array_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(strings = {"row", "json"})
  public void mapTypeAndBadNaming(String sourceType) {
    JavaRDD<String> rdd = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_map_in.json");
    InputBatch<Dataset<Row>> inputBatch;
    switch (sourceType) {
      case "row":
        inputBatch = fetchRowData(rdd, getSchemaWithBadAvroNamingForMapType(false));
        break;
      case "json":
        inputBatch = fetchJsonData(rdd, getSchemaWithBadAvroNamingForMapType(true));
        break;
      default:
        throw new HoodieException("Invalid test source");
    }
    assertTrue(inputBatch.getBatch().isPresent());
    Dataset<Row> ds = inputBatch.getBatch().get();
    assertTrue(ds.collectAsList().size() == 2);
    assertTrue(getSchemaWithBadAvroNamingForMapType(true).equals(ds.schema()));
    JavaRDD<String> expectedData = jsc.textFile("src/test/resources/data/avro_sanitization_bad_naming_nested_map_out.json");
    assertEquals(expectedData.collect(), ds.toJSON().collectAsList());
  }

  public static class TestRowDataSource extends Source<Dataset<Row>> {
    private final InputBatch<Dataset<Row>> batch;

    public TestRowDataSource(TypedProperties props, JavaSparkContext sparkContext, SparkSession sparkSession,
                             SchemaProvider schemaProvider, InputBatch<Dataset<Row>> batch) {
      super(props, sparkContext, sparkSession, schemaProvider, SourceType.ROW);
      this.batch = batch;
    }

    @Override
    protected InputBatch<Dataset<Row>> fetchNewData(Option<String> lastCkptStr, long sourceLimit) {
      return batch;
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

    private final Schema schema;

    public BasicSchemaProvider(Schema schema) {
      this(null, null, schema);
    }

    public BasicSchemaProvider(TypedProperties props, JavaSparkContext jssc, Schema schema) {
      super(props, jssc);
      this.schema = schema;
    }

    @Override
    public Schema getSourceSchema() {
      return schema;
    }
  }
}
