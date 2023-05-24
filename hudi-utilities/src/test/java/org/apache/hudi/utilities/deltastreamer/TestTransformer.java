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

import org.apache.hudi.StructUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.HoodieSchemaException;
import org.apache.hudi.utilities.sources.ParquetDFSSource;
import org.apache.hudi.utilities.transform.Transformer;
import org.apache.hudi.utilities.transform.FlatteningTransformer;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestHelpers.assertRecordCount;
import static org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer.TestHelpers.makeConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestTransformer extends HoodieDeltaStreamerTestBase {

  @Test
  public void testMultipleTransformersWithIdentifiers() throws Exception {
    // Configure 3 transformers of same type. 2nd transformer has no suffix
    String[] arr = new String [] {
        "1:" + TimestampTransformer.class.getName(),
        "2:" + TimestampTransformer.class.getName(),
        "3:" + TimestampTransformer.class.getName()};
    List<String> transformerClassNames = Arrays.asList(arr);

    // Create source using TRIP_SCHEMA
    boolean useSchemaProvider = true;
    PARQUET_SOURCE_ROOT = basePath + "/parquetFilesDfs" + testNum;
    int parquetRecordsCount = 10;
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, true, "source.avsc", "source.avsc", PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "");
    String tableBasePath = basePath + "/testMultipleTransformersWithIdentifiers" + testNum;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(
        makeConfig(tableBasePath, WriteOperationType.INSERT, ParquetDFSSource.class.getName(),
            transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false,
            useSchemaProvider, 100000, false, null, null, "timestamp", null), jsc);

    // Set properties for multi transformer
    // timestamp.transformer.increment is a common config and varies between the transformers
    // timestamp.transformer.multiplier is also a common config but doesn't change between transformers
    Properties properties = ((HoodieDeltaStreamer.DeltaSyncService) deltaStreamer.getIngestionService()).getProps();
    // timestamp value initially is set to 0
    // timestamp = 0 * 2 + 10; (transformation 1)
    // timestamp = 10 * 2 + 20 = 40 (transformation 2)
    // timestamp = 40 * 2 + 30 = 110 (transformation 3)
    properties.setProperty("timestamp.transformer.increment.1", "10");
    properties.setProperty("timestamp.transformer.increment.3", "30");
    properties.setProperty("timestamp.transformer.increment", "20");
    properties.setProperty("timestamp.transformer.multiplier", "2");
    properties.setProperty("transformer.suffix", ".1,.2,.3");
    deltaStreamer.sync();

    assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    assertEquals(0, sqlContext.read().format("org.apache.hudi").load(tableBasePath).where("timestamp != 110").count());
  }

  @Test
  public void testTransformerSchemaValidationPasses() throws Exception {
    List<String> transformerClassNames = Arrays.asList(
        // Flattens the nested schema
        FlatteningTransformerWithTransformedSchema.class.getName(),
        // No change to schema
        TimestampTransformer.class.getName(),
        // Adds a new column named random in the schema
        AddColumnTransformerWithTransformedSchema.class.getName());
    runDeltaStreamerWithTransformerSchemaValidation(transformerClassNames);
  }

  @Test
  public void testTransformerSchemaValidationPassesWithDefaultTransformedSchema() throws Exception {
    List<String> transformerClassNames = Arrays.asList(
        // Flattens the nested schema
        FlatteningTransformer.class.getName(),
        // No change to schema
        TimestampTransformer.class.getName(),
        // Adds a new column named random in the schema
        AddColumnTransformer.class.getName());
    runDeltaStreamerWithTransformerSchemaValidation(transformerClassNames);
  }

  @Test
  public void testTransformerSchemaValidationFailsWithInvalidTransformer() {
    String expectedErrorMsg = "Invalid transformer org.apache.hudi.utilities.deltastreamer.TestTransformer$InvalidAddColumnTransformer";
    List<String> transformerClassNames = Arrays.asList(
        FlatteningTransformerWithTransformedSchema.class.getName(),
        TimestampTransformer.class.getName(),
        // InvalidAddColumnTransformer uses a non_existent column in the transformation
        // The transformedSchema API adds field random1 whereas transformation adds field random
        InvalidAddColumnTransformer.class.getName());
    Throwable t = testTransformerSchemaValidationFails(transformerClassNames, expectedErrorMsg);
    assertTrue(Arrays.stream(t.getCause().getStackTrace())
        .anyMatch(ste -> ste.toString().contains("org.apache.hudi.utilities.transform.Transformer.transformedSchema")));
    assertTrue(t.getMessage().contains("Missing Input Columns: {'non_existent}"), t.getMessage());
    assertTrue(t.getMessage().contains("New Columns: ['random]"), t.getMessage());
  }

  @Test
  public void testTransformerSchemaValidationFailsWithSchemaMismatch() {
    String expectedErrorMsg = "Schema of transformed data does not match expected schema for transformer org.apache.hudi.utilities.deltastreamer.TestTransformer"
        + "$AddColumnTransformerWithWrongTransformedSchema";
    List<String> transformerClassNames = Collections.singletonList(
        AddColumnTransformerWithWrongTransformedSchema.class.getName());
    testTransformerSchemaValidationFails(transformerClassNames, "source.avsc", expectedErrorMsg);

    transformerClassNames = Arrays.asList(
        FlatteningTransformerWithTransformedSchema.class.getName(),
        TimestampTransformer.class.getName(),
        // AddColumnTransformerWithWrongTransformedSchema provides a wrong transformedSchema.
        // The transformedSchema API adds field random1 whereas transformation adds field random
        AddColumnTransformerWithWrongTransformedSchema.class.getName());
    testTransformerSchemaValidationFails(transformerClassNames, expectedErrorMsg);
  }

  private Throwable testTransformerSchemaValidationFails(List<String> transformerClasses, Option<String> targetSchemaFile,
                                                    String expectedErrorMsg, Class<? extends Throwable> errorClass) {
    Throwable t = assertThrows(errorClass, () -> runDeltaStreamerWithTransformerSchemaValidation(transformerClasses,
        targetSchemaFile.orElse("target-flattened-addcolumn-transformer.avsc")));
    assertTrue(t.getMessage().contains(expectedErrorMsg), "Expected error \n" + expectedErrorMsg + "\nbut got\n" + t.getMessage());
    return t;
  }

  private Throwable testTransformerSchemaValidationFails(List<String> transformerClasses, String expectedErrorMsg) {
    return testTransformerSchemaValidationFails(transformerClasses, Option.empty(), expectedErrorMsg, HoodieSchemaException.class);
  }

  private Throwable testTransformerSchemaValidationFails(List<String> transformerClasses, String targetSchemaFile, String expectedErrorMsg) {
    return testTransformerSchemaValidationFails(transformerClasses, Option.of(targetSchemaFile), expectedErrorMsg, HoodieSchemaException.class);
  }

  private void runDeltaStreamerWithTransformerSchemaValidation(List<String> transformerClassNames) throws Exception {
    runDeltaStreamerWithTransformerSchemaValidation(transformerClassNames, "target-flattened-addcolumn-transformer.avsc");
  }

  private void runDeltaStreamerWithTransformerSchemaValidation(List<String> transformerClassNames, String targetSchemaFile) throws Exception {
    // Create source using TRIP_EXAMPLE_SCHEMA
    boolean useSchemaProvider = true;
    PARQUET_SOURCE_ROOT = basePath + "parquetFilesDfs";
    FileIOUtils.deleteDirectory(new File(new URI(PARQUET_SOURCE_ROOT).getPath()));
    int parquetRecordsCount = 10;
    prepareParquetDFSFiles(parquetRecordsCount, PARQUET_SOURCE_ROOT, FIRST_PARQUET_FILE_NAME, false, null, null);
    prepareParquetDFSSource(useSchemaProvider, true, "source.avsc", targetSchemaFile, PROPS_FILENAME_TEST_PARQUET,
        PARQUET_SOURCE_ROOT, false, "partition_path", "");
    String tableBasePath = basePath + "testTransformerSchemaValidation";
    FileIOUtils.deleteDirectory(new File(new URI(tableBasePath).getPath()));
    HoodieDeltaStreamer.Config config = TestHoodieDeltaStreamer.TestHelpers.makeConfig(tableBasePath, WriteOperationType.INSERT,
        ParquetDFSSource.class.getName(), transformerClassNames, PROPS_FILENAME_TEST_PARQUET, false, useSchemaProvider,
        100000, false, null, null, "timestamp", null);
    config.enableTransformerSchemaValidation = true;
    HoodieDeltaStreamer deltaStreamer = new HoodieDeltaStreamer(config, jsc);
    Properties properties = ((HoodieDeltaStreamer.DeltaSyncService) deltaStreamer.getIngestionService()).getProps();
    properties.setProperty("timestamp.transformer.increment", "20");
    properties.setProperty("timestamp.transformer.multiplier", "2");

    deltaStreamer.sync();
    TestHoodieDeltaStreamer.TestHelpers.assertRecordCount(parquetRecordsCount, tableBasePath, sqlContext);
    FileIOUtils.deleteDirectory(new File(tableBasePath));
  }

  /**
   * Performs transformation on `timestamp` field.
   */
  public static class TimestampTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      String[] suffixes = Option.ofNullable((String) properties.get("transformer.suffix")).map(s -> s.split(",")).orElse(new String[0]);
      for (String suffix : suffixes) {
        // verify no configs with suffix are in properties
        properties.keySet().forEach(k -> assertFalse(((String) k).endsWith(suffix)));
      }
      int multiplier = Integer.parseInt((String) properties.get("timestamp.transformer.multiplier"));
      int increment = Integer.parseInt((String) properties.get("timestamp.transformer.increment"));
      return rowDataset.withColumn("timestamp", functions.col("timestamp").multiply(multiplier).plus(increment));
    }
  }

  /**
   * Provides a transformedSchema implementation for FlatteningTransformer.
   */
  public static class FlatteningTransformerWithTransformedSchema extends FlatteningTransformer {

    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
      String flattenedSelect = flattenSchema(incomingStruct, null);
      String[] cols = flattenedSelect.split(",");
      List<Pair<String, String>> replacements = new LinkedList<>();
      for (String col : cols) {
        String[] names = col.split(" as ");
        if (!names[0].equals(names[1])) {
          replacements.add(Pair.of(names[0], names[1]));
        }
      }

      List<StructField> incomingFields = Arrays.asList(incomingStruct.fields());
      List<StructField> transformedFields = new LinkedList<>(incomingFields);
      Set<StructField> fieldsToRemove = new HashSet<>();
      for (Pair<String, String> replacement : replacements) {
        String fieldToRemoveName = replacement.getKey().replaceAll("\\..*", "");
        StructField fieldToAdd = StructUtils.getField(incomingStruct, replacement.getKey()).get();
        StructField fieldToRemove = transformedFields.stream().filter(f -> f.name().equals(fieldToRemoveName)).findAny().get();
        fieldsToRemove.add(fieldToRemove);
        transformedFields.add(transformedFields.indexOf(fieldToRemove), new StructField(replacement.getKey().replaceAll("\\.", "_"),
            fieldToAdd.dataType(), fieldToAdd.nullable(), fieldToAdd.metadata()));
      }
      transformedFields.removeAll(fieldsToRemove);

      return new StructType(transformedFields.toArray(new StructField[0]));
    }
  }

  /**
   * Adds a new column named random in the dataset.
   */
  public static class AddColumnTransformer implements Transformer {

    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      return rowDataset.withColumn("random", functions.lit(5).multiply(functions.col("timestamp")));
    }
  }

  /**
   * Provides a transformedSchema implementation for AddColumnTransformer.
   */
  public static class AddColumnTransformerWithTransformedSchema extends AddColumnTransformer {

    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
      StructField newField = new StructField("random", DataTypes.LongType, true, Metadata.empty());
      return incomingStruct.add(newField);
    }
  }

  /**
   * Provides a wrong implementation for transformedSchema of AddColumnTransformer.
   */
  public static class AddColumnTransformerWithWrongTransformedSchema extends AddColumnTransformer {

    @Override
    public StructType transformedSchema(JavaSparkContext jsc, SparkSession sparkSession, StructType incomingStruct, TypedProperties properties) {
      StructField newField = new StructField("random1", DataTypes.LongType, true, Metadata.empty());
      return incomingStruct.add(newField);
    }
  }

  /**
   * Provides a wrong implementation for transformedSchema of AddColumnTransformer.
   */
  public static class InvalidAddColumnTransformer extends AddColumnTransformer {
    @Override
    public Dataset<Row> apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                              TypedProperties properties) {
      return rowDataset.withColumn("random", functions.lit(5).multiply(functions.col("non_existent")));
    }
  }

}
