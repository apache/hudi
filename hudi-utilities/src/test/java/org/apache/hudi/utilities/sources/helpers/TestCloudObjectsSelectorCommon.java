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

package org.apache.hudi.utilities.sources.helpers;

import org.apache.hudi.HoodieSchemaConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.config.S3EventsHoodieIncrSourceConfig;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.RowBasedSchemaProvider;

import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestCloudObjectsSelectorCommon extends HoodieSparkClientTestHarness {

  @BeforeEach
  void setUp() {
    initSparkContexts();
  }

  @AfterEach
  public void teardown() throws Exception {
    cleanupResources();
  }

  @Test
  public void emptyMetadataReturnsEmptyOption() {
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(new TypedProperties());
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, Collections.emptyList(), "json", Option.empty(), 1);
    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void filesFromMetadataRead() {
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(new TypedProperties());
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void partitionValueAddedToRow(boolean includeSourcePathField) {
    String dataPath = "src/test/resources/data/partitioned/country=US/state=CA/data.json";
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataPath, 1));

    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    setIncludeSourcePathField(properties, includeSourcePathField);
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(includeSourcePathField, Collections.singletonList(dataPath), result.get(),
        new Object[]{"some data", "US", "CA"});
  }

  @Test
  public void loadDatasetWithSchema() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "CA");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  void loadDatasetWithSchemaAndAliasFields() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=TX/old_data.json", 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "TX");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void loadDatasetWithSchemaAndRepartition(boolean includeSourcePathField) {
    TypedProperties props = new TypedProperties();
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    setIncludeSourcePathField(props, includeSourcePathField);

    String dataPath1 = "src/test/resources/data/partitioned/country=US/state=CA/data.json";
    String dataPath2 = "src/test/resources/data/partitioned/country=US/state=TX/data.json";
    String dataPath3 = "src/test/resources/data/partitioned/country=IND/state=TS/data.json";

    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata(dataPath1, 1000),
        new CloudObjectMetadata(dataPath2, 1000),
        new CloudObjectMetadata(dataPath3, 1000));

    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);

    Assertions.assertTrue(result.isPresent());
    assertRowResult(
        includeSourcePathField,
        Arrays.asList(dataPath1, dataPath2, dataPath3),
        result.get(),
        new Object[]{"some data", "US", "CA"},
        new Object[]{"some data", "US", "TX"},
        new Object[]{"some data", "IND", "TS"});
  }

  @Test
  void loadDatasetWithSchemaAndCoalesceAliases() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "country,state");
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=TX/old_data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=IND/state=TS/data.json", 1000)
    );
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);
    Assertions.assertTrue(result.isPresent());
    List<Row> expected = Arrays.asList(RowFactory.create("some data", "US", "CA"), RowFactory.create("some data", "US", "TX"), RowFactory.create("some data", "IND", "TS"));
    List<Row> actual = result.get().collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
  }

  @Test
  void loadDatasetWithNestedSchemaAndCoalesceAliases() throws IOException {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/nested_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/nested_data_schema.avsc").getPath();
    props.put("hoodie.deltastreamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.deltastreamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    props.put("hoodie.streamer.source.cloud.data.reader.coalesce.aliases", "true");
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata("src/test/resources/data/nested_data_1.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/nested_data_2.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/nested_data_3.json", 1000)
    );
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30);
    Assertions.assertTrue(result.isPresent());
    Row address1 = RowFactory.create("123 Main St", "Springfield", "12345", RowFactory.create("India", "IN"));
    Row person1 = RowFactory.create("John", "Doe", RowFactory.create(1990, 5, 15), address1);
    Row address2 = RowFactory.create("456 Elm St", "Shelbyville", "67890", RowFactory.create("Spain", "SPN"));
    Row person2 = RowFactory.create("Jane", "Smith", RowFactory.create(1992, 9, 2), address2);
    Row address3 = RowFactory.create("789 Maple Ave", "Paris", "98765", RowFactory.create("France", "FRA"));
    Row person3 = RowFactory.create("John", "James", RowFactory.create(1985, 6, 15), address3);
    List<Row> expected = Arrays.asList(person1, person2, person3);
    List<Row> actual = result.get().collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
    HoodieSchema schema = HoodieSchema.parse(new FileInputStream(schemaFilePath));
    StructType expectedSchema = HoodieSchemaConversionUtils.convertHoodieSchemaToStructType(schema);
    // assert final output schema matches with the source schema
    Assertions.assertEquals(expectedSchema, result.get().schema(), "output dataset schema should match source schema");
  }

  @Test
  void parquetMixedSchemasMergedByDefault(@TempDir Path tempDir) {
    String p1 = tempDir.resolve("part1").toString();
    String p2 = tempDir.resolve("part2").toString();

    StructType schema1 = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, true),
        DataTypes.createStructField("b", DataTypes.StringType, true)));
    sparkSession.createDataFrame(Collections.singletonList(RowFactory.create(1, "x")), schema1)
        .write().parquet(p1);

    StructType schema2 = DataTypes.createStructType(Arrays.asList(
        DataTypes.createStructField("id", DataTypes.IntegerType, true),
        DataTypes.createStructField("c", DataTypes.IntegerType, true)));
    sparkSession.createDataFrame(Collections.singletonList(RowFactory.create(1, 99)), schema2)
        .write().parquet(p2);

    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(new TypedProperties());
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata(p1, 1L),
        new CloudObjectMetadata(p2, 1L));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "parquet", Option.empty(), 1);
    assertTrue(result.isPresent());
    Dataset<Row> ds = result.get();
    Assertions.assertEquals(2, ds.count());
    Set<String> colNames = Arrays.stream(ds.schema().fields()).map(StructField::name).collect(Collectors.toSet());
    assertTrue(colNames.contains("b"));
    assertTrue(colNames.contains("c"));
  }

  /**
   * Verifies that the format-gating predicate for the cloud-incremental mergeSchema option recognises
   * Parquet and ORC and rejects everything else. End-to-end ORC ingestion is not exercised here because
   * {@code hudi-utilities} pulls in {@code orc-core-nohive} while Spark 3.x's ORC writer expects the
   * regular {@code orc-core}; that classpath conflict makes {@code sparkSession.write().orc(...)} fail
   * with {@code NoSuchFieldError: type} in this module's tests. The end-to-end behaviour for ORC is
   * covered by Parquet's tests via the shared helper, plus this predicate test for the format dispatch.
   */
  @Test
  void isParquetOrOrcFileFormatRecognisesBothFormats() {
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("parquet"));
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("PARQUET"));
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("orc"));
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("ORC"));
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat(" parquet "));
    assertTrue(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat(" orc "));
    assertFalse(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("json"));
    assertFalse(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("csv"));
    assertFalse(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat("avro"));
    assertFalse(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat(""));
    assertFalse(CloudObjectsSelectorCommon.isParquetOrOrcFileFormat(null));
  }

  @Test
  public void partitionKeyNotPresentInPath() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.deltastreamer.source.cloud.data.reader.comma.separated.path.format", "false");
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "unknown");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", null);
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  void sourcePathColumnIsUriEncodedAndOverwritesExistingColumn(@TempDir Path tempDir) throws IOException {
    // file name with a space: input_file_name() returns the percent-encoded URI, and the fixture already
    // carries a same-named column that must be overwritten rather than duplicated
    Path dataFile = tempDir.resolve("we ird.json");
    Files.write(dataFile, Collections.singletonList(
        "{\"data\": \"some data\", \"" + CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN + "\": \"existing/path\"}"));
    TypedProperties properties = new TypedProperties();
    setIncludeSourcePathField(properties, true);
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata(dataFile.toString(), 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1);

    Assertions.assertTrue(result.isPresent());
    String expectedPath = dataFile.toUri().toString();
    Assertions.assertTrue(expectedPath.contains("%20"), expectedPath);
    // JSON schema inference sorts the inferred fields by name, and overwriting a column keeps its position,
    // so the source path column stays first here instead of being appended
    Assertions.assertEquals(Arrays.asList(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN, "data"),
        Arrays.asList(result.get().schema().fieldNames()));
    Assertions.assertTrue(result.get().schema().apply(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN).nullable());
    // the streamer derives the writer schema from the row schema; a non-nullable field would be a required avro
    // field without a default and could not be added to an existing table
    HoodieSchemaField sourcePathField = HoodieSchemaConversionUtils.convertStructTypeToHoodieSchema(
            result.get().schema(), RowBasedSchemaProvider.HOODIE_RECORD_STRUCT_NAME, RowBasedSchemaProvider.HOODIE_RECORD_NAMESPACE)
        .getField(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN).get();
    Assertions.assertTrue(sourcePathField.isNullable());
    Assertions.assertTrue(sourcePathField.hasDefaultValue());
    Assertions.assertEquals(Collections.singletonList(RowFactory.create(expectedPath, "some data")), result.get().collectAsList());
  }

  @Test
  void s3ObjectMetadataDedupesAndFiltersMissingFilesWithExistsCheck() throws IOException {
    Path existingFile1 = Files.write(tempDir.resolve("file1.json"), Collections.singletonList("{}"));
    Path existingFile2 = Files.write(tempDir.resolve("file2.json"), Collections.singletonList("{}"));
    Path missingFile = tempDir.resolve("missing.json");
    // bucket "" + key = absolute path without its leading separator, so prefix + bucket + "/" + key is a file:// URL
    List<String> jsonRecords = Arrays.asList(
        s3EventJson(existingFile1, 100),
        s3EventJson(existingFile2, 200),
        s3EventJson(missingFile, 300),
        // duplicate event, dropped by distinct()
        s3EventJson(existingFile1, 100));
    Dataset<Row> cloudObjectMetadataDF = sparkSession.read().json(sparkSession.createDataset(jsonRecords, Encoders.STRING()));
    TypedProperties props = new TypedProperties();
    props.put(S3EventsHoodieIncrSourceConfig.S3_FS_PREFIX.key(), "file");
    props.put(CloudSourceConfig.EXISTS_CHECK_PARALLELISM.key(), "0");
    Assertions.assertThrows(IllegalArgumentException.class, () -> CloudObjectsSelectorCommon.getObjectMetadata(
        CloudObjectsSelectorCommon.Type.S3, jsc, cloudObjectMetadataDF, true, props));

    props.put(CloudSourceConfig.EXISTS_CHECK_PARALLELISM.key(), "4");
    List<CloudObjectMetadata> result = CloudObjectsSelectorCommon.getObjectMetadata(
        CloudObjectsSelectorCommon.Type.S3, jsc, cloudObjectMetadataDF, true, props);

    Map<String, Long> pathToSize = result.stream()
        .collect(Collectors.toMap(CloudObjectMetadata::getPath, CloudObjectMetadata::getSize));
    Map<String, Long> expected = new HashMap<>();
    expected.put(existingFile1.toUri().toString(), 100L);
    expected.put(existingFile2.toUri().toString(), 200L);
    Assertions.assertEquals(expected, pathToSize);
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8})
  void existsCheckDropsMissingFiles(int parallelism) throws Exception {
    Path existingFile1 = Files.write(tempDir.resolve("file1.json"), Collections.singletonList("{}"));
    Path existingFile2 = Files.write(tempDir.resolve("file2.json"), Collections.singletonList("{}"));
    Path missingFile = tempDir.resolve("missing.json");
    List<Row> rows = Arrays.asList(
        cloudEventRow(existingFile1, 100L),
        cloudEventRow(missingFile, 300L),
        cloudEventRow(existingFile2, 200L));

    List<CloudObjectMetadata> result = new ArrayList<>();
    CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition("file://", storageConf, true, parallelism)
        .call(rows.iterator()).forEachRemaining(result::add);

    // both branches must produce the same objects, in input order
    Assertions.assertEquals(
        Arrays.asList(existingFile1.toUri().toString(), existingFile2.toUri().toString()),
        result.stream().map(CloudObjectMetadata::getPath).collect(Collectors.toList()));
    Assertions.assertEquals(Arrays.asList(100L, 200L),
        result.stream().map(CloudObjectMetadata::getSize).collect(Collectors.toList()));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 8})
  void existsCheckSurfacesRowFailure(int parallelism) throws Exception {
    // a key with a malformed percent escape fails URL decoding inside processRow; both branches must throw the
    // same exception instead of dropping the file
    List<Row> rows = Arrays.asList(
        cloudEventRow(Files.write(tempDir.resolve("file1.json"), Collections.singletonList("{}")), 100L),
        RowFactory.create("", "path/bad%zz.json", 200L));
    MapPartitionsFunction<Row, CloudObjectMetadata> fn =
        CloudObjectsSelectorCommon.getCloudObjectMetadataPerPartition("file://", storageConf, true, parallelism);

    HoodieException e = Assertions.assertThrows(HoodieException.class, () -> fn.call(rows.iterator()));
    Assertions.assertTrue(e.getMessage().contains("path/bad%zz.json"), e.getMessage());
    Assertions.assertTrue(e.getCause() instanceof IllegalArgumentException, String.valueOf(e.getCause()));
  }

  /**
   * Asserts that a Dataset contains expected rows; when the source path column is enabled it is expected
   * to be appended last, nullable, and to hold the file URI of the row's source file.
   */
  private static void assertRowResult(
      boolean includeSourcePathField,
      List<String> dataPaths,
      Dataset<Row> actualResult,
      Object[]... rowContents) {
    Assertions.assertEquals(dataPaths.size(), rowContents.length, "dataPaths and rowContents must align");
    Assertions.assertEquals(rowContents.length, actualResult.count());
    List<String> fieldNames = Arrays.asList(actualResult.schema().fieldNames());

    List<Row> expected = new ArrayList<>();
    if (includeSourcePathField) {
      Assertions.assertEquals(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN, fieldNames.get(fieldNames.size() - 1));
      Assertions.assertTrue(actualResult.schema().apply(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN).nullable());
      for (int i = 0; i < dataPaths.size(); i++) {
        List<Object> values = new ArrayList<>(Arrays.asList(rowContents[i]));
        // input_file_name() returns the file URI, which java.nio's Path.toUri() reproduces byte for byte
        values.add(new File(dataPaths.get(i)).getAbsoluteFile().toPath().toUri().toString());
        expected.add(RowFactory.create(values.toArray()));
      }
    } else {
      Assertions.assertFalse(fieldNames.contains(CloudObjectsSelectorCommon.CLOUD_SOURCE_PATH_COLUMN));
      for (Object[] row : rowContents) {
        expected.add(RowFactory.create(row));
      }
    }

    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actualResult.collectAsList()));
  }

  private static void setIncludeSourcePathField(TypedProperties properties, boolean include) {
    properties.put(CloudSourceConfig.INCLUDE_SOURCE_PATH_FIELD.key(), String.valueOf(include));
  }

  /** Row with the [bucket, key, size] shape getCloudObjectMetadataPerPartition expects; see s3EventJson for the key. */
  private static Row cloudEventRow(Path file, long size) {
    return RowFactory.create("", localFileKey(file), size);
  }

  /** S3 event notification whose bucket is empty and whose key is the absolute path without the leading separator, so
   * that prefix "file://" + bucket + "/" + key resolves to the local file. */
  private static String s3EventJson(Path file, long size) {
    return "{\"s3\":{\"bucket\":{\"name\":\"\"},\"object\":{\"key\":\"" + localFileKey(file) + "\",\"size\":" + size + "}}}";
  }

  private static String localFileKey(Path file) {
    return file.toUri().getPath().substring(1);
  }
}
