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

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.HoodieSparkClientTestHarness;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, Collections.emptyList(), "json", Option.empty(), 1, false);
    Assertions.assertFalse(result.isPresent());
  }

  @Test
  public void filesFromMetadataRead() {
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(new TypedProperties());
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1, false);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  public void partitionValueAddedToRow() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));

    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1, false);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "CA");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
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
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1, false);
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
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 1, false);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", "US", "TX");
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }

  @Test
  public void loadDatasetWithSchemaAndRepartition() {
    TypedProperties props = new TypedProperties();
    TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc");
    String schemaFilePath = TestCloudObjectsSelectorCommon.class.getClassLoader().getResource("schema/sample_data_schema.avsc").getPath();
    props.put("hoodie.streamer.schemaprovider.source.schema.file", schemaFilePath);
    props.put("hoodie.streamer.schema.provider.class.name", FilebasedSchemaProvider.class.getName());
    props.put("hoodie.streamer.source.cloud.data.partition.fields.from.path", "country,state");
    // Setting this config so that dataset repartition happens inside `loadAsDataset`
    props.put("hoodie.streamer.source.cloud.data.partition.max.size", "1");
    List<CloudObjectMetadata> input = Arrays.asList(
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=TX/data.json", 1000),
        new CloudObjectMetadata("src/test/resources/data/partitioned/country=IND/state=TS/data.json", 1000)
    );
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(props);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30, false);
    Assertions.assertTrue(result.isPresent());
    List<Row> expected = Arrays.asList(RowFactory.create("some data", "US", "CA"), RowFactory.create("some data", "US", "TX"), RowFactory.create("some data", "IND", "TS"));
    List<Row> actual = result.get().collectAsList();
    Assertions.assertEquals(new HashSet<>(expected), new HashSet<>(actual));
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
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30, false);
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
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.of(new FilebasedSchemaProvider(props, jsc)), 30, false);
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
    Schema schema = new Schema.Parser().parse(new File(schemaFilePath));
    StructType expectedSchema = AvroConversionUtils.convertAvroSchemaToStructType(schema);
    // assert final output schema matches with the source schema
    Assertions.assertEquals(expectedSchema, result.get().schema(), "output dataset schema should match source schema");
  }

  @Test
  public void partitionKeyNotPresentInPath() {
    List<CloudObjectMetadata> input = Collections.singletonList(new CloudObjectMetadata("src/test/resources/data/partitioned/country=US/state=CA/data.json", 1));
    TypedProperties properties = new TypedProperties();
    properties.put("hoodie.deltastreamer.source.cloud.data.reader.comma.separated.path.format", "false");
    properties.put("hoodie.deltastreamer.source.cloud.data.partition.fields.from.path", "unknown");
    CloudObjectsSelectorCommon cloudObjectsSelectorCommon = new CloudObjectsSelectorCommon(properties);
    Option<Dataset<Row>> result = cloudObjectsSelectorCommon.loadAsDataset(sparkSession, input, "json", Option.empty(), 1, false);
    Assertions.assertTrue(result.isPresent());
    Assertions.assertEquals(1, result.get().count());
    Row expected = RowFactory.create("some data", null);
    Assertions.assertEquals(Collections.singletonList(expected), result.get().collectAsList());
  }
}
