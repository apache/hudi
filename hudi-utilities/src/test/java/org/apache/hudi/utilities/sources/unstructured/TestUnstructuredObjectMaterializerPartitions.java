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

package org.apache.hudi.utilities.sources.unstructured;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.testutils.SparkClientFunctionalTestHarness;
import org.apache.hudi.utilities.config.CloudSourceConfig;
import org.apache.hudi.utilities.sources.helpers.CloudObjectMetadata;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestUnstructuredObjectMaterializerPartitions extends SparkClientFunctionalTestHarness {
  @TempDir
  Path objects;

  @ParameterizedTest
  @ValueSource(strings = {"region,month", "region, month", " region, month ", "region,,month", "region, ,month"})
  public void materializedPartitionNamesMatchTheSourceContract(String partitionFields) throws Exception {
    Path file = objects.resolve("region=west/month=11/one.txt");
    Files.createDirectories(file.getParent());
    byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    Files.write(file, bytes);
    TypedProperties props = new TypedProperties();
    props.setProperty(CloudSourceConfig.PATH_BASED_PARTITION_FIELDS.key(), partitionFields);
    Dataset<Row> output = new UnstructuredObjectMaterializer(props, new JavaSparkContext(spark().sparkContext()))
        .materialize(spark(), Collections.singletonList(new CloudObjectMetadata(file.toUri().toString(), (long) bytes.length)),
            Option.empty(), 1).get();
    assertArrayEquals(UnstructuredSchemaContract.contract(UnstructuredMode.BLOB, props, Option.empty()).fieldNames(), output.columns());
    Row row = output.first();
    assertEquals("west", row.getAs("region"));
    assertEquals("11", row.getAs("month"));
  }
}
