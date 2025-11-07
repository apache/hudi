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

package org.apache.hudi.sync.common.util;

import org.apache.hudi.sync.common.model.FieldSchema;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.parquet.schema.OriginalType.UTF8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkDataSourceTableUtils {

  @Test
  public void testGetSparkTablePropertiesWithComments() {
    // Create a test Parquet schema
    MessageType schema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8),
        new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "partition_col", UTF8)
    );

    // Create field schema list with comments
    List<FieldSchema> fieldSchemas = Arrays.asList(
        new FieldSchema("id", "int", "Unique identifier"),
        new FieldSchema("name", "string", "Person's full name"),
        new FieldSchema("partition_col", "string", "Partition column for data organization")
    );

    List<String> partitionNames = Arrays.asList("partition_col");
    String sparkVersion = "3.3.0";
    int schemaLengthThreshold = 4000;

    // Get Spark table properties with comments
    Map<String, String> properties = SparkDataSourceTableUtils.getSparkTableProperties(
        partitionNames, sparkVersion, schemaLengthThreshold, schema, fieldSchemas);

    // Verify that properties contain the expected Spark DataSource settings
    assertTrue(properties.containsKey("spark.sql.sources.provider"),
               "Should contain Spark provider property");
    assertTrue(properties.containsKey("spark.sql.sources.schema.numParts"),
               "Should contain schema parts count");
    assertTrue(properties.containsKey("spark.sql.sources.schema.part.0"),
               "Should contain schema part");

    // Verify that comments are included in the schema JSON
    String schemaPart = properties.get("spark.sql.sources.schema.part.0");
    assertTrue(schemaPart.contains("\"comment\":\"Unique identifier\""),
               "Should contain comment for id field");
    assertTrue(schemaPart.contains("\"comment\":\"Person's full name\""),
               "Should contain comment for name field");
    assertTrue(schemaPart.contains("\"comment\":\"Partition column for data organization\""),
               "Should contain comment for partition column");

    System.out.println("Generated Spark table properties with comments:");
    properties.forEach((key, value) -> {
      if (key.startsWith("spark.sql.sources.schema.part")) {
        System.out.println(key + " = " + value);
      }
    });
  }

  @Test
  public void testGetSparkTablePropertiesWithoutComments() {
    // Create a test Parquet schema
    MessageType schema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8)
    );

    List<String> partitionNames = Arrays.asList();
    String sparkVersion = "3.3.0";
    int schemaLengthThreshold = 4000;

    // Get Spark table properties without comments (existing behavior)
    Map<String, String> properties = SparkDataSourceTableUtils.getSparkTableProperties(
        partitionNames, sparkVersion, schemaLengthThreshold, schema, Collections.emptyList());

    // Verify that properties are generated correctly
    assertTrue(properties.containsKey("spark.sql.sources.provider"),
               "Should contain Spark provider property");
    assertTrue(properties.containsKey("spark.sql.sources.schema.part.0"),
               "Should contain schema part");

    // Verify that no comments are present
    String schemaPart = properties.get("spark.sql.sources.schema.part.0");
    assertTrue(!schemaPart.contains("\"comment\":"),
               "Should not contain any comment fields when no comments provided");
    assertTrue(schemaPart.contains("\"metadata\":{}"),
               "Should contain empty metadata when no comments provided");

    System.out.println("Generated Spark table properties without comments:");
    properties.forEach((key, value) -> {
      if (key.startsWith("spark.sql.sources.schema.part")) {
        System.out.println(key + " = " + value);
      }
    });
  }
}