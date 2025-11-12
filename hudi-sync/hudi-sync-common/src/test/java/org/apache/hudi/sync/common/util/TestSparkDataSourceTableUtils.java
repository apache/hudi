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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestSparkDataSourceTableUtils {

  @Test
  public void testGetSparkTablePropertiesWithComments() {
    // Create Avro schema with comments
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"test_schema\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\", \"doc\": \"Unique identifier\"},\n"
        + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"doc\": \"Person's full name\"},\n"
        + "    {\"name\": \"partition_col\", \"type\": \"string\", \"doc\": \"Partition column for data organization\"}\n"
        + "  ]\n"
        + "}";
    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    List<String> partitionNames = Arrays.asList("partition_col");
    String sparkVersion = "3.3.0";
    int schemaLengthThreshold = 4000;

    // Get Spark table properties with comments
    Map<String, String> properties = SparkDataSourceTableUtils.getSparkTableProperties(
        partitionNames, sparkVersion, schemaLengthThreshold, avroSchema);

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
  }

  @Test
  public void testGetSparkTablePropertiesWithoutComments() {
    // Create equivalent Avro schema without comments (matching the original MessageType)
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"test_schema\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
        + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"]}\n"
        + "  ]\n"
        + "}";
    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    List<String> partitionNames = Arrays.asList();
    String sparkVersion = "3.3.0";
    int schemaLengthThreshold = 4000;

    // Get Spark table properties with Avro schema but test the behavior without comments
    Map<String, String> properties = SparkDataSourceTableUtils.getSparkTableProperties(
        partitionNames, sparkVersion, schemaLengthThreshold, avroSchema);

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
  }
}