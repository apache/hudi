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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class AvroToSparkJsonTest {

  @Test
  public void testSimpleRecordConversion() {
    // Create a simple Avro schema with basic types
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
        + "    {\"name\": \"name\", \"type\": \"string\"},\n"
        + "    {\"name\": \"active\", \"type\": \"boolean\"},\n"
        + "    {\"name\": \"score\", \"type\": [\"null\", \"double\"], \"default\": null},\n"
        + "    {\"name\": \"tags\", \"type\": {\"type\": \"array\", \"items\": \"string\"}},\n"
        + "    {\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    // Verify the overall structure
    assertTrue(sparkSchemaJson.contains("\"type\":\"struct\""));
    assertTrue(sparkSchemaJson.contains("\"fields\":["));

    // Verify basic field types
    assertTrue(sparkSchemaJson.contains("\"name\":\"id\""));
    assertTrue(sparkSchemaJson.contains("\"name\":\"name\""));
    assertTrue(sparkSchemaJson.contains("\"name\":\"active\""));
    assertTrue(sparkSchemaJson.contains("\"name\":\"score\""));
    assertTrue(sparkSchemaJson.contains("\"name\":\"tags\""));
    assertTrue(sparkSchemaJson.contains("\"name\":\"metadata\""));

    // Verify array type is correctly converted (not wrapped in struct)
    assertTrue(sparkSchemaJson.contains("\"type\":\"array\""));
    assertTrue(sparkSchemaJson.contains("\"elementType\":\"string\""));

    // Verify map type is correctly converted (not wrapped in struct)
    assertTrue(sparkSchemaJson.contains("\"type\":\"map\""));
    assertTrue(sparkSchemaJson.contains("\"keyType\":\"string\""));
    assertTrue(sparkSchemaJson.contains("\"valueType\":\"string\""));

    // Verify no struct wrapping around arrays or maps
    assertTrue(!sparkSchemaJson.contains("struct<array:")
               && !sparkSchemaJson.contains("struct<key_value:"),
               "Arrays and maps should not be wrapped in structs");

    System.out.println("Generated Spark Schema JSON:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testNestedRecordWithComments() {
    // Create Avro schema with nested structure and comments
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\", \"doc\": \"Unique identifier\"},\n"
        + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Person's name\"},\n"
        + "    {\"name\": \"address\", \"type\": [\"null\", {\n"
        + "      \"type\": \"record\", \"name\": \"AddressRecord\", \"fields\": [\n"
        + "        {\"name\": \"street\", \"type\": \"string\", \"doc\": \"Street address\"},\n"
        + "        {\"name\": \"city\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"City name\"}\n"
        + "      ]\n"
        + "    }], \"default\": null, \"doc\": \"Address information\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    // Verify top-level comments
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Unique identifier\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Person's name\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Address information\""));

    // Verify nested comments within address struct
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Street address\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"City name\""));
  }

  @Test
  public void testArrayOfStructs() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"users\", \"type\": {\"type\": \"array\", \"items\": {\n"
        + "      \"type\": \"record\", \"name\": \"UserRecord\", \"fields\": [\n"
        + "        {\"name\": \"name\", \"type\": \"string\", \"doc\": \"User name\"},\n"
        + "        {\"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Email address\"}\n"
        + "      ]\n"
        + "    }}, \"doc\": \"List of users\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"type\":\"array\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"List of users\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User name\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Email address\""));
  }

  @Test
  public void testMapWithStructValues() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"profiles\", \"type\": {\"type\": \"map\", \"values\": {\n"
        + "      \"type\": \"record\", \"name\": \"ProfileRecord\", \"fields\": [\n"
        + "        {\"name\": \"preferences\", \"type\": \"string\", \"doc\": \"User preferences\"},\n"
        + "        {\"name\": \"settings\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"User settings\"}\n"
        + "      ]\n"
        + "    }}, \"doc\": \"User profiles map\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"type\":\"map\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User profiles map\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User preferences\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User settings\""));
  }

  @Test
  public void testNestedArrays() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"matrix\", \"type\": {\"type\": \"array\", \"items\": {\n"
        + "      \"type\": \"array\", \"items\": \"int\"\n"
        + "    }}, \"doc\": \"2D integer matrix\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"type\":\"array\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"2D integer matrix\""));
  }

  @Test
  public void testComplexNestedStructure() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"organization\", \"type\": {\n"
        + "      \"type\": \"record\", \"name\": \"OrgRecord\", \"fields\": [\n"
        + "        {\"name\": \"departments\", \"type\": {\"type\": \"array\", \"items\": {\n"
        + "          \"type\": \"record\", \"name\": \"DeptRecord\", \"fields\": [\n"
        + "            {\"name\": \"name\", \"type\": \"string\", \"doc\": \"Department name\"},\n"
        + "            {\"name\": \"employees\", \"type\": {\"type\": \"map\", \"values\": {\n"
        + "              \"type\": \"record\", \"name\": \"EmpRecord\", \"fields\": [\n"
        + "                {\"name\": \"role\", \"type\": \"string\", \"doc\": \"Employee role\"},\n"
        + "                {\"name\": \"skills\", \"type\": {\"type\": \"array\", \"items\": \"string\"}, \"doc\": \"Skills list\"}\n"
        + "              ]\n"
        + "            }}, \"doc\": \"Employee map\"}\n"
        + "          ]\n"
        + "        }}, \"doc\": \"Departments list\"}\n"
        + "      ]\n"
        + "    }, \"doc\": \"Organization structure\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"comment\":\"Organization structure\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Departments list\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Department name\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Employee map\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Employee role\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Skills list\""));
  }

  @Test
  public void testEdgeCasesWithSpecialCharacters() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\", \"doc\": \"ID with \\\"quotes\\\" and \\\\backslashes\\\\\"},\n"
        + "    {\"name\": \"special_chars\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Special chars: @#$%^&*()+=[]{}|;':,.<>?/~`\"},\n"
        + "    {\"name\": \"unicode_field\", \"type\": [\"null\", \"string\"], \"default\": null, \"doc\": \"Unicode: 中文 العربية русский\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    // Verify proper escaping and character preservation
    assertTrue(sparkSchemaJson.contains("\"comment\":\"ID with") && sparkSchemaJson.contains("quotes") && sparkSchemaJson.contains("backslashes"));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Special chars: @#$%^&*()+=[]{}|;':,.<>?/~`\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Unicode: 中文 العربية русский\""));
  }

  @Test
  public void testRecordWithoutComments() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"id\", \"type\": \"int\"},\n"
        + "    {\"name\": \"name\", \"type\": [\"null\", \"string\"], \"default\": null},\n"
        + "    {\"name\": \"nested\", \"type\": {\n"
        + "      \"type\": \"record\", \"name\": \"NestedRecord\", \"fields\": [\n"
        + "        {\"name\": \"value\", \"type\": \"string\"}\n"
        + "      ]\n"
        + "    }}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    // Verify that fields without comments have empty metadata
    assertTrue(sparkSchemaJson.contains("\"metadata\":{}"));
    // Verify no comment fields are present
    assertTrue(!sparkSchemaJson.contains("\"comment\":"));
  }

  @Test
  public void testArrayOfMaps() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"config_sets\", \"type\": {\"type\": \"array\", \"items\": {\n"
        + "      \"type\": \"map\", \"values\": \"string\"\n"
        + "    }}, \"doc\": \"Array of configuration maps\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"type\":\"array\""));
    assertTrue(sparkSchemaJson.contains("\"type\":\"map\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Array of configuration maps\""));
  }

  @Test
  public void testMapWithArrayValues() {
    String avroSchemaJson = "{\n"
        + "  \"type\": \"record\",\n"
        + "  \"name\": \"TestRecord\",\n"
        + "  \"fields\": [\n"
        + "    {\"name\": \"tag_groups\", \"type\": {\"type\": \"map\", \"values\": {\n"
        + "      \"type\": \"array\", \"items\": \"string\"\n"
        + "    }}, \"doc\": \"Map of tag groups\"}\n"
        + "  ]\n"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);
    String sparkSchemaJson = AvroToSparkJson.convertToSparkSchemaJson(avroSchema);

    assertTrue(sparkSchemaJson.contains("\"type\":\"map\""));
    assertTrue(sparkSchemaJson.contains("\"type\":\"array\""));
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Map of tag groups\""));
  }
}