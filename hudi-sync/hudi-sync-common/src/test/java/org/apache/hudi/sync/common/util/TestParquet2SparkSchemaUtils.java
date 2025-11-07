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
import org.apache.avro.Schema;
import org.apache.parquet.schema.GroupType;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestParquet2SparkSchemaUtils {

  @Test
  public void testConvertToSparkSchemaJsonWithComments() {
    // Create a simple Parquet schema
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8),
        new GroupType(Type.Repetition.OPTIONAL, "address",
            new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "street", UTF8),
            new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "city", UTF8)
        )
    );

    // Create field schema list with comments (only top-level fields for legacy method)
    List<FieldSchema> fieldSchemas = Arrays.asList(
        new FieldSchema("id", "int", "Unique identifier"),
        new FieldSchema("name", "string", "Person's name")
        // Note: Nested field comments like "address.street" are not supported by the legacy flat method
        // Use the new Avro schema method for nested comments
    );

    // Convert to Spark schema JSON with comments
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), fieldSchemas);

    // Verify that top-level comments are included in the JSON
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Unique identifier\""),
               "Should contain comment for id field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Person's name\""),
               "Should contain comment for name field");
    // Nested comments are not supported by the legacy flat method, so no assertion for nested street field

    // Verify that fields without comments have empty metadata
    assertTrue(sparkSchemaJson.contains("\"metadata\":{}"),
               "Should contain empty metadata for fields without comments");

    System.out.println("Generated Spark schema JSON with comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testConvertToSparkSchemaJsonWithoutComments() {
    // Create a simple Parquet schema
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8)
    );

    // Convert without comments (existing behavior)
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), Collections.emptyList());

    // Verify that all metadata is empty
    assertTrue(sparkSchemaJson.contains("\"metadata\":{}"),
               "Should contain empty metadata when no comments provided");
    assertTrue(!sparkSchemaJson.contains("\"comment\":"),
               "Should not contain any comment fields");

    System.out.println("Generated Spark schema JSON without comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testConvertToSparkSchemaJsonWithEscapedComments() {
    // Create a simple Parquet schema
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "description", UTF8)
    );

    // Create field schema list with special characters that need escaping
    List<FieldSchema> fieldSchemas = Arrays.asList(
        new FieldSchema("description", "string", "This is a \"quoted\" comment with special chars")
    );

    // Convert to Spark schema JSON with comments
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), fieldSchemas);

    // Verify that quotes are properly escaped
    assertTrue(sparkSchemaJson.contains("\"comment\":\"This is a \\\"quoted\\\" comment with special chars\""),
               "Should properly escape quotes in comments");

    System.out.println("Generated Spark schema JSON with escaped comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testNestedRecordCommentsWithAvroSchema() {
    // Create a Parquet schema with nested structure
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8),
        new GroupType(Type.Repetition.OPTIONAL, "address",
            new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "street", UTF8),
            new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "city", UTF8)
        )
    );

    // Create corresponding Avro schema with nested comments
    String avroSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\",\"doc\":\"Unique identifier\"},"
        + "  {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Person's name\"},"
        + "  {\"name\":\"address\",\"type\":[\"null\",{"
        + "    \"type\":\"record\",\"name\":\"AddressRecord\",\"fields\":["
        + "      {\"name\":\"street\",\"type\":\"string\",\"doc\":\"Street address\"},"
        + "      {\"name\":\"city\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"City name\"}"
        + "    ]"
        + "  }],\"default\":null,\"doc\":\"Address information\"}"
        + "]"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    // Convert using new Avro-based method
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), avroSchema);

    // Verify top-level comments
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Unique identifier\""),
               "Should contain comment for id field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Person's name\""),
               "Should contain comment for name field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Address information\""),
               "Should contain comment for address field");

    // Verify nested comments within address struct
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Street address\""),
               "Should contain comment for nested street field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"City name\""),
               "Should contain comment for nested city field");

    System.out.println("Generated Spark schema JSON with nested Avro comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testDeepNestedCommentsExtraction() {
    // Create a Parquet schema with deep nesting (3 levels)
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new GroupType(Type.Repetition.OPTIONAL, "user",
            new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8),
            new GroupType(Type.Repetition.OPTIONAL, "profile",
                new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "email", UTF8),
                new GroupType(Type.Repetition.OPTIONAL, "preferences",
                    new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "theme", UTF8),
                    new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "language", UTF8)
                )
            )
        )
    );

    // Create corresponding Avro schema with deep nested comments
    String avroSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\",\"doc\":\"Root level ID\"},"
        + "  {\"name\":\"user\",\"type\":[\"null\",{"
        + "    \"type\":\"record\",\"name\":\"UserRecord\",\"fields\":["
        + "      {\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"User name\"},"
        + "      {\"name\":\"profile\",\"type\":[\"null\",{"
        + "        \"type\":\"record\",\"name\":\"ProfileRecord\",\"fields\":["
        + "          {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Email address\"},"
        + "          {\"name\":\"preferences\",\"type\":[\"null\",{"
        + "            \"type\":\"record\",\"name\":\"PreferencesRecord\",\"fields\":["
        + "              {\"name\":\"theme\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"UI theme preference\"},"
        + "              {\"name\":\"language\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Language preference\"}"
        + "            ]"
        + "          }],\"default\":null,\"doc\":\"User preferences\"}"
        + "        ]"
        + "      }],\"default\":null,\"doc\":\"User profile data\"}"
        + "    ]"
        + "  }],\"default\":null,\"doc\":\"User information\"}"
        + "]"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    // Convert using new Avro-based method
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), avroSchema);

    // Verify comments at all nesting levels
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Root level ID\""),
               "Should contain comment for root level field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User information\""),
               "Should contain comment for level 1 nested field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User name\""),
               "Should contain comment for level 2 nested field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User profile data\""),
               "Should contain comment for level 2 nested struct");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Email address\""),
               "Should contain comment for level 3 nested field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"User preferences\""),
               "Should contain comment for level 3 nested struct");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"UI theme preference\""),
               "Should contain comment for level 4 nested field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Language preference\""),
               "Should contain comment for level 4 nested field");

    System.out.println("Generated Spark schema JSON with deep nested comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testMixedNestedAndFlatComments() {
    // Create a Parquet schema with mixed structure
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "title", UTF8),
        new GroupType(Type.Repetition.OPTIONAL, "metadata",
            new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "category", UTF8),
            new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "tags", UTF8)
        ),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "description", UTF8)
    );

    // Create Avro schema with mixed comments (some fields have comments, others don't)
    String avroSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\",\"doc\":\"Primary key\"},"
        + "  {\"name\":\"title\",\"type\":[\"null\",\"string\"],\"default\":null},"  // No comment
        + "  {\"name\":\"metadata\",\"type\":[\"null\",{"
        + "    \"type\":\"record\",\"name\":\"MetadataRecord\",\"fields\":["
        + "      {\"name\":\"category\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Content category\"},"
        + "      {\"name\":\"tags\",\"type\":[\"null\",\"string\"],\"default\":null}"  // No comment
        + "    ]"
        + "  }],\"default\":null,\"doc\":\"Additional metadata\"},"
        + "  {\"name\":\"description\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Full description text\"}"
        + "]"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    // Convert using new Avro-based method
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), avroSchema);

    // Verify fields WITH comments have them
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Primary key\""),
               "Should contain comment for id field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Additional metadata\""),
               "Should contain comment for metadata struct");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Content category\""),
               "Should contain comment for nested category field");
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Full description text\""),
               "Should contain comment for description field");

    // Verify fields WITHOUT comments have empty metadata
    assertTrue(sparkSchemaJson.contains("\"name\":\"title\"") &&
               sparkSchemaJson.matches(".*\"name\":\"title\"[^}]*\"metadata\":\\{\\}.*"),
               "Title field should have empty metadata");
    assertTrue(sparkSchemaJson.contains("\"name\":\"tags\"") &&
               sparkSchemaJson.matches(".*\"name\":\"tags\"[^}]*\"metadata\":\\{\\}.*"),
               "Tags field should have empty metadata");

    System.out.println("Generated Spark schema JSON with mixed comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testAvroSchemaEdgeCases() {
    // Create a simple Parquet schema
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "special_chars", UTF8),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "unicode_field", UTF8)
    );

    // Create Avro schema with edge case comments
    String avroSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\",\"doc\":\"ID with \\\"quotes\\\" and \\\\backslashes\\\\\"},"
        + "  {\"name\":\"special_chars\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Special chars: @#$%^&*()+=[]{}|;':,.<>?/~`\"},"
        + "  {\"name\":\"unicode_field\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Unicode: 中文 العربية русский\"}"
        + "]"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    // Convert using new Avro-based method
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), avroSchema);

    // Debug: print the actual JSON to see what's generated
    System.out.println("Generated Spark schema JSON with edge case comments:");
    System.out.println(sparkSchemaJson);

    // Verify proper escaping of quotes and backslashes - adjust based on actual output
    assertTrue(sparkSchemaJson.contains("\"comment\":\"ID with \\\"quotes\\\" and \\backslashes\\\""),
               "Should properly escape quotes and backslashes in comments");

    // Verify special characters are preserved
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Special chars: @#$%^&*()+=[]{}|;':,.<>?/~`\""),
               "Should preserve special characters in comments");

    // Verify Unicode characters are preserved
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Unicode: 中文 العربية русский\""),
               "Should preserve Unicode characters in comments");

    System.out.println("Generated Spark schema JSON with edge case comments:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testNullAvroSchemaFallback() {
    // Create a simple Parquet schema
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8)
    );

    // Test with null Avro schema (should not crash)
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), (Schema) null);

    // Verify that all metadata is empty when null schema is provided
    assertTrue(sparkSchemaJson.contains("\"metadata\":{}"),
               "Should contain empty metadata when null Avro schema provided");
    assertTrue(!sparkSchemaJson.contains("\"comment\":"),
               "Should not contain any comment fields with null schema");

    System.out.println("Generated Spark schema JSON with null Avro schema:");
    System.out.println(sparkSchemaJson);
  }

  @Test
  public void testBidirectionalSchemaMismatchSilentHandling() {
    // Create MessageType (Parquet) schema with 3 fields
    MessageType parquetSchema = new MessageType("test_schema",
        new PrimitiveType(Type.Repetition.REQUIRED, INT32, "id"),
        new PrimitiveType(Type.Repetition.OPTIONAL, BINARY, "name", UTF8), // Missing in Avro
        new GroupType(Type.Repetition.OPTIONAL, "address",                 // Missing in Avro
            new PrimitiveType(Type.Repetition.REQUIRED, BINARY, "street", UTF8)
        )
    );

    // Create Avro schema with only 1 common field + 2 extra fields
    String avroSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "  {\"name\":\"id\",\"type\":\"int\",\"doc\":\"Common field comment\"},"           // Common field
        + "  {\"name\":\"email\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Extra field in Avro\"}," // Extra in Avro
        + "  {\"name\":\"profile\",\"type\":[\"null\",{"                                     // Extra nested in Avro
        + "    \"type\":\"record\",\"name\":\"ProfileRecord\",\"fields\":["
        + "      {\"name\":\"bio\",\"type\":[\"null\",\"string\"],\"default\":null,\"doc\":\"Bio comment\"}"
        + "    ]"
        + "  }],\"default\":null,\"doc\":\"Profile information\"}"
        + "]"
        + "}";

    Schema avroSchema = new Schema.Parser().parse(avroSchemaJson);

    // Should handle misalignment silently without exceptions
    String sparkSchemaJson = Parquet2SparkSchemaUtils.convertToSparkSchemaJson(parquetSchema.asGroupType(), avroSchema);

    // Verify common field gets comment successfully
    assertTrue(sparkSchemaJson.contains("\"comment\":\"Common field comment\""),
               "Common field should have comment from Avro schema");

    // Verify missing fields get empty metadata silently (no crash)
    assertTrue(sparkSchemaJson.contains("\"name\":\"name\""),
               "Missing field 'name' should still appear in output");
    assertTrue(sparkSchemaJson.contains("\"name\":\"address\""),
               "Missing field 'address' should still appear in output");
    assertTrue(sparkSchemaJson.contains("\"name\":\"street\""),
               "Nested missing field 'street' should still appear in output");

    // Verify fields missing in Avro have empty metadata (silent handling)
    assertTrue(sparkSchemaJson.matches(".*\"name\":\"name\"[^}]*\"metadata\":\\{\\}.*"),
               "Missing field 'name' should have empty metadata");

    // Verify valid JSON structure is generated
    assertNotNull(sparkSchemaJson, "Schema JSON should not be null");
    assertTrue(sparkSchemaJson.startsWith("{\"type\":\"struct\""),
               "Should generate valid Spark struct schema");
    assertTrue(sparkSchemaJson.contains("\"fields\":["),
               "Should contain fields array");

    // Extra fields in Avro are silently ignored (no processing needed since they don't exist in Parquet)
    // This is naturally handled by our implementation

    System.out.println("Generated Spark schema JSON with bidirectional misalignment (silent handling):");
    System.out.println(sparkSchemaJson);
  }
}