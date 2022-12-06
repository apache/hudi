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

package org.apache.hudi.avro;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAvroSchemaUtils {

  private static final String SOURCE_SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          },\n"
      + "          {\n"
      + "            \"name\": \"long\",\n"
      + "            \"type\": [\"null\", \"long\"]\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_STRICT = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          }\n"
      + "        ]\n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_WITH_PROMOTION = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"long\"]\n"
      + "    },\n"
      + "    {\n"
      + "      \"name\": \"nested_record\",\n"
      + "      \"type\": {\n"
      + "        \"name\": \"nested\",\n"
      + "        \"type\": \"record\",\n"
      + "        \"fields\": [\n"
      + "          {\n"
      + "            \"name\": \"string\",\n"
      + "            \"type\": [\"null\", \"string\"]\n"
      + "          }\n"
      + "        ]  \n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  @Test
  public void testIsStrictProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_STRICT);

    // Case #1: Validate proper (nested) projected record schema

    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));
  }

  @Test
  public void testIsCompatibleProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_WITH_PROMOTION);

    // Case #1: Validate proper (nested) projected record schema (with promotion,
    //          number field promoted from int to long)

    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, projectedNestedSchema));

    // NOTE: That [[isStrictProjectionOf]] should be false in that case
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isCompatibleProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));
  }
}
