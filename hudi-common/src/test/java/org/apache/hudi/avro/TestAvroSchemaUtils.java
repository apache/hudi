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

  private static final String PROJECTED_NESTED_SCHEMA = "{\n"
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
      + "        ]  \n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  @Test
  public void testIsProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA);

    assertTrue(AvroSchemaUtils.isProjectionOf(sourceSchema, sourceSchema));

    assertTrue(AvroSchemaUtils.isProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isProjectionOf(projectedNestedSchema, sourceSchema));

  }

}
