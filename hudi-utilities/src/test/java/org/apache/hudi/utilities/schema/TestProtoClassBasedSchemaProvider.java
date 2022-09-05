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
 *
 */

package org.apache.hudi.utilities.schema;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.utilities.test.proto.Sample;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Scanner;

public class TestProtoClassBasedSchemaProvider {

  @Test
  public void validateDefaultSchemaGeneration() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME, Sample.class.getName());
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema protoSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getExpectedSchema("schema-provider/proto/sample_schema_nested.txt"));
    Assertions.assertEquals(expectedSchema, protoSchema);
  }

  @Test
  public void validateFlattenedSchemaGeneration() {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_CLASS_NAME, Sample.class.getName());
    properties.setProperty(ProtoClassBasedSchemaProvider.Config.PROTO_SCHEMA_FLATTEN_WRAPPED_PRIMITIVES, "true");
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema protoSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getExpectedSchema("schema-provider/proto/sample_schema_flattened.txt"));
    Assertions.assertEquals(expectedSchema, protoSchema);
  }

  private String getExpectedSchema(String pathToExpectedSchema) {
    try (Scanner scanner = new Scanner(getClass().getClassLoader().getResourceAsStream(pathToExpectedSchema))) {
      return scanner.next();
    }
  }
}
