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
import org.apache.hudi.utilities.config.ProtoClassBasedSchemaProviderConfig;
import org.apache.hudi.utilities.test.proto.Parent;
import org.apache.hudi.utilities.test.proto.Sample;
import org.apache.hudi.utilities.test.proto.WithOneOf;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class TestProtoClassBasedSchemaProvider {

  @Test
  public void validateDefaultSchemaGeneration() throws IOException {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Sample.class.getName());
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema convertedSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_defaults.avsc"));
    Assertions.assertEquals(expectedSchema, convertedSchema);
  }

  @Test
  public void validateWrappedPrimitiveAndTimestampsAsRecordSchemaGeneration() throws IOException {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Sample.class.getName());
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_WRAPPED_PRIMITIVES_AS_RECORDS.key(), "true");
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_TIMESTAMPS_AS_RECORDS.key(), "true");
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema convertedSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/sample_schema_wrapped_and_timestamp_as_record.avsc"));
    Assertions.assertEquals(expectedSchema, convertedSchema);
  }

  @Test
  public void validateRecursiveSchemaGeneration_depth2() throws IOException {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Parent.class.getName());
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_MAX_RECURSION_DEPTH.key(), String.valueOf(2));
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema convertedSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/parent_schema_recursive_depth_2.avsc"));
    Assertions.assertEquals(expectedSchema, convertedSchema);
  }

  @Test
  public void validateRecursiveSchemaGeneration_defaultDepth() throws IOException {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), Parent.class.getName());
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema convertedSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/parent_schema_recursive_default_limit.avsc"));
    Assertions.assertEquals(expectedSchema, convertedSchema);
  }

  @Test
  public void validateOneOfSchemaGeneration() throws IOException {
    TypedProperties properties = new TypedProperties();
    properties.setProperty(ProtoClassBasedSchemaProviderConfig.PROTO_SCHEMA_CLASS_NAME.key(), WithOneOf.class.getName());
    ProtoClassBasedSchemaProvider protoToAvroSchemaProvider = new ProtoClassBasedSchemaProvider(properties, null);
    Schema protoSchema = protoToAvroSchemaProvider.getSourceSchema();
    Schema.Parser parser = new Schema.Parser();
    Schema expectedSchema = parser.parse(getClass().getClassLoader().getResourceAsStream("schema-provider/proto/oneof_schema.avsc"));
    Assertions.assertEquals(expectedSchema, protoSchema);
  }
}
