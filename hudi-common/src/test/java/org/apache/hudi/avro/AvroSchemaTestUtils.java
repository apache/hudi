/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.avro;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import java.util.Arrays;

public class AvroSchemaTestUtils {
  public static Schema.Field createNestedField(String name, Schema.Type type) {
    return createNestedField(name, Schema.create(type));
  }

  public static Schema.Field createNestedField(String name, Schema schema) {
    return new Schema.Field(name, createRecord(name, new Schema.Field("nested", schema, null, null)), null, null);
  }

  public static Schema.Field createArrayField(String name, Schema.Type type) {
    return createArrayField(name, Schema.create(type));
  }

  public static Schema.Field createArrayField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createArray(schema), null, null);
  }

  public static Schema.Field createNullableArrayField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.createArray(schema)), null, Schema.Field.NULL_VALUE);
  }

  public static Schema.Field createMapField(String name, Schema.Type type) {
    return createMapField(name, Schema.create(type));
  }

  public static Schema.Field createMapField(String name, Schema schema) {
    return new Schema.Field(name, Schema.createMap(schema), null, null);
  }

  public static Schema.Field createPrimitiveField(String name, Schema.Type type) {
    return new Schema.Field(name, Schema.create(type), null, null);
  }

  public static Schema.Field createNullablePrimitiveField(String name, Schema.Type type) {
    return new Schema.Field(name, AvroSchemaUtils.createNullableSchema(type), null, JsonProperties.NULL_VALUE);
  }

  public static Schema createRecord(String name, Schema.Field... fields) {
    return Schema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  public static Schema createNullableRecord(String name, Schema.Field... fields) {
    return AvroSchemaUtils.createNullableSchema(Schema.createRecord(name, null, null, false, Arrays.asList(fields)));
  }
}
