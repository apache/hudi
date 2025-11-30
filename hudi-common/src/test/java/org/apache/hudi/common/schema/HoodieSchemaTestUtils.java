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

package org.apache.hudi.common.schema;

import org.apache.avro.JsonProperties;

import java.util.Arrays;

public class HoodieSchemaTestUtils {
  public static HoodieSchemaField createNestedField(String name, HoodieSchemaType type) {
    return createNestedField(name, HoodieSchema.create(type));
  }

  public static HoodieSchemaField createNestedField(String name, HoodieSchema schema) {
    return HoodieSchemaField.of(name, createRecord(name, HoodieSchemaField.of("nested", schema, null, null)), null, null);
  }

  public static HoodieSchemaField createArrayField(String name, HoodieSchemaType type) {
    return createArrayField(name, HoodieSchema.create(type));
  }

  public static HoodieSchemaField createArrayField(String name, HoodieSchema schema) {
    return HoodieSchemaField.of(name, HoodieSchema.createArray(schema), null, null);
  }

  public static HoodieSchemaField createNullableArrayField(String name, HoodieSchema schema) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchema.createArray(schema)), null, HoodieJsonProperties.NULL_VALUE);
  }

  public static HoodieSchemaField createMapField(String name, HoodieSchemaType type) {
    return createMapField(name, HoodieSchema.create(type));
  }

  public static HoodieSchemaField createMapField(String name, HoodieSchema schema) {
    return HoodieSchemaField.of(name, HoodieSchema.createMap(schema), null, null);
  }

  public static HoodieSchemaField createPrimitiveField(String name, HoodieSchemaType type) {
    return HoodieSchemaField.of(name, HoodieSchema.create(type), null, null);
  }

  public static HoodieSchemaField createNullablePrimitiveField(String name, HoodieSchemaType type) {
    return HoodieSchemaField.of(name, HoodieSchema.createNullable(HoodieSchema.create(type)), null, JsonProperties.NULL_VALUE);
  }

  public static HoodieSchema createRecord(String name, HoodieSchemaField... fields) {
    return HoodieSchema.createRecord(name, null, null, false, Arrays.asList(fields));
  }

  public static HoodieSchema createNullableRecord(String name, HoodieSchemaField... fields) {
    return HoodieSchema.createNullable(HoodieSchema.createRecord(name, null, null, false, Arrays.asList(fields)));
  }
}
