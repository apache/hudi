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

package org.apache.hudi.schema;

import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Factory class for creating common HoodieSchema instances.
 *
 * <p>This factory provides convenient methods for creating commonly used
 * schema patterns without requiring complex builder logic.</p>
 *
 * @since 1.2.0
 */
public final class HoodieSchemaFactory {

  private HoodieSchemaFactory() {
    // Utility class, no instantiation
  }

  /**
   * Creates a record schema with the specified name and fields.
   *
   * @param recordName the name of the record
   * @param fields     the fields for the record
   * @return new HoodieSchema representing a record
   * @throws IllegalArgumentException if recordName is null/empty or fields is null/empty
   */
  public static HoodieSchema createRecord(String recordName, List<HoodieSchemaField> fields) {
    ValidationUtils.checkArgument(recordName != null && !recordName.isEmpty(),
        "Record name cannot be null or empty");
    ValidationUtils.checkArgument(fields != null && !fields.isEmpty(),
        "Fields list cannot be null or empty");

    List<Schema.Field> avroFields = new ArrayList<>();
    for (HoodieSchemaField field : fields) {
      avroFields.add(field.getAvroField());
    }

    Schema recordSchema = Schema.createRecord(recordName, null, null, false, avroFields);
    return new HoodieSchema(recordSchema);
  }

  /**
   * Creates a record schema with the specified name, namespace, and fields.
   *
   * @param recordName the name of the record
   * @param namespace  the namespace for the record
   * @param doc        the documentation string
   * @param fields     the fields for the record
   * @return new HoodieSchema representing a record
   */
  public static HoodieSchema createRecord(String recordName, String namespace, String doc,
                                          List<HoodieSchemaField> fields) {
    ValidationUtils.checkArgument(recordName != null && !recordName.isEmpty(),
        "Record name cannot be null or empty");
    ValidationUtils.checkArgument(fields != null && !fields.isEmpty(),
        "Fields list cannot be null or empty");

    List<Schema.Field> avroFields = new ArrayList<>();
    for (HoodieSchemaField field : fields) {
      avroFields.add(field.getAvroField());
    }

    Schema recordSchema = Schema.createRecord(recordName, doc, namespace, false, avroFields);
    return new HoodieSchema(recordSchema);
  }

  /**
   * Creates an enum schema with the specified name and symbols.
   *
   * @param enumName the name of the enum
   * @param symbols  the enum symbols
   * @return new HoodieSchema representing an enum
   */
  public static HoodieSchema createEnum(String enumName, List<String> symbols) {
    ValidationUtils.checkArgument(enumName != null && !enumName.isEmpty(),
        "Enum name cannot be null or empty");
    ValidationUtils.checkArgument(symbols != null && !symbols.isEmpty(),
        "Symbols list cannot be null or empty");

    Schema enumSchema = Schema.createEnum(enumName, null, null, symbols);
    return new HoodieSchema(enumSchema);
  }

  /**
   * Creates a fixed schema with the specified name and size.
   *
   * @param fixedName the name of the fixed schema
   * @param size      the size in bytes
   * @return new HoodieSchema representing a fixed type
   */
  public static HoodieSchema createFixed(String fixedName, int size) {
    ValidationUtils.checkArgument(fixedName != null && !fixedName.isEmpty(),
        "Fixed name cannot be null or empty");
    ValidationUtils.checkArgument(size > 0, "Fixed size must be positive");

    Schema fixedSchema = Schema.createFixed(fixedName, null, null, size);
    return new HoodieSchema(fixedSchema);
  }

  /**
   * Creates a union schema with the specified types.
   *
   * @param types the schemas to include in the union
   * @return new HoodieSchema representing a union
   */
  public static HoodieSchema createUnion(List<HoodieSchema> types) {
    ValidationUtils.checkArgument(types != null && !types.isEmpty(),
        "Types list cannot be null or empty");

    List<Schema> avroSchemas = new ArrayList<>();
    for (HoodieSchema schema : types) {
      avroSchemas.add(schema.getAvroSchema());
    }

    Schema unionSchema = Schema.createUnion(avroSchemas);
    return new HoodieSchema(unionSchema);
  }

  /**
   * Creates a union schema with the specified types.
   *
   * @param types the schemas to include in the union
   * @return new HoodieSchema representing a union
   */
  public static HoodieSchema createUnion(HoodieSchema... types) {
    ValidationUtils.checkArgument(types != null && types.length > 0,
        "Types array cannot be null or empty");

    return createUnion(Arrays.asList(types));
  }

  /**
   * Creates a nullable union (null + specified type).
   *
   * @param type the non-null type
   * @return new HoodieSchema representing a nullable union
   */
  public static HoodieSchema createNullableUnion(HoodieSchema type) {
    ValidationUtils.checkArgument(type != null, "Type cannot be null");

    HoodieSchema nullSchema = HoodieSchema.create(HoodieSchemaType.NULL);
    return createUnion(nullSchema, type);
  }
}
