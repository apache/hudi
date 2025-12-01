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

package org.apache.hudi.common.schema;

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for HoodieSchema operations including table schema manipulation,
 * compatibility checking, and schema evolution operations.
 *
 * <p>This class provides HoodieSchema equivalents of operations found in AvroSchemaUtils
 * and HoodieAvroUtils, focusing on table schema management rather than record-level operations.</p>
 *
 * <p>All methods in this class delegate to the corresponding Avro utilities internally
 * while providing a clean HoodieSchema-based API. This approach ensures consistency
 * with existing behavior while enabling migration to HoodieSchema types.</p>
 *
 * @since 1.2.0
 */
public final class HoodieSchemaUtils {

  // Private constructor to prevent instantiation
  private HoodieSchemaUtils() {
    throw new UnsupportedOperationException("Utility class cannot be instantiated");
  }

  /**
   * Creates a write schema for Hudi operations, adding necessary metadata fields.
   * This is equivalent to HoodieAvroUtils.createHoodieWriteSchema() but returns HoodieSchema.
   *
   * @param schema             the base schema string (JSON format)
   * @param withOperationField whether to include operation metadata field
   * @return HoodieSchema configured for write operations
   * @throws IllegalArgumentException if schema is null or empty
   */
  public static HoodieSchema createHoodieWriteSchema(String schema, boolean withOperationField) {
    ValidationUtils.checkArgument(schema != null && !schema.trim().isEmpty(),
        "Schema string cannot be null or empty");

    // Delegate to existing HoodieAvroUtils, convert result to HoodieSchema
    Schema avroSchema = HoodieAvroUtils.createHoodieWriteSchema(schema, withOperationField);
    return HoodieSchema.fromAvroSchema(avroSchema);
  }

  /**
   * Adds Hudi metadata fields to the given schema.
   * This is equivalent to HoodieAvroUtils.addMetadataFields() but operates on HoodieSchema.
   *
   * @param schema             the input schema
   * @param withOperationField whether to include operation metadata field
   * @return new HoodieSchema with metadata fields added
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema addMetadataFields(HoodieSchema schema, boolean withOperationField) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Convert to Avro, delegate to existing utility, convert back
    Schema avroSchema = schema.toAvroSchema();
    Schema resultAvro = HoodieAvroUtils.addMetadataFields(avroSchema, withOperationField);
    return HoodieSchema.fromAvroSchema(resultAvro);
  }

  /**
   * Removes Hudi metadata fields from the given schema.
   * This is equivalent to HoodieAvroUtils.removeMetadataFields() but operates on HoodieSchema.
   *
   * @param schema the input schema with metadata fields
   * @return new HoodieSchema with metadata fields removed
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema removeMetadataFields(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Convert to Avro, delegate to existing utility, convert back
    Schema avroSchema = schema.toAvroSchema();
    Schema resultAvro = HoodieAvroUtils.removeMetadataFields(avroSchema);
    return HoodieSchema.fromAvroSchema(resultAvro);
  }

  /**
   * Merges two schemas, combining fields from both with conflict resolution.
   * This is equivalent to AvroSchemaUtils.mergeSchemas() but operates on HoodieSchemas.
   *
   * @param sourceSchema source schema to merge from
   * @param targetSchema target schema to merge into
   * @return new HoodieSchema representing the merged result
   * @throws IllegalArgumentException if either schema is null
   */
  public static HoodieSchema mergeSchemas(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    ValidationUtils.checkArgument(sourceSchema != null, "Source schema cannot be null");
    ValidationUtils.checkArgument(targetSchema != null, "Target schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema mergedAvro = AvroSchemaUtils.mergeSchemas(
        sourceSchema.toAvroSchema(),
        targetSchema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(mergedAvro);
  }

  /**
   * Creates a nullable version of the given schema (union with null).
   * This is equivalent to AvroSchemaUtils.createNullableSchema() but operates on HoodieSchema.
   *
   * @param schema the input schema
   * @return new HoodieSchema that allows null values
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema createNullableSchema(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema nullableAvro = AvroSchemaUtils.createNullableSchema(schema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(nullableAvro);
  }

  /**
   * Create a new schema by force changing all the fields as nullable.
   * This is equivalent to AvroSchemaUtils.asNullable() but operates on HoodieSchema.
   *
   * @return a new schema with all the fields updated as nullable
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema asNullable(HoodieSchema schema) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema nullableAvro = AvroSchemaUtils.asNullable(schema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(nullableAvro);
  }

  /**
   * Extracts the non-null type from a union schema.
   * This is equivalent to AvroSchemaUtils.getNonNullTypeFromUnion() but operates on HoodieSchema.
   *
   * @param unionSchema union schema containing null and other types
   * @return HoodieSchema representing the non-null type
   * @throws IllegalStateException    if schema is not a union or doesn't contain non-null type
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema getNonNullTypeFromUnion(HoodieSchema unionSchema) {
    ValidationUtils.checkArgument(unionSchema != null, "Union schema cannot be null");

    // Delegate to AvroSchemaUtils
    Schema nonNullAvro = AvroSchemaUtils.getNonNullTypeFromUnion(unionSchema.toAvroSchema());
    return HoodieSchema.fromAvroSchema(nonNullAvro);
  }

  /**
   * Finds fields that are present in the table schema but missing in the writer schema.
   * This is equivalent to AvroSchemaUtils.findMissingFields() but operates on HoodieSchemas.
   *
   * @param tableSchema  the complete table schema
   * @param writerSchema the writer schema to check against
   * @return list of HoodieSchemaFields that are missing in writer schema
   * @throws IllegalArgumentException if either schema is null
   */
  public static List<HoodieSchemaField> findMissingFields(HoodieSchema tableSchema, HoodieSchema writerSchema) {
    return findMissingFields(tableSchema, writerSchema, Collections.emptySet());
  }

  /**
   * Finds fields that are present in the table schema but missing in the writer schema,
   * excluding partition columns from the check.
   * This is equivalent to AvroSchemaUtils.findMissingFields() but operates on HoodieSchemas.
   *
   * @param tableSchema    the complete table schema
   * @param writerSchema   the writer schema to check against
   * @param excludeColumns column names to exclude from missing field check
   * @return list of HoodieSchemaFields that are missing in writer schema
   * @throws IllegalArgumentException if either schema is null
   */
  public static List<HoodieSchemaField> findMissingFields(HoodieSchema tableSchema, HoodieSchema writerSchema,
                                                          Set<String> excludeColumns) {
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    if (tableSchema.getType() != HoodieSchemaType.RECORD || writerSchema.getType() != HoodieSchemaType.RECORD) {
      return Collections.emptyList();
    }

    Set<String> exclusions = excludeColumns != null ? excludeColumns : Collections.emptySet();
    Set<String> writerFieldNames = writerSchema.getFields().stream()
        .map(HoodieSchemaField::name)
        .collect(Collectors.toSet());

    // Find fields in table schema that are not present in writer schema and not excluded
    return tableSchema.getFields().stream()
        .filter(field -> !exclusions.contains(field.name()))
        .filter(field -> !writerFieldNames.contains(field.name()))
        .collect(Collectors.toList());
  }

  /**
   * Creates a new schema field with the specified properties.
   * This is equivalent to HoodieAvroUtils.createNewSchemaField() but returns HoodieSchemaField.
   *
   * @param name         field name
   * @param schema       field schema
   * @param doc          field documentation (can be null)
   * @param defaultValue default value (can be null)
   * @return new HoodieSchemaField instance
   * @throws IllegalArgumentException if name or schema is null/empty
   */
  public static HoodieSchemaField createNewSchemaField(String name, HoodieSchema schema,
                                                       String doc, Object defaultValue) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Field schema cannot be null");

    // Delegate to HoodieAvroUtils
    Schema.Field avroField = HoodieAvroUtils.createNewSchemaField(
        name, schema.toAvroSchema(), doc, defaultValue);
    return HoodieSchemaField.fromAvroField(avroField);
  }
}
