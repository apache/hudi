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
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
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

  public static final HoodieSchema METADATA_FIELD_SCHEMA = HoodieSchema.createNullable(HoodieSchemaType.STRING);
  public static final HoodieSchema RECORD_KEY_SCHEMA = initRecordKeySchema();

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
   * Adds Hudi metadata fields to the given schema with the withOperationField flag set as false.
   * This is equivalent to HoodieAvroUtils#.addMetadataFields() but operates on HoodieSchema.
   *
   * @param schema             the input schema
   * @return new HoodieSchema with metadata fields added
   * @throws IllegalArgumentException if schema is null
   */
  public static HoodieSchema addMetadataFields(HoodieSchema schema) {
    return addMetadataFields(schema, false);
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
   * Removes specified fields from a RECORD schema.
   * This is equivalent to HoodieAvroUtils.removeFields() but operates on HoodieSchema.
   *
   * @param schema original schema (must be RECORD type)
   * @param fieldNamesToRemove set of field names to remove
   * @return new HoodieSchema without the specified fields
   * @throws IllegalArgumentException if schema is null or not a RECORD type
   */
  public static HoodieSchema removeFields(HoodieSchema schema, Set<String> fieldNamesToRemove) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    ValidationUtils.checkArgument(schema.getType() == HoodieSchemaType.RECORD,
        () -> "Only RECORD schemas can have fields removed, got: " + schema.getType());

    if (fieldNamesToRemove == null || fieldNamesToRemove.isEmpty()) {
      return schema;
    }

    // Filter and copy fields (must create new instances, can't reuse Avro fields)
    List<HoodieSchemaField> filteredFields = schema.getFields().stream()
        .filter(field -> !fieldNamesToRemove.contains(field.name()))
        .map(HoodieSchemaUtils::createNewSchemaField)
        .collect(Collectors.toList());

    if (filteredFields.size() == schema.getFields().size()) {
      return schema; // No fields were removed
    }

    // Create record with isError flag preserved
    HoodieSchema newSchema = HoodieSchema.createRecord(
        schema.getName(),
        schema.getDoc().orElse(null),
        schema.getNamespace().orElse(null),
        schema.isError(),
        filteredFields
    );

    // Copy custom properties
    Map<String, Object> props = schema.getObjectProps();
    for (Map.Entry<String, Object> prop : props.entrySet()) {
      newSchema.addProp(prop.getKey(), prop.getValue());
    }

    return newSchema;
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

  /**
   * Creates a new schema field with the specified properties, including field order.
   * This is equivalent to HoodieAvroUtils.createNewSchemaField() but returns HoodieSchemaField.
   *
   * @param name         field name
   * @param schema       field schema
   * @param doc          field documentation (can be null)
   * @param defaultValue default value (can be null)
   * @param order        field order for sorting
   * @return new HoodieSchemaField instance
   * @throws IllegalArgumentException if name, schema, or order is null/empty
   * @since 1.2.0
   */
  public static HoodieSchemaField createNewSchemaField(String name, HoodieSchema schema,
                                                       String doc, Object defaultValue, HoodieFieldOrder order) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Field schema cannot be null");
    ValidationUtils.checkArgument(order != null, "Field order cannot be null");

    // Delegate to HoodieAvroUtils
    Schema.Field avroField = HoodieAvroUtils.createNewSchemaField(
        name, schema.toAvroSchema(), doc, defaultValue, order.toAvroOrder());
    return HoodieSchemaField.fromAvroField(avroField);
  }

  /**
   * Creates a new HoodieSchemaField from an existing field.
   * This is equivalent to HoodieAvroUtils.createNewSchemaField() but returns HoodieSchemaField.
   *
   * @param field the original HoodieSchemaField to create a new field from
   * @return a new HoodieSchemaField with the same properties but properly formatted default value
   */
  public static HoodieSchemaField createNewSchemaField(HoodieSchemaField field) {
    return createNewSchemaField(field.name(), field.schema(), field.doc().orElse(null), field.defaultVal().orElse(null));
  }

  /**
   * Converts a byte array to a BigDecimal using the given decimal schema.
   *
   * @param value         the byte array to convert
   * @param decimalSchema the decimal schema containing precision and scale
   * @return the resulting BigDecimal
   * @throws IllegalArgumentException if the schema is not a DECIMAL type
   */
  public static BigDecimal convertBytesToBigDecimal(byte[] value, HoodieSchema decimalSchema) {
    ValidationUtils.checkArgument(decimalSchema != null, "Decimal schema cannot be null");
    ValidationUtils.checkArgument(decimalSchema.getType() == HoodieSchemaType.DECIMAL,
        () -> "Schema must be of DECIMAL type, but is " + decimalSchema.getType());

    HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) decimalSchema;
    return convertBytesToBigDecimal(value, decimal.getPrecision(), decimal.getScale());
  }

  /**
   * Converts a byte array to a BigDecimal with the specified precision and scale.
   *
   * @param value     the byte array to convert
   * @param precision the precision of the decimal
   * @param scale     the scale of the decimal
   * @return the resulting BigDecimal
   */
  public static BigDecimal convertBytesToBigDecimal(byte[] value, int precision, int scale) {
    return new BigDecimal(new BigInteger(value),
        scale, new MathContext(precision, RoundingMode.HALF_UP));
  }

  /**
   * Gets a field (including nested fields) from the schema using dot notation.
   * This is equivalent to HoodieAvroUtils.getSchemaForField() but operates on HoodieSchema.
   * <p>
   * Supports nested field access using dot notation. For example:
   * <ul>
   *   <li>"name" - retrieves top-level field</li>
   *   <li>"user.profile.displayName" - retrieves nested field</li>
   * </ul>
   *
   * @param schema    the schema to search in
   * @param fieldName the field name (may contain dots for nested fields)
   * @return Option containing Pair of canonical field name and the HoodieSchemaField, or Option.empty() if field not found
   * @throws IllegalArgumentException if schema or fieldName is null/empty
   * @since 1.2.0
   */
  public static Option<Pair<String, HoodieSchemaField>> getNestedField(HoodieSchema schema, String fieldName) {
    ValidationUtils.checkArgument(schema != null, "Schema cannot be null");
    ValidationUtils.checkArgument(fieldName != null && !fieldName.isEmpty(), "Field name cannot be null or empty");
    return getNestedFieldInternal(schema, fieldName, "");
  }

  /**
   * Internal helper method for recursively retrieving nested fields.
   *
   * @param schema    the current schema to search in
   * @param fieldName the remaining field path
   * @param prefix    the accumulated field path prefix
   * @return Option containing Pair of canonical field name and the HoodieSchemaField, or Option.empty() if field not found
   */
  private static Option<Pair<String, HoodieSchemaField>> getNestedFieldInternal(HoodieSchema schema, String fieldName, String prefix) {
    HoodieSchema nonNullableSchema = getNonNullTypeFromUnion(schema);

    if (!fieldName.contains(".")) {
      // Base case: simple field name
      if (nonNullableSchema.getType() != HoodieSchemaType.RECORD) {
        return Option.empty();
      }
      return nonNullableSchema.getField(fieldName)
          .map(field -> Pair.of(prefix + fieldName, field));
    } else {
      // Recursive case: nested field
      int dotIndex = fieldName.indexOf(".");
      String rootFieldName = fieldName.substring(0, dotIndex);
      String remainingPath = fieldName.substring(dotIndex + 1);

      // Handle RECORD types - standard field navigation
      if (nonNullableSchema.getType() == HoodieSchemaType.RECORD) {
        return nonNullableSchema.getField(rootFieldName)
            .flatMap(rootField -> getNestedFieldInternal(
                rootField.schema(),
                remainingPath,
                prefix + rootFieldName + "."
            ));
      }

      // Handle ARRAY types - expect ".list.element" pattern
      if (nonNullableSchema.getType() == HoodieSchemaType.ARRAY && "list".equals(rootFieldName)) {
        if (!remainingPath.startsWith("element")) {
          return Option.empty();  // Invalid path for ARRAY
        }

        // Skip "element" and get remaining path
        String pathAfterElement = remainingPath.substring("element".length());
        if (!pathAfterElement.isEmpty() && !pathAfterElement.startsWith(".")) {
          return Option.empty();  // Invalid format
        }
        if (pathAfterElement.startsWith(".")) {
          pathAfterElement = pathAfterElement.substring(1);
        }

        HoodieSchema elementSchema = nonNullableSchema.getElementType();
        if (pathAfterElement.isEmpty()) {
          // We've reached the end - return synthetic field for element
          HoodieSchemaField syntheticField = HoodieSchemaField.of(
              "element",
              elementSchema,
              null,  // doc
              null   // defaultVal
          );
          return Option.of(Pair.of(prefix + rootFieldName + ".element", syntheticField));
        } else {
          // Continue navigating into element schema
          return getNestedFieldInternal(
              elementSchema,
              pathAfterElement,
              prefix + rootFieldName + ".element."
          );
        }
      }

      // Handle MAP types - expect ".key_value.key" or ".key_value.value" pattern
      if (nonNullableSchema.getType() == HoodieSchemaType.MAP && "key_value".equals(rootFieldName)) {
        if (remainingPath.startsWith("key")) {
          // Skip "key" and get remaining path
          String pathAfterKey = remainingPath.substring("key".length());
          if (!pathAfterKey.isEmpty() && !pathAfterKey.startsWith(".")) {
            return Option.empty();  // Invalid format
          }
          if (pathAfterKey.startsWith(".")) {
            pathAfterKey = pathAfterKey.substring(1);
          }

          if (pathAfterKey.isEmpty()) {
            // We've reached the end - return synthetic field for key (always STRING)
            HoodieSchema keySchema = nonNullableSchema.getKeyType();
            HoodieSchemaField syntheticField = HoodieSchemaField.of(
                "key",
                keySchema,
                null,  // doc
                null   // defaultVal
            );
            return Option.of(Pair.of(prefix + rootFieldName + ".key", syntheticField));
          } else {
            // Keys are primitives, cannot navigate further
            return Option.empty();
          }
        } else if (remainingPath.startsWith("value")) {
          // Skip "value" and get remaining path
          String pathAfterValue = remainingPath.substring("value".length());
          if (!pathAfterValue.isEmpty() && !pathAfterValue.startsWith(".")) {
            return Option.empty();  // Invalid format
          }
          if (pathAfterValue.startsWith(".")) {
            pathAfterValue = pathAfterValue.substring(1);
          }

          HoodieSchema valueSchema = nonNullableSchema.getValueType();
          if (pathAfterValue.isEmpty()) {
            // We've reached the end - return synthetic field for value
            HoodieSchemaField syntheticField = HoodieSchemaField.of(
                "value",
                valueSchema,
                null,  // doc
                null   // defaultVal
            );
            return Option.of(Pair.of(prefix + rootFieldName + ".value", syntheticField));
          } else {
            // Continue navigating into value schema
            return getNestedFieldInternal(
                valueSchema,
                pathAfterValue,
                prefix + rootFieldName + ".value."
            );
          }
        }
        // Invalid MAP path
        return Option.empty();
      }
      // For all other types (primitives, etc.), cannot navigate
      return Option.empty();
    }
  }

  /**
   * Generates a projection schema from the original schema, including only the specified fields.
   * This is equivalent to HoodieAvroUtils.generateProjectionSchema() but operates on HoodieSchema.
   *
   * @param originalSchema the source schema
   * @param fieldNames     the list of field names to include in the projection
   * @return new HoodieSchema containing only the specified fields
   * @throws IllegalArgumentException if schema is null or not a record type
   * @since 1.2.0
   */
  public static HoodieSchema generateProjectionSchema(HoodieSchema originalSchema, List<String> fieldNames) {
    ValidationUtils.checkArgument(originalSchema != null, "Original schema cannot be null");
    ValidationUtils.checkArgument(fieldNames != null, "Field names cannot be null");

    // Delegate to HoodieAvroUtils
    Schema projectedAvro = HoodieAvroUtils.generateProjectionSchema(originalSchema.toAvroSchema(), fieldNames);
    return HoodieSchema.fromAvroSchema(projectedAvro);
  }

  /**
   * Prunes the data schema to only include fields that are required by the required schema,
   * plus any mandatory fields specified.
   * This is equivalent to {@link AvroSchemaUtils#pruneDataSchema(Schema, Schema, Set)} but operates on HoodieSchema.
   *
   * @param dataSchema      the full data schema
   * @param requiredSchema  the schema containing required fields
   * @param mandatoryFields set of field names that must be included regardless
   * @return new HoodieSchema with pruned fields
   * @throws IllegalArgumentException if either schema is null
   * @since 1.2.0
   */
  public static HoodieSchema pruneDataSchema(HoodieSchema dataSchema, HoodieSchema requiredSchema, Set<String> mandatoryFields) {
    ValidationUtils.checkArgument(dataSchema != null, "Data schema cannot be null");
    ValidationUtils.checkArgument(requiredSchema != null, "Required schema cannot be null");

    Set<String> mandatorySet = mandatoryFields != null ? mandatoryFields : Collections.emptySet();

    // Delegate to AvroSchemaUtils
    Schema prunedAvro = AvroSchemaUtils.pruneDataSchema(
        dataSchema.toAvroSchema(),
        requiredSchema.toAvroSchema(),
        mandatorySet);
    return HoodieSchema.fromAvroSchema(prunedAvro);
  }

  /**
   * Checks if two schemas are projection equivalent (i.e., they have the same fields and types
   * for projection purposes, ignoring certain metadata differences).
   * This is equivalent to {@link AvroSchemaUtils#areSchemasProjectionEquivalent(Schema, Schema)} but operates on HoodieSchema.
   *
   * @param schema1 the first schema
   * @param schema2 the second schema
   * @return true if schemas are projection equivalent
   * @throws IllegalArgumentException if either schema is null
   * @since 1.2.0
   */
  public static boolean areSchemasProjectionEquivalent(HoodieSchema schema1, HoodieSchema schema2) {
    // Delegate to AvroSchemaUtils
    return AvroSchemaUtils.areSchemasProjectionEquivalent(schema1 == null ? null : schema1.toAvroSchema(), schema2 == null ? null : schema2.toAvroSchema());
  }

  /**
   * Adds newFields to the schema. Will add nested fields without duplicating the field
   * For example if your schema is "a.b.{c,e}" and newfields contains "a.{b.{d,e},x.y}",
   * It will stitch them together to be "a.{b.{c,d,e},x.y}
   * This is equivalent to {@link AvroSchemaUtils#appendFieldsToSchemaDedupNested(Schema, List)} but operates on HoodieSchema.
   *
   * @param schema    the original schema
   * @param newFields list of new fields to add
   * @return the updated schema with new fields added
   */
  public static HoodieSchema appendFieldsToSchemaDedupNested(HoodieSchema schema, List<HoodieSchemaField> newFields) {
    return HoodieSchema.fromAvroSchema(AvroSchemaUtils.appendFieldsToSchemaDedupNested(schema.toAvroSchema(),
        newFields.stream().map(HoodieSchemaField::getAvroField).collect(Collectors.toList())));
  }

  /**
   * Appends provided new fields at the end of the given schema
   *
   * NOTE: No deduplication is made, this method simply appends fields at the end of the list
   *       of the source schema as is
   *
   * This is equivalent to {@link AvroSchemaUtils#appendFieldsToSchema(Schema, List)} but operates on HoodieSchema.
   */
  public static HoodieSchema appendFieldsToSchema(HoodieSchema schema, List<HoodieSchemaField> newFields) {
    return HoodieSchema.fromAvroSchema(AvroSchemaUtils.appendFieldsToSchema(schema.toAvroSchema(),
        newFields.stream().map(HoodieSchemaField::getAvroField).collect(Collectors.toList())));
  }

  /**
   * Create a new schema but maintain all meta info from the old schema.
   * This is equivalent to {@link AvroSchemaUtils#createNewSchemaFromFieldsWithReference(Schema, List)} but operates on HoodieSchema.
   *
   * @param schema schema to get the meta info from
   * @param fields list of fields in order that will be in the new schema
   *
   * @return schema with fields from fields, and metadata from schema
   */
  public static HoodieSchema createNewSchemaFromFieldsWithReference(HoodieSchema schema, List<HoodieSchemaField> fields) {
    if (schema == null) {
      throw new IllegalArgumentException("Schema must not be null");
    }
    return HoodieSchema.fromAvroSchema(AvroSchemaUtils.createNewSchemaFromFieldsWithReference(
        schema.toAvroSchema(),
        fields.stream().map(HoodieSchemaField::getAvroField).collect(Collectors.toList())
    ));
  }

  /**
   * Get gets a field from a record, works on nested fields as well (if you provide the whole name, eg: toplevel.nextlevel.child)
   * @return the field, including its lineage.
   * For example, if you have a schema: record(a:int, b:record(x:int, y:long, z:record(z1: int, z2: float, z3: double), c:bool)
   * "fieldName" | output
   * ---------------------------------
   * "a"         | a:int
   * "b"         | b:record(x:int, y:long, z:record(z1: int, z2: int, z3: int)
   * "c"         | c:bool
   * "b.x"       | b:record(x:int)
   * "b.z.z2"    | b:record(z:record(z2:float))
   *
   * this is intended to be used with appendFieldsToSchemaDedupNested
   */
  public static Option<HoodieSchemaField> findNestedField(HoodieSchema schema, String fieldName) {
    return findNestedField(schema, fieldName.split("\\."), 0);
  }

  private static Option<HoodieSchemaField> findNestedField(HoodieSchema schema, String[] fieldParts, int index) {
    if (schema.getType() == HoodieSchemaType.UNION) {
      Option<HoodieSchemaField> notUnion = findNestedField(getNonNullTypeFromUnion(schema), fieldParts, index);
      if (!notUnion.isPresent()) {
        return Option.empty();
      }
      HoodieSchemaField nu = notUnion.get();
      return Option.of(createNewSchemaField(nu));
    }
    if (fieldParts.length <= index) {
      return Option.empty();
    }

    Option<HoodieSchemaField> foundFieldOpt = schema.getField(fieldParts[index]);
    if (foundFieldOpt.isEmpty()) {
      return Option.empty();
    }
    HoodieSchemaField foundField = foundFieldOpt.get();

    if (index == fieldParts.length - 1) {
      return Option.of(createNewSchemaField(foundField));
    }

    HoodieSchema foundSchema = foundField.schema();
    Option<HoodieSchemaField> nestedPart = findNestedField(foundSchema, fieldParts, index + 1);
    if (!nestedPart.isPresent()) {
      return Option.empty();
    }
    boolean isUnion = false;
    if (foundSchema.getType() == HoodieSchemaType.UNION) {
      isUnion = true;
      foundSchema = getNonNullTypeFromUnion(foundSchema);
    }
    HoodieSchema newSchema = createNewSchemaFromFieldsWithReference(foundSchema, Collections.singletonList(nestedPart.get()));
    return Option.of(createNewSchemaField(foundField.name(), isUnion ? createNullableSchema(newSchema) : newSchema, foundField.doc().orElse(null), foundField.defaultVal().orElse(null)));
  }

  private static HoodieSchema initRecordKeySchema() {
    HoodieSchemaField recordKeyField =
            createNewSchemaField(HoodieRecord.RECORD_KEY_METADATA_FIELD, METADATA_FIELD_SCHEMA, "", JsonProperties.NULL_VALUE);
    return HoodieSchema.createRecord(
            "HoodieRecordKey",
            "",
            "",
            false,
            Collections.singletonList(recordKeyField)
    );
  }

  public static HoodieSchema getRecordKeySchema() {
    return RECORD_KEY_SCHEMA;
  }

  /**
   * Converts field values for specific data types with logical type handling.
   * This is equivalent to HoodieAvroUtils.convertValueForSpecificDataTypes() but operates on HoodieSchema.
   * <p>
   * Handles special conversions for Avro logical types:
   * <ul>
   *   <li>Date type - converts epoch day integer to LocalDate</li>
   *   <li>Timestamp types - converts epoch milliseconds/microseconds to Timestamp</li>
   *   <li>Decimal type - converts bytes/fixed to BigDecimal</li>
   * </ul>
   *
   * @param fieldSchema the field schema
   * @param fieldValue the field value to convert
   * @param consistentLogicalTimestampEnabled whether to use consistent logical timestamp handling
   * @return converted value for logical types, or original value
   * @throws IllegalStateException if fieldValue is null but schema is not nullable
   * @since 1.2.0
   */
  public static Object convertValueForSpecificDataTypes(HoodieSchema fieldSchema,
                                                        Object fieldValue,
                                                        boolean consistentLogicalTimestampEnabled) {
    if (fieldSchema == null) {
      return fieldValue;
    } else if (fieldValue == null) {
      ValidationUtils.checkState(fieldSchema.isNullable(),
          "Field value is null but schema is not nullable");
      return null;
    }

    // Delegate to existing Avro utility
    return HoodieAvroUtils.convertValueForSpecificDataTypes(
        fieldSchema.toAvroSchema(),
        fieldValue,
        consistentLogicalTimestampEnabled
    );
  }

  /**
   * Fetches projected schema given list of fields to project. The field can be nested in format `a.b.c` where a is
   * the top level field, b is at second level and so on.
   * This is equivalent to {@link HoodieAvroUtils#projectSchema(Schema, List)} but operates on HoodieSchema.
   *
   * @param fileSchema the original schema
   * @param fields     list of fields to project
   * @return projected schema containing only specified fields
   */
  public static HoodieSchema projectSchema(HoodieSchema fileSchema, List<String> fields) {
    return HoodieSchema.fromAvroSchema(HoodieAvroUtils.projectSchema(fileSchema.toAvroSchema(), fields));
  }

  /**
   * Gets the fully-qualified Avro record name for a Hudi table.
   * This is equivalent to {@link AvroSchemaUtils#getAvroRecordQualifiedName(String)}
   * but provides a HoodieSchema-context API.
   *
   * <p>The qualified name follows the pattern: hoodie.{tableName}.{tableName}_record
   * where tableName is sanitized for Avro compatibility.</p>
   *
   * @param tableName the Hudi table name
   * @return the fully-qualified Avro record name (e.g., "hoodie.my_table.my_table_record")
   * @throws IllegalArgumentException if tableName is null or empty
   * @since 1.2.0
   */
  public static String getRecordQualifiedName(String tableName) {
    ValidationUtils.checkArgument(tableName != null && !tableName.trim().isEmpty(),
        "Table name cannot be null or empty");

    // Delegate to AvroSchemaUtils
    return AvroSchemaUtils.getAvroRecordQualifiedName(tableName);
  }

  public static boolean hasDecimalField(HoodieSchema schema) {
    return hasDecimalWithCondition(schema, unused -> true);
  }

  /**
   * Checks whether the provided schema contains a decimal with a precision less than or equal to 18,
   * which allows the decimal to be stored as int/long instead of a fixed size byte array in
   * <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md">parquet logical types</a>
   * @param schema the input schema to search
   * @return true if the schema contains a small precision decimal field and false otherwise
   */
  public static boolean hasSmallPrecisionDecimalField(HoodieSchema schema) {
    return hasDecimalWithCondition(schema, HoodieSchemaUtils::isSmallPrecisionDecimalField);
  }

  private static boolean hasDecimalWithCondition(HoodieSchema schema, Function<HoodieSchema.Decimal, Boolean> condition) {
    switch (schema.getType()) {
      case RECORD:
        for (HoodieSchemaField field : schema.getFields()) {
          if (hasDecimalWithCondition(field.schema(), condition)) {
            return true;
          }
        }
        return false;
      case ARRAY:
        return hasDecimalWithCondition(schema.getElementType(), condition);
      case MAP:
        return hasDecimalWithCondition(schema.getValueType(), condition);
      case UNION:
        return hasDecimalWithCondition(schema.getNonNullType(), condition);
      case DECIMAL:
        HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) schema;
        return condition.apply(decimal);
      default:
        return false;
    }
  }

  private static boolean isSmallPrecisionDecimalField(HoodieSchema.Decimal decimal) {
    return decimal.getPrecision() <= 18;
  }

  /**
   * Resolves a union schema by finding the schema matching the given full name.
   * Handles both simple nullable unions (null + non-null) and complex unions with multiple types.
   *
   * <p>This method supports the following union types:
   * <ul>
   *   <li>Simple nullable unions: {@code ["null", "Type"]} - returns the non-null type</li>
   *   <li>Complex unions: {@code ["null", "TypeA", "TypeB"]} - returns the type matching fieldSchemaFullName</li>
   *   <li>Non-union schemas - returns the schema as-is</li>
   * </ul>
   *
   * @param schema the schema to resolve (may or may not be a union)
   * @param fieldSchemaFullName the full name of the schema to find within the union
   * @return the resolved schema
   * @throws HoodieSchemaException if the union cannot be resolved or no matching type is found
   */
  public static HoodieSchema resolveUnionSchema(HoodieSchema schema, String fieldSchemaFullName) {
    if (schema.getType() != HoodieSchemaType.UNION) {
      return schema;
    }

    List<HoodieSchema> innerTypes = schema.getTypes();
    if (innerTypes.size() == 2 && schema.isNullable()) {
      // this is a basic nullable field so handle it more efficiently
      return schema.getNonNullType();
    }

    HoodieSchema nonNullType = innerTypes.stream()
        .filter(it -> it.getType() != HoodieSchemaType.NULL && Objects.equals(it.getFullName(), fieldSchemaFullName))
        .findFirst()
        .orElse(null);

    if (nonNullType == null) {
      throw new HoodieSchemaException(
          String.format("Unsupported UNION type %s: Only UNION of a null type and a non-null type is supported", schema));
    }

    return nonNullType;
  }

  public static String addMetadataColumnTypes(String hiveColumnTypes) {
    return "string,string,string,string,string," + hiveColumnTypes;
  }
}
