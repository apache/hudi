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

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility class for checking HoodieSchema compatibility and evolution rules.
 * This provides HoodieSchema-native methods that implement schema compatibility checking
 * without requiring conversion to Avro schemas.
 *
 * <p>This class handles schema compatibility checking, which is crucial for:
 * <ul>
 *   <li>Table schema evolution validation</li>
 *   <li>Writer schema compatibility with table schema</li>
 *   <li>Projection compatibility for query optimization</li>
 *   <li>Metadata field handling during schema checks</li>
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class HoodieSchemaCompatibility {

  public static boolean areSchemasCompatible(HoodieSchema tableSchema, HoodieSchema writerSchema) {
    return HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(tableSchema, writerSchema, false).getType() == HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.COMPATIBLE;
  }

  /**
   * Checks if writer schema is compatible with table schema for write operations.
   * Performs schema compatibility checking for write operations.
   *
   * @param tableSchema     the table schema to check against
   * @param writerSchema    the writer schema to validate
   * @param shouldValidate  whether to perform compatibility validation
   * @param allowProjection whether to allow projection (fewer fields in writer)
   * @throws IllegalArgumentException                               if schemas are null
   * @throws org.apache.hudi.exception.SchemaCompatibilityException if schemas are incompatible
   */
  public static void checkSchemaCompatible(HoodieSchema tableSchema, HoodieSchema writerSchema,
                                           boolean shouldValidate, boolean allowProjection) {
    checkSchemaCompatible(
        tableSchema,
        writerSchema,
        shouldValidate,
        allowProjection,
        Collections.emptySet()); // Default to no partition columns
  }

  /**
   * Checks if writer schema is compatible with table schema, excluding specified partition columns.
   * Performs schema compatibility checking with partition column exclusions.
   *
   * @param tableSchema      the table schema to check against
   * @param writerSchema     the writer schema to validate
   * @param shouldValidate   whether to perform compatibility validation
   * @param allowProjection  whether to allow projection (fewer fields in writer)
   * @param partitionColumns set of partition column names to exclude from compatibility checks
   * @throws IllegalArgumentException                               if schemas are null
   * @throws org.apache.hudi.exception.SchemaCompatibilityException if schemas are incompatible
   */
  public static void checkSchemaCompatible(HoodieSchema tableSchema, HoodieSchema writerSchema,
                                           boolean shouldValidate, boolean allowProjection,
                                           Set<String> partitionColumns) {
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    Set<String> partitionCols = partitionColumns != null ? partitionColumns : Collections.emptySet();

    // Step 1: Check for missing fields (if projection not allowed)
    if (!allowProjection) {
      List<HoodieSchemaField> missingFields = HoodieSchemaUtils.findMissingFields(tableSchema, writerSchema, partitionCols);
      if (!missingFields.isEmpty()) {
        throw new MissingSchemaFieldException(
            missingFields.stream().map(HoodieSchemaField::name).collect(Collectors.toList()),
            writerSchema,
            tableSchema);
      }
    }

    // Step 2: Check compatibility (if validation enabled and no partition column drops)
    // TODO(HUDI-4772) re-enable validations in case partition columns
    //                 being dropped from the data-file after fixing the write schema
    if (partitionCols.isEmpty() && shouldValidate) {
      HoodieSchemaCompatibilityChecker.SchemaPairCompatibility result =
          HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(writerSchema, tableSchema, true);
      if (result.getType() != HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.COMPATIBLE) {
        throw new SchemaBackwardsCompatibilityException(result, writerSchema, tableSchema);
      }
    }
  }

  /**
   * Validates that the incoming schema represents a valid evolution of the table schema.
   * Ensures the incoming schema can read existing table data and has all required fields.
   *
   * @param incomingSchema the new schema being introduced
   * @param tableSchema    the existing table schema
   * @throws IllegalArgumentException                           if schemas are null
   * @throws org.apache.hudi.exception.SchemaCompatibilityException if evolution is invalid
   */
  public static void checkValidEvolution(HoodieSchema incomingSchema, HoodieSchema tableSchema) {
    ValidationUtils.checkArgument(incomingSchema != null, "Incoming schema cannot be null");
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");

    // Early return if incoming schema is NULL type
    if (incomingSchema.getType() == HoodieSchemaType.NULL) {
      return;
    }

    // Step 1: Find all missing fields recursively (incoming must have all table fields)
    // not really needed for `hoodie.write.set.null.for.missing.columns` but good to check anyway
    List<String> missingFields = new ArrayList<>();
    findAllMissingFieldsRecursive(incomingSchema, tableSchema, new ArrayDeque<>(), missingFields);
    if (!missingFields.isEmpty()) {
      throw new MissingSchemaFieldException(
          missingFields,
          incomingSchema,
          tableSchema);
    }

    // Step 2: Check compatibility (incoming as reader, table as writer)
    // make sure that the table schema can be read using the incoming schema
    HoodieSchemaCompatibilityChecker.SchemaPairCompatibility result =
        HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(incomingSchema, tableSchema, false);
    if (result.getType() != HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.COMPATIBLE) {
      throw new SchemaBackwardsCompatibilityException(result, tableSchema, incomingSchema);
    }
  }

  /**
   * Checks if two schemas are compatible in terms of data reading.
   * Uses native HoodieSchema compatibility checking.
   *
   * @param prevSchema previous instance of the schema
   * @param newSchema new instance of the schema
   * @return true if reader schema can read data written with writer schema
   * @throws IllegalArgumentException if schemas are null
   */
  public static boolean isSchemaCompatible(HoodieSchema prevSchema, HoodieSchema newSchema) {
    return isSchemaCompatible(prevSchema, newSchema, true);
  }

  /**
   * Establishes whether {@code newSchema} is compatible w/ {@code prevSchema}, as
   * defined by Avro's schema compatibility rules.
   * From avro's compatibility standpoint, prevSchema is writer schema and new schema is reader schema.
   * {@code newSchema} is considered compatible to {@code prevSchema}, iff data written using {@code prevSchema}
   * could be read by {@code newSchema}
   *
   * @param prevSchema previous instance of the schema
   * @param newSchema new instance of the schema
   * @param checkNaming     controls whether schemas fully-qualified names should be checked
   * @param allowProjection whether to allow fewer fields in reader schema
   * @return true if reader schema can read data written with writer schema
   * @throws IllegalArgumentException if schemas are null
   */
  public static boolean isSchemaCompatible(HoodieSchema prevSchema, HoodieSchema newSchema,
      boolean checkNaming, boolean allowProjection) {
    ValidationUtils.checkArgument(prevSchema != null, "Prev schema cannot be null");
    ValidationUtils.checkArgument(newSchema != null, "New schema cannot be null");

    // NOTE: We're establishing compatibility of the {@code prevSchema} and {@code newSchema}
    //       as following: {@code newSchema} is considered compatible to {@code prevSchema},
    //       iff data written using {@code prevSchema} could be read by {@code newSchema}

    // In case schema projection is not allowed, new schema has to have all the same fields as the
    // old schema
    if (!allowProjection && !canProject(prevSchema, newSchema)) {
      return false;
    }

    HoodieSchemaCompatibilityChecker.SchemaPairCompatibility result =
        HoodieSchemaCompatibilityChecker.checkReaderWriterCompatibility(newSchema, prevSchema, checkNaming);
    return result.getType() == HoodieSchemaCompatibilityChecker.SchemaCompatibilityType.COMPATIBLE;
  }

  /**
   * Checks if two schemas are compatible with projection support.
   * This allows the reader schema to have fewer fields than the writer schema.
   *
   * @param readerSchema    the schema used to read the data
   * @param writerSchema    the schema used to write the data
   * @param allowProjection whether to allow fewer fields in reader schema
   * @return true if reader schema can read data written with writer schema
   * @throws IllegalArgumentException if schemas are null
   */
  public static boolean isSchemaCompatible(HoodieSchema readerSchema, HoodieSchema writerSchema,
                                           boolean allowProjection) {
    return isSchemaCompatible(readerSchema, writerSchema, true, allowProjection);
  }

  /**
   * Validate whether the {@code targetSchema} is a "compatible" projection of {@code sourceSchema}.
   * Only difference of this method from {@link #isStrictProjectionOf(HoodieSchema, HoodieSchema)} is
   * the fact that it allows some legitimate type promotions (like {@code int -> long},
   * {@code decimal(3, 2) -> decimal(5, 2)}, etc.) that allows projection to have a "wider"
   * atomic type (whereas strict projection requires atomic type to be identical)
   */
  public static boolean isCompatibleProjectionOf(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    if (sourceSchema == null || targetSchema == null) {
      return false;
    }
    return HoodieSchemaProjectionChecker.isCompatibleProjectionOf(sourceSchema, targetSchema);
  }

  /**
   * Validate whether the {@code targetSchema} is a strict projection of {@code sourceSchema}.
   *
   * Schema B is considered a strict projection of schema A iff
   * <ol>
   *   <li>Schemas A and B are equal, or</li>
   *   <li>Schemas A and B are array schemas and element-type of B is a strict projection
   *   of the element-type of A, or</li>
   *   <li>Schemas A and B are map schemas and value-type of B is a strict projection
   *   of the value-type of A, or</li>
   *   <li>Schemas A and B are union schemas (of the same size) and every element-type of B
   *   is a strict projection of the corresponding element-type of A, or</li>
   *   <li>Schemas A and B are record schemas and every field of the record B has corresponding
   *   counterpart (w/ the same name) in the schema A, such that the schema of the field of the schema
   *   B is also a strict projection of the A field's schema</li>
   * </ol>
   */
  public static boolean isStrictProjectionOf(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    if (sourceSchema == null || targetSchema == null) {
      return false;
    }
    return HoodieSchemaProjectionChecker.isStrictProjectionOf(sourceSchema, targetSchema);
  }

  /**
   * If schemas are projection equivalent, then a record with schema1 does not need to be projected to schema2
   * because the projection will be the identity.
   *
   *  Two schemas are considered projection equivalent if the field names and types are equivalent.
   *  The names of records, namespaces, or docs do not need to match. Nullability is ignored.
   */
  public static boolean areSchemasProjectionEquivalent(HoodieSchema schema1, HoodieSchema schema2) {
    if (schema1 == null || schema2 == null) {
      return false;
    }
    return HoodieSchemaComparatorForRecordProjection.areSchemasProjectionEquivalent(schema1, schema2);
  }

  /**
   * Identifies the writer field that corresponds to the specified reader field.
   *
   * <p>
   * Matching includes reader name aliases.
   * </p>
   *
   * @param writerSchema Schema of the record where to look for the writer field.
   * @param readerField  Reader field to identify the corresponding writer field
   *                     of.
   * @return the writer field, if any does correspond, or None.
   */
  public static HoodieSchemaField lookupWriterField(final HoodieSchema writerSchema, final HoodieSchemaField readerField) {
    ValidationUtils.checkArgument(writerSchema.getType() == HoodieSchemaType.RECORD, writerSchema + " is not a record");
    Option<HoodieSchemaField> directOpt = writerSchema.getField(readerField.name());
    // Check aliases
    for (final String readerFieldAliasName : readerField.getAvroField().aliases()) {
      final Option<HoodieSchemaField> writerFieldOpt = writerSchema.getField(readerFieldAliasName);
      if (writerFieldOpt.isPresent()) {
        if (directOpt.isPresent()) {
          // Multiple matches found, fail fast
          throw new HoodieSchemaException(String.format(
              "Reader record field %s matches multiple fields in writer record schema %s", readerField, writerSchema));
        }
        directOpt = writerFieldOpt;
      }
    }

    return directOpt.orElse(null);
  }

  /**
   * Check that each field in the prevSchema can be populated in the newSchema.
   *
   * @param prevSchema prev schema
   * @param newSchema new schema
   * @return true if prev schema is a projection of new schema
   */
  private static boolean canProject(HoodieSchema prevSchema, HoodieSchema newSchema) {
    return HoodieSchemaUtils.findMissingFields(prevSchema, newSchema, Collections.emptySet()).isEmpty();
  }

  /**
   * Recursively finds all missing fields in the incoming schema compared to the table schema.
   * This is used for schema evolution validation.
   *
   * @param incomingSchema the incoming schema
   * @param tableSchema the table schema
   * @param visited stack of visited field names for error reporting
   * @param missingFields list to collect missing field paths
   */
  private static void findAllMissingFieldsRecursive(HoodieSchema incomingSchema,
                                                     HoodieSchema tableSchema,
                                                     Deque<String> visited,
                                                     List<String> missingFields) {
    if (incomingSchema.getType() == tableSchema.getType()) {
      switch (incomingSchema.getType()) {
        case RECORD:
          visited.addLast(tableSchema.getName());
          for (HoodieSchemaField targetField : tableSchema.getFields()) {
            visited.addLast(targetField.name());
            Option<HoodieSchemaField> sourceFieldOpt = incomingSchema.getField(targetField.name());
            if (!sourceFieldOpt.isPresent()) {
              missingFields.add(String.join(".", visited));
            } else {
              findAllMissingFieldsRecursive(sourceFieldOpt.get().schema(), targetField.schema(), visited, missingFields);
            }
            visited.removeLast();
          }
          visited.removeLast();
          break;

        case ARRAY:
          visited.addLast("element");
          findAllMissingFieldsRecursive(incomingSchema.getElementType(), tableSchema.getElementType(), visited, missingFields);
          visited.removeLast();
          break;

        case MAP:
          visited.addLast("value");
          findAllMissingFieldsRecursive(incomingSchema.getValueType(), tableSchema.getValueType(), visited, missingFields);
          visited.removeLast();
          break;

        case UNION:
          List<HoodieSchema> incomingTypes = incomingSchema.getTypes();
          List<HoodieSchema> tableTypes = tableSchema.getTypes();
          if (incomingTypes.size() != tableTypes.size()) {
            throw new HoodieSchemaException(String.format(
                "Incoming batch field '%s' has union with %d types, while the table schema has %d types",
                String.join(".", visited), incomingTypes.size(), tableTypes.size()));
          }
          if (incomingTypes.size() > 2) {
            throw new HoodieSchemaException(String.format(
                "Incoming batch field '%s' has complex union with %d types. Only unions with null are supported.",
                String.join(".", visited), incomingTypes.size()));
          }
          for (int i = 0; i < incomingTypes.size(); i++) {
            findAllMissingFieldsRecursive(incomingTypes.get(i), tableTypes.get(i), visited, missingFields);
          }
          break;

        default:
          // Primitive types - no nested fields to check
          break;
      }
    }
  }
}
