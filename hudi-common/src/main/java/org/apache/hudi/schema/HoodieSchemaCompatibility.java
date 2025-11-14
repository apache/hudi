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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.common.util.ValidationUtils;

import java.util.Collections;
import java.util.Set;

/**
 * Utility class for checking HoodieSchema compatibility and evolution rules.
 * This provides HoodieSchema-native methods that delegate to existing AvroSchemaUtils
 * functionality while maintaining the architectural separation.
 *
 * <p>This class handles schema compatibility checking, which is crucial for:
 * <ul>
 *   <li>Table schema evolution validation</li>
 *   <li>Writer schema compatibility with table schema</li>
 *   <li>Projection compatibility for query optimization</li>
 *   <li>Metadata field handling during schema checks</li>
 * </ul>
 */
public final class HoodieSchemaCompatibility {

  // Prevent instantiation
  private HoodieSchemaCompatibility() {
  }

  /**
   * Checks if writer schema is compatible with table schema for write operations.
   * This is equivalent to AvroSchemaUtils.checkSchemaCompatible() but operates on HoodieSchemas.
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
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    if (!shouldValidate) {
      return;
    }

    // Delegate to AvroSchemaUtils for the actual compatibility check
    AvroSchemaUtils.checkSchemaCompatible(
        tableSchema.toAvroSchema(),
        writerSchema.toAvroSchema(),
        shouldValidate,
        allowProjection,
        Collections.emptySet()); // Default to no partition columns
  }

  /**
   * Checks if writer schema is compatible with table schema, excluding specified partition columns.
   * This is equivalent to AvroSchemaUtils.checkSchemaCompatible() with partition column exclusions.
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

    if (!shouldValidate) {
      return;
    }

    // Delegate to AvroSchemaUtils for the actual compatibility check
    AvroSchemaUtils.checkSchemaCompatible(
        tableSchema.toAvroSchema(),
        writerSchema.toAvroSchema(),
        shouldValidate,
        allowProjection,
        partitionColumns != null ? partitionColumns : Collections.emptySet());
  }

  /**
   * Validates that the incoming schema represents a valid evolution of the table schema.
   * This is equivalent to AvroSchemaUtils.checkValidEvolution() but operates on HoodieSchemas.
   *
   * @param incomingSchema the new schema being introduced
   * @param tableSchema    the existing table schema
   * @throws IllegalArgumentException                           if schemas are null
   * @throws org.apache.hudi.exception.SchemaEvolutionException if evolution is invalid
   */
  public static void checkValidEvolution(HoodieSchema incomingSchema, HoodieSchema tableSchema) {
    ValidationUtils.checkArgument(incomingSchema != null, "Incoming schema cannot be null");
    ValidationUtils.checkArgument(tableSchema != null, "Table schema cannot be null");

    // Delegate to AvroSchemaUtils for evolution validation
    AvroSchemaUtils.checkValidEvolution(incomingSchema.toAvroSchema(), tableSchema.toAvroSchema());
  }

  /**
   * Checks if two schemas are compatible in terms of data reading.
   * This uses the same logic as AvroSchemaUtils.isSchemaCompatible() but for HoodieSchemas.
   *
   * @param readerSchema the schema used to read the data
   * @param writerSchema the schema used to write the data
   * @return true if reader schema can read data written with writer schema
   * @throws IllegalArgumentException if schemas are null
   */
  public static boolean isSchemaCompatible(HoodieSchema readerSchema, HoodieSchema writerSchema) {
    ValidationUtils.checkArgument(readerSchema != null, "Reader schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    // Use HoodieSchemaUtils delegation for consistency
    return HoodieSchemaUtils.isSchemaCompatible(readerSchema, writerSchema);
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
    ValidationUtils.checkArgument(readerSchema != null, "Reader schema cannot be null");
    ValidationUtils.checkArgument(writerSchema != null, "Writer schema cannot be null");

    // Use HoodieSchemaUtils delegation for consistency
    return HoodieSchemaUtils.isSchemaCompatible(readerSchema, writerSchema, allowProjection);
  }
}
