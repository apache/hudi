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

package org.apache.parquet.schema;

import org.apache.hudi.avro.AvroSchemaCache;
import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AvroSchemaRepair {
  public static Schema repairLogicalTypes(Schema fileSchema, Schema tableSchema) {
    Schema repairedSchema = repairAvroSchema(fileSchema, tableSchema);
    if (repairedSchema != fileSchema) {
      return AvroSchemaCache.intern(repairedSchema);
    }
    return fileSchema;
  }

  /**
   * Performs schema repair on a schema, handling nullable unions.
   */
  private static Schema repairAvroSchema(Schema fileSchema, Schema tableSchema) {
    // Always resolve nullable schemas first (returns unchanged if not a union)
    Schema nonNullFileSchema = AvroSchemaUtils.getNonNullTypeFromUnion(fileSchema);
    Schema nonNullTableSchema = AvroSchemaUtils.getNonNullTypeFromUnion(tableSchema);

    // Perform repair on the non-null types
    Schema nonNullRepairedSchema = repairAvroSchemaNonNull(nonNullFileSchema, nonNullTableSchema);

    // If nothing changed, return the original schema
    if (nonNullRepairedSchema == nonNullFileSchema) {
      return fileSchema;
    }

    // If the original was a union, wrap the repaired schema back in a nullable union
    if (fileSchema.getType() == Schema.Type.UNION) {
      return AvroSchemaUtils.createNullableSchema(nonNullRepairedSchema);
    }

    return nonNullRepairedSchema;
  }

  /**
   * Repairs non-nullable schemas (after unions have been resolved).
   */
  private static Schema repairAvroSchemaNonNull(Schema fileSchema, Schema tableSchema) {
    // If schemas are already equal, nothing to repair
    if (fileSchema.equals(tableSchema)) {
      return fileSchema;
    }

    // If types are different, no repair can be done
    if (fileSchema.getType() != tableSchema.getType()) {
      return fileSchema;
    }

    // Handle record types (nested structs)
    if (fileSchema.getType() == Schema.Type.RECORD) {
      return repairRecord(fileSchema, tableSchema);
    }

    // Handle array types
    if (fileSchema.getType() == Schema.Type.ARRAY) {
      Schema repairedElementSchema = repairAvroSchema(fileSchema.getElementType(), tableSchema.getElementType());
      // If element didn't change, return original array schema
      if (repairedElementSchema == fileSchema.getElementType()) {
        return fileSchema;
      }
      return Schema.createArray(repairedElementSchema);
    }

    // Handle map types
    if (fileSchema.getType() == Schema.Type.MAP) {
      Schema repairedValueSchema = repairAvroSchema(fileSchema.getValueType(), tableSchema.getValueType());
      // If value didn't change, return original map schema
      if (repairedValueSchema == fileSchema.getValueType()) {
        return fileSchema;
      }
      return Schema.createMap(repairedValueSchema);
    }

    // Check primitive if we need to repair
    if (needsLogicalTypeRepair(fileSchema, tableSchema)) {
      // If we need to repair, return the table schema
      return tableSchema;
    }

    // Default: return file schema
    return fileSchema;
  }

  /**
   * Quick check if a logical type repair is needed (no allocations).
   */
  private static boolean needsLogicalTypeRepair(Schema fileSchema, Schema tableSchema) {
    if (fileSchema.getType() != Schema.Type.LONG || tableSchema.getType() != Schema.Type.LONG) {
      return false;
    }

    LogicalType fileSchemaLogicalType = fileSchema.getLogicalType();
    LogicalType tableSchemaLogicalType = tableSchema.getLogicalType();

    // if file scheam has no logical type, and the table has a local timestamp, then we need to repair
    if (fileSchemaLogicalType == null) {
      return tableSchemaLogicalType instanceof LogicalTypes.LocalTimestampMillis
          || tableSchemaLogicalType instanceof LogicalTypes.LocalTimestampMicros;
    }

    // if file schema is timestamp-micros, and the table is timestamp-millis, then we need to repair
    return fileSchemaLogicalType instanceof LogicalTypes.TimestampMicros
        && tableSchemaLogicalType instanceof LogicalTypes.TimestampMillis;
  }

  /**
   * Performs record repair, returning the original schema if nothing changed.
   */
  private static Schema repairRecord(Schema fileSchema, Schema tableSchema) {
    List<Schema.Field> fields = fileSchema.getFields();

    // First pass: find the first field that changes
    int firstChangedIndex = -1;
    Schema firstRepairedSchema = null;

    for (int i = 0; i < fields.size(); i++) {
      Schema.Field requestedField = fields.get(i);
      Schema.Field tableField = tableSchema.getField(requestedField.name());
      if (tableField != null) {
        Schema repairedSchema = repairAvroSchema(requestedField.schema(), tableField.schema());
        if (repairedSchema != requestedField.schema()) {
          firstChangedIndex = i;
          firstRepairedSchema = repairedSchema;
          break;
        }
      }
    }

    // If nothing changed, return the original schema
    if (firstChangedIndex == -1) {
      return fileSchema;
    }

    // Second pass: build the new schema with repaired fields
    List<Schema.Field> repairedFields = new ArrayList<>(fields.size());

    // Copy all fields before the first changed field
    for (int i = 0; i < firstChangedIndex; i++) {
      Schema.Field field = fields.get(i);
      // Must create new Field since they cannot be reused
      repairedFields.add(HoodieAvroUtils.createNewSchemaField(field));
    }

    // Add the first changed field (using cached repaired schema)
    Schema.Field firstChangedField = fields.get(firstChangedIndex);
    repairedFields.add(HoodieAvroUtils.createNewSchemaField(
        firstChangedField.name(),
        firstRepairedSchema,
        firstChangedField.doc(),
        firstChangedField.defaultVal()
    ));

    // Process remaining fields
    for (int i = firstChangedIndex + 1; i < fields.size(); i++) {
      Schema.Field requestedField = fields.get(i);
      Schema.Field tableField = tableSchema.getField(requestedField.name());
      Schema repairedSchema;

      if (tableField != null) {
        repairedSchema = repairAvroSchema(requestedField.schema(), tableField.schema());
      } else {
        repairedSchema = requestedField.schema();
      }

      // Must create new Field since they cannot be reused
      repairedFields.add(HoodieAvroUtils.createNewSchemaField(
          requestedField.name(),
          repairedSchema,
          requestedField.doc(),
          requestedField.defaultVal()
      ));
    }

    return Schema.createRecord(
        fileSchema.getName(),
        fileSchema.getDoc(),
        fileSchema.getNamespace(),
        fileSchema.isError(),
        repairedFields
    );
  }

  public static boolean hasTimestampMillisField(Schema tableSchema) {
    switch (tableSchema.getType()) {
      case RECORD:
        for (Schema.Field field : tableSchema.getFields()) {
          if (hasTimestampMillisField(field.schema())) {
            return true;
          }
        }
        return false;

      case ARRAY:
        return hasTimestampMillisField(tableSchema.getElementType());

      case MAP:
        return hasTimestampMillisField(tableSchema.getValueType());

      case UNION:
        return hasTimestampMillisField(AvroSchemaUtils.getNonNullTypeFromUnion(tableSchema));

      default:
        return tableSchema.getType() == Schema.Type.LONG
            && (tableSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis || tableSchema.getLogicalType() instanceof LogicalTypes.LocalTimestampMillis);
    }
  }
}
