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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AvroSchemaRepair {
  public static Schema repairLogicalTypes(Schema requestedSchema, Schema tableSchema) {
    Schema repairedSchema = repairAvroSchema(requestedSchema, tableSchema);
    if (repairedSchema != requestedSchema) {
      return AvroSchemaCache.intern(repairedSchema);
    }
    return requestedSchema;
  }

  /**
   * Performs schema repair on a schema, handling nullable unions.
   */
  private static Schema repairAvroSchema(Schema requested, Schema table) {
    // Always resolve nullable schemas first (returns unchanged if not a union)
    Schema requestedNonNull = AvroSchemaUtils.resolveNullableSchema(requested);
    Schema tableNonNull = AvroSchemaUtils.resolveNullableSchema(table);

    // Perform repair on the non-null types
    Schema repairedNonNull = repairAvroSchemaNonNull(requestedNonNull, tableNonNull);

    // If nothing changed, return the original schema
    if (repairedNonNull == requestedNonNull) {
      return requested;
    }

    // If the original was a union, wrap the repaired schema back in a nullable union
    if (requested.getType() == Schema.Type.UNION) {
      return AvroSchemaUtils.createNullableSchema(repairedNonNull);
    }

    return repairedNonNull;
  }

  /**
   * Repairs non-nullable schemas (after unions have been resolved).
   */
  private static Schema repairAvroSchemaNonNull(Schema requested, Schema table) {
    // If schemas are already equal, nothing to repair
    if (requested.equals(table)) {
      return requested;
    }

    // If types are different, no repair can be done
    if (requested.getType() != table.getType()) {
      return requested;
    }

    // Handle record types (nested structs)
    if (requested.getType() == Schema.Type.RECORD) {
      return repairRecord(requested, table);
    }

    // Handle array types
    if (requested.getType() == Schema.Type.ARRAY) {
      Schema repairedElementSchema = repairAvroSchema(requested.getElementType(), table.getElementType());
      // If element didn't change, return original array schema
      if (repairedElementSchema == requested.getElementType()) {
        return requested;
      }
      return Schema.createArray(repairedElementSchema);
    }

    // Handle map types
    if (requested.getType() == Schema.Type.MAP) {
      Schema repairedValueSchema = repairAvroSchema(requested.getValueType(), table.getValueType());
      // If value didn't change, return original map schema
      if (repairedValueSchema == requested.getValueType()) {
        return requested;
      }
      return Schema.createMap(repairedValueSchema);
    }

    // Check primitive if we need to repair
    if (needsLogicalTypeRepair(requested, table)) {
      // If we need to repair, return the table schema
      return table;
    }

    // Default: return requested schema
    return requested;
  }

  /**
   * Quick check if a logical type repair is needed (no allocations).
   */
  private static boolean needsLogicalTypeRepair(Schema requested, Schema table) {
    if (requested.getType() != Schema.Type.LONG || table.getType() != Schema.Type.LONG) {
      return false;
    }

    LogicalType reqLogical = requested.getLogicalType();
    LogicalType tblLogical = table.getLogicalType();

    // if requested has no logical type, and the table has a local timestamp, then we need to repair
    if (reqLogical == null) {
      return tblLogical instanceof LogicalTypes.LocalTimestampMillis
          || tblLogical instanceof LogicalTypes.LocalTimestampMicros;
    }

    // if requested is timestamp-micros, and the table is timestamp-millis, then we need to repair
    return reqLogical instanceof LogicalTypes.TimestampMicros
        && tblLogical instanceof LogicalTypes.TimestampMillis;
  }

  /**
   * Performs record repair, returning the original schema if nothing changed.
   */
  private static Schema repairRecord(Schema requestedSchema, Schema tableSchema) {
    List<Schema.Field> fields = requestedSchema.getFields();

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
      return requestedSchema;
    }

    // Second pass: build the new schema with repaired fields
    List<Schema.Field> repairedFields = new ArrayList<>(fields.size());

    // Copy all fields before the first changed field
    for (int i = 0; i < firstChangedIndex; i++) {
      Schema.Field field = fields.get(i);
      // Must create new Field since they cannot be reused
      repairedFields.add(new Schema.Field(
          field.name(),
          field.schema(),
          field.doc(),
          field.defaultVal()
      ));
    }

    // Add the first changed field (using cached repaired schema)
    Schema.Field firstChangedField = fields.get(firstChangedIndex);
    repairedFields.add(new Schema.Field(
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
      repairedFields.add(new Schema.Field(
          requestedField.name(),
          repairedSchema,
          requestedField.doc(),
          requestedField.defaultVal()
      ));
    }

    return Schema.createRecord(
        requestedSchema.getName(),
        requestedSchema.getDoc(),
        requestedSchema.getNamespace(),
        requestedSchema.isError(),
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
        return hasTimestampMillisField(AvroSchemaUtils.resolveNullableSchema(tableSchema));

      default:
        return tableSchema.getType() == Schema.Type.LONG
            && (tableSchema.getLogicalType() instanceof LogicalTypes.TimestampMillis || tableSchema.getLogicalType() instanceof LogicalTypes.LocalTimestampMillis);
    }
  }
}
