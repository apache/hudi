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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCache;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.List;

public class HoodieSchemaRepair {
  public static HoodieSchema repairLogicalTypes(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    HoodieSchema repairedSchema = repairSchema(fileSchema, tableSchema);
    if (repairedSchema != fileSchema) {
      return HoodieSchemaCache.intern(repairedSchema);
    }
    return fileSchema;
  }

  /**
   * Performs schema repair on a schema, handling nullable unions.
   */
  private static HoodieSchema repairSchema(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    // Always resolve nullable schemas first (returns unchanged if not a union)
    HoodieSchema nonNullFileSchema = fileSchema.getNonNullType();
    HoodieSchema nonNullTableSchema = tableSchema.getNonNullType();

    // Perform repair on the non-null types
    HoodieSchema nonNullRepairedSchema = repairSchemaNonNull(nonNullFileSchema, nonNullTableSchema);

    // If nothing changed, return the original schema
    if (nonNullRepairedSchema == nonNullFileSchema) {
      return fileSchema;
    }

    // If the original was a union, wrap the repaired schema back in a nullable union
    if (fileSchema.getType() == HoodieSchemaType.UNION) {
      return HoodieSchema.createNullable(nonNullRepairedSchema);
    }

    return nonNullRepairedSchema;
  }

  private static boolean canRepair(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    // repair can only be performed if the types are the same or if we are repairing long -> timestamp mismatches between the file and table schema
    return fileSchema.getType() == tableSchema.getType() || fileSchema.getType() == HoodieSchemaType.LONG && tableSchema.getType() == HoodieSchemaType.TIMESTAMP;
  }

  /**
   * Repairs non-nullable schemas (after unions have been resolved).
   */
  private static HoodieSchema repairSchemaNonNull(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    // If schemas are already equal, nothing to repair
    if (fileSchema.equals(tableSchema)) {
      return fileSchema;
    }

    // If types are different, no repair can be done
    if (!canRepair(fileSchema, tableSchema)) {
      return fileSchema;
    }

    // Handle record types (nested structs)
    if (fileSchema.getType() == HoodieSchemaType.RECORD) {
      return repairRecord(fileSchema, tableSchema);
    }

    // Handle array types
    if (fileSchema.getType() == HoodieSchemaType.ARRAY) {
      HoodieSchema repairedElementSchema = repairSchema(fileSchema.getElementType(), tableSchema.getElementType());
      // If element didn't change, return original array schema
      if (repairedElementSchema == fileSchema.getElementType()) {
        return fileSchema;
      }
      return HoodieSchema.createArray(repairedElementSchema);
    }

    // Handle map types
    if (fileSchema.getType() == HoodieSchemaType.MAP) {
      HoodieSchema repairedValueSchema = repairSchema(fileSchema.getValueType(), tableSchema.getValueType());
      // If value didn't change, return original map schema
      if (repairedValueSchema == fileSchema.getValueType()) {
        return fileSchema;
      }
      return HoodieSchema.createMap(repairedValueSchema);
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
  private static boolean needsLogicalTypeRepair(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    // If file schema is a long or timestamp and the table schema is a timestamp, then we may need to repair
    if ((fileSchema.getType() != HoodieSchemaType.LONG && fileSchema.getType() != HoodieSchemaType.TIMESTAMP) || tableSchema.getType() != HoodieSchemaType.TIMESTAMP) {
      return false;
    }

    // if file schema has no logical type, and the table has a local timestamp, then we need to repair
    if (fileSchema.getType() == HoodieSchemaType.LONG) {
      HoodieSchema.Timestamp tableTimestamp = (HoodieSchema.Timestamp) tableSchema;
      return !tableTimestamp.isUtcAdjusted();
    }

    // if file schema is timestamp-micros, and the table is timestamp-millis, then we need to repair
    HoodieSchema.Timestamp fileTimestamp = (HoodieSchema.Timestamp) fileSchema;
    HoodieSchema.Timestamp tableTimestamp = (HoodieSchema.Timestamp) tableSchema;
    return fileTimestamp.getPrecision() == HoodieSchema.TimePrecision.MICROS
        && tableTimestamp.getPrecision() == HoodieSchema.TimePrecision.MILLIS;
  }

  /**
   * Performs record repair, returning the original schema if nothing changed.
   */
  private static HoodieSchema repairRecord(HoodieSchema fileSchema, HoodieSchema tableSchema) {
    List<HoodieSchemaField> fields = fileSchema.getFields();

    // First pass: find the first field that changes
    int firstChangedIndex = -1;
    HoodieSchema firstRepairedSchema = null;

    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField requestedField = fields.get(i);
      Option<HoodieSchemaField> tableField = tableSchema.getField(requestedField.name());
      if (tableField.isPresent()) {
        HoodieSchema repairedSchema = repairSchema(requestedField.schema(), tableField.get().schema());
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
    List<HoodieSchemaField> repairedFields = new ArrayList<>(fields.size());

    // Copy all fields before the first changed field
    for (int i = 0; i < firstChangedIndex; i++) {
      HoodieSchemaField field = fields.get(i);
      // Must create new Field since they cannot be reused
      repairedFields.add(HoodieSchemaUtils.createNewSchemaField(field));
    }

    // Add the first changed field (using cached repaired schema)
    HoodieSchemaField firstChangedField = fields.get(firstChangedIndex);
    repairedFields.add(HoodieSchemaUtils.createNewSchemaField(
        firstChangedField.name(),
        firstRepairedSchema,
        firstChangedField.doc().orElse(null),
        firstChangedField.defaultVal().orElse(null)
    ));

    // Process remaining fields
    for (int i = firstChangedIndex + 1; i < fields.size(); i++) {
      HoodieSchemaField requestedField = fields.get(i);
      Option<HoodieSchemaField> tableField = tableSchema.getField(requestedField.name());
      HoodieSchema repairedSchema;

      if (tableField.isPresent()) {
        repairedSchema = repairSchema(requestedField.schema(), tableField.get().schema());
      } else {
        repairedSchema = requestedField.schema();
      }

      // Must create new Field since they cannot be reused
      repairedFields.add(HoodieSchemaUtils.createNewSchemaField(
          requestedField.name(),
          repairedSchema,
          requestedField.doc().orElse(null),
          requestedField.defaultVal().orElse(null)
      ));
    }

    return HoodieSchema.createRecord(
        fileSchema.getName(),
        fileSchema.getDoc().orElse(null),
        fileSchema.getNamespace().orElse(null),
        fileSchema.isError(),
        repairedFields
    );
  }

  public static boolean hasTimestampMillisField(HoodieSchema tableSchema) {
    switch (tableSchema.getType()) {
      case RECORD:
        for (HoodieSchemaField field : tableSchema.getFields()) {
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
        return hasTimestampMillisField(tableSchema.getNonNullType());

      case TIMESTAMP:
        HoodieSchema.Timestamp timestampType = (HoodieSchema.Timestamp) tableSchema;
        return timestampType.getPrecision() == HoodieSchema.TimePrecision.MILLIS;

      default:
        return false;
    }
  }
}
