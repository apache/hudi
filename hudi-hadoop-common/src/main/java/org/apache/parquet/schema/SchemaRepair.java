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

import org.apache.hudi.common.util.Option;

import java.util.ArrayList;
import java.util.List;

public class SchemaRepair {

  public static MessageType repairLogicalTypes(MessageType requestedSchema, Option<MessageType> tableSchema) {
    if (tableSchema.isEmpty()) {
      return requestedSchema;
    }
    return repairLogicalTypes(requestedSchema, tableSchema.get());
  }

  static MessageType repairLogicalTypes(MessageType requestedSchema, MessageType tableSchema) {
    List<Type> repairedFields = repairFields(requestedSchema.getFields(), tableSchema);

    // If nothing changed, return the original schema
    if (repairedFields == null) {
      return requestedSchema;
    }

    return new MessageType(requestedSchema.getName(), repairedFields);
  }

  /**
   * Repairs a list of fields against a table schema (MessageType or GroupType).
   * Returns null if no changes were made, otherwise returns the repaired field list.
   */
  private static List<Type> repairFields(List<Type> requestedFields, GroupType tableSchema) {
    // First pass: find the first field that changes
    int firstChangedIndex = -1;
    Type firstRepairedField = null;

    for (int i = 0; i < requestedFields.size(); i++) {
      Type requestedField = requestedFields.get(i);
      if (tableSchema.containsField(requestedField.getName())) {
        Type tableField = tableSchema.getType(requestedField.getName());
        Type repaired = repairField(requestedField, tableField);
        if (repaired != requestedField) {
          firstChangedIndex = i;
          firstRepairedField = repaired;
          break;
        }
      }
    }

    // If nothing changed, return null
    if (firstChangedIndex == -1) {
      return null;
    }

    // Second pass: build the new field list with repaired fields
    List<Type> repairedFields = new ArrayList<>(requestedFields.size());

    // Copy all fields before the first changed field
    for (int i = 0; i < firstChangedIndex; i++) {
      repairedFields.add(requestedFields.get(i));
    }

    // Add the first changed field (using cached repaired field)
    repairedFields.add(firstRepairedField);

    // Process remaining fields
    for (int i = firstChangedIndex + 1; i < requestedFields.size(); i++) {
      Type requestedField = requestedFields.get(i);
      Type repaired = requestedField;
      if (tableSchema.containsField(requestedField.getName())) {
        Type tableField = tableSchema.getType(requestedField.getName());
        repaired = repairField(requestedField, tableField);
      }
      repairedFields.add(repaired);
    }

    return repairedFields;
  }

  private static Type repairField(Type requested, Type table) {
    if (requested.isPrimitive() && table.isPrimitive()) {
      return repairPrimitiveType(requested.asPrimitiveType(), table.asPrimitiveType());
    } else if (!requested.isPrimitive() && !table.isPrimitive()) {
      // recurse into nested structs
      GroupType reqGroup = requested.asGroupType();
      GroupType tblGroup = table.asGroupType();

      // Repair fields directly without creating MessageType intermediaries
      List<Type> repairedFields = repairFields(reqGroup.getFields(), tblGroup);

      // If nothing changed, return the original field
      if (repairedFields == null) {
        return requested;
      }

      return new GroupType(
          reqGroup.getRepetition(),
          reqGroup.getName(),
          reqGroup.getLogicalTypeAnnotation(),
          repairedFields
      );
    } else {
      // fallback: keep requested
      return requested;
    }
  }

  private static PrimitiveType repairPrimitiveType(PrimitiveType requested, PrimitiveType table) {
    // Quick check if repair is needed (no allocations)
    if (needsLogicalTypeRepair(requested, table)) {
      return Types.primitive(table.getPrimitiveTypeName(), requested.getRepetition())
          .as(table.getLogicalTypeAnnotation())
          .named(requested.getName());
    }
    return requested;
  }

  /**
   * Quick check if a logical type repair is needed (no allocations).
   */
  private static boolean needsLogicalTypeRepair(PrimitiveType requested, PrimitiveType table) {
    if (requested.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64
        || table.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
      return false;
    }
    LogicalTypeAnnotation reqLogical = requested.getLogicalTypeAnnotation();
    LogicalTypeAnnotation tblLogical = table.getLogicalTypeAnnotation();

    // if requested has no logical type, and the table has a local timestamp, then we need to repair
    if (reqLogical == null) {
      return tblLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
          && !((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tblLogical).isAdjustedToUTC();
    }

    // if requested is timestamp-micros and table is timestamp-millis then we need to repair
    return reqLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && tblLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) reqLogical).getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tblLogical).getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) reqLogical).isAdjustedToUTC()
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tblLogical).isAdjustedToUTC();
  }
}
