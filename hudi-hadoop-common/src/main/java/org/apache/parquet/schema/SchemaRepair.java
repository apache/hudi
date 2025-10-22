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
    List<Type> repairedFields = new ArrayList<>();

    for (Type requestedField : requestedSchema.getFields()) {
      Type repaired = requestedField;
      if (tableSchema.containsField(requestedField.getName())) {
        Type tableField = tableSchema.getType(requestedField.getName());
        repaired = repairField(requestedField, tableField);
      }
      repairedFields.add(repaired);
    }

    return new MessageType(requestedSchema.getName(), repairedFields);
  }

  private static Type repairField(Type requested, Type table) {
    if (requested.isPrimitive() && table.isPrimitive()) {
      return repairPrimitiveType(requested.asPrimitiveType(), table.asPrimitiveType());
    } else if (!requested.isPrimitive() && !table.isPrimitive()) {
      // recurse into nested structs
      GroupType reqGroup = requested.asGroupType();
      GroupType tblGroup = table.asGroupType();
      MessageType nestedReq = new MessageType(reqGroup.getName(), reqGroup.getFields());
      MessageType nestedTbl = new MessageType(tblGroup.getName(), tblGroup.getFields());
      MessageType repairedNested = repairLogicalTypes(nestedReq, nestedTbl);

      return new GroupType(
          reqGroup.getRepetition(),
          reqGroup.getName(),
          reqGroup.getLogicalTypeAnnotation(),
          repairedNested.getFields()
      );
    } else {
      // fallback: keep requested
      return requested;
    }
  }

  private static PrimitiveType repairPrimitiveType(PrimitiveType requested, PrimitiveType table) {
    LogicalTypeAnnotation reqLogical = requested.getLogicalTypeAnnotation();
    LogicalTypeAnnotation tblLogical = table.getLogicalTypeAnnotation();

    boolean useTableType = false;

    // Rule 1: requested is timestamp(micros), table is timestamp(millis)
    if (reqLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && tblLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {

      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation reqTs = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) reqLogical;
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation tblTs = (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tblLogical;

      if (reqTs.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS
          && tblTs.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS
          && tblTs.isAdjustedToUTC()
          && reqTs.isAdjustedToUTC()) {
        useTableType = true;
      }
    }

    // Rule 2: requested is LONG (no logical type), table is local-timestamp
    if (reqLogical == null
        && requested.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64
        && tblLogical instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && !((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tblLogical).isAdjustedToUTC()) {
      useTableType = true;
    }

    if (useTableType) {
      return Types.primitive(table.getPrimitiveTypeName(), requested.getRepetition())
          .as(table.getLogicalTypeAnnotation())
          .named(requested.getName());
    } else {
      return requested;
    }
  }
}
