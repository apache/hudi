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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AvroSchemaRepair {
  public static Schema repairLogicalTypes(Schema requestedSchema, Schema tableSchema) {
    if (requestedSchema.getType() != Schema.Type.RECORD) {
      return requestedSchema;
    }

    List<Schema.Field> repairedFields = new ArrayList<>();

    for (Schema.Field requestedField : requestedSchema.getFields()) {
      Schema.Field tableField = tableSchema.getField(requestedField.name());
      Schema.Field repaired;
      if (tableField != null) {
        repaired = repairAvroField(requestedField, tableField);
      } else {
        repaired = new Schema.Field(requestedField.name(), requestedField.schema(), requestedField.doc(), requestedField.defaultVal());
      }
      repairedFields.add(repaired);
    }

    return Schema.createRecord(
        requestedSchema.getName(),
        requestedSchema.getDoc(),
        requestedSchema.getNamespace(),
        requestedSchema.isError(),
        repairedFields
    );
  }

  private static Schema.Field repairAvroField(Schema.Field requested, Schema.Field table) {
    Schema repairedSchema = repairAvroSchema(requested.schema(), table.schema());

    return new Schema.Field(
        requested.name(),
        repairedSchema,
        requested.doc(),
        requested.defaultVal(),
        requested.order()
    );
  }

  private static Schema repairAvroSchema(Schema requested, Schema table) {
    // Handle union types (nullable fields)
    if (requested.getType() == Schema.Type.UNION) {
      List<Schema> repairedUnionTypes = new ArrayList<>();
      for (Schema unionType : requested.getTypes()) {
        if (unionType.getType() == Schema.Type.NULL) {
          repairedUnionTypes.add(unionType);
        } else {
          // Find corresponding non-null type in table schema
          Schema tableNonNull = table;
          if (table.getType() == Schema.Type.UNION) {
            for (Schema tableUnionType : table.getTypes()) {
              if (tableUnionType.getType() != Schema.Type.NULL) {
                tableNonNull = tableUnionType;
                break;
              }
            }
          }
          repairedUnionTypes.add(repairAvroSchema(unionType, tableNonNull));
        }
      }
      return Schema.createUnion(repairedUnionTypes);
    }

    // Handle record types (nested structs)
    if (requested.getType() == Schema.Type.RECORD && table.getType() == Schema.Type.RECORD) {
      return repairLogicalTypes(requested, table);
    }

    // Handle array types
    if (requested.getType() == Schema.Type.ARRAY && table.getType() == Schema.Type.ARRAY) {
      Schema repairedElementSchema = repairAvroSchema(requested.getElementType(), table.getElementType());
      return Schema.createArray(repairedElementSchema);
    }

    // Handle map types
    if (requested.getType() == Schema.Type.MAP && table.getType() == Schema.Type.MAP) {
      Schema repairedValueSchema = repairAvroSchema(requested.getValueType(), table.getValueType());
      return Schema.createMap(repairedValueSchema);
    }

    // Handle primitive types with logical types
    if (isPrimitiveType(requested) && isPrimitiveType(table)) {
      return repairAvroLogicalType(requested, table);
    }

    // Default: return requested schema
    return requested;
  }

  private static boolean isPrimitiveType(Schema schema) {
    Schema.Type type = schema.getType();
    return type == Schema.Type.INT || type == Schema.Type.LONG
        || type == Schema.Type.FLOAT || type == Schema.Type.DOUBLE
        || type == Schema.Type.BOOLEAN || type == Schema.Type.STRING
        || type == Schema.Type.BYTES;
  }

  private static Schema repairAvroLogicalType(Schema requested, Schema table) {
    LogicalType reqLogical = requested.getLogicalType();
    LogicalType tblLogical = table.getLogicalType();

    boolean useTableType = false;

    // Rule 1: requested is timestamp-micros, table is timestamp-millis
    if (reqLogical instanceof LogicalTypes.TimestampMicros
        && tblLogical instanceof LogicalTypes.TimestampMillis) {
      useTableType = true;
    }

    // Rule 2: requested is LONG (no logical type), table is local-timestamp-millis
    if (reqLogical == null
        && requested.getType() == Schema.Type.LONG
        && tblLogical instanceof LogicalTypes.LocalTimestampMillis) {
      useTableType = true;
    }

    // Rule 3: requested is LONG (no logical type), table is local-timestamp-micros
    if (reqLogical == null
        && requested.getType() == Schema.Type.LONG
        && tblLogical instanceof LogicalTypes.LocalTimestampMicros) {
      useTableType = true;
    }

    if (useTableType) {
      // Create a new schema with the table's logical type
      Schema repaired = Schema.create(table.getType());
      if (tblLogical != null) {
        tblLogical.addToSchema(repaired);
      }
      return repaired;
    } else {
      return requested;
    }
  }
}
