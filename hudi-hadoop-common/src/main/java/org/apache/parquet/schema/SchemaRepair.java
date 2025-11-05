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

import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import java.util.ArrayList;
import java.util.List;

public class SchemaRepair {

  public static MessageType repairLogicalTypes(MessageType fileSchema, Option<MessageType> tableSchema) {
    if (tableSchema.isEmpty()) {
      return fileSchema;
    }
    return repairLogicalTypes(fileSchema, tableSchema.get());
  }

  static MessageType repairLogicalTypes(MessageType fileSchema, MessageType tableSchema) {
    List<Type> repairedFields = repairFields(fileSchema.getFields(), tableSchema);

    // If nothing changed, return the original schema
    if (repairedFields == null) {
      return fileSchema;
    }

    return new MessageType(fileSchema.getName(), repairedFields);
  }

  /**
   * Repairs a list of fields against a table schema (MessageType or GroupType).
   * Returns null if no changes were made, otherwise returns the repaired field list.
   */
  private static List<Type> repairFields(List<Type> fileSchemaFields, GroupType tableSchema) {
    // First pass: find the first field that changes
    int firstChangedIndex = -1;
    Type firstRepairedField = null;

    for (int i = 0; i < fileSchemaFields.size(); i++) {
      Type requestedField = fileSchemaFields.get(i);
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
    List<Type> repairedFields = new ArrayList<>(fileSchemaFields.size());

    // Copy all fields before the first changed field
    for (int i = 0; i < firstChangedIndex; i++) {
      repairedFields.add(fileSchemaFields.get(i));
    }

    // Add the first changed field (using cached repaired field)
    repairedFields.add(firstRepairedField);

    // Process remaining fields
    for (int i = firstChangedIndex + 1; i < fileSchemaFields.size(); i++) {
      Type fileSchemaField = fileSchemaFields.get(i);
      Type repaired = fileSchemaField;
      if (tableSchema.containsField(fileSchemaField.getName())) {
        Type tableSchemaField = tableSchema.getType(fileSchemaField.getName());
        repaired = repairField(fileSchemaField, tableSchemaField);
      }
      repairedFields.add(repaired);
    }

    return repairedFields;
  }

  private static Type repairField(Type fileSchemaFieldType, Type tableSchemaFieldType) {
    if (fileSchemaFieldType.isPrimitive() && tableSchemaFieldType.isPrimitive()) {
      return repairPrimitiveType(fileSchemaFieldType.asPrimitiveType(), tableSchemaFieldType.asPrimitiveType());
    } else if (!fileSchemaFieldType.isPrimitive() && !tableSchemaFieldType.isPrimitive()) {
      // recurse into nested structs
      GroupType reqGroup = fileSchemaFieldType.asGroupType();
      GroupType tblGroup = tableSchemaFieldType.asGroupType();

      // Repair fields directly without creating MessageType intermediaries
      List<Type> repairedFields = repairFields(reqGroup.getFields(), tblGroup);

      // If nothing changed, return the original field
      if (repairedFields == null) {
        return fileSchemaFieldType;
      }

      return new GroupType(
          reqGroup.getRepetition(),
          reqGroup.getName(),
          reqGroup.getLogicalTypeAnnotation(),
          repairedFields
      );
    } else {
      // fallback: keep requested
      return fileSchemaFieldType;
    }
  }

  private static PrimitiveType repairPrimitiveType(PrimitiveType fileSchemaPrimitiveType, PrimitiveType tableSchemaPrimitiveType) {
    // Quick check if repair is needed (no allocations)
    if (needsLogicalTypeRepair(fileSchemaPrimitiveType, tableSchemaPrimitiveType)) {
      return Types.primitive(tableSchemaPrimitiveType.getPrimitiveTypeName(), fileSchemaPrimitiveType.getRepetition())
          .as(tableSchemaPrimitiveType.getLogicalTypeAnnotation())
          .named(fileSchemaPrimitiveType.getName());
    }
    return fileSchemaPrimitiveType;
  }

  /**
   * Quick check if a logical type repair is needed (no allocations).
   */
  private static boolean needsLogicalTypeRepair(PrimitiveType fileSchemaPrimitiveType, PrimitiveType tableSchemaPrimitiveType) {
    if (fileSchemaPrimitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64
        || tableSchemaPrimitiveType.getPrimitiveTypeName() != PrimitiveType.PrimitiveTypeName.INT64) {
      return false;
    }
    LogicalTypeAnnotation fileLogicalTypeAnnotation = fileSchemaPrimitiveType.getLogicalTypeAnnotation();
    LogicalTypeAnnotation tableLogicalTypeAnnotation = tableSchemaPrimitiveType.getLogicalTypeAnnotation();

    // if requested has no logical type, and the table has a local timestamp, then we need to repair
    if (fileLogicalTypeAnnotation == null) {
      return tableLogicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
          && !((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tableLogicalTypeAnnotation).isAdjustedToUTC();
    }

    // if requested is timestamp-micros and table is timestamp-millis then we need to repair
    return fileLogicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && tableLogicalTypeAnnotation instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) fileLogicalTypeAnnotation).getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tableLogicalTypeAnnotation).getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) fileLogicalTypeAnnotation).isAdjustedToUTC()
        && ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) tableLogicalTypeAnnotation).isAdjustedToUTC();
  }

  /**
   * Repairs the Parquet footer schema if needed.
   *
   * @param original        The original Parquet metadata
   * @param tableSchemaOpt  Optional table schema for logical type repair
   * @return Repaired Parquet metadata with updated schema
   */
  public static ParquetMetadata repairFooterSchema(
      ParquetMetadata original,
      Option<org.apache.parquet.schema.MessageType> tableSchemaOpt) {

    org.apache.parquet.schema.MessageType repairedSchema =
        SchemaRepair.repairLogicalTypes(
            original.getFileMetaData().getSchema(),
            tableSchemaOpt
        );

    FileMetaData oldMeta = original.getFileMetaData();

    FileMetaData newMeta = new FileMetaData(
        repairedSchema,
        oldMeta.getKeyValueMetaData(),
        oldMeta.getCreatedBy(),
        oldMeta.getEncryptionType(),
        oldMeta.getFileDecryptor()
    );

    return new ParquetMetadata(newMeta, original.getBlocks());
  }
}
