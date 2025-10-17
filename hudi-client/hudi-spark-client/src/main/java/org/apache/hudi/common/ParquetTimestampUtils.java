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

package org.apache.hudi.common;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for handling Parquet timestamp precision conversions.
 */
public class ParquetTimestampUtils {

  /**
   * Identifies columns that need multiplication by 1000 when converting from TIMESTAMP_MICROS to TIMESTAMP_MILLIS.
   *
   * This method compares two Parquet schemas and identifies fields where:
   * - The file schema has TIMESTAMP_MICROS precision
   * - The table schema has TIMESTAMP_MILLIS precision
   *
   * @param fileSchema The Parquet schema from the file (source)
   * @param tableSchema The Parquet schema from the table (target)
   * @return Set of column paths (e.g., "timestamp", "metadata.created_at") that need multiplication
   */
  public static Set<String> findColumnsToMultiply(MessageType fileSchema, MessageType tableSchema) {
    Set<String> columnsToMultiply = new HashSet<>();
    compareTypes(fileSchema, tableSchema, "", columnsToMultiply);
    return columnsToMultiply;
  }

  /**
   * Recursively compares two Parquet types and identifies timestamp precision mismatches.
   *
   * @param fileType The type from the file schema
   * @param tableType The type from the table schema
   * @param path The current column path (dotted notation for nested fields)
   * @param columnsToMultiply Set to accumulate columns that need multiplication
   */
  private static void compareTypes(Type fileType, Type tableType, String path, Set<String> columnsToMultiply) {
    // Handle group types (structs)
    if (!fileType.isPrimitive() && !tableType.isPrimitive()) {
      GroupType fileGroup = fileType.asGroupType();
      GroupType tableGroup = tableType.asGroupType();

      List<Type> fileFields = fileGroup.getFields();

      for (Type fileField : fileFields) {
        String fieldName = fileField.getName();

        // Check if field exists in table schema
        if (tableGroup.containsField(fieldName)) {
          Type tableField = tableGroup.getType(fieldName);
          String nestedPath = path.isEmpty() ? fieldName : path + "." + fieldName;
          compareTypes(fileField, tableField, nestedPath, columnsToMultiply);
        }
        // If field doesn't exist in table, skip it
      }
      return;
    }

    // Handle primitive types
    if (fileType.isPrimitive() && tableType.isPrimitive()) {
      if (isTimestampMicros(fileType) && isTimestampMillis(tableType)) {
        columnsToMultiply.add(path);
      } else if (isLong(fileType) && isLocalTimestampMillis(tableType)) {
        columnsToMultiply.add(path);
      }
    }

    // Type mismatch (one primitive, one group) - skip
  }

  /**
   * Checks if a Parquet type is TIMESTAMP_MICROS.
   *
   * @param parquetType The Parquet type to check
   * @return true if the type is TIMESTAMP_MICROS, false otherwise
   */
  private static boolean isTimestampMicros(Type parquetType) {
    if (!parquetType.isPrimitive()) {
      return false;
    }

    PrimitiveType primitiveType = parquetType.asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
      return timestampType.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS;
    }

    return false;
  }

  /**
   * Checks if a Parquet type is TIMESTAMP_MILLIS.
   *
   * @param parquetType The Parquet type to check
   * @return true if the type is TIMESTAMP_MILLIS, false otherwise
   */
  private static boolean isTimestampMillis(Type parquetType) {
    if (!parquetType.isPrimitive()) {
      return false;
    }

    PrimitiveType primitiveType = parquetType.asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
      return timestampType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS;
    }

    return false;
  }

  private static boolean isLocalTimestampMillis(Type parquetType) {
    if (!parquetType.isPrimitive()) {
      return false;
    }

    PrimitiveType primitiveType = parquetType.asPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
      LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampType =
          (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType;
      return timestampType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS
          && !timestampType.isAdjustedToUTC();
    }

    return false;
  }

  private static boolean isLong(Type parquetType) {
    if (!parquetType.isPrimitive()) {
      return false;
    }

    PrimitiveType primitiveType = parquetType.asPrimitiveType();

    return primitiveType.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64
        && primitiveType.getLogicalTypeAnnotation() == null;
  }
}
