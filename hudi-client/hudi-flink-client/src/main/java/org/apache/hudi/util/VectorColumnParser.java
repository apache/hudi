/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Parses the {@code hoodie.vector.columns} table option and converts the configured top-level
 * Flink array fields into Hoodie VECTOR schema fields.
 *
 * <p>The option uses {@code colName[:dimension]} entries separated by commas, for example
 * {@code embedding:4,features:3,codes:4}. The dimension defaults to {@value #DEFAULT_VECTOR_DIMENSION}
 * when omitted. Column names are matched case-insensitively. The vector element type is inferred
 * from the Flink array element type: {@code ARRAY<FLOAT>}, {@code ARRAY<DOUBLE>}, or
 * {@code ARRAY<TINYINT>} map to FLOAT, DOUBLE, and INT8 respectively.
 *
 * <p>The parser validates the descriptor syntax (well-formed entries, no duplicate columns,
 * positive dimensions); existence of the referenced columns in the table schema is validated by
 * {@link HoodieSchemaConverter} during schema conversion.
 */
public class VectorColumnParser {

  public static final int DEFAULT_VECTOR_DIMENSION = 128;

  private VectorColumnParser() {
  }

  /**
   * Parses the {@code hoodie.vector.columns} descriptor string into a map from the
   * (lower-cased) column name to its vector dimension, preserving declaration order.
   *
   * @param vectorColumns comma-separated {@code colName[:dimension]} descriptors
   * @return map from normalized column name to dimension
   * @throws IllegalArgumentException if a descriptor is malformed, duplicated, or has a non-positive dimension
   */
  public static Map<String, Integer> parse(String vectorColumns) {
    Map<String, Integer> parsed = new LinkedHashMap<>();
    for (String rawEntry : vectorColumns.split(",")) {
      String entry = rawEntry.trim();
      if (entry.isEmpty()) {
        continue;
      }
      String[] parts = entry.split(":", -1);
      if (parts.length > 2 || parts[0].trim().isEmpty()) {
        throw new IllegalArgumentException(
            "Invalid VECTOR column descriptor '" + entry + "'. Expected format: columnName[:dimension].");
      }
      String columnName = parts[0].trim();
      String normalizedColumnName = columnName.toLowerCase(Locale.ROOT);
      if (parsed.containsKey(normalizedColumnName)) {
        throw new IllegalArgumentException("Duplicate VECTOR column descriptor for column: " + columnName);
      }
      int dimension = DEFAULT_VECTOR_DIMENSION;
      if (parts.length == 2) {
        String dimensionText = parts[1].trim();
        if (dimensionText.isEmpty()) {
          throw new IllegalArgumentException(
              "Invalid VECTOR column descriptor '" + entry + "'. Dimension must not be empty.");
        }
        try {
          dimension = Integer.parseInt(dimensionText);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid VECTOR dimension for column '" + columnName + "': " + dimensionText, e);
        }
      }
      if (dimension <= 0) {
        throw new IllegalArgumentException("VECTOR dimension must be positive for column '" + columnName + "': " + dimension);
      }
      parsed.put(normalizedColumnName, dimension);
    }
    return parsed;
  }

  /**
   * Converts a Flink array field into a Hoodie VECTOR schema field with the given dimension.
   *
   * @param fieldName the column name (for error messages)
   * @param fieldType the Flink field type, which must be an {@link ArrayType}
   * @param dimension the configured vector dimension
   * @return the corresponding VECTOR {@link HoodieSchema}, wrapped as nullable when the field is nullable
   */
  public static HoodieSchema convertVectorField(String fieldName, LogicalType fieldType, int dimension) {
    if (!(fieldType instanceof ArrayType)) {
      throw new IllegalArgumentException(
          "VECTOR column '" + fieldName + "' must be declared as ARRAY<FLOAT>, ARRAY<DOUBLE>, or ARRAY<TINYINT>, but got: "
              + fieldType.asSummaryString());
    }
    HoodieSchema.Vector.VectorElementType elementType = inferVectorElementType(fieldName, ((ArrayType) fieldType).getElementType());
    HoodieSchema vectorSchema = HoodieSchema.createVector(dimension, elementType);
    return fieldType.isNullable() ? HoodieSchema.createNullable(vectorSchema) : vectorSchema;
  }

  private static HoodieSchema.Vector.VectorElementType inferVectorElementType(String fieldName, LogicalType elementType) {
    switch (elementType.getTypeRoot()) {
      case FLOAT:
        return HoodieSchema.Vector.VectorElementType.FLOAT;
      case DOUBLE:
        return HoodieSchema.Vector.VectorElementType.DOUBLE;
      case TINYINT:
        return HoodieSchema.Vector.VectorElementType.INT8;
      default:
        throw new IllegalArgumentException(
            "VECTOR column '" + fieldName + "' must use ARRAY<FLOAT>, ARRAY<DOUBLE>, or ARRAY<TINYINT>, but got ARRAY<"
                + elementType.asSummaryString() + ">.");
    }
  }
}
