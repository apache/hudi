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

package org.apache.hudi.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.HoodieVectorUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities for reading Hoodie VECTOR columns into Flink {@link RowData}.
 *
 * <p>VECTOR columns are stored as fixed-length bytes in parquet/log files. The Flink reader
 * reads those physical bytes first and then decodes them into Flink array data according to
 * the vector element type declared in {@link HoodieSchema.Vector}.
 */
public final class VectorConversionUtils {

  private VectorConversionUtils() {
  }

  /**
   * Detects VECTOR columns in the selected read projection.
   *
   * <p>The returned map is keyed by the projected field ordinal, not the ordinal in the full
   * table schema. This matches the ordinal layout of the {@link RowData} emitted by the reader.
   *
   * @param fullFieldNames field names in the full query/table row type
   * @param selectedFields ordinals selected from {@code fullFieldNames}
   * @param tableSchema    hoodie table schema containing logical VECTOR type metadata
   * @return projected ordinal to vector schema information
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumns(
      String[] fullFieldNames,
      int[] selectedFields,
      HoodieSchema tableSchema) {
    Map<String, HoodieSchema.Vector> vectorFields = getVectorFields(tableSchema);
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new LinkedHashMap<>();
    for (int i = 0; i < selectedFields.length; i++) {
      HoodieSchema.Vector vector = vectorFields.get(fullFieldNames[selectedFields[i]].toLowerCase(Locale.ROOT));
      if (vector != null) {
        vectorColumnInfo.put(i, vector);
      }
    }
    return vectorColumnInfo;
  }

  /**
   * Rewrites VECTOR fields in a requested row data type to BYTES for parquet reads.
   *
   * <p>Parquet stores VECTOR values as fixed-length byte arrays. This method keeps the requested
   * row shape unchanged, but asks the physical parquet reader to materialize VECTOR columns as
   * bytes. The resulting rows should be passed through
   * {@link #wrapVectorColumnIterator(ClosableIterator, RowType, Map)} to decode the bytes into
   * array data.
   *
   * @param dataType         requested row data type
   * @param requestedSchema  requested Hudi schema
   * @param vectorColumnInfo projected VECTOR column metadata
   * @return row data type to use for the physical parquet read
   */
  public static DataType getParquetReadDataType(
      DataType dataType,
      HoodieSchema requestedSchema,
      Map<Integer, HoodieSchema.Vector> vectorColumnInfo) {
    if (vectorColumnInfo.isEmpty()) {
      return dataType;
    }

    RowType rowType = (RowType) dataType.getLogicalType();
    List<RowType.RowField> readFields = new ArrayList<>(rowType.getFields().size());
    for (RowType.RowField field : rowType.getFields()) {
      readFields.add(field.copy());
    }
    for (Integer requestedOrdinal : vectorColumnInfo.keySet()) {
      String fieldName = requestedSchema.getFields().get(requestedOrdinal).name();
      int fieldOrdinal = rowType.getFieldIndex(fieldName);
      if (fieldOrdinal >= 0) {
        RowType.RowField field = readFields.get(fieldOrdinal);
        readFields.set(fieldOrdinal, newRowField(field, bytesType(field.getType())));
      }
    }
    return DataTypes.of(new RowType(rowType.isNullable(), readFields));
  }

  /**
   * Rewrites VECTOR field types in the full field type array to BYTES for parquet reads.
   *
   * <p>This variant is used by copy-on-write input format code paths that keep the full field
   * type array and apply projection separately.
   *
   * @param fullFieldNames field names in the full query/table row type
   * @param fullFieldTypes field types corresponding to {@code fullFieldNames}
   * @param tableSchema    hoodie table schema containing logical VECTOR type metadata
   * @return field types to use for the physical parquet read
   */
  public static DataType[] getParquetReadFieldTypes(
      String[] fullFieldNames,
      DataType[] fullFieldTypes,
      HoodieSchema tableSchema) {
    Map<String, HoodieSchema.Vector> vectorFields = getVectorFields(tableSchema);
    if (vectorFields.isEmpty()) {
      return fullFieldTypes;
    }
    DataType[] readFieldTypes = Arrays.copyOf(fullFieldTypes, fullFieldTypes.length);
    for (int i = 0; i < fullFieldNames.length; i++) {
      if (vectorFields.containsKey(fullFieldNames[i].toLowerCase(Locale.ROOT))) {
        readFieldTypes[i] = DataTypes.of(bytesType(fullFieldTypes[i].getLogicalType()));
      }
    }
    return readFieldTypes;
  }

  /**
   * Wraps a row iterator and decodes VECTOR byte values into Flink array values.
   *
   * <p>{@code rowType} must describe the physical rows emitted by {@code rowDataItr}, where VECTOR
   * columns have already been read as BYTES. Non-vector fields are copied with type-aware Flink
   * field getters to preserve their original representation.
   *
   * @param rowDataItr       physical row iterator
   * @param rowType          row type of the physical rows emitted by {@code rowDataItr}
   * @param vectorColumnInfo projected VECTOR column metadata keyed by row ordinal
   * @return iterator emitting rows with VECTOR columns decoded as arrays
   */
  public static ClosableIterator<RowData> wrapVectorColumnIterator(
      ClosableIterator<RowData> rowDataItr,
      RowType rowType,
      Map<Integer, HoodieSchema.Vector> vectorColumnInfo) {
    RowData.FieldGetter[] fieldGetters = createFieldGetters(rowType);
    return new CloseableMappingIterator<>(
        rowDataItr, rowData -> convertVectorColumns(rowData, fieldGetters, vectorColumnInfo));
  }

  /**
   * Wraps a projected row iterator and decodes VECTOR byte values into Flink array values.
   *
   * <p>The selected physical row type is derived from {@code fullFieldTypes} and
   * {@code selectedFields}, then delegated to {@link #wrapVectorColumnIterator(ClosableIterator, RowType, Map)}.
   *
   * @param rowDataItr       physical row iterator
   * @param fullFieldTypes   field types before projection
   * @param selectedFields   ordinals selected from {@code fullFieldTypes}
   * @param vectorColumnInfo projected VECTOR column metadata keyed by row ordinal
   * @return iterator emitting rows with VECTOR columns decoded as arrays
   */
  public static ClosableIterator<RowData> wrapVectorColumnIterator(
      ClosableIterator<RowData> rowDataItr,
      DataType[] fullFieldTypes,
      int[] selectedFields,
      Map<Integer, HoodieSchema.Vector> vectorColumnInfo) {
    return wrapVectorColumnIterator(rowDataItr, createRowType(fullFieldTypes, selectedFields), vectorColumnInfo);
  }

  /**
   * Returns VECTOR fields keyed by lower-cased field name.
   */
  private static Map<String, HoodieSchema.Vector> getVectorFields(HoodieSchema tableSchema) {
    return tableSchema.getFields().stream()
        .filter(field -> field.schema().getNonNullType().getType() == HoodieSchemaType.VECTOR)
        .collect(Collectors.toMap(
            field -> field.name().toLowerCase(Locale.ROOT),
            field -> (HoodieSchema.Vector) field.schema().getNonNullType()));
  }

  /**
   * Creates a BYTES logical type while preserving the original nullability.
   */
  private static LogicalType bytesType(LogicalType originalType) {
    return DataTypes.BYTES().getLogicalType().copy(originalType.isNullable());
  }

  /**
   * Creates a row field with a replacement logical type while preserving metadata.
   */
  private static RowType.RowField newRowField(RowType.RowField field, LogicalType type) {
    return field.getDescription()
        .map(description -> new RowType.RowField(field.getName(), type, description))
        .orElseGet(() -> new RowType.RowField(field.getName(), type));
  }

  /**
   * Converts VECTOR columns in one row from physical bytes to Flink array data.
   */
  private static RowData convertVectorColumns(
      RowData rowData,
      RowData.FieldGetter[] fieldGetters,
      Map<Integer, HoodieSchema.Vector> vectorColumnInfo) {
    GenericRowData converted = new GenericRowData(rowData.getArity());
    converted.setRowKind(rowData.getRowKind());
    for (int i = 0; i < rowData.getArity(); i++) {
      if (rowData.isNullAt(i)) {
        converted.setField(i, null);
      } else if (vectorColumnInfo.containsKey(i)) {
        converted.setField(i, createVectorArrayData(rowData.getBinary(i), vectorColumnInfo.get(i)));
      } else {
        converted.setField(i, fieldGetters[i].getFieldOrNull(rowData));
      }
    }
    return converted;
  }

  /**
   * Creates type-aware field getters for the physical row type.
   */
  private static RowData.FieldGetter[] createFieldGetters(RowType rowType) {
    RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[rowType.getFieldCount()];
    for (int i = 0; i < fieldGetters.length; i++) {
      fieldGetters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
    }
    return fieldGetters;
  }

  /**
   * Builds a projected row type from full field types and selected ordinals.
   */
  private static RowType createRowType(DataType[] fullFieldTypes, int[] selectedFields) {
    List<RowType.RowField> rowFields = new ArrayList<>(selectedFields.length);
    for (int i = 0; i < selectedFields.length; i++) {
      rowFields.add(new RowType.RowField(String.valueOf(i), fullFieldTypes[selectedFields[i]].getLogicalType()));
    }
    return new RowType(rowFields);
  }

  /**
   * Decodes raw vector bytes and wraps the decoded primitive vector array as Flink array data.
   */
  public static GenericArrayData createVectorArrayData(byte[] bytes, HoodieSchema.Vector vectorSchema) {
    Object vectorArray = HoodieVectorUtils.decodeVectorBytes(bytes, vectorSchema);
    if (vectorArray instanceof float[]) {
      return new GenericArrayData((float[]) vectorArray);
    } else if (vectorArray instanceof double[]) {
      return new GenericArrayData((double[]) vectorArray);
    } else if (vectorArray instanceof byte[]) {
      return new GenericArrayData((byte[]) vectorArray);
    }
    throw new UnsupportedOperationException("Unsupported decoded vector array type: " + vectorArray.getClass());
  }

  /**
   * Encodes Flink array data into the canonical Hudi VECTOR fixed-bytes representation.
   */
  public static byte[] encodeVectorArrayData(ArrayData arrayData, HoodieSchema.Vector vectorSchema) {
    int dimension = vectorSchema.getDimension();
    HoodieSchema.Vector.VectorElementType elementType = vectorSchema.getVectorElementType();
    ValidationUtils.checkArgument(arrayData.size() == dimension,
        () -> "Vector dimension mismatch: schema expects " + dimension + " elements but got " + arrayData.size());
    int bufferSize = Math.multiplyExact(dimension, elementType.getElementSize());
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    switch (elementType) {
      case FLOAT:
        for (int i = 0; i < dimension; i++) {
          buffer.putFloat(arrayData.getFloat(i));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < dimension; i++) {
          buffer.putDouble(arrayData.getDouble(i));
        }
        break;
      case INT8:
        for (int i = 0; i < dimension; i++) {
          buffer.put(arrayData.getByte(i));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported VECTOR element type: " + elementType);
    }
    return buffer.array();
  }

  /**
   * Validates that a Flink ARRAY logical type uses the element type required by the VECTOR schema.
   *
   * @param vectorSchema VECTOR schema defining the expected element type
   * @param type         Flink ARRAY logical type to validate
   * @throws SchemaCompatibilityException if the ARRAY element type does not match the VECTOR element type
   */
  public static void validateVectorLogicalType(HoodieSchema.Vector vectorSchema, LogicalType type) {
    LogicalTypeRoot elementTypeRoot = ((ArrayType) type).getElementType().getTypeRoot();
    LogicalTypeRoot expectedElementTypeRoot = expectedVectorElementTypeRoot(vectorSchema.getVectorElementType());
    if (elementTypeRoot != expectedElementTypeRoot) {
      throw new SchemaCompatibilityException(
          "VECTOR element type "
              + vectorSchema.getVectorElementType()
              + " must be converted to Flink ARRAY<"
              + expectedElementTypeRoot
              + ">, but got ARRAY<"
              + elementTypeRoot
              + ">.");
    }
  }

  private static LogicalTypeRoot expectedVectorElementTypeRoot(HoodieSchema.Vector.VectorElementType elementType) {
    switch (elementType) {
      case FLOAT:
        return LogicalTypeRoot.FLOAT;
      case DOUBLE:
        return LogicalTypeRoot.DOUBLE;
      case INT8:
        return LogicalTypeRoot.TINYINT;
      default:
        throw new UnsupportedOperationException("Unsupported VECTOR element type: " + elementType);
    }
  }
}
