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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Shared utility methods for vector column handling during Parquet read/write.
 *
 * Vectors are stored as Parquet FIXED_LEN_BYTE_ARRAY columns. On read, Spark maps these
 * to BinaryType. This class provides the canonical conversion between the binary
 * representation and Spark's typed ArrayData (float[], double[], byte[]).
 *
 * All byte buffers use little-endian order ({@link HoodieSchema.VectorLogicalType#VECTOR_BYTE_ORDER})
 * for compatibility with common vector search libraries (FAISS, ScaNN, etc.) and to match
 * native x86/ARM byte order for zero-copy reads.
 */
public final class VectorConversionUtils {

  private VectorConversionUtils() {
  }

  /**
   * Detects VECTOR columns in a HoodieSchema record and returns a map of field ordinal
   * to the corresponding {@link HoodieSchema.Vector} schema.
   *
   * @param schema a HoodieSchema of type RECORD (or null)
   * @return map from field index to Vector schema; empty map if schema is null or has no vectors
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumns(HoodieSchema schema) {
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new HashMap<>();
    if (schema == null) {
      return vectorColumnInfo;
    }
    List<HoodieSchemaField> fields = schema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchema fieldSchema = fields.get(i).schema().getNonNullType();
      if (fieldSchema.getType() == HoodieSchemaType.VECTOR) {
        vectorColumnInfo.put(i, (HoodieSchema.Vector) fieldSchema);
      }
    }
    return vectorColumnInfo;
  }

  /**
   * Detects VECTOR columns from Spark StructType metadata annotations.
   * Fields with metadata key {@link HoodieSchema#TYPE_METADATA_FIELD} starting with "VECTOR"
   * are parsed and included.
   *
   * @param schema Spark StructType
   * @return map from field index to Vector schema; empty map if no vectors found
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumnsFromMetadata(StructType schema) {
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new HashMap<>();
    if (schema == null) {
      return vectorColumnInfo;
    }
    StructField[] fields = schema.fields();
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)) {
        String typeStr = field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
        if (typeStr.startsWith("VECTOR")) {
          HoodieSchema parsed = HoodieSchema.parseTypeDescriptor(typeStr);
          if (parsed.getType() == HoodieSchemaType.VECTOR) {
            vectorColumnInfo.put(i, (HoodieSchema.Vector) parsed);
          }
        }
      }
    }
    return vectorColumnInfo;
  }

  /**
   * Replaces ArrayType with BinaryType for VECTOR columns so the Parquet reader
   * can read FIXED_LEN_BYTE_ARRAY data without type mismatch.
   *
   * @param structType     the original Spark schema
   * @param vectorColumns  map of ordinal to vector info (only the key set is used)
   * @return a new StructType with vector columns replaced by BinaryType
   */
  public static StructType replaceVectorColumnsWithBinary(StructType structType, Map<Integer, ?> vectorColumns) {
    StructField[] fields = structType.fields();
    StructField[] newFields = new StructField[fields.length];
    for (int i = 0; i < fields.length; i++) {
      if (vectorColumns.containsKey(i)) {
        newFields[i] = new StructField(fields[i].name(), BinaryType$.MODULE$, fields[i].nullable(), Metadata.empty());
      } else {
        newFields[i] = fields[i];
      }
    }
    return new StructType(newFields);
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array
   * based on the vector's element type and dimension.
   *
   * @param bytes        raw bytes read from Parquet
   * @param vectorSchema the vector schema describing dimension and element type
   * @return a GenericArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static GenericArrayData convertBinaryToVectorArray(byte[] bytes, HoodieSchema.Vector vectorSchema) {
    return convertBinaryToVectorArray(bytes, vectorSchema.getDimension(), vectorSchema.getVectorElementType());
  }

  /**
   * Convenience overload for Catalyst expression invocation paths that pass the element type
   * as a string literal.
   */
  public static GenericArrayData convertBinaryToVectorArray(byte[] bytes, int dim, String elemTypeName) {
    if (bytes == null) {
      return null;
    }
    String normalized = elemTypeName == null ? "FLOAT" : elemTypeName.toUpperCase();
    int elementSize;
    switch (normalized) {
      case "FLOAT":
        elementSize = Float.BYTES;
        break;
      case "DOUBLE":
        elementSize = Double.BYTES;
        break;
      case "INT8":
        elementSize = 1;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported vector element type: " + elemTypeName);
    }

    int expectedSize = dim * elementSize;
    checkArgument(bytes.length == expectedSize,
        "Vector byte array length mismatch: expected " + expectedSize + " but got " + bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    switch (normalized) {
      case "FLOAT":
        float[] floats = new float[dim];
        for (int j = 0; j < dim; j++) {
          floats[j] = buffer.getFloat();
        }
        return new GenericArrayData(floats);
      case "DOUBLE":
        double[] doubles = new double[dim];
        for (int j = 0; j < dim; j++) {
          doubles[j] = buffer.getDouble();
        }
        return new GenericArrayData(doubles);
      case "INT8":
        byte[] int8s = new byte[dim];
        buffer.get(int8s);
        return new GenericArrayData(int8s);
      default:
        throw new UnsupportedOperationException("Unsupported vector element type: " + elemTypeName);
    }
  }

  /**
   * Overload for Spark Catalyst expression evaluation, which passes string literals as UTF8String.
   */
  public static GenericArrayData convertBinaryToVectorArray(byte[] bytes, int dim, UTF8String elemTypeName) {
    return convertBinaryToVectorArray(bytes, dim, elemTypeName == null ? null : elemTypeName.toString());
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array.
   *
   * @param bytes    raw bytes read from Parquet
   * @param dim      vector dimension (number of elements)
   * @param elemType element type (FLOAT, DOUBLE, or INT8)
   * @return a GenericArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static GenericArrayData convertBinaryToVectorArray(byte[] bytes, int dim,
                                                            HoodieSchema.Vector.VectorElementType elemType) {
    if (bytes == null) {
      return null;
    }
    int expectedSize = dim * elemType.getElementSize();
    checkArgument(bytes.length == expectedSize,
        "Vector byte array length mismatch: expected " + expectedSize + " but got " + bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    switch (elemType) {
      case FLOAT:
        float[] floats = new float[dim];
        for (int j = 0; j < dim; j++) {
          floats[j] = buffer.getFloat();
        }
        return new GenericArrayData(floats);
      case DOUBLE:
        double[] doubles = new double[dim];
        for (int j = 0; j < dim; j++) {
          doubles[j] = buffer.getDouble();
        }
        return new GenericArrayData(doubles);
      case INT8:
        byte[] int8s = new byte[dim];
        buffer.get(int8s);
        return new GenericArrayData(int8s);
      default:
        throw new UnsupportedOperationException(
            "Unsupported vector element type: " + elemType);
    }
  }

  /**
   * Converts a Spark array-backed vector value into the fixed-length binary representation
   * used by Parquet/Avro VECTOR logical types.
   *
   * @param array        Spark array data containing vector elements
   * @param vectorSchema the vector schema describing dimension and element type
   * @return packed little-endian bytes matching the VECTOR fixed-size backing
   */
  public static byte[] convertVectorArrayToBinary(ArrayData array, HoodieSchema.Vector vectorSchema) {
    return convertVectorArrayToBinary(array, vectorSchema.getDimension(), vectorSchema.getVectorElementType());
  }

  /**
   * Converts a Spark array-backed vector value into packed bytes.
   *
   * @param array    Spark array data containing vector elements
   * @param dim      vector dimension (number of elements)
   * @param elemType element type (FLOAT, DOUBLE, or INT8)
   * @return packed little-endian bytes matching the VECTOR fixed-size backing
   */
  public static byte[] convertVectorArrayToBinary(ArrayData array, int dim,
                                                  HoodieSchema.Vector.VectorElementType elemType) {
    checkArgument(array.numElements() == dim,
        "Vector dimension mismatch: expected " + dim + " but got " + array.numElements());
    ByteBuffer buffer = ByteBuffer.allocate(dim * elemType.getElementSize())
        .order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    switch (elemType) {
      case FLOAT:
        for (int i = 0; i < dim; i++) {
          buffer.putFloat(array.getFloat(i));
        }
        break;
      case DOUBLE:
        for (int i = 0; i < dim; i++) {
          buffer.putDouble(array.getDouble(i));
        }
        break;
      case INT8:
        for (int i = 0; i < dim; i++) {
          buffer.put(array.getByte(i));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported vector element type: " + elemType);
    }
    return buffer.array();
  }

  /**
   * Returns a {@link Function} that converts a single {@link InternalRow} by converting binary
   * vector columns back to typed arrays and then applying the given projection callback.
   *
   * <p>Ordinals in {@code vectorColumns} must be relative to {@code readSchema} — the schema
   * that has {@code BinaryType} for vector columns (as produced by
   * {@link #replaceVectorColumnsWithBinary}).
   *
   * <p><b>Thread safety:</b> The returned function is NOT thread-safe; it reuses a single
   * {@link GenericInternalRow} buffer across calls. Each call to this factory creates its own
   * buffer, so separate functions returned by separate calls are independent.
   *
   * @param readSchema         the Spark schema of incoming rows (BinaryType for vector columns)
   * @param vectorColumns      map of ordinal → Vector schema for vector columns, keyed by
   *                           ordinals relative to {@code readSchema}
   * @param projectionCallback called with the converted {@link GenericInternalRow}; must copy
   *                           any data it needs to retain (e.g. {@code UnsafeProjection::apply})
   * @return a function that converts one row and returns the projected result
   */
  public static Function<InternalRow, InternalRow> buildRowMapper(
      StructType readSchema,
      Map<Integer, HoodieSchema.Vector> vectorColumns,
      Function<GenericInternalRow, InternalRow> projectionCallback) {
    GenericInternalRow converted = new GenericInternalRow(readSchema.fields().length);
    return row -> {
      convertRowVectorColumns(row, converted, readSchema, vectorColumns);
      return projectionCallback.apply(converted);
    };
  }

  /**
   * Returns a {@link Function} that converts vector columns from Spark arrays into binary values
   * suitable for Avro/Parquet fixed-length VECTOR serialization.
   *
   * <p>Ordinals in {@code vectorColumns} must be relative to {@code sourceSchema} where vector
   * columns are represented as Spark arrays.
   */
  public static Function<InternalRow, InternalRow> buildBinaryRowMapper(
      StructType sourceSchema,
      Map<Integer, HoodieSchema.Vector> vectorColumns,
      Function<GenericInternalRow, InternalRow> projectionCallback) {
    GenericInternalRow converted = new GenericInternalRow(sourceSchema.fields().length);
    return row -> {
      convertRowVectorColumnsToBinary(row, converted, sourceSchema, vectorColumns);
      return projectionCallback.apply(converted);
    };
  }

  /**
   * Converts vector columns in a row from binary (BinaryType) back to typed arrays,
   * copying non-vector columns as-is. The caller must supply a pre-allocated
   * {@link GenericInternalRow} for reuse across iterations to reduce GC pressure.
   *
   * @param row           the source row (with BinaryType for vector columns)
   * @param result        a pre-allocated GenericInternalRow to write into (reused across calls)
   * @param readSchema    the Spark schema of the source row (BinaryType for vector columns)
   * @param vectorColumns map of ordinal to Vector schema for vector columns
   */
  public static void convertRowVectorColumns(InternalRow row, GenericInternalRow result,
                                             StructType readSchema,
                                             Map<Integer, HoodieSchema.Vector> vectorColumns) {
    int numFields = readSchema.fields().length;
    for (int i = 0; i < numFields; i++) {
      if (row.isNullAt(i)) {
        result.setNullAt(i);
      } else if (vectorColumns.containsKey(i)) {
        result.update(i, convertBinaryToVectorArray(row.getBinary(i), vectorColumns.get(i)));
      } else {
        // Non-vector column: copy value as-is using the read schema's data type
        result.update(i, row.get(i, readSchema.apply(i).dataType()));
      }
    }
  }

  /**
   * Converts vector columns in a row from Spark arrays into binary values, copying non-vector
   * columns as-is. The caller must supply a pre-allocated {@link GenericInternalRow} for reuse
   * across iterations to reduce GC pressure.
   */
  public static void convertRowVectorColumnsToBinary(InternalRow row, GenericInternalRow result,
                                                     StructType sourceSchema,
                                                     Map<Integer, HoodieSchema.Vector> vectorColumns) {
    int numFields = sourceSchema.fields().length;
    for (int i = 0; i < numFields; i++) {
      if (row.isNullAt(i)) {
        result.setNullAt(i);
      } else if (vectorColumns.containsKey(i)) {
        result.update(i, convertVectorArrayToBinary(row.getArray(i), vectorColumns.get(i)));
      } else {
        result.update(i, row.get(i, sourceSchema.apply(i).dataType()));
      }
    }
  }
}
