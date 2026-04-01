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

import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.nio.ByteBuffer;
import java.util.function.Function;

import static org.apache.hudi.common.util.ValidationUtils.checkArgument;

/**
 * Shared utility methods for vector column handling during Parquet read/write.
 *
 * Vectors are stored as Parquet FIXED_LEN_BYTE_ARRAY columns. On read, Spark maps these
 * to BinaryType. This class provides the canonical conversion between the binary
 * representation and Spark's typed ArrayData (float[], double[], byte[]).
 *
 */
public final class VectorConversionUtils {

  private VectorConversionUtils() {
  }

  /**
   * Checks whether a HoodieSchema record contains any VECTOR columns, including
   * vectors nested inside RECORD (struct), ARRAY, or MAP fields at any depth.
   *
   * <p><b>Invariant:</b> This method and {@link #hasVectorColumnsFromMetadata(StructType)} must
   * return the same result for equivalent schemas. They use different sources of truth
   * (HoodieSchema type tree vs Spark StructField metadata) but must agree. The metadata
   * propagation in {@code HoodieSparkSchemaConverters.toSqlTypeHelper} (ARRAY/MAP cases)
   * ensures this for all supported nesting patterns.
   *
   * @param schema a HoodieSchema of type RECORD (or null)
   * @return true if any VECTOR columns exist at any nesting level
   */
  public static boolean hasVectorColumns(HoodieSchema schema) {
    if (schema == null) {
      return false;
    }
    for (HoodieSchemaField field : schema.getFields()) {
      if (hasVectorType(field.schema().getNonNullType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively checks if a HoodieSchema type contains VECTOR at any depth
   * (through RECORD, ARRAY, or MAP nesting).
   */
  private static boolean hasVectorType(HoodieSchema schema) {
    switch (schema.getType()) {
      case VECTOR:
        return true;
      case RECORD:
        return hasVectorColumns(schema);
      case ARRAY:
        return hasVectorType(schema.getElementType().getNonNullType());
      case MAP:
        return hasVectorType(schema.getValueType().getNonNullType());
      default:
        return false;
    }
  }

  /**
   * Checks whether a Spark StructType contains any VECTOR columns (via metadata),
   * including vectors nested inside StructType fields at any depth.
   */
  public static boolean hasVectorColumnsFromMetadata(StructType schema) {
    if (schema == null) {
      return false;
    }
    for (StructField field : schema.fields()) {
      if (isVectorField(field)) {
        return true;
      }
      if (hasVectorColumnsInDataType(field.dataType())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Recursively checks if a DataType contains any VECTOR columns via metadata.
   * Recurses into StructType, ArrayType element, and MapType value.
   */
  private static boolean hasVectorColumnsInDataType(DataType dataType) {
    if (dataType instanceof StructType) {
      return hasVectorColumnsFromMetadata((StructType) dataType);
    } else if (dataType instanceof ArrayType) {
      return hasVectorColumnsInDataType(((ArrayType) dataType).elementType());
    } else if (dataType instanceof MapType) {
      return hasVectorColumnsInDataType(((MapType) dataType).valueType());
    }
    return false;
  }

  /**
   * Checks if a StructField has VECTOR metadata.
   */
  private static boolean isVectorField(StructField field) {
    if (field.metadata().contains(HoodieSchema.TYPE_METADATA_FIELD)) {
      String typeStr = field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
      // Match "VECTOR" or "VECTOR(" — the type descriptor is always "VECTOR(dim[,elemType[,backing]])"
      return typeStr.equals("VECTOR") || typeStr.startsWith("VECTOR(");
    }
    return false;
  }

  /**
   * Parses and returns the VECTOR metadata from a StructField, throwing if not found or unparseable.
   */
  private static HoodieSchema.Vector requireVectorMetadata(StructField field) {
    checkArgument(isVectorField(field),
        "Field '" + field.name() + "' does not have VECTOR metadata");
    String typeStr = field.metadata().getString(HoodieSchema.TYPE_METADATA_FIELD);
    HoodieSchema parsed = HoodieSchema.parseTypeDescriptor(typeStr);
    checkArgument(parsed.getType() == HoodieSchemaType.VECTOR,
        "Field '" + field.name() + "' has hudi_type '" + typeStr + "' but parsed to " + parsed.getType());
    return (HoodieSchema.Vector) parsed;
  }

  /**
   * Recursively replaces VECTOR columns (identified by {@code hudi_type} metadata) with
   * BinaryType so the Parquet reader can read FIXED_LEN_BYTE_ARRAY data without type mismatch.
   * Recurses into nested StructType fields to handle vectors at any nesting depth.
   *
   * @param structType the original Spark schema (may contain nested structs with vectors)
   * @return a new StructType with vector columns replaced by BinaryType at all levels
   */
  public static StructType replaceVectorColumnsWithBinary(StructType structType) {
    StructField[] fields = structType.fields();
    StructField[] newFields = new StructField[fields.length];
    boolean changed = false;
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      if (isVectorField(field)) {
        DataType dataType = field.dataType();
        DataType replaced = replaceDirectVectorType(field.name(), dataType);
        if (replaced != dataType) {
          newFields[i] = new StructField(field.name(), replaced, field.nullable(), field.metadata());
          changed = true;
        } else {
          newFields[i] = field;
        }
      } else {
        // Recurse into StructType, ArrayType element, MapType value to find nested vectors
        DataType replaced = replaceNestedVectorColumns(field.dataType());
        if (replaced != field.dataType()) {
          newFields[i] = new StructField(field.name(), replaced, field.nullable(), field.metadata());
          changed = true;
        } else {
          newFields[i] = field;
        }
      }
    }
    return changed ? new StructType(newFields) : structType;
  }

  /**
   * Replaces the vector portion of a DataType with BinaryType. Supported patterns:
   * - ArrayType(primitiveType) → BinaryType (direct vector)
   * - ArrayType(ArrayType(primitiveType)) → ArrayType(BinaryType) (array of vectors)
   * - MapType(K, ArrayType(primitiveType)) → MapType(K, BinaryType) (map of vectors)
   *
   * @throws UnsupportedOperationException if the field uses an unsupported nesting pattern
   */
  private static DataType replaceDirectVectorType(String fieldName, DataType dataType) {
    if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType elementType = arrayType.elementType();
      if (elementType instanceof ArrayType) {
        // array<array<float>> → array<binary> (array of vectors)
        DataType innerElem = ((ArrayType) elementType).elementType();
        if (innerElem instanceof ArrayType || innerElem instanceof MapType) {
          throw new UnsupportedOperationException(
              "VECTOR field '" + fieldName + "' uses unsupported nesting: " + dataType
                  + ". Supported patterns: direct VECTOR, array<VECTOR>, map<K, VECTOR>.");
        }
        return new ArrayType(BinaryType$.MODULE$, arrayType.containsNull());
      } else if (elementType instanceof MapType || elementType instanceof StructType) {
        throw new UnsupportedOperationException(
            "VECTOR field '" + fieldName + "' uses unsupported nesting: " + dataType
                + ". Supported patterns: direct VECTOR, array<VECTOR>, map<K, VECTOR>.");
      } else {
        // array<float> → binary (direct vector)
        return BinaryType$.MODULE$;
      }
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType valueType = mapType.valueType();
      if (!(valueType instanceof ArrayType) || ((ArrayType) valueType).elementType() instanceof ArrayType) {
        throw new UnsupportedOperationException(
            "VECTOR field '" + fieldName + "' uses unsupported nesting: " + dataType
                + ". Supported patterns: direct VECTOR, array<VECTOR>, map<K, VECTOR>.");
      }
      // map<K, array<float>> → map<K, binary>
      return new MapType(mapType.keyType(), BinaryType$.MODULE$, mapType.valueContainsNull());
    }
    throw new UnsupportedOperationException(
        "VECTOR field '" + fieldName + "' uses unsupported DataType: " + dataType
            + ". Supported patterns: direct VECTOR, array<VECTOR>, map<K, VECTOR>.");
  }

  /**
   * Recursively replaces vector columns inside a DataType. Handles:
   * - StructType: recurses via replaceVectorColumnsWithBinary
   * - ArrayType(StructType): recurses into the element struct
   * - MapType(K, StructType): recurses into the value struct
   */
  private static DataType replaceNestedVectorColumns(DataType dataType) {
    if (dataType instanceof StructType) {
      StructType inner = (StructType) dataType;
      StructType replaced = replaceVectorColumnsWithBinary(inner);
      return replaced != inner ? replaced : dataType;
    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      DataType replaced = replaceNestedVectorColumns(arrayType.elementType());
      return replaced != arrayType.elementType()
          ? new ArrayType(replaced, arrayType.containsNull()) : dataType;
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      DataType replaced = replaceNestedVectorColumns(mapType.valueType());
      return replaced != mapType.valueType()
          ? new MapType(mapType.keyType(), replaced, mapType.valueContainsNull()) : dataType;
    }
    return dataType;
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array
   * based on the vector's element type and dimension.
   *
   * @param bytes        raw bytes read from Parquet
   * @param vectorSchema the vector schema describing dimension and element type
   * @return an ArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static ArrayData convertBinaryToVectorArray(byte[] bytes, HoodieSchema.Vector vectorSchema) {
    return convertBinaryToVectorArray(bytes, vectorSchema.getDimension(), vectorSchema.getVectorElementType());
  }

  /**
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array.
   *
   * @param bytes    raw bytes read from Parquet
   * @param dim      vector dimension (number of elements)
   * @param elemType element type (FLOAT, DOUBLE, or INT8)
   * @return an ArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static ArrayData convertBinaryToVectorArray(byte[] bytes, int dim,
                                                     HoodieSchema.Vector.VectorElementType elemType) {
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
        // Use UnsafeArrayData to avoid boxing each byte into a Byte object.
        // GenericArrayData(byte[]) would box every element into Object[].
        return UnsafeArrayData.fromPrimitiveArray(int8s);
      default:
        throw new UnsupportedOperationException(
            "Unsupported vector element type: " + elemType);
    }
  }

  /**
   * Returns a {@link Function} that converts a single {@link InternalRow} by converting binary
   * vector columns back to typed arrays and then applying the given projection callback.
   * Handles vectors at any nesting depth by comparing the read schema (BinaryType for vectors)
   * against the output schema (ArrayType for vectors).
   *
   * <p><b>Thread safety:</b> The returned function is NOT thread-safe; it reuses
   * {@link GenericInternalRow} buffers across calls. Each call to this factory creates its own
   * buffers, so separate functions returned by separate calls are independent.
   *
   * @param readSchema         the Spark schema of incoming rows (BinaryType for vector columns)
   * @param outputSchema       the Spark schema of output rows (ArrayType for vector columns)
   * @param projectionCallback called with the converted row; must copy any data it needs
   *                           to retain (e.g. {@code UnsafeProjection::apply})
   * @return a function that converts one row and returns the projected result
   */
  public static Function<InternalRow, InternalRow> buildRowMapper(
      StructType readSchema,
      StructType outputSchema,
      Function<InternalRow, InternalRow> projectionCallback) {
    GenericInternalRow converted = new GenericInternalRow(readSchema.fields().length);
    return row -> {
      convertRowVectorColumns(row, converted, readSchema, outputSchema);
      return projectionCallback.apply(converted);
    };
  }

  /**
   * Recursively converts vector columns in a row from binary (BinaryType) back to typed arrays.
   * Detects vectors by comparing read schema (BinaryType) vs output schema (ArrayType) at each
   * field. Recurses into nested StructType fields to handle vectors at any depth.
   *
   * @param row          the source row (with BinaryType for vector columns)
   * @param result       a pre-allocated GenericInternalRow to write into (reused across calls)
   * @param readSchema   the Spark schema of the source row (BinaryType for vector columns)
   * @param outputSchema the Spark schema of the output row (ArrayType for vector columns)
   */
  public static void convertRowVectorColumns(InternalRow row, GenericInternalRow result,
                                             StructType readSchema, StructType outputSchema) {
    StructField[] readFields = readSchema.fields();
    StructField[] outputFields = outputSchema.fields();
    for (int i = 0; i < readFields.length; i++) {
      if (row.isNullAt(i)) {
        result.setNullAt(i);
      } else if (i >= outputFields.length) {
        // Extra field in readSchema (e.g., row-index appended by Spark) — copy as-is
        result.update(i, row.get(i, readFields[i].dataType()));
      } else if (isVectorField(outputFields[i])) {
        // --- Group 1: field has VECTOR metadata (direct vector, array<VECTOR>, map<K, VECTOR>) ---
        convertVectorField(row, result, i, readFields[i].dataType(), outputFields[i]);
      } else {
        // --- Group 2: field may CONTAIN vectors deeper (struct, array<struct>, map<K, struct>) ---
        convertContainerField(row, result, i, readFields[i].dataType(), outputFields[i].dataType());
      }
    }
  }

  /**
   * Converts a field that has VECTOR metadata. Dispatches based on the DataType to handle
   * direct vectors, array-of-vectors, and map-of-vectors.
   */
  private static void convertVectorField(InternalRow row, GenericInternalRow result,
                                          int ordinal, DataType readType, StructField outputField) {
    HoodieSchema.Vector vec = requireVectorMetadata(outputField);
    if (readType == BinaryType$.MODULE$) {
      // Direct vector: binary → typed array
      result.update(ordinal, convertBinaryToVectorArray(row.getBinary(ordinal), vec));
    } else if (readType instanceof ArrayType) {
      // Array of vectors: iterate elements, convert each binary → typed array
      ArrayData arrayData = row.getArray(ordinal);
      Object[] converted = new Object[arrayData.numElements()];
      for (int j = 0; j < arrayData.numElements(); j++) {
        converted[j] = arrayData.isNullAt(j) ? null
            : convertBinaryToVectorArray(arrayData.getBinary(j), vec);
      }
      result.update(ordinal, new GenericArrayData(converted));
    } else if (readType instanceof MapType) {
      // Map of vectors: convert each value from binary → typed array
      MapData mapData = row.getMap(ordinal);
      ArrayData keys = mapData.keyArray();
      ArrayData values = mapData.valueArray();
      Object[] convertedValues = new Object[values.numElements()];
      for (int j = 0; j < values.numElements(); j++) {
        convertedValues[j] = values.isNullAt(j) ? null
            : convertBinaryToVectorArray(values.getBinary(j), vec);
      }
      result.update(ordinal, new ArrayBasedMapData(
          keys, new GenericArrayData(convertedValues)));
    } else {
      throw new IllegalStateException(
          "Field '" + outputField.name() + "' has VECTOR metadata but unsupported DataType: " + readType);
    }
  }

  /**
   * Converts a field that may contain vectors nested inside structs, arrays-of-structs,
   * or maps-of-structs. If the field doesn't contain nested vectors, copies it as-is.
   */
  private static void convertContainerField(InternalRow row, GenericInternalRow result,
                                             int ordinal, DataType readType, DataType outputType) {
    StructType innerRead = innerStructType(readType);
    StructType innerOutput = innerStructType(outputType);
    if (innerRead == null || innerOutput == null) {
      // No nested struct — no vectors to convert, copy as-is
      result.update(ordinal, row.get(ordinal, readType));
    } else if (readType instanceof StructType) {
      InternalRow innerRow = row.getStruct(ordinal, innerRead.fields().length);
      GenericInternalRow innerResult = new GenericInternalRow(innerRead.fields().length);
      convertRowVectorColumns(innerRow, innerResult, innerRead, innerOutput);
      result.update(ordinal, innerResult);
    } else if (readType instanceof ArrayType) {
      result.update(ordinal, new GenericArrayData(
          convertStructElements(row.getArray(ordinal), innerRead, innerOutput)));
    } else { // MapType
      MapData mapData = row.getMap(ordinal);
      result.update(ordinal, new ArrayBasedMapData(
          mapData.keyArray(), new GenericArrayData(
              convertStructElements(mapData.valueArray(), innerRead, innerOutput))));
    }
  }

  /**
   * Returns the StructType nested inside a DataType, or null if the type does not
   * contain a struct. Handles: StructType directly, ArrayType(StructType),
   * MapType(K, StructType).
   */
  private static StructType innerStructType(DataType type) {
    if (type instanceof StructType) {
      return (StructType) type;
    } else if (type instanceof ArrayType && ((ArrayType) type).elementType() instanceof StructType) {
      return (StructType) ((ArrayType) type).elementType();
    } else if (type instanceof MapType && ((MapType) type).valueType() instanceof StructType) {
      return (StructType) ((MapType) type).valueType();
    }
    return null;
  }

  /**
   * Iterates over struct elements in an ArrayData, recursively converting vector columns
   * in each struct. Returns an Object[] suitable for wrapping in GenericArrayData.
   */
  private static Object[] convertStructElements(ArrayData elements,
                                                 StructType elemRead, StructType elemOutput) {
    Object[] out = new Object[elements.numElements()];
    for (int j = 0; j < elements.numElements(); j++) {
      if (elements.isNullAt(j)) {
        out[j] = null;
      } else {
        InternalRow elemRow = elements.getStruct(j, elemRead.fields().length);
        GenericInternalRow elemResult = new GenericInternalRow(elemRead.fields().length);
        convertRowVectorColumns(elemRow, elemResult, elemRead, elemOutput);
        out[j] = elemResult;
      }
    }
    return out;
  }
}
