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
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * Detects VECTOR columns in a HoodieSchema record and returns a map of field ordinal
   * to the corresponding {@link HoodieSchema.Vector} schema.
   *
   * @param schema a HoodieSchema of type RECORD (or null)
   * @return map from field index to Vector schema; empty map if schema is null or has no vectors
   */
  public static Map<Integer, HoodieSchema.Vector> detectVectorColumns(HoodieSchema schema) {
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new LinkedHashMap<>();
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
   * Builds the {@link HoodieSchema#VECTOR_COLUMNS_METADATA_KEY} footer value
   * from a Spark {@link StructType} by detecting VECTOR metadata annotations and
   * delegating to {@link HoodieSchema#serializeVectorColumnsMetadata}.
   *
   * @param schema Spark StructType (may be null)
   * @return comma-separated descriptor list, or empty string if no VECTOR columns
   * @see HoodieSchema#serializeVectorColumnsMetadata(java.util.Map)
   */
  public static String buildVectorColumnsFooterValue(StructType schema) {
    if (schema == null) {
      return "";
    }
    Map<Integer, HoodieSchema.Vector> detected = detectVectorColumnsFromMetadata(schema);
    StructField[] fields = schema.fields();
    LinkedHashMap<String, HoodieSchema.Vector> named = new LinkedHashMap<>();
    for (Map.Entry<Integer, HoodieSchema.Vector> entry : detected.entrySet()) {
      named.put(fields[entry.getKey()].name(), entry.getValue());
    }
    return HoodieSchema.serializeVectorColumnsMetadata(named);
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
    // Use LinkedHashMap so callers iterate in field-ordinal order (stable across JDKs).
    Map<Integer, HoodieSchema.Vector> vectorColumnInfo = new LinkedHashMap<>();
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
        // Preserve the original field metadata (including hudi_type) so that downstream code
        // calling detectVectorColumnsFromMetadata on the modified schema still finds vectors.
        newFields[i] = new StructField(fields[i].name(), BinaryType$.MODULE$, fields[i].nullable(), fields[i].metadata());
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
      Function<InternalRow, InternalRow> projectionCallback) {
    GenericInternalRow converted = new GenericInternalRow(readSchema.fields().length);
    return row -> {
      convertRowVectorColumns(row, converted, readSchema, vectorColumns);
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
   * Re-attaches {@link HoodieSchema#TYPE_METADATA_FIELD} to Spark fields that are
   * Arrow {@code FixedSizeList<Float32|Float64, dim>} in the Lance file.
   * {@code LanceArrowUtils.fromArrowSchema} strips Hudi's VECTOR descriptor during
   * Arrow→Spark conversion but preserves the fixed-size-list dimension under the
   * lance-spark metadata key {@link LanceArrowUtils#ARROW_FIXED_SIZE_LIST_SIZE_KEY()}.
   *
   * <p>A FixedSizeList alone does not prove the column is a Hudi VECTOR — a
   * non-Hudi Lance file could contain one. Callers must pass {@code vectorColumnNames}
   * (derived from the Hudi schema's VECTOR-tagged fields, e.g. via
   * {@link #detectVectorColumnsFromMetadata(StructType)}) so that only fields known to
   * be Hudi VECTORs are restored. Pass an empty set to skip the restore entirely.
   *
   * <p>Nested structs are not recursed.
   */
  public static StructType restoreVectorMetadata(StructType convertedSpark, Set<String> vectorColumnNames) {
    if (convertedSpark == null) {
      return null;
    }
    if (vectorColumnNames == null || vectorColumnNames.isEmpty()) {
      return convertedSpark;
    }
    StructField[] sparkFields = convertedSpark.fields();
    StructField[] newFields = new StructField[sparkFields.length];
    boolean changed = false;
    for (int i = 0; i < sparkFields.length; i++) {
      StructField sf = sparkFields[i];
      String descriptor = vectorColumnNames.contains(sf.name()) ? deriveVectorDescriptor(sf) : null;
      if (descriptor == null) {
        newFields[i] = sf;
      } else {
        // VECTOR contract: elements are non-nullable. lance-spark's Arrow→Spark
        // conversion produces ArrayType(containsNull=true); force containsNull=false
        // so the field round-trips through HoodieSchema conversion.
        DataType arrayType = DataTypes.createArrayType(
            ((ArrayType) sf.dataType()).elementType(), false);
        newFields[i] = new StructField(
            sf.name(),
            arrayType,
            sf.nullable(),
            new MetadataBuilder()
                .withMetadata(sf.metadata())
                .putString(HoodieSchema.TYPE_METADATA_FIELD, descriptor)
                .build());
        changed = true;
      }
    }
    return changed ? new StructType(newFields) : convertedSpark;
  }

  /**
   * Derives Hudi's VECTOR type descriptor for a Spark field if lance-spark tagged it
   * with {@link LanceArrowUtils#ARROW_FIXED_SIZE_LIST_SIZE_KEY()} and its data type is
   * {@code ArrayType(Float|Double, containsNull=false)}; otherwise returns null.
   */
  private static String deriveVectorDescriptor(StructField sf) {
    String sizeKey = LanceArrowUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY();
    if (!sf.metadata().contains(sizeKey)) {
      return null;
    }
    if (!(sf.dataType() instanceof ArrayType)) {
      return null;
    }
    DataType elemType = ((ArrayType) sf.dataType()).elementType();
    HoodieSchema.Vector.VectorElementType elementType;
    if (DataTypes.FloatType.equals(elemType)) {
      elementType = HoodieSchema.Vector.VectorElementType.FLOAT;
    } else if (DataTypes.DoubleType.equals(elemType)) {
      elementType = HoodieSchema.Vector.VectorElementType.DOUBLE;
    } else {
      return null;
    }
    int dim = (int) sf.metadata().getLong(sizeKey);
    return HoodieSchema.createVector(dim, elementType).toTypeDescriptor();
  }
}
