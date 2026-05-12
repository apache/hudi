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

package org.apache.hudi.common.util;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for decoding Hudi VECTOR fixed-bytes payloads.
 */
public final class HoodieVectorUtils {

  private HoodieVectorUtils() {
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
   * Converts binary bytes from a FIXED_LEN_BYTE_ARRAY Parquet column back to a typed array.
   *
   * @param bytes        raw bytes read from Parquet
   * @param vectorSchema vector schema
   * @return an ArrayData containing the decoded float[], double[], or byte[] array
   * @throws IllegalArgumentException if byte array length doesn't match expected size
   */
  public static Object decodeVectorBytes(byte[] bytes, HoodieSchema.Vector vectorSchema) {
    return decodeVectorBytes(bytes, vectorSchema.getDimension(), vectorSchema.getVectorElementType());
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
  public static Object decodeVectorBytes(
      byte[] bytes,
      int dim,
      HoodieSchema.Vector.VectorElementType elemType) {
    int expectedSize = dim * elemType.getElementSize();
    ValidationUtils.checkArgument(bytes.length == expectedSize,
        "Vector byte array length mismatch: expected " + expectedSize + " but got " + bytes.length);
    ByteBuffer buffer = ByteBuffer.wrap(bytes).order(HoodieSchema.VectorLogicalType.VECTOR_BYTE_ORDER);
    switch (elemType) {
      case FLOAT:
        float[] floats = new float[dim];
        for (int i = 0; i < dim; i++) {
          floats[i] = buffer.getFloat();
        }
        return floats;
      case DOUBLE:
        double[] doubles = new double[dim];
        for (int i = 0; i < dim; i++) {
          doubles[i] = buffer.getDouble();
        }
        return doubles;
      case INT8:
        byte[] int8s = new byte[dim];
        buffer.get(int8s);
        return int8s;
      default:
        throw new UnsupportedOperationException("Unsupported vector element type: " + elemType);
    }
  }
}
