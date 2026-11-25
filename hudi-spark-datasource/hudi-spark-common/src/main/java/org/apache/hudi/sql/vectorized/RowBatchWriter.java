/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sql.vectorized;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.vectorized.ColumnVector;

import java.util.List;

/**
 * Writes rows of a flat schema into on-heap column vectors, for the few rows a merge-on-read scan
 * takes from log files rather than from the base file.
 */
public final class RowBatchWriter {

  private RowBatchWriter() {
  }

  /**
   * Whether every field of {@code schema} is a type {@link #write} supports.
   */
  public static boolean isSupported(StructType schema) {
    for (StructField field : schema.fields()) {
      if (!isSupported(field.dataType())) {
        return false;
      }
    }
    return true;
  }

  private static boolean isSupported(DataType type) {
    return type instanceof BooleanType || type instanceof ByteType || type instanceof ShortType
        || type instanceof IntegerType || type instanceof LongType || type instanceof FloatType
        || type instanceof DoubleType || type instanceof StringType || type instanceof BinaryType
        || type instanceof DecimalType || type instanceof DateType || type instanceof TimestampType
        || type instanceof TimestampNTZType;
  }

  /**
   * Writes {@code rows}, laid out as {@code schema}, into new column vectors, one per field.
   */
  public static ColumnVector[] write(StructType schema, List<InternalRow> rows) {
    int numRows = rows.size();
    StructField[] fields = schema.fields();
    ColumnVector[] vectors = new ColumnVector[fields.length];
    for (int col = 0; col < fields.length; col++) {
      DataType type = fields[col].dataType();
      OnHeapColumnVector vector = new OnHeapColumnVector(Math.max(numRows, 1), type);
      for (int row = 0; row < numRows; row++) {
        InternalRow internalRow = rows.get(row);
        if (internalRow.isNullAt(col)) {
          vector.putNull(row);
        } else {
          put(vector, row, internalRow, col, type);
        }
      }
      vectors[col] = vector;
    }
    return vectors;
  }

  private static void put(OnHeapColumnVector vector, int row, InternalRow internalRow, int col, DataType type) {
    if (type instanceof BooleanType) {
      vector.putBoolean(row, internalRow.getBoolean(col));
    } else if (type instanceof ByteType) {
      vector.putByte(row, internalRow.getByte(col));
    } else if (type instanceof ShortType) {
      vector.putShort(row, internalRow.getShort(col));
    } else if (type instanceof IntegerType || type instanceof DateType) {
      vector.putInt(row, internalRow.getInt(col));
    } else if (type instanceof LongType || type instanceof TimestampType || type instanceof TimestampNTZType) {
      vector.putLong(row, internalRow.getLong(col));
    } else if (type instanceof FloatType) {
      vector.putFloat(row, internalRow.getFloat(col));
    } else if (type instanceof DoubleType) {
      vector.putDouble(row, internalRow.getDouble(col));
    } else if (type instanceof StringType) {
      vector.putByteArray(row, internalRow.getUTF8String(col).getBytes());
    } else if (type instanceof BinaryType) {
      vector.putByteArray(row, internalRow.getBinary(col));
    } else if (type instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) type;
      vector.putDecimal(row, internalRow.getDecimal(col, decimalType.precision(), decimalType.scale()), decimalType.precision());
    } else {
      throw new IllegalArgumentException("Unsupported type for a merge-on-read column batch: " + type);
    }
  }
}
