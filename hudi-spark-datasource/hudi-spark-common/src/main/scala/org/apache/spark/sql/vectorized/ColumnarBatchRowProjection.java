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

package org.apache.spark.sql.vectorized;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.types.PhysicalArrayType;
import org.apache.spark.sql.catalyst.types.PhysicalBinaryType;
import org.apache.spark.sql.catalyst.types.PhysicalBooleanType;
import org.apache.spark.sql.catalyst.types.PhysicalByteType;
import org.apache.spark.sql.catalyst.types.PhysicalDataType;
import org.apache.spark.sql.catalyst.types.PhysicalDecimalType;
import org.apache.spark.sql.catalyst.types.PhysicalDoubleType;
import org.apache.spark.sql.catalyst.types.PhysicalFloatType;
import org.apache.spark.sql.catalyst.types.PhysicalIntegerType;
import org.apache.spark.sql.catalyst.types.PhysicalLongType;
import org.apache.spark.sql.catalyst.types.PhysicalMapType;
import org.apache.spark.sql.catalyst.types.PhysicalShortType;
import org.apache.spark.sql.catalyst.types.PhysicalStringType;
import org.apache.spark.sql.catalyst.types.PhysicalStructType;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DayTimeIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.YearMonthIntervalType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Pretty much a copy of org.apache.spark.sql.vectorized.ColumnarBatchRow
 * but has projection
 */
public class ColumnarBatchRowProjection extends InternalRow {
  public int rowId;
  protected final ColumnVector[] columns;
  protected final int[] projection;

  public ColumnarBatchRowProjection(ColumnVector[] columns, int[] projection) {
    this.columns = columns;
    this.projection = projection;
  }

  @Override
  public int numFields() { return projection.length; }

  @Override
  public InternalRow copy() {
    GenericInternalRow row = new GenericInternalRow(projection.length);
    for (int i = 0; i < numFields(); i++) {
      if (isNullAt(i)) {
        row.setNullAt(i);
      } else {
        DataType dt = columns[projection[i]].dataType();
        PhysicalDataType pdt = PhysicalDataType.apply(dt);
        if (pdt instanceof PhysicalBooleanType) {
          row.setBoolean(i, getBoolean(i));
        } else if (pdt instanceof PhysicalByteType) {
          row.setByte(i, getByte(i));
        } else if (pdt instanceof PhysicalShortType) {
          row.setShort(i, getShort(i));
        } else if (pdt instanceof PhysicalIntegerType) {
          row.setInt(i, getInt(i));
        } else if (pdt instanceof PhysicalLongType) {
          row.setLong(i, getLong(i));
        } else if (pdt instanceof PhysicalFloatType) {
          row.setFloat(i, getFloat(i));
        } else if (pdt instanceof PhysicalDoubleType) {
          row.setDouble(i, getDouble(i));
        } else if (pdt instanceof PhysicalStringType) {
          row.update(i, getUTF8String(i).copy());
        } else if (pdt instanceof PhysicalBinaryType) {
          row.update(i, getBinary(i));
        } else if (pdt instanceof PhysicalDecimalType) {
          PhysicalDecimalType t = (PhysicalDecimalType)pdt;
          row.setDecimal(i, getDecimal(i, t.precision(), t.scale()), t.precision());
        } else if (pdt instanceof PhysicalStructType) {
          row.update(i, getStruct(i, ((PhysicalStructType) pdt).fields().length).copy());
        } else if (pdt instanceof PhysicalArrayType) {
          row.update(i, getArray(i).copy());
        } else if (pdt instanceof PhysicalMapType) {
          row.update(i, getMap(i).copy());
        } else {
          throw new RuntimeException("Not implemented. " + dt);
        }
      }
    }
    return row;
  }

  @Override
  public boolean anyNull() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNullAt(int ordinal) { return columns[projection[ordinal]].isNullAt(rowId); }

  @Override
  public boolean getBoolean(int ordinal) { return columns[projection[ordinal]].getBoolean(rowId); }

  @Override
  public byte getByte(int ordinal) { return columns[projection[ordinal]].getByte(rowId); }

  @Override
  public short getShort(int ordinal) { return columns[projection[ordinal]].getShort(rowId); }

  @Override
  public int getInt(int ordinal) { return columns[projection[ordinal]].getInt(rowId); }

  @Override
  public long getLong(int ordinal) { return columns[projection[ordinal]].getLong(rowId); }

  @Override
  public float getFloat(int ordinal) { return columns[projection[ordinal]].getFloat(rowId); }

  @Override
  public double getDouble(int ordinal) { return columns[projection[ordinal]].getDouble(rowId); }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return columns[projection[ordinal]].getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return columns[projection[ordinal]].getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return columns[projection[ordinal]].getBinary(rowId);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return columns[projection[ordinal]].getInterval(rowId);
  }

  @Override
  public ColumnarRow getStruct(int ordinal, int numFields) {
    return columns[projection[ordinal]].getStruct(rowId);
  }

  @Override
  public ColumnarArray getArray(int ordinal) {
    return columns[projection[ordinal]].getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return columns[projection[ordinal]].getMap(rowId);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (dataType instanceof BooleanType) {
      return getBoolean(ordinal);
    } else if (dataType instanceof ByteType) {
      return getByte(ordinal);
    } else if (dataType instanceof ShortType) {
      return getShort(ordinal);
    } else if (dataType instanceof IntegerType || dataType instanceof YearMonthIntervalType) {
      return getInt(ordinal);
    } else if (dataType instanceof LongType || dataType instanceof DayTimeIntervalType) {
      return getLong(ordinal);
    } else if (dataType instanceof FloatType) {
      return getFloat(ordinal);
    } else if (dataType instanceof DoubleType) {
      return getDouble(ordinal);
    } else if (dataType instanceof StringType) {
      return getUTF8String(ordinal);
    } else if (dataType instanceof BinaryType) {
      return getBinary(ordinal);
    } else if (dataType instanceof DecimalType) {
      DecimalType t = (DecimalType) dataType;
      return getDecimal(ordinal, t.precision(), t.scale());
    } else if (dataType instanceof DateType) {
      return getInt(ordinal);
    } else if (dataType instanceof TimestampType) {
      return getLong(ordinal);
    } else if (dataType instanceof TimestampNTZType) {
      return getLong(ordinal);
    } else if (dataType instanceof ArrayType) {
      return getArray(ordinal);
    } else if (dataType instanceof StructType) {
      return getStruct(ordinal, ((StructType)dataType).fields().length);
    } else if (dataType instanceof MapType) {
      return getMap(ordinal);
    } else {
      throw new UnsupportedOperationException("Datatype not supported " + dataType);
    }
  }

  @Override
  public void update(int ordinal, Object value) { throw new UnsupportedOperationException(); }

  @Override
  public void setNullAt(int ordinal) { throw new UnsupportedOperationException(); }
}
