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

package org.apache.hudi.io.storage;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A column that has the same value in all rows: one partition value of a file.
 *
 * <p>Spark has a constant column vector from version 3.4. This class also works with older
 * versions. Partition values are of atomic types, thus this column has no child columns.
 */
final class VortexPartitionColumnVector extends ColumnVector {

  private final InternalRow values;
  private final int ordinal;

  /**
   * @param type    type of the column
   * @param values  partition values of the file
   * @param ordinal position of the value of this column in {@code values}
   */
  VortexPartitionColumnVector(DataType type, InternalRow values, int ordinal) {
    super(type);
    this.values = values;
    this.ordinal = ordinal;
  }

  @Override
  public void close() {
  }

  @Override
  public boolean hasNull() {
    return values.isNullAt(ordinal);
  }

  @Override
  public int numNulls() {
    // Spark uses the count only to know if the column has nulls.
    return hasNull() ? 1 : 0;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return values.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return values.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int rowId) {
    return values.getByte(ordinal);
  }

  @Override
  public short getShort(int rowId) {
    return values.getShort(ordinal);
  }

  @Override
  public int getInt(int rowId) {
    return values.getInt(ordinal);
  }

  @Override
  public long getLong(int rowId) {
    return values.getLong(ordinal);
  }

  @Override
  public float getFloat(int rowId) {
    return values.getFloat(ordinal);
  }

  @Override
  public double getDouble(int rowId) {
    return values.getDouble(ordinal);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return values.getDecimal(ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return values.getUTF8String(ordinal);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return values.getBinary(ordinal);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("A partition column of type " + dataType() + " is not supported");
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException("A partition column of type " + dataType() + " is not supported");
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("A partition column of type " + dataType() + " is not supported");
  }
}
