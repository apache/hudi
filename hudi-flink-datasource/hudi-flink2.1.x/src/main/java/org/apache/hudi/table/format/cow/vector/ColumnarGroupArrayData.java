/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.format.cow.vector;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.types.variant.Variant;

public class ColumnarGroupArrayData implements ArrayData {

  WritableColumnVector vector;
  int rowId;

  public ColumnarGroupArrayData(WritableColumnVector vector, int rowId) {
    this.vector = vector;
    this.rowId = rowId;
  }

  @Override
  public int size() {
    if (vector == null) {
      return 0;
    }

    if (vector instanceof HeapRowColumnVector) {
      // assume all fields have the same size
      if (((HeapRowColumnVector) vector).vectors == null || ((HeapRowColumnVector) vector).vectors.length == 0) {
        return 0;
      }
      return ((HeapArrayVector) ((HeapRowColumnVector) vector).vectors[0]).getArray(rowId).size();
    }
    throw new UnsupportedOperationException(vector.getClass().getName() + " is not supported. Supported vector types: HeapRowColumnVector");
  }

  @Override
  public boolean isNullAt(int index) {
    if (vector == null) {
      return true;
    }

    if (vector instanceof HeapRowColumnVector) {
      return ((HeapRowColumnVector) vector).vectors == null;
    }

    throw new UnsupportedOperationException(vector.getClass().getName() + " is not supported. Supported vector types: HeapRowColumnVector");
  }

  @Override
  public boolean getBoolean(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public byte getByte(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public short getShort(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public int getInt(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public long getLong(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public float getFloat(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public double getDouble(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public StringData getString(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public DecimalData getDecimal(int index, int precision, int scale) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public TimestampData getTimestamp(int index, int precision) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public <T> RawValueData<T> getRawValue(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public Variant getVariant(int i) {
    throw new UnsupportedOperationException("Variant is not supported yet.");
  }

  @Override
  public byte[] getBinary(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public ArrayData getArray(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public MapData getMap(int index) {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public RowData getRow(int index, int numFields) {
    return new ColumnarGroupRowData((HeapRowColumnVector) vector, rowId, index);
  }

  @Override
  public boolean[] toBooleanArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public byte[] toByteArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public short[] toShortArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public int[] toIntArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public long[] toLongArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public float[] toFloatArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

  @Override
  public double[] toDoubleArray() {
    throw new UnsupportedOperationException("Not support the operation!");
  }

}
