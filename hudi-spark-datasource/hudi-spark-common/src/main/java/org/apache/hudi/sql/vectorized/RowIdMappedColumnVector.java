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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A read-only view of a subset of another column vector's rows. Row {@code i} of this vector is row
 * {@code rowIds[i]} of the delegate, so a batch can drop rows without copying their values.
 *
 * <p>The delegate is not owned: closing this vector does not close it, and the view is only valid while
 * the delegate's batch is.
 */
public final class RowIdMappedColumnVector extends ColumnVector {

  private final ColumnVector delegate;
  private final int[] rowIds;
  private final int numRows;
  private RowIdMappedColumnVector[] children;
  private int numNulls = -1;

  public RowIdMappedColumnVector(ColumnVector delegate, int[] rowIds, int numRows) {
    super(delegate.dataType());
    this.delegate = delegate;
    this.rowIds = rowIds;
    this.numRows = numRows;
  }

  @Override
  public void close() {
    // The delegate belongs to the batch it was read into.
  }

  @Override
  public boolean hasNull() {
    return numNulls() > 0;
  }

  @Override
  public int numNulls() {
    if (numNulls < 0) {
      int count = 0;
      if (delegate.hasNull()) {
        for (int i = 0; i < numRows; i++) {
          if (delegate.isNullAt(rowIds[i])) {
            count++;
          }
        }
      }
      numNulls = count;
    }
    return numNulls;
  }

  @Override
  public boolean isNullAt(int rowId) {
    return delegate.isNullAt(rowIds[rowId]);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return delegate.getBoolean(rowIds[rowId]);
  }

  @Override
  public byte getByte(int rowId) {
    return delegate.getByte(rowIds[rowId]);
  }

  @Override
  public short getShort(int rowId) {
    return delegate.getShort(rowIds[rowId]);
  }

  @Override
  public int getInt(int rowId) {
    return delegate.getInt(rowIds[rowId]);
  }

  @Override
  public long getLong(int rowId) {
    return delegate.getLong(rowIds[rowId]);
  }

  @Override
  public float getFloat(int rowId) {
    return delegate.getFloat(rowIds[rowId]);
  }

  @Override
  public double getDouble(int rowId) {
    return delegate.getDouble(rowIds[rowId]);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    return delegate.getArray(rowIds[rowId]);
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    return delegate.getMap(rowIds[ordinal]);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return delegate.getDecimal(rowIds[rowId], precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    return delegate.getUTF8String(rowIds[rowId]);
  }

  @Override
  public byte[] getBinary(int rowId) {
    return delegate.getBinary(rowIds[rowId]);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (children == null) {
      children = new RowIdMappedColumnVector[ordinal + 1];
    } else if (children.length <= ordinal) {
      RowIdMappedColumnVector[] grown = new RowIdMappedColumnVector[ordinal + 1];
      System.arraycopy(children, 0, grown, 0, children.length);
      children = grown;
    }
    if (children[ordinal] == null) {
      children[ordinal] = new RowIdMappedColumnVector(delegate.getChild(ordinal), rowIds, numRows);
    }
    return children[ordinal];
  }
}
