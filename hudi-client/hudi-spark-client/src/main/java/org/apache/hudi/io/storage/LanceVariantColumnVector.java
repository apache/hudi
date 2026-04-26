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

import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * {@link ColumnVector} wrapper that exposes a Hudi/parquet-variant on-disk struct
 * ({@code metadata: binary, value: binary}) as a Spark {@code VariantType} column.
 *
 * <p>Spark's {@link ColumnVector#getVariant(int)} is {@code final} and reads its
 * children positionally:
 * <pre>
 *   new VariantVal(getChild(0).getBinary(rowId), getChild(1).getBinary(rowId))
 * </pre>
 * - i.e. {@code child(0) = value}, {@code child(1) = metadata}. Hudi follows the
 * parquet-variant spec on disk ({@code metadata} first, {@code value} second), so we
 * swap children at the {@code ColumnVector} boundary: {@code getChild(0)} returns the
 * value column and {@code getChild(1)} returns the metadata column. The on-disk
 * Lance file is unchanged. See #18334 and future actionable items required to fix this.
 */
class LanceVariantColumnVector extends ColumnVector {

  private final ColumnVector innerStructVector;
  private final ColumnVector valueChild;
  private final ColumnVector metadataChild;

  LanceVariantColumnVector(DataType variantType,
                           ColumnVector innerStructVector,
                           ColumnVector valueChild,
                           ColumnVector metadataChild) {
    super(variantType);
    this.innerStructVector = innerStructVector;
    this.valueChild = valueChild;
    this.metadataChild = metadataChild;
  }

  @Override
  public void close() {
    // Only close the inner struct wrapper. The metadata/value child wrappers
    // reference Arrow buffers owned by the parent struct's allocator, so closing
    // them would double-release.
    innerStructVector.close();
  }

  @Override
  public boolean hasNull() {
    return innerStructVector.hasNull();
  }

  @Override
  public int numNulls() {
    return innerStructVector.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return innerStructVector.isNullAt(rowId);
  }

  @Override
  public ColumnVector getChild(int ordinal) {
    if (ordinal == 0) {
      return valueChild;
    }
    if (ordinal == 1) {
      return metadataChild;
    }
    throw new IndexOutOfBoundsException("Variant child ordinal out of range: " + ordinal);
  }

  // The remaining accessors are unreachable: Spark's getVariant only calls isNullAt
  // and getChild on a VariantType column. Fail loudly if anything else is invoked.

  @Override
  public boolean getBoolean(int rowId) {
    throw new UnsupportedOperationException("getBoolean not supported on variant column");
  }

  @Override
  public byte getByte(int rowId) {
    throw new UnsupportedOperationException("getByte not supported on variant column");
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("getShort not supported on variant column");
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException("getInt not supported on variant column");
  }

  @Override
  public long getLong(int rowId) {
    throw new UnsupportedOperationException("getLong not supported on variant column");
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("getFloat not supported on variant column");
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException("getDouble not supported on variant column");
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("getArray not supported on variant column");
  }

  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException("getMap not supported on variant column");
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    throw new UnsupportedOperationException("getDecimal not supported on variant column");
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    throw new UnsupportedOperationException("getUTF8String not supported on variant column");
  }

  @Override
  public byte[] getBinary(int rowId) {
    throw new UnsupportedOperationException("getBinary not supported on variant column");
  }
}
