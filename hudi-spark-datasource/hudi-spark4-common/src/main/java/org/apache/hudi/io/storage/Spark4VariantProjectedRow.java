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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Spark 4 implementation of {@link VariantProjectedRow}. Pass-through
 * {@link InternalRow} that delegates every accessor to the wrapped input row
 * except at variant ordinals, where it returns a pre-allocated
 * {@code (metadata, value)} {@link GenericInternalRow} that the extractor
 * populates on demand.
 *
 * <p>Single-row, single-threaded; {@link #wrap(InternalRow)} mutates state.
 * {@link #copy()} / {@link #update(int, Object)} / {@link #setNullAt(int)}
 * throw - lance-spark consumes each row synchronously inside
 * {@code LanceArrowWriter.write(InternalRow)}, so no copy is ever needed.
 */
public abstract class Spark4VariantProjectedRow extends InternalRow implements VariantProjectedRow {

  private final int numFields;
  private final GenericInternalRow[] variantStructByOrdinal;
  private final List<BiConsumer<SpecializedGetters, Integer>> extractorByOrdinal;
  protected InternalRow input;

  public Spark4VariantProjectedRow(int numFields,
                                   GenericInternalRow[] variantStructByOrdinal,
                                   List<BiConsumer<SpecializedGetters, Integer>> extractorByOrdinal) {
    this.numFields = numFields;
    this.variantStructByOrdinal = variantStructByOrdinal;
    this.extractorByOrdinal = extractorByOrdinal;
  }

  @Override
  public InternalRow wrap(InternalRow input) {
    this.input = input;
    return this;
  }

  @Override
  public int numFields() {
    return numFields;
  }

  @Override
  public boolean isNullAt(int ordinal) {
    return input.isNullAt(ordinal);
  }

  @Override
  public InternalRow getStruct(int ordinal, int n) {
    GenericInternalRow variantStruct = variantStructByOrdinal[ordinal];
    if (variantStruct != null) {
      // Populate metadata (slot 0) and value (slot 1) via the captured lambdas.
      extractorByOrdinal.get(ordinal).accept(input, ordinal);
      return variantStruct;
    }
    return input.getStruct(ordinal, n);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return input.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return input.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return input.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return input.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return input.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return input.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return input.getDouble(ordinal);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return input.getDecimal(ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    return input.getUTF8String(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return input.getBinary(ordinal);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return input.getInterval(ordinal);
  }

  @Override
  public VariantVal getVariant(int ordinal) {
    return input.getVariant(ordinal);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return input.getArray(ordinal);
  }

  @Override
  public MapData getMap(int ordinal) {
    return input.getMap(ordinal);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    return input.get(ordinal, dataType);
  }

  @Override
  public void setNullAt(int i) {
    throw new UnsupportedOperationException("Spark4VariantProjectedRow is read-only");
  }

  @Override
  public void update(int i, Object value) {
    throw new UnsupportedOperationException("Spark4VariantProjectedRow is read-only");
  }

  @Override
  public InternalRow copy() {
    throw new UnsupportedOperationException("Spark4VariantProjectedRow is single-row scope");
  }
}
