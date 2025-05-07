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

package org.apache.hudi.client.model;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

import java.util.Map;
import java.util.function.Function;

/**
 * RowData implementation used when reading from Spark bootstrapped table. In these tables, the partition values
 * are not always written to the data files, so we need to use the values inferred from the file's partition path.
 */
public class BootstrapRowData implements RowData {
  private final RowData row;
  private final Map<Integer, Object> partitionOrdinalToValues;

  public BootstrapRowData(RowData row, Map<Integer, Object> partitionOrdinalToValues) {
    this.row = row;
    this.partitionOrdinalToValues = partitionOrdinalToValues;
  }

  @Override
  public int getArity() {
    return row.getArity();
  }

  @Override
  public RowKind getRowKind() {
    return row.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    row.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int pos) {
    return !partitionOrdinalToValues.containsKey(pos) || row.isNullAt(pos);
  }

  @Override
  public boolean getBoolean(int pos) {
    return getValue(pos, row::getBoolean);
  }

  @Override
  public byte getByte(int pos) {
    return getValue(pos, row::getByte);
  }

  @Override
  public short getShort(int pos) {
    return getValue(pos, row::getShort);
  }

  @Override
  public int getInt(int pos) {
    return getValue(pos, row::getInt);
  }

  @Override
  public long getLong(int pos) {
    return getValue(pos, row::getLong);
  }

  @Override
  public float getFloat(int pos) {
    return getValue(pos, row::getFloat);
  }

  @Override
  public double getDouble(int pos) {
    return getValue(pos, row::getDouble);
  }

  @Override
  public StringData getString(int pos) {
    return getValue(pos, row::getString);
  }

  @Override
  public DecimalData getDecimal(int pos, int precision, int scale) {
    return getValue(pos, (p) -> row.getDecimal(p, precision, scale));
  }

  @Override
  public TimestampData getTimestamp(int pos, int precision) {
    return getValue(pos, (p) -> row.getTimestamp(p, precision));
  }

  @Override
  public <T> RawValueData<T> getRawValue(int pos) {
    return getValue(pos, row::getRawValue);
  }

  @Override
  public byte[] getBinary(int pos) {
    return getValue(pos, row::getBinary);
  }

  @Override
  public ArrayData getArray(int pos) {
    // bootstrap partition values cannot be arrays
    return row.getArray(pos);
  }

  @Override
  public MapData getMap(int pos) {
    // bootstrap partition values cannot be maps
    return row.getMap(pos);
  }

  @Override
  public RowData getRow(int pos, int numFields) {
    // bootstrap partition values cannot be rows
    return row.getRow(pos, numFields);
  }

  private <T> T getValue(int pos, Function<Integer, T> getter) {
    if (row.isNullAt(pos) && partitionOrdinalToValues.containsKey(pos)) {
      return (T) partitionOrdinalToValues.get(pos);
    }
    return getter.apply(pos);
  }
}
