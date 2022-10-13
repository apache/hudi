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

import org.apache.hudi.common.model.HoodieOperation;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RawValueData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;

/**
 * RowData implementation for Hoodie Row. It wraps an {@link RowData} and keeps meta columns locally. But the {@link RowData}
 * does include the meta columns as well just that {@link AbstractHoodieRowData} will intercept queries for meta columns and serve from its
 * copy rather than fetching from {@link RowData}.
 */
public abstract class AbstractHoodieRowData implements RowData {
  private final String[] metaColumns;
  protected final RowData row;
  protected final int metaColumnsNum;

  public AbstractHoodieRowData(String commitTime,
                               String commitSeqNumber,
                               String recordKey,
                               String partitionPath,
                               String fileName,
                               RowData row,
                               boolean withOperation) {
    this.metaColumnsNum = withOperation ? 6 : 5;
    this.metaColumns = new String[metaColumnsNum];
    metaColumns[0] = commitTime;
    metaColumns[1] = commitSeqNumber;
    metaColumns[2] = recordKey;
    metaColumns[3] = partitionPath;
    metaColumns[4] = fileName;
    if (withOperation) {
      metaColumns[5] = HoodieOperation.fromValue(row.getRowKind().toByteValue()).getName();
    }
    this.row = row;
  }

  @Override
  public RowKind getRowKind() {
    return row.getRowKind();
  }

  @Override
  public void setRowKind(RowKind kind) {
    this.row.setRowKind(kind);
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < metaColumnsNum) {
      return null == getMetaColumnVal(ordinal);
    }
    return row.isNullAt(rebaseOrdinal(ordinal));
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return row.getBoolean(rebaseOrdinal(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    return row.getByte(rebaseOrdinal(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    return row.getShort(rebaseOrdinal(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    return row.getInt(rebaseOrdinal(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    return row.getLong(rebaseOrdinal(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    return row.getFloat(rebaseOrdinal(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    return row.getDouble(rebaseOrdinal(ordinal));
  }

  @Override
  public DecimalData getDecimal(int ordinal, int precision, int scale) {
    return row.getDecimal(rebaseOrdinal(ordinal), precision, scale);
  }

  @Override
  public TimestampData getTimestamp(int ordinal, int precision) {
    return row.getTimestamp(rebaseOrdinal(ordinal), precision);
  }

  @Override
  public <T> RawValueData<T> getRawValue(int ordinal) {
    return row.getRawValue(rebaseOrdinal(ordinal));
  }

  @Override
  public StringData getString(int ordinal) {
    if (ordinal < metaColumnsNum) {
      return StringData.fromString(getMetaColumnVal(ordinal));
    }
    return row.getString(rebaseOrdinal(ordinal));
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return row.getBinary(rebaseOrdinal(ordinal));
  }

  @Override
  public RowData getRow(int ordinal, int numFields) {
    return row.getRow(rebaseOrdinal(ordinal), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return row.getArray(rebaseOrdinal(ordinal));
  }

  @Override
  public MapData getMap(int ordinal) {
    return row.getMap(rebaseOrdinal(ordinal));
  }

  private String getMetaColumnVal(int ordinal) {
    return this.metaColumns[ordinal];
  }

  protected abstract int rebaseOrdinal(int ordinal);
}
