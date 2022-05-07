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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Internal Row implementation for Hoodie Row. It wraps an {@link InternalRow} and keeps meta columns locally. But the {@link InternalRow}
 * does include the meta columns as well just that {@link HoodieInternalRow} will intercept queries for meta columns and serve from its
 * copy rather than fetching from {@link InternalRow}.
 */
public class HoodieInternalRow extends InternalRow {

  private String commitTime;
  private String commitSeqNumber;
  private String recordKey;
  private String partitionPath;
  private String fileName;
  private InternalRow row;

  public HoodieInternalRow(String commitTime, String commitSeqNumber, String recordKey, String partitionPath,
      String fileName, InternalRow row) {
    this.commitTime = commitTime;
    this.commitSeqNumber = commitSeqNumber;
    this.recordKey = recordKey;
    this.partitionPath = partitionPath;
    this.fileName = fileName;
    this.row = row;
  }

  @Override
  public int numFields() {
    return row.numFields();
  }

  @Override
  public void setNullAt(int i) {
    if (i < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      switch (i) {
        case 0: {
          this.commitTime = null;
          break;
        }
        case 1: {
          this.commitSeqNumber = null;
          break;
        }
        case 2: {
          this.recordKey = null;
          break;
        }
        case 3: {
          this.partitionPath = null;
          break;
        }
        case 4: {
          this.fileName = null;
          break;
        }
        default: throw new IllegalArgumentException("Not expected");
      }
    } else {
      row.setNullAt(i);
    }
  }

  @Override
  public void update(int i, Object value) {
    if (i < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      switch (i) {
        case 0: {
          this.commitTime = value.toString();
          break;
        }
        case 1: {
          this.commitSeqNumber = value.toString();
          break;
        }
        case 2: {
          this.recordKey = value.toString();
          break;
        }
        case 3: {
          this.partitionPath = value.toString();
          break;
        }
        case 4: {
          this.fileName = value.toString();
          break;
        }
        default: throw new IllegalArgumentException("Not expected");
      }
    } else {
      row.update(i, value);
    }
  }

  private String getMetaColumnVal(int ordinal) {
    switch (ordinal) {
      case 0: {
        return commitTime;
      }
      case 1: {
        return commitSeqNumber;
      }
      case 2: {
        return recordKey;
      }
      case 3: {
        return partitionPath;
      }
      case 4: {
        return fileName;
      }
      default: throw new IllegalArgumentException("Not expected");
    }
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return null == getMetaColumnVal(ordinal);
    }
    return row.isNullAt(ordinal);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return row.getBoolean(ordinal);
  }

  @Override
  public byte getByte(int ordinal) {
    return row.getByte(ordinal);
  }

  @Override
  public short getShort(int ordinal) {
    return row.getShort(ordinal);
  }

  @Override
  public int getInt(int ordinal) {
    return row.getInt(ordinal);
  }

  @Override
  public long getLong(int ordinal) {
    return row.getLong(ordinal);
  }

  @Override
  public float getFloat(int ordinal) {
    return row.getFloat(ordinal);
  }

  @Override
  public double getDouble(int ordinal) {
    return row.getDouble(ordinal);
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return row.getDecimal(ordinal, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return UTF8String.fromBytes(getMetaColumnVal(ordinal).getBytes());
    }
    return row.getUTF8String(ordinal);
  }

  @Override
  public String getString(int ordinal) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return new String(getMetaColumnVal(ordinal).getBytes());
    }
    return row.getString(ordinal);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return row.getBinary(ordinal);
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return row.getInterval(ordinal);
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return row.getStruct(ordinal, numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return row.getArray(ordinal);
  }

  @Override
  public MapData getMap(int ordinal) {
    return row.getMap(ordinal);
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return UTF8String.fromBytes(getMetaColumnVal(ordinal).getBytes());
    }
    return row.get(ordinal, dataType);
  }

  @Override
  public InternalRow copy() {
    return new HoodieInternalRow(commitTime, commitSeqNumber, recordKey, partitionPath, fileName, row.copy());
  }
}
