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

import static org.apache.hudi.common.util.TypeUtils.unsafeCast;

/**
 * TODO should be only limited to be used to add meta columns
 * TODO rebase to extend BaseGenericInternalRow
 */
public class HoodieInternalRow extends InternalRow {

  private static final int INTERNAL_META_COLUMNS_COUNT = HoodieRecord.HOODIE_META_COLUMNS.size();

  private UTF8String commitTime;
  private UTF8String commitSeqNumber;
  private UTF8String recordKey;
  private UTF8String partitionPath;
  private UTF8String fileName;
  private final InternalRow row;

  /**
   * Specifies whether source row contains meta-fields
   */
  private final boolean containsMetaFields;

  public HoodieInternalRow(UTF8String commitTime,
                           UTF8String commitSeqNumber,
                           UTF8String recordKey,
                           UTF8String partitionPath,
                           UTF8String fileName,
                           InternalRow row,
                           boolean containsMetaFields) {
    this.commitTime = commitTime;
    this.commitSeqNumber = commitSeqNumber;
    this.recordKey = recordKey;
    this.partitionPath = partitionPath;
    this.fileName = fileName;
    this.row = row;
    this.containsMetaFields = containsMetaFields;
  }

  @Override
  public int numFields() {
    return row.numFields();
  }

  @Override
  public void setNullAt(int ordinal) {
    if (ordinal >= HoodieRecord.HOODIE_META_COLUMNS.size()) {
      row.setNullAt(rebaseOrdinal(ordinal));
    } else {
      switch (ordinal) {
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
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    if (ordinal >= HoodieRecord.HOODIE_META_COLUMNS.size()) {
      row.update(rebaseOrdinal(ordinal), value);
    } else {
      switch (ordinal) {
        case 0:
          this.commitTime = unsafeCast(value);
          break;
        case 1:
          this.commitSeqNumber = unsafeCast(value);
          break;
        case 2:
          this.recordKey = unsafeCast(value);
          break;
        case 3:
          this.partitionPath = unsafeCast(value);
          break;
        case 4:
          this.fileName = unsafeCast(value);
          break;
        default:
          throw new IllegalArgumentException(String.format("Invalid ordinal (%d)", ordinal));
      }
    }
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return getMetaColumnVal(ordinal) == null;
    }
    return row.isNullAt(rebaseOrdinal(ordinal));
  }

  @Override
  public boolean getBoolean(int ordinal) {
    // TODO throw for meta columns
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
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    return row.getDecimal(rebaseOrdinal(ordinal), precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      return getMetaColumnVal(ordinal);
    }
    return row.getUTF8String(rebaseOrdinal(ordinal));
  }

  @Override
  public byte[] getBinary(int ordinal) {
    return row.getBinary(rebaseOrdinal(ordinal));
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    return row.getInterval(rebaseOrdinal(ordinal));
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    return row.getStruct(rebaseOrdinal(ordinal), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    return row.getArray(rebaseOrdinal(ordinal));
  }

  @Override
  public MapData getMap(int ordinal) {
    return row.getMap(rebaseOrdinal(ordinal));
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (ordinal < HoodieRecord.HOODIE_META_COLUMNS.size()) {
      // TODO assert data-type
      return getMetaColumnVal(ordinal);
    }
    return row.get(rebaseOrdinal(ordinal), dataType);
  }

  @Override
  public InternalRow copy() {
    return new HoodieInternalRow(commitTime, commitSeqNumber, recordKey, partitionPath, fileName, row.copy(), containsMetaFields);
  }

  private UTF8String getMetaColumnVal(int ordinal) {
    switch (ordinal) {
      case 0:
        return commitTime;
      case 1:
        return commitSeqNumber;
      case 2:
        return recordKey;
      case 3:
        return partitionPath;
      case 4:
        return fileName;
      default:
        throw new IllegalArgumentException("Not expected");
    }
  }

  private int rebaseOrdinal(int ordinal) {
    // NOTE: In cases when source row does not contain meta fields, we will have to
    //       rebase ordinal onto its indexes
    return containsMetaFields ? ordinal : ordinal - INTERNAL_META_COLUMNS_COUNT;
  }
}
