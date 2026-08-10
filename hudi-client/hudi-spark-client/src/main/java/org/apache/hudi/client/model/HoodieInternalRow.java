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
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Hudi internal implementation of the {@link InternalRow} allowing to extend arbitrary
 * {@link InternalRow} overlaying Hudi-internal meta-fields on top of it.
 *
 * Capable of overlaying meta-fields in both cases: whether original {@link #sourceRow} contains
 * meta columns or not. This allows to handle following use-cases allowing to avoid any
 * manipulation (reshuffling) of the source row, by simply creating new instance
 * of {@link HoodieInternalRow} with all the meta-values provided
 *
 * <ul>
 *   <li>When meta-fields need to be prepended to the source {@link InternalRow}</li>
 *   <li>When meta-fields need to be updated w/in the source {@link InternalRow}
 *   ({@link org.apache.spark.sql.catalyst.expressions.UnsafeRow} currently does not
 *   allow in-place updates due to its memory layout)</li>
 * </ul>
 */
public abstract class HoodieInternalRow extends InternalRow {

  /**
   * Collection of meta-fields as defined by {@link HoodieRecord#HOODIE_META_COLUMNS}
   *
   * NOTE: {@code HoodieInternalRow} *always* overlays its own meta-fields even in case
   *       when source row also contains them, to make sure these fields are mutable and
   *       can be updated (for ex, {@link UnsafeRow} doesn't support mutations due to
   *       its memory layout, as it persists field offsets)
   */
  protected final UTF8String[] metaFields;
  protected final InternalRow sourceRow;

  /**
   * Specifies whether source {@link #sourceRow} contains meta-fields
   */
  protected final boolean sourceContainsMetaFields;

  public HoodieInternalRow(UTF8String[] metaFields,
                           InternalRow sourceRow,
                           boolean sourceContainsMetaFields) {
    this.metaFields = metaFields;
    this.sourceRow = sourceRow;
    this.sourceContainsMetaFields = sourceContainsMetaFields;
  }

  @Override
  public int numFields() {
    if (sourceContainsMetaFields) {
      return sourceRow.numFields();
    } else {
      return sourceRow.numFields() + metaFields.length;
    }
  }

  @Override
  public void setNullAt(int ordinal) {
    if (ordinal < metaFields.length) {
      metaFields[ordinal] = null;
    } else {
      sourceRow.setNullAt(rebaseOrdinal(ordinal));
    }
  }

  @Override
  public void update(int ordinal, Object value) {
    if (ordinal < metaFields.length) {
      if (value instanceof UTF8String) {
        metaFields[ordinal] = (UTF8String) value;
      } else if (value instanceof String) {
        metaFields[ordinal] = UTF8String.fromString((String) value);
      } else {
        throw new IllegalArgumentException(
            String.format("Could not update the row at (%d) with value of type (%s), either UTF8String or String are expected", ordinal, value.getClass().getSimpleName()));
      }
    } else {
      sourceRow.update(rebaseOrdinal(ordinal), value);
    }
  }

  @Override
  public boolean isNullAt(int ordinal) {
    if (ordinal < metaFields.length) {
      return metaFields[ordinal] == null;
    }
    return sourceRow.isNullAt(rebaseOrdinal(ordinal));
  }

  @Override
  public UTF8String getUTF8String(int ordinal) {
    if (ordinal < metaFields.length) {
      return metaFields[ordinal];
    }
    return sourceRow.getUTF8String(rebaseOrdinal(ordinal));
  }

  @Override
  public Object get(int ordinal, DataType dataType) {
    if (ordinal < metaFields.length) {
      validateMetaFieldDataType(dataType);
      return metaFields[ordinal];
    }
    return sourceRow.get(rebaseOrdinal(ordinal), dataType);
  }

  @Override
  public boolean getBoolean(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Boolean.class);
    return sourceRow.getBoolean(rebaseOrdinal(ordinal));
  }

  @Override
  public byte getByte(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Byte.class);
    return sourceRow.getByte(rebaseOrdinal(ordinal));
  }

  @Override
  public short getShort(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Short.class);
    return sourceRow.getShort(rebaseOrdinal(ordinal));
  }

  @Override
  public int getInt(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Integer.class);
    return sourceRow.getInt(rebaseOrdinal(ordinal));
  }

  @Override
  public long getLong(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Long.class);
    return sourceRow.getLong(rebaseOrdinal(ordinal));
  }

  @Override
  public float getFloat(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Float.class);
    return sourceRow.getFloat(rebaseOrdinal(ordinal));
  }

  @Override
  public double getDouble(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Double.class);
    return sourceRow.getDouble(rebaseOrdinal(ordinal));
  }

  @Override
  public Decimal getDecimal(int ordinal, int precision, int scale) {
    ruleOutMetaFieldsAccess(ordinal, Decimal.class);
    return sourceRow.getDecimal(rebaseOrdinal(ordinal), precision, scale);
  }

  @Override
  public byte[] getBinary(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, Byte[].class);
    return sourceRow.getBinary(rebaseOrdinal(ordinal));
  }

  @Override
  public CalendarInterval getInterval(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, CalendarInterval.class);
    return sourceRow.getInterval(rebaseOrdinal(ordinal));
  }

  @Override
  public InternalRow getStruct(int ordinal, int numFields) {
    ruleOutMetaFieldsAccess(ordinal, InternalRow.class);
    return sourceRow.getStruct(rebaseOrdinal(ordinal), numFields);
  }

  @Override
  public ArrayData getArray(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, ArrayData.class);
    return sourceRow.getArray(rebaseOrdinal(ordinal));
  }

  @Override
  public MapData getMap(int ordinal) {
    ruleOutMetaFieldsAccess(ordinal, MapData.class);
    return sourceRow.getMap(rebaseOrdinal(ordinal));
  }

  protected int rebaseOrdinal(int ordinal) {
    // NOTE: In cases when source row does not contain meta fields, we will have to
    //       rebase ordinal onto its indexes
    return sourceContainsMetaFields ? ordinal : ordinal - metaFields.length;
  }

  private void validateMetaFieldDataType(DataType dataType) {
    if (!dataType.sameType(StringType$.MODULE$)) {
      throw new ClassCastException(String.format("Can not cast meta-field of type UTF8String to %s", dataType.simpleString()));
    }
  }

  protected void ruleOutMetaFieldsAccess(int ordinal, Class<?> expectedDataType) {
    if (ordinal < metaFields.length) {
      throw new ClassCastException(String.format("Can not cast meta-field of type UTF8String at (%d) as %s", ordinal, expectedDataType.getName()));
    }
  }
}
