/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.sink.append.sort;

import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Field comparators for sorting fields in buffer.
 */
public class FieldComparators {

  public static List<FieldComparator> createFieldComparators(
      RowType rowType, List<String> fields) {
    List<FieldComparator> comparators = new ArrayList<>();

    for (String field : fields) {
      int index = rowType.getFieldIndex(field);
      if (index == -1) {
        throw new IllegalArgumentException(
            String.format("Not found field %s in data for sort.", field));
      }

      LogicalType logicalType = rowType.getTypeAt(index);

      if (logicalType.getClass().equals(VarCharType.class)) {
        comparators.add(new StringComparator(index));
      } else if (logicalType.getClass().equals(DoubleType.class)) {
        comparators.add(new DoubleComparator(index));
      } else if (logicalType.getClass().equals(IntType.class)) {
        comparators.add(new IntegerComparator(index));
      } else if (logicalType.getClass().equals(FloatType.class)) {
        comparators.add(new FloatComparator(index));
      } else if (logicalType.getClass().equals(BinaryType.class)
          || logicalType.getClass().equals(VarBinaryType.class)) {
        comparators.add(new BinaryComparator(index));
      } else if (logicalType.getClass().equals(BigIntType.class)) {
        comparators.add(new LongComparator(index));
      } else if (logicalType.getClass().equals(SmallIntType.class)) {
        comparators.add(new ShortComparator(index));
      } else if (logicalType.getClass().equals(TimestampType.class)) {
        comparators.add(new TimestampComparator(index));
      } else {
        throw new IllegalArgumentException(
            String.format("Unsupported field %s with type %s for sort.", field, logicalType));
      }
    }

    return comparators;
  }

  public abstract static class FieldComparator implements Comparator<RowData>, Serializable {
    protected final int pos;

    public FieldComparator(int pos) {
      this.pos = pos;
    }

    public int getPos() {
      return pos;
    }
  }

  public static class StringComparator extends FieldComparator {
    public StringComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      StringData left = leftRow.getString(pos);
      StringData right = rightRow.getString(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class IntegerComparator extends FieldComparator {

    public IntegerComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Integer left = leftRow.isNullAt(pos) ? null : leftRow.getInt(pos);
      Integer right = rightRow.isNullAt(pos) ? null : rightRow.getInt(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class LongComparator extends FieldComparator {

    public LongComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Long left = leftRow.isNullAt(pos) ? null : leftRow.getLong(pos);
      Long right = rightRow.isNullAt(pos) ? null : rightRow.getLong(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class FloatComparator extends FieldComparator {

    public FloatComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Float left = leftRow.isNullAt(pos) ? null : leftRow.getFloat(pos);
      Float right = rightRow.isNullAt(pos) ? null : rightRow.getFloat(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class DoubleComparator extends FieldComparator {

    public DoubleComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Double left = leftRow.isNullAt(pos) ? null : leftRow.getDouble(pos);
      Double right = rightRow.isNullAt(pos) ? null : rightRow.getDouble(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class ByteComparator extends FieldComparator {

    public ByteComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Byte left = leftRow.isNullAt(pos) ? null : leftRow.getByte(pos);
      Byte right = rightRow.isNullAt(pos) ? null : rightRow.getByte(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class ShortComparator extends FieldComparator {

    public ShortComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      Short left = leftRow.isNullAt(pos) ? null : leftRow.getShort(pos);
      Short right = rightRow.isNullAt(pos) ? null : rightRow.getShort(pos);

      return compareNullableObject(left, right);
    }
  }

  public static class BinaryComparator extends FieldComparator {
    private final BytePrimitiveArrayComparator comparator;

    public BinaryComparator(int pos) {
      super(pos);
      this.comparator = new BytePrimitiveArrayComparator(true);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      byte[] left = leftRow.isNullAt(pos) ? null : leftRow.getBinary(pos);
      byte[] right = rightRow.isNullAt(pos) ? null : rightRow.getBinary(pos);

      if (left == null && right == null) {
        return 0;
      }

      if (left == null) {
        return  1;
      } else if (right == null) {
        return -1;
      }

      return comparator.compare(left, right);
    }
  }

  public static class TimestampComparator extends FieldComparator {

    public TimestampComparator(int pos) {
      super(pos);
    }

    @Override
    public int compare(RowData leftRow, RowData rightRow) {
      TimestampData left = leftRow.isNullAt(pos) ? null : leftRow.getTimestamp(pos, 3);
      TimestampData right = rightRow.isNullAt(pos) ? null : rightRow.getTimestamp(pos, 3);

      return compareNullableObject(left, right);
    }
  }

  private static int compareNullableObject(Comparable left, Comparable right) {
    if (left == null && right == null) {
      return 0;
    }

    if (left == null) {
      return  1;
    } else if (right == null) {
      return -1;
    }

    return left.compareTo(right);
  }
}
