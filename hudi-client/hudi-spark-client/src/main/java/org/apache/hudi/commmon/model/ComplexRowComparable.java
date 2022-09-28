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

package org.apache.hudi.commmon.model;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.jetbrains.annotations.NotNull;

public class ComplexRowComparable implements Comparable<Object> {

  private final Object data;

  private final DataType type;

  public ComplexRowComparable(Object data, DataType type) {
    this.data = data;
    this.type = type;
  }

  public static Comparable<Object> getInstance(Object data, DataType type) {
    return new ComplexRowComparable(data, type);
  }

  @Override
  public int compareTo(@NotNull Object o) {
    return innerCompare(data, ((ComplexRowComparable) o).data, type);
  }

  private static int innerCompare(Object left, Object right, DataType type) {
    if (left instanceof ArrayData) {
      return compareArray((ArrayData) left, (ArrayData) right, (ArrayType) type);
    } else if (left instanceof InternalRow) {
      return compareStruct((InternalRow) left, (InternalRow) right, (StructType) type);
    } else if (left instanceof MapData) {
      return compareMap((MapData) left, (MapData) right, (MapType) type);
    } else if (left instanceof CalendarInterval) {
      return compareInterval((CalendarInterval) left, (CalendarInterval) right);
    } else {
      return compare(left, right);
    }
  }

  private static int compareArray(ArrayData left, ArrayData right, ArrayType type) {
    int num = Math.min(left.numElements(), right.numElements());
    DataType dataType = type.elementType();
    for (int i = 0; i < num; i++) {
      int res = compareNullable(left, right, i);
      if (res != 0) {
        return res;
      }
      res = innerCompare(left.get(i, dataType), right.get(i, dataType), dataType);
      if (res != 0) {
        return res;
      }
    }
    return left.numElements() - right.numElements();
  }

  private static int compareStruct(InternalRow left, InternalRow right, StructType type) {
    StructField[] fields = type.fields();
    for (int i = 0; i < fields.length; i++) {
      int res = compareNullable(left, right, i);
      if (res != 0) {
        return res;
      }
      DataType elementType = fields[i].dataType();
      res = innerCompare(left.get(i, elementType), right.get(i, elementType), elementType);
      if (res != 0) {
        return res;
      }
    }
    return -1;
  }

  private static int compareMap(MapData left, MapData right, MapType type) {
    int res = compareArray(left.keyArray(), right.keyArray(), ArrayType.apply(type.keyType()));
    if (res != 0) {
      return res;
    } else {
      return compareArray(left.valueArray(), right.valueArray(), ArrayType.apply(type.valueType()));
    }
  }

  private static int compareInterval(CalendarInterval left, CalendarInterval right) {
    return left.toString().compareTo(right.toString());
  }

  private static int compare(Object left, Object right) {
    return ((Comparable) left).compareTo(right);
  }

  private static int compareNullable(SpecializedGetters left, SpecializedGetters right, int i) {
    if (left.isNullAt(i) && right.isNullAt(i)) {
      return 0;
    } else if (left.isNullAt(i)) {
      return -1;
    } else if (right.isNullAt(i)) {
      return 1;
    } else {
      return 0;
    }
  }
}
