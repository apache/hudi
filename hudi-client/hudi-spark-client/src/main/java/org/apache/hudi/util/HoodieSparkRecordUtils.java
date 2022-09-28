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

package org.apache.hudi.util;

import java.util.LinkedList;
import java.util.List;
import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.commmon.model.ComplexRowComparable;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import org.apache.hudi.common.util.collection.FlatLists;
import org.apache.hudi.common.util.collection.FlatLists.ComparableList;
import org.apache.spark.sql.HoodieCatalystExpressionUtils$;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils.NestedFieldPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.MapData;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;

public class HoodieSparkRecordUtils {

  public static Object getValue(StructType structType, String fieldName, InternalRow row) {
    NestedFieldPath posList = HoodieInternalRowUtils.getCachedPosList(structType, fieldName);
    return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, posList);
  }

  /**
   * Returns the string value of the given record {@code rec} and field {@code fieldName}. The field and value both could be missing.
   *
   * @param row       The record
   * @param fieldName The field name
   * @return the string form of the field or empty if the schema does not contain the field name or the value is null
   */
  public static Option<String> getNullableValAsString(StructType structType, InternalRow row, String fieldName) {
    String fieldVal = !HoodieCatalystExpressionUtils$.MODULE$.existField(structType, fieldName)
        ? null : StringUtils.objToString(getValue(structType, fieldName, row));
    return Option.ofNullable(fieldVal);
  }

  /**
   * Gets record column values into one object.
   *
   * @param row  InternalRow.
   * @param columns Names of the columns to get values.
   * @param structType  {@link StructType} instance.
   * @return Column value if a single column, or concatenated String values by comma.
   */
  public static ComparableList getRecordColumnValues(InternalRow row,
      String[] columns,
      StructType structType, boolean consistentLogicalTimestampEnabled) {
    List<Comparable> list = new LinkedList<>();
    for (String column : columns) {
      NestedFieldPath posList = HoodieInternalRowUtils.getCachedPosList(structType, column);
      Object value = HoodieUnsafeRowUtils.getNestedInternalRowValue(row, posList);
      DataType dataType = posList.parts()[posList.parts().length - 1]._2.dataType();
      if (value instanceof InternalRow | value instanceof MapData | value instanceof ArrayData
          | value instanceof CalendarInterval) {
        value = ComplexRowComparable.getInstance(value, dataType);
      }
      list.add((Comparable) value);
    }
    return FlatLists.ofComparable(list);
  }
}
