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

import org.apache.hudi.HoodieInternalRowUtils;
import org.apache.hudi.commmon.model.HoodieSparkRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.sql.HoodieCatalystExpressionUtils$;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils.NestedFieldPath;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

public class HoodieSparkRecordUtils {

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, boolean withOperationField) {
    return convertToHoodieSparkRecord(structType, data,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, boolean withOperationField,
      Option<String> partitionName) {
    return convertToHoodieSparkRecord(structType, data,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(StructType structType, InternalRow data, Pair<String, String> recordKeyPartitionPathFieldPair,
      boolean withOperationField, Option<String> partitionName) {
    final String recKey = getValue(structType, recordKeyPartitionPathFieldPair.getKey(), data).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        getValue(structType, recordKeyPartitionPathFieldPair.getRight(), data).toString());

    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(structType, data, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    return new HoodieSparkRecord(new HoodieKey(recKey, partitionPath), data, structType, operation);
  }

  private static Object getValue(StructType structType, String fieldName, InternalRow row) {
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
  private static Option<String> getNullableValAsString(StructType structType, InternalRow row, String fieldName) {
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
  public static Object getRecordColumnValues(InternalRow row,
      String[] columns,
      StructType structType, boolean consistentLogicalTimestampEnabled) {
    if (columns.length == 1) {
      NestedFieldPath posList = HoodieInternalRowUtils.getCachedPosList(structType, columns[0]);
      return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, posList);
    } else {
      // TODO this is inefficient, instead we can simply return array of Comparable
      StringBuilder sb = new StringBuilder();
      for (String col : columns) {
        // TODO support consistentLogicalTimestampEnabled
        NestedFieldPath posList = HoodieInternalRowUtils.getCachedPosList(structType, columns[0]);
        return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, posList);
      }
      return sb.toString();
    }
  }
}
