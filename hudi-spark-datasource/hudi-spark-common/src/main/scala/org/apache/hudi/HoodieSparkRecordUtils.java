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

package org.apache.hudi;

import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.MapperUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;

import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.table.HoodieTableConfig.PRECOMBINE_FIELD;
import static org.apache.hudi.common.util.MapperUtils.PARTITION_NAME;
import static org.apache.hudi.common.util.MapperUtils.POPULATE_META_FIELDS;
import static org.apache.hudi.common.util.MapperUtils.SIMPLE_KEY_GEN_FIELDS_OPT;
import static org.apache.hudi.common.util.MapperUtils.WITH_OPERATION_FIELD;

public class HoodieSparkRecordUtils {

  /**
   * Utility method to convert InternalRow to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(Schema schema, InternalRow data, String preCombineField, boolean withOperationField) {
    return convertToHoodieSparkRecord(schema, data, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, Option.empty());
  }

  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(Schema schema, InternalRow data, String preCombineField, boolean withOperationField,
      Option<String> partitionName) {
    return convertToHoodieSparkRecord(schema, data, preCombineField,
        Pair.of(HoodieRecord.RECORD_KEY_METADATA_FIELD, HoodieRecord.PARTITION_PATH_METADATA_FIELD),
        withOperationField, partitionName);
  }

  /**
   * Utility method to convert bytes to HoodieRecord using schema and payload class.
   */
  public static HoodieRecord<InternalRow> convertToHoodieSparkRecord(Schema schema, InternalRow data, String preCombineField, Pair<String, String> recordKeyPartitionPathFieldPair,
      boolean withOperationField, Option<String> partitionName) {
    final String recKey = getValue(schema, recordKeyPartitionPathFieldPair.getKey(), data).toString();
    final String partitionPath = (partitionName.isPresent() ? partitionName.get() :
        getValue(schema, recordKeyPartitionPathFieldPair.getRight(), data).toString());

    Object preCombineVal = getPreCombineVal(schema, data, preCombineField);
    HoodieOperation operation = withOperationField
        ? HoodieOperation.fromName(getNullableValAsString(schema, data, HoodieRecord.OPERATION_METADATA_FIELD)) : null;
    return new HoodieSparkRecord(new HoodieKey(recKey, partitionPath), data, operation, (Comparable<?>) preCombineVal);
  }

  /**
   * Returns the preCombine value with given field name.
   *
   * @param data            The avro record
   * @param preCombineField The preCombine field name
   * @return the preCombine field value or 0 if the field does not exist in the avro schema
   */
  private static Object getPreCombineVal(Schema schema, InternalRow data, String preCombineField) {
    if (preCombineField == null) {
      return 0;
    }
    return schema.getField(preCombineField) == null ? 0 : getValue(schema, preCombineField, data);
  }

  private static Object getValue(Schema schema, String fieldName, InternalRow row) {
    Schema.Field field = schema.getField(fieldName);
    DataType type = HoodieInternalRowUtils.getCacheSchema(schema).fields()[field.pos()].dataType();
    return row.get(field.pos(), type);
  }

  /**
   * Returns the string value of the given record {@code rec} and field {@code fieldName}. The field and value both could be missing.
   *
   * @param row       The record
   * @param fieldName The field name
   * @return the string form of the field or empty if the schema does not contain the field name or the value is null
   */
  private static Option<String> getNullableValAsString(Schema schema, InternalRow row, String fieldName) {
    Schema.Field field = schema.getField(fieldName);
    String fieldVal = field == null ? null : StringUtils.objToString(getValue(schema, fieldName, row));
    return Option.ofNullable(fieldVal);
  }
}
