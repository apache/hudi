/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi;

import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.UTF8String;

import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.HoodieInternalRowUtils.getCachedSchema;

public abstract class BaseSparkInternalRecordContext extends RecordContext<InternalRow> {

  public BaseSparkInternalRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig);
  }

  public static Object getFieldValueFromInternalRow(InternalRow row, Schema recordSchema, String fieldName) {
    StructType structType = getCachedSchema(recordSchema);
    scala.Option<HoodieUnsafeRowUtils.NestedFieldPath> cachedNestedFieldPath =
        HoodieInternalRowUtils.getCachedPosList(structType, fieldName);
    if (cachedNestedFieldPath.isDefined()) {
      HoodieUnsafeRowUtils.NestedFieldPath nestedFieldPath = cachedNestedFieldPath.get();
      return HoodieUnsafeRowUtils.getNestedInternalRowValue(row, nestedFieldPath);
    } else {
      return null;
    }
  }

  @Override
  public Object getValue(InternalRow row, Schema schema, String fieldName) {
    return getFieldValueFromInternalRow(row, schema, fieldName);
  }

  @Override
  public String getMetaFieldValue(InternalRow record, int pos) {
    return record.getString(pos);
  }

  @Override
  public HoodieRecord<InternalRow> constructHoodieRecord(BufferedRecord<InternalRow> bufferedRecord, String partitionPath) {
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(
          hoodieKey,
          HoodieRecord.HoodieRecordType.SPARK);
    }

    Schema schema = getSchemaFromBufferRecord(bufferedRecord);
    InternalRow row = bufferedRecord.getRecord();
    return new HoodieSparkRecord(hoodieKey, row, HoodieInternalRowUtils.getCachedSchema(schema),
        false, bufferedRecord.getHoodieOperation(), bufferedRecord.isDelete());
  }

  @Override
  public InternalRow mergeWithEngineRecord(Schema schema,
                                           Map<Integer, Object> updateValues,
                                           BufferedRecord<InternalRow> baseRecord) {
    List<Schema.Field> fields = schema.getFields();
    Object[] values = new Object[fields.size()];
    for (Schema.Field field : fields) {
      int pos = field.pos();
      if (updateValues.containsKey(pos)) {
        values[pos] = updateValues.get(pos);
      } else {
        values[pos] = getValue(baseRecord.getRecord(), schema, field.name());
      }
    }
    return new GenericInternalRow(values);
  }

  @Override
  public Comparable convertValueToEngineType(Comparable value) {
    if (value instanceof String) {
      // Spark reads String field values as UTF8String.
      // To foster value comparison, if the value is of String type, e.g., from
      // the delete record, we convert it to UTF8String type.
      return UTF8String.fromString((String) value);
    }
    return value;
  }

  @Override
  public InternalRow getDeleteRow(InternalRow record, String recordKey) {
    if (record != null) {
      return record;
    }
    return new HoodieInternalRow(null, null, UTF8String.fromString(recordKey), UTF8String.fromString(partitionPath), null, null, false);
  }
}
