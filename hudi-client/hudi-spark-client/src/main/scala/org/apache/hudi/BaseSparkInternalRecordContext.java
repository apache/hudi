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

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieSparkRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.DefaultJavaTypeConverter;

import org.apache.avro.Schema;
import org.apache.spark.sql.HoodieInternalRowUtils;
import org.apache.spark.sql.HoodieUnsafeRowUtils;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import scala.Function1;

import static org.apache.spark.sql.HoodieInternalRowUtils.getCachedSchema;

public abstract class BaseSparkInternalRecordContext extends RecordContext<InternalRow> {

  protected BaseSparkInternalRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig, new DefaultJavaTypeConverter());
  }

  protected BaseSparkInternalRecordContext() {
    super(new DefaultJavaTypeConverter());
  }

  public static Object getFieldValueFromInternalRow(InternalRow row, Schema recordSchema, String fieldName) {
    return getFieldValueFromInternalRowInternal(row, recordSchema, fieldName, false);
  }

  public static Object getFieldValueFromInternalRowAsJava(InternalRow row, Schema recordSchema, String fieldName) {
    return getFieldValueFromInternalRowInternal(row, recordSchema, fieldName, true);
  }

  private static Object getFieldValueFromInternalRowInternal(InternalRow row, Schema recordSchema, String fieldName, boolean convertToJavaType) {
    StructType structType = getCachedSchema(recordSchema);
    scala.Option<HoodieUnsafeRowUtils.NestedFieldPath> cachedNestedFieldPath =
        HoodieInternalRowUtils.getCachedPosList(structType, fieldName);
    if (cachedNestedFieldPath.isDefined()) {
      HoodieUnsafeRowUtils.NestedFieldPath nestedFieldPath = cachedNestedFieldPath.get();
      Object value = HoodieUnsafeRowUtils.getNestedInternalRowValue(row, nestedFieldPath);
      return convertToJavaType ? sparkTypeToJavaType(value) : value;
    } else {
      return null;
    }
  }

  private static Object sparkTypeToJavaType(Object value) {
    if (value == null) {
      return null;
    } else if (value instanceof UTF8String) {
      return ((UTF8String) value).toString();
    } else if (value instanceof Decimal) {
      return ((Decimal) value).toJavaBigDecimal();
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof CalendarInterval
        || value instanceof InternalRow
        || value instanceof org.apache.spark.sql.catalyst.util.MapData
        || value instanceof org.apache.spark.sql.catalyst.util.ArrayData) {
      throw new UnsupportedOperationException(String.format("Unsupported value type (%s)", value.getClass().getName()));
    } else {
      return value;
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
          bufferedRecord.getHoodieOperation(),
          bufferedRecord.getOrderingValue(),
          HoodieRecord.HoodieRecordType.SPARK);
    }

    Schema schema = getSchemaFromBufferRecord(bufferedRecord);
    InternalRow row = bufferedRecord.getRecord();
    return new HoodieSparkRecord(hoodieKey, row, HoodieInternalRowUtils.getCachedSchema(schema),
        false, bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), bufferedRecord.isDelete());
  }

  @Override
  public InternalRow constructEngineRecord(Schema recordSchema, Object[] fieldValues) {
    return new GenericInternalRow(fieldValues);
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
      // [SPARK-46832] UTF8String doesn't support compareTo anymore
      return SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory().wrapUTF8String(UTF8String.fromString((String) value));
    } else if (value instanceof UTF8String) {
      return SparkAdapterSupport$.MODULE$.sparkAdapter().getUTF8StringFactory().wrapUTF8String((UTF8String) value);
    }
    return value;
  }

  @Override
  public Comparable convertPartitionValueToEngineType(Comparable value) {
    if (value instanceof String) {
      // Spark reads String field values as UTF8String.
      // To foster value comparison, if the value is of String type, e.g., from
      // the delete record, we convert it to UTF8String type.
      return UTF8String.fromString((String) value);
    }
    return value;
  }

  @Override
  public InternalRow getDeleteRow(String recordKey) {
    UTF8String[] metaFields = new UTF8String[]{
        null,
        null,
        UTF8String.fromString(recordKey),
        UTF8String.fromString(partitionPath),
        null
    };
    return SparkAdapterSupport$.MODULE$.sparkAdapter().createInternalRow(metaFields, null, false);
  }

  @Override
  public InternalRow seal(InternalRow internalRow) {
    return internalRow.copy();
  }

  @Override
  public InternalRow toBinaryRow(Schema schema, InternalRow internalRow) {
    if (internalRow instanceof UnsafeRow) {
      return internalRow;
    }
    final UnsafeProjection unsafeProjection = HoodieInternalRowUtils.getCachedUnsafeProjection(schema);
    return unsafeProjection.apply(internalRow);
  }

  @Override
  public UnaryOperator<InternalRow> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    Function1<InternalRow, UnsafeRow> unsafeRowWriter =
        HoodieInternalRowUtils.getCachedUnsafeRowWriter(getCachedSchema(from), getCachedSchema(to), renamedColumns, Collections.emptyMap());
    return row -> (InternalRow) unsafeRowWriter.apply(row);
  }
}
