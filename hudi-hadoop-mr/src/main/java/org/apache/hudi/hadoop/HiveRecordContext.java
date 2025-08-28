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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
import org.apache.hudi.hadoop.utils.HiveJavaTypeConverter;
import org.apache.hudi.hadoop.utils.HoodieArrayWritableAvroUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public class HiveRecordContext extends RecordContext<ArrayWritable> {

  private final Map<Schema, HiveAvroSerializer> serializerCache = new ConcurrentHashMap<>();

  private HiveAvroSerializer getHiveAvroSerializer(Schema schema) {
    return serializerCache.computeIfAbsent(schema, HiveAvroSerializer::new);
  }

  public HiveRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig, new HiveJavaTypeConverter());
  }

  @Override
  public Object getValue(ArrayWritable record, Schema schema, String fieldName) {
    return getHiveAvroSerializer(schema).getValue(record, fieldName);
  }

  @Override
  public String getMetaFieldValue(ArrayWritable record, int pos) {
    return record.get()[pos].toString();
  }

  @Override
  public HoodieRecord<ArrayWritable> constructHoodieRecord(BufferedRecord<ArrayWritable> bufferedRecord, String partitionPath) {
    HoodieKey key = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(
          key,
          bufferedRecord.getHoodieOperation(),
          bufferedRecord.getOrderingValue(),
          HoodieRecord.HoodieRecordType.HIVE);
    }
    Schema schema = getSchemaFromBufferRecord(bufferedRecord);
    ArrayWritable writable = bufferedRecord.getRecord();
    return new HoodieHiveRecord(key, writable, schema, getHiveAvroSerializer(schema), bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), bufferedRecord.isDelete());
  }

  @Override
  public ArrayWritable constructEngineRecord(Schema recordSchema, Object[] fieldValues) {
    return new ArrayWritable(Writable.class, (Writable[]) fieldValues);
  }

  @Override
  public ArrayWritable mergeWithEngineRecord(Schema schema,
                                             Map<Integer, Object> updateValues,
                                             BufferedRecord<ArrayWritable> baseRecord) {
    Writable[] engineRecord = baseRecord.getRecord().get();
    for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
      engineRecord[value.getKey()] = (Writable) value.getValue();
    }
    return baseRecord.getRecord();
  }

  @Override
  public Comparable convertValueToEngineType(Comparable value) {
    if (value instanceof WritableComparable) {
      return value;
    }
    //TODO: [HUDI-8261] cover more types
    if (value == null) {
      return null;
    } else if (value instanceof String) {
      return new Text((String) value);
    } else if (value instanceof Integer) {
      return new IntWritable((int) value);
    } else if (value instanceof Long) {
      return new LongWritable((long) value);
    } else if (value instanceof Float) {
      return new FloatWritable((float) value);
    } else if (value instanceof Double) {
      return new DoubleWritable((double) value);
    } else if (value instanceof Boolean) {
      return new BooleanWritable((boolean) value);
    }
    return value;
  }

  @Override
  public ArrayWritable convertAvroRecord(IndexedRecord avroRecord) {
    return (ArrayWritable) HoodieRealtimeRecordReaderUtils.avroToArrayWritable(avroRecord, avroRecord.getSchema(), true);
  }

  @Override
  public GenericRecord convertToAvroRecord(ArrayWritable record, Schema schema) {
    return getHiveAvroSerializer(schema).serialize(record);
  }

  @Override
  public ArrayWritable getDeleteRow(String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public ArrayWritable seal(ArrayWritable record) {
    return new ArrayWritable(Writable.class, Arrays.copyOf(record.get(), record.get().length));
  }

  @Override
  public ArrayWritable toBinaryRow(Schema schema, ArrayWritable record) {
    return record;
  }

  @Override
  public UnaryOperator<ArrayWritable> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    return record -> HoodieArrayWritableAvroUtils.rewriteRecordWithNewSchema(record, from, to, renamedColumns);
  }
}
