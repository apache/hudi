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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.hadoop.utils.HiveAvroSerializer;
import org.apache.hudi.hadoop.utils.HiveJavaTypeConverter;
import org.apache.hudi.hadoop.utils.HoodieArrayWritableAvroUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.UnaryOperator;

public class HiveRecordContext extends RecordContext<ArrayWritable> {
  private static final HiveRecordContext FIELD_ACCESSOR_INSTANCE = new HiveRecordContext();

  public static HiveRecordContext getFieldAccessorInstance() {
    return FIELD_ACCESSOR_INSTANCE;
  }

  private final Map<Schema, HiveAvroSerializer> serializerCache = new ConcurrentHashMap<>();

  private HiveAvroSerializer getHiveAvroSerializer(Schema schema) {
    return serializerCache.computeIfAbsent(schema, HiveAvroSerializer::new);
  }

  public HiveRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig, new HiveJavaTypeConverter());
  }

  private HiveRecordContext() {
    super(new HiveJavaTypeConverter());
  }

  @Override
  public Object getValue(ArrayWritable record, HoodieSchema schema, String fieldName) {
    return getHiveAvroSerializer(schema.toAvroSchema()).getValue(record, fieldName);
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
    HoodieSchema schema = getSchemaFromBufferRecord(bufferedRecord);
    ArrayWritable writable = bufferedRecord.getRecord();
    return new HoodieHiveRecord(key, writable, schema.toAvroSchema(), getHiveAvroSerializer(schema.toAvroSchema()),
        bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), bufferedRecord.isDelete());
  }

  @Override
  public ArrayWritable constructEngineRecord(HoodieSchema recordSchema, Object[] fieldValues) {
    Schema avroSchema = recordSchema.toAvroSchema();
    List<Schema.Field> fields = avroSchema.getFields();
    if (fields.size() != fieldValues.length) {
      throw new IllegalArgumentException("Mismatch between schema fields and values");
    }

    Writable[] writables = new Writable[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      Schema fieldSchema = resolveUnion(fields.get(i).schema());
      writables[i] = convertToWritable(fieldSchema, fieldValues[i]);
    }
    return new ArrayWritable(Writable.class, writables);
  }

  @Override
  public ArrayWritable mergeWithEngineRecord(HoodieSchema schema,
                                             Map<Integer, Object> updateValues,
                                             BufferedRecord<ArrayWritable> baseRecord) {
    Writable[] engineRecord = baseRecord.getRecord().get();
    Schema avroSchema = schema.toAvroSchema();
    List<Schema.Field> fields = avroSchema.getFields();
    for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
      int pos = value.getKey();
      Object updateValue = value.getValue();
      
      // If value is already a Writable, use it directly
      if (updateValue instanceof Writable) {
        if (pos >= 0 && pos < engineRecord.length) {
          engineRecord[pos] = (Writable) updateValue;
        }
      } else if (pos >= 0 && pos < fields.size() && pos < engineRecord.length) {
        // Convert using schema if position is valid
        Schema fieldSchema = resolveUnion(fields.get(pos).schema());
        engineRecord[pos] = convertToWritable(fieldSchema, updateValue);
      }
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
  public GenericRecord convertToAvroRecord(ArrayWritable record, HoodieSchema schema) {
    return getHiveAvroSerializer(schema.toAvroSchema()).serialize(record);
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
  public ArrayWritable toBinaryRow(HoodieSchema schema, ArrayWritable record) {
    return record;
  }

  @Override
  public UnaryOperator<ArrayWritable> projectRecord(HoodieSchema from, HoodieSchema to, Map<String, String> renamedColumns) {
    return record -> HoodieArrayWritableAvroUtils.rewriteRecordWithNewSchema(record, from.toAvroSchema(), to.toAvroSchema(), renamedColumns);
  }

  /**
   * Resolves a union schema by returning the first non-null type.
   * If the schema is not a union, returns the schema as-is.
   *
   * @param schema The schema to resolve
   * @return The resolved schema (first non-null type for unions, or the schema itself)
   */
  private Schema resolveUnion(Schema schema) {
    if (schema.getType() == Schema.Type.UNION) {
      // Return the first non-null type
      return schema.getTypes().stream()
          .filter(s -> s.getType() != Schema.Type.NULL)
          .findFirst()
          .orElseThrow(() -> new IllegalArgumentException("Union must contain a non-null type"));
    }
    return schema;
  }

  /**
   * Converts a Java object to a Hive Writable based on the Avro schema type.
   * Handles all Avro primitive types, complex types (arrays, maps, records), and null values.
   *
   * @param schema The Avro schema for the value
   * @param value  The value to convert
   * @return A Writable instance representing the value
   */
  private Writable convertToWritable(Schema schema, Object value) {
    if (value == null) {
      return NullWritable.get();
    }

    // If value is already a Writable, return it directly
    if (value instanceof Writable) {
      return (Writable) value;
    }

    switch (schema.getType()) {
      case INT:
        return new IntWritable((Integer) value);
      case LONG:
        return new LongWritable((Long) value);
      case FLOAT:
        return new FloatWritable((Float) value);
      case DOUBLE:
        return new DoubleWritable((Double) value);
      case BOOLEAN:
        return new BooleanWritable((Boolean) value);
      case STRING:
        return new Text(value.toString());
      case BYTES:
        if (value instanceof ByteBuffer) {
          ByteBuffer buffer = (ByteBuffer) value;
          byte[] bytes = new byte[buffer.remaining()];
          buffer.get(bytes);
          return new BytesWritable(bytes);
        } else if (value instanceof byte[]) {
          return new BytesWritable((byte[]) value);
        }
        throw new IllegalArgumentException("Invalid value for BYTES: " + value);
      case FIXED:
        return new BytesWritable(((GenericFixed) value).bytes());
      case ENUM:
        return new Text(value.toString());
      case ARRAY:
        List<?> list = (List<?>) value;
        Schema elementSchema = resolveUnion(schema.getElementType());
        Writable[] arrayElements = new Writable[list.size()];
        for (int i = 0; i < list.size(); i++) {
          arrayElements[i] = convertToWritable(elementSchema, list.get(i));
        }
        return new ArrayWritable(Writable.class, arrayElements);
      case MAP:
        Map<?, ?> map = (Map<?, ?>) value;
        MapWritable mapWritable = new MapWritable();
        Schema valueSchema = resolveUnion(schema.getValueType());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
          Writable keyWritable = new Text(entry.getKey().toString());
          Writable valWritable = convertToWritable(valueSchema, entry.getValue());
          mapWritable.put(keyWritable, valWritable);
        }
        return mapWritable;
      case RECORD:
        GenericRecord record = (GenericRecord) value;
        List<Schema.Field> fields = schema.getFields();
        Writable[] recordFields = new Writable[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
          Schema fieldSchema = resolveUnion(fields.get(i).schema());
          Object fieldValue = record.get(fields.get(i).name());
          recordFields[i] = convertToWritable(fieldSchema, fieldValue);
        }
        return new ArrayWritable(Writable.class, recordFields);
      default:
        throw new UnsupportedOperationException("Unsupported Avro type: " + schema.getType());
    }
  }
}
