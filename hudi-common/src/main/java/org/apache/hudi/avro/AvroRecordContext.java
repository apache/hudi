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

package org.apache.hudi.avro;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.AvroJavaTypeConverter;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.function.UnaryOperator;

import static org.apache.hudi.common.util.HoodieRecordUtils.generateEmptyAvroRecord;

/**
 * Record context for reading and transforming avro indexed records.
 */
public class AvroRecordContext extends RecordContext<IndexedRecord> {
  private static final AvroRecordContext FIELD_ACCESSOR_INSTANCE = new AvroRecordContext();

  public static AvroRecordContext getFieldAccessorInstance() {
    return FIELD_ACCESSOR_INSTANCE;
  }

  private final String payloadClass;

  public AvroRecordContext(HoodieTableConfig tableConfig, String payloadClass) {
    super(tableConfig, new AvroJavaTypeConverter());
    this.payloadClass = payloadClass;
  }

  public AvroRecordContext() {
    super(new AvroJavaTypeConverter());
    this.payloadClass = null;
  }

  public static Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      String fieldName) {
    Schema currentSchema = record.getSchema();
    IndexedRecord currentRecord = record;
    String[] path = fieldName.split("\\.");
    for (int i = 0; i < path.length; i++) {
      if (currentSchema.isUnion()) {
        currentSchema = AvroSchemaUtils.getNonNullTypeFromUnion(currentSchema);
      }
      Schema.Field field = currentSchema.getField(path[i]);
      if (field == null) {
        return null;
      }
      Object value = currentRecord.get(field.pos());
      if (i == path.length - 1) {
        return value;
      }
      currentSchema = field.schema();
      currentRecord = (IndexedRecord) value;
    }
    return null;
  }

  @Override
  public Object getValue(IndexedRecord record, HoodieSchema schema, String fieldName) {
    return getFieldValueFromIndexedRecord(record, fieldName);
  }

  @Override
  public String getMetaFieldValue(IndexedRecord record, int pos) {
    return record.get(pos).toString();
  }

  @Override
  public HoodieRecord constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord, String partitionPath) {
    // HoodieKey is not required so do not generate it if partitionPath is null
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);

    if (bufferedRecord.getRecord() == null && bufferedRecord.isDelete()) {
      return generateEmptyAvroRecord(
          hoodieKey,
          bufferedRecord.getOrderingValue(),
          payloadClass,
          bufferedRecord.getHoodieOperation());
    }

    return HoodieRecordUtils.createHoodieRecord((GenericRecord) bufferedRecord.getRecord(), bufferedRecord.getOrderingValue(),
        hoodieKey, payloadClass, bufferedRecord.getHoodieOperation(), Option.empty(), false, bufferedRecord.isDelete());
  }

  @Override
  public HoodieRecord<IndexedRecord> constructFinalHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord) {
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);

    if (bufferedRecord.isDelete()) {
      return new HoodieEmptyRecord<>(hoodieKey, bufferedRecord.getHoodieOperation(), bufferedRecord.getOrderingValue(), HoodieRecord.HoodieRecordType.AVRO);
    }
    return new HoodieAvroIndexedRecord(hoodieKey, bufferedRecord.getRecord(), bufferedRecord.getOrderingValue(), bufferedRecord.getHoodieOperation(), bufferedRecord.isDelete());
  }

  @Override
  public IndexedRecord extractDataFromRecord(HoodieRecord record, HoodieSchema schema, Properties properties) {
    try {
      return record.toIndexedRecord(schema.toAvroSchema(), properties).map(HoodieAvroIndexedRecord::getData).orElse(null);
    } catch (IOException e) {
      throw new HoodieException("Failed to extract data from record: " + record, e);
    }
  }

  @Override
  public IndexedRecord constructEngineRecord(HoodieSchema recordSchema, Object[] fieldValues) {
    GenericData.Record record = new GenericData.Record(recordSchema.toAvroSchema());
    for (int i = 0; i < fieldValues.length; i++) {
      record.put(i, fieldValues[i]);
    }
    return record;
  }

  @Override
  public IndexedRecord mergeWithEngineRecord(HoodieSchema schema,
                                             Map<Integer, Object> updateValues,
                                             BufferedRecord<IndexedRecord> baseRecord) {
    IndexedRecord engineRecord = baseRecord.getRecord();
    for (Map.Entry<Integer, Object> value : updateValues.entrySet()) {
      engineRecord.put(value.getKey(), value.getValue());
    }
    return engineRecord;
  }

  @Override
  public IndexedRecord convertAvroRecord(IndexedRecord record) {
    return record;
  }

  @Override
  public GenericRecord convertToAvroRecord(IndexedRecord record, HoodieSchema schema) {
    return (GenericRecord) record;
  }

  @Override
  public IndexedRecord getDeleteRow(String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }

  @Override
  public IndexedRecord seal(IndexedRecord record) {
    return record;
  }

  @Override
  public IndexedRecord toBinaryRow(HoodieSchema schema, IndexedRecord record) {
    return record;
  }

  @Override
  public UnaryOperator<IndexedRecord> projectRecord(Schema from, Schema to, Map<String, String> renamedColumns) {
    return record -> HoodieAvroUtils.rewriteRecordWithNewSchema(record, to, renamedColumns);
  }

  @Override
  public Comparable convertValueToEngineType(Comparable value) {
    if (value instanceof String) {
      return new Utf8((String) value);
    }
    return value;
  }
}
