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
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.AvroJavaTypeConverter;
import org.apache.hudi.common.util.SpillableMapUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.util.Map;

public class AvroRecordContext extends RecordContext<IndexedRecord> {

  private final String payloadClass;

  public AvroRecordContext(HoodieTableConfig tableConfig) {
    super(tableConfig);
    this.payloadClass = tableConfig.getPayloadClass();
    this.typeConverter = new AvroJavaTypeConverter();
  }

  public static Object getFieldValueFromIndexedRecord(
      IndexedRecord record,
      String fieldName) {
    Schema currentSchema = record.getSchema();
    IndexedRecord currentRecord = record;
    String[] path = fieldName.split("\\.");
    for (int i = 0; i < path.length; i++) {
      if (currentSchema.isUnion()) {
        currentSchema = AvroSchemaUtils.resolveNullableSchema(currentSchema);
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
  public Object getValue(IndexedRecord record, Schema schema, String fieldName) {
    return getFieldValueFromIndexedRecord(record, fieldName);
  }

  @Override
  public String getMetaFieldValue(IndexedRecord record, int pos) {
    return record.get(pos).toString();
  }

  @Override
  public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord) {
    return constructHoodieRecord(bufferedRecord, partitionPath);
  }

  @Override
  public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord, String partitionPath) {
    if (bufferedRecord.isDelete()) {
      return SpillableMapUtils.generateEmptyPayload(
          bufferedRecord.getRecordKey(),
          partitionPath,
          bufferedRecord.getOrderingValue(),
          payloadClass);
    }
    HoodieKey hoodieKey = new HoodieKey(bufferedRecord.getRecordKey(), partitionPath);
    return new HoodieAvroIndexedRecord(hoodieKey, bufferedRecord.getRecord());
  }

  @Override
  public IndexedRecord mergeWithEngineRecord(Schema schema,
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
  public GenericRecord convertToAvroRecord(IndexedRecord record, Schema schema) {
    return (GenericRecord) record;
  }

  @Override
  public IndexedRecord getDeleteRow(IndexedRecord record, String recordKey) {
    throw new UnsupportedOperationException("Not supported for " + this.getClass().getSimpleName());
  }
}
