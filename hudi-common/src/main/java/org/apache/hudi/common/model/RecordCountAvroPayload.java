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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

/**
 * Payload clazz that is used for pv/uv.
 * In order to use 'RecordCountAvroPayload', we need to add field [hoodie_record_count bigint]
 * to the schema when creating the hudi table to record the result of pv/uv, the field 'hoodie_record_count'
 * does not need to be filled in, flink will automatically set 'hoodie_record_count' to 'null',
 * and will update 'null' to '1' in #getInsertValue and #mergeOldRecord.
 *
 * <p>Simplified pv/uv calculation Logic:
 * <pre>
 *  1. #preCombine
 *  For records with the same record key in one batch
 *  or in the delta logs that belongs to same File Group,
 *  Add their 'hoodie_record_count' field value and
 *  overwrite the record with the larger ordering value.
 *
 *  2. #combineAndGetUpdateValue
 *  For every incoming record with existing record in storage (same record key)
 *  Add their 'hoodie_record_count' field value and overwrite the record
 *  with the larger ordering value and returns a merged record.
 *
 *  Illustration with simple data.
 *  let's say the order field is 'ts' and schema is :
 *  {
 *    [
 *      {"name":"id","type":"string"},
 *      {"name":"ts","type":"long"},
 *      {"name":"name","type":"string"},
 *      {"name":"hoodie_record_count","type":"long"}
 *    ]
 *  }
 *
 *  case 1
 *  Current data:
 *      id      ts      name    hoodie_record_count
 *      1       1       name_1  1
 *  Insert data:
 *      id      ts      name    hoodie_record_count
 *      1       2       name_2  2
 *
 *  Result data after #preCombine or #combineAndGetUpdateValue:
 *      id      ts      name    hoodie_record_count
 *      1       2       name_2  3
 *
 *  case 2
 *  Current data:
 *      id      ts      name    hoodie_record_count
 *      1       2       name_1  null
 *  Insert data:
 *      id      ts      name    hoodie_record_count
 *      1       1       name_2  1
 *
 *  Result data after #preCombine or #combineAndGetUpdateValue:
 *      id      ts      name    hoodie_record_count
 *      1       2       name_1  2
 *</pre>
 */
public class RecordCountAvroPayload extends OverwriteWithLatestAvroPayload {
  private static final Logger LOG = LogManager.getLogger(RecordCountAvroPayload.class);
  private static final String DEFAULT_RECORD_COUNT_FIELD_VAL = "hoodie_record_count";

  public RecordCountAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public RecordCountAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public RecordCountAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema, Properties properties) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    // pick the payload with greater ordering value as insert record
    final boolean shouldPickOldRecord = oldValue.orderingVal.compareTo(orderingVal) > 0 ? true : false;
    try {
      GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
      // Get the record that needs to be stored.
      Option<IndexedRecord> mergedRecord = mergeOldRecord(oldRecord, schema, shouldPickOldRecord);
      if (mergedRecord.isPresent()) {
        return new RecordCountAvroPayload((GenericRecord) mergedRecord.get(),
                shouldPickOldRecord ? oldValue.orderingVal : this.orderingVal);
      }
    } catch (Exception e) {
      LOG.warn("RecordCountAvroPayload will use the current value, Exception is: " + e);
      return this;
    }
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.mergeOldRecord(currentValue, schema, false);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop)
      throws IOException {
    return mergeOldRecord(currentValue, schema, isRecordNewer(orderingVal, currentValue, prop));
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      try {
        // Flink automatically set 'hoodie_record_count' to 'null', here updated to 1, so that the query result is 1.
        if (((GenericRecord) indexedRecord).get(DEFAULT_RECORD_COUNT_FIELD_VAL) == null) {
          ((GenericRecord) indexedRecord).put(DEFAULT_RECORD_COUNT_FIELD_VAL, 1L);
        }
      } catch (AvroRuntimeException e) {
        throw new HoodieException(
                String.format("When using RecordCountAvroPayload, an additional column (hoodie_record_count bigint) needs to be added to the source schema,"
                        + "current schema is [%s].", schema.toString()), e);
      }
      return Option.of(indexedRecord);
    }
  }

  /**
   * Get the latest delta record and update the 'hoodie_record_count' field.
   *
   * @param currentValue The current record from file
   * @param schema       The record schema
   * @param isBaseRecord Pick the payload with greatest ordering value as base record
   *
   * @return the merged record option.
   */
  private Option<IndexedRecord> mergeOldRecord(IndexedRecord currentValue, Schema schema, boolean isBaseRecord) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }
    final GenericRecord baseRecord;
    final GenericRecord mergedRecord;
    if (isBaseRecord) {
      baseRecord = (GenericRecord) currentValue;
      mergedRecord = (GenericRecord) recordOption.get();
    } else {
      baseRecord = (GenericRecord) recordOption.get();
      mergedRecord = (GenericRecord) currentValue;
    }
    if (isDeleteRecord(baseRecord)) {
      return Option.empty();
    }

    try {
      // When adding, 'null' represents '1'
      long currentRecordCount = mergedRecord.get(DEFAULT_RECORD_COUNT_FIELD_VAL) == null ? 1L : (long) mergedRecord.get(DEFAULT_RECORD_COUNT_FIELD_VAL);
      long insertRecordCount = baseRecord.get(DEFAULT_RECORD_COUNT_FIELD_VAL) == null ? 1L : (long) baseRecord.get(DEFAULT_RECORD_COUNT_FIELD_VAL);
      baseRecord.put(DEFAULT_RECORD_COUNT_FIELD_VAL, currentRecordCount + insertRecordCount);
    } catch (AvroRuntimeException e) {
      throw new HoodieException(
              String.format("When using RecordCountAvroPayload, an additional column (hoodie_record_count bigint) needs to be added to the source schema,"
                      + "current schema is [%s].", schema.toString()), e);
    }

    return Option.of(baseRecord);
  }
}
