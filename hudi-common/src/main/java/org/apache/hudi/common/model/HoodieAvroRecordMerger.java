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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.HoodieRecord.HoodieRecordType;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Record merger for Hoodie avro record.
 *
 * <p>It should only be used for base record from disk to merge with incoming record.
 */
public class HoodieAvroRecordMerger implements HoodieRecordMerger, OperationModeAwareness {
  public static final String PAYLOAD_CLASS_PROP = "_hoodie.merger.payload.class";
  protected String payloadClass;
  protected String[] orderingFields;

  @Override
  public String getMergingStrategy() {
    return HoodieRecordMerger.PAYLOAD_BASED_MERGE_STRATEGY_UUID;
  }

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    // special case for handling commit time ordering deletes
    if (HoodieRecordMerger.isCommitTimeOrderingDelete(older, newer)) {
      return newer;
    }
    init(props);
    IndexedRecord previousAvroData = older.getRecord() == null ? null : recordContext.convertToAvroRecord(older.getRecord(), recordContext.getSchemaFromBufferRecord(older));
    Schema newerSchema = recordContext.getSchemaFromBufferRecord(newer);
    GenericRecord newerAvroRecord = newer.getRecord() == null ? null : recordContext.convertToAvroRecord(newer.getRecord(), recordContext.getSchemaFromBufferRecord(newer));
    HoodieRecordPayload payload = HoodieRecordUtils.loadPayload(payloadClass, newerAvroRecord, newer.getOrderingValue());

    if (previousAvroData == null) {
      // No data to merge with, simply return the newer one
      return newer;
    } else {
      Option<IndexedRecord> updatedValue = payload.combineAndGetUpdateValue(previousAvroData, newerSchema, props);
      if (updatedValue.isPresent()) {
        IndexedRecord updatedRecord = updatedValue.get();
        // If there is no change in the record or the merge results in SENTINEL record (returned by expression payload when condition is not matched), then return the older record
        if (updatedRecord == previousAvroData || updatedRecord == HoodieRecord.SENTINEL) {
          return older;
        }
        if (updatedRecord == newerAvroRecord) {
          // simply return the newer record instead of creating a new record
          return newer;
        }
        // Construct a new BufferedRecord with updated value
        T resultRecord = recordContext.convertAvroRecord(updatedRecord);
        return BufferedRecords.fromEngineRecord(resultRecord, updatedValue.map(IndexedRecord::getSchema).orElseGet(() -> newerSchema),
            recordContext, orderingFields, newer.getRecordKey(), updatedValue.isEmpty());
      } else {
        // If the updated value is empty, it means the result is a DELETE operation
        // If one of the inputs is a DELETE operation, simply return that
        if (newer.isDelete()) {
          return newer;
        } else if (older.isDelete()) {
          return older;
        } else {
          // Edge case when neither input is a deletion but the output is a deletion
          return BufferedRecords.createDelete(newer.getRecordKey(), HoodieRecordMerger.maxOrderingValue(older, newer));
        }
      }
    }
  }

  protected void init(TypedProperties props) {
    if (payloadClass == null) {
      payloadClass = Option.ofNullable(props.getString(PAYLOAD_CLASS_PROP, null)).orElseGet(() -> ConfigUtils.getPayloadClass(props));
    }
    if (orderingFields == null) {
      orderingFields = ConfigUtils.getOrderingFields(props);
      // if there are no ordering fields, use empty array
      if (orderingFields == null) {
        orderingFields = new String[0];
      }
    }
  }

  @Override
  public HoodieRecordType getRecordType() {
    return HoodieRecordType.AVRO;
  }

  @Override
  public HoodieRecordMerger asPreCombiningMode() {
    return new HoodiePreCombineAvroRecordMerger();
  }
}
