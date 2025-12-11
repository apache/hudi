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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.table.read.BufferedRecords;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Record merger for Hoodie avro record.
 *
 * <p>It should only be used for deduplication among incoming records.
 */
public class HoodiePreCombineAvroRecordMerger extends HoodieAvroRecordMerger {

  @Override
  public <T> BufferedRecord<T> merge(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, TypedProperties props) throws IOException {
    if (HoodieRecordMerger.isCommitTimeOrderingDelete(older, newer)) {
      return newer;
    }
    init(props);
    return preCombine(older, newer, recordContext, recordContext.getSchemaFromBufferRecord(newer), props);
  }

  @SuppressWarnings("rawtypes, unchecked")
  private <T> BufferedRecord<T> preCombine(BufferedRecord<T> older, BufferedRecord<T> newer, RecordContext<T> recordContext, HoodieSchema newSchema, TypedProperties props) {
    GenericRecord newerAvroRecord = recordContext.convertToAvroRecord(newer.getRecord(), recordContext.getSchemaFromBufferRecord(newer));
    GenericRecord olderAvroRecord = recordContext.convertToAvroRecord(older.getRecord(), recordContext.getSchemaFromBufferRecord(older));
    HoodieRecordPayload newerPayload = HoodieRecordUtils.loadPayload(payloadClass, newerAvroRecord, newer.getOrderingValue());
    HoodieRecordPayload olderPayload = HoodieRecordUtils.loadPayload(payloadClass, olderAvroRecord, older.getOrderingValue());
    HoodieRecordPayload payload = newerPayload.preCombine(olderPayload, newSchema.toAvroSchema(), props);
    try {
      if (payload == olderPayload) {
        return older;
      } else if (payload == newerPayload) {
        return newer;
      } else {
        Option<IndexedRecord> indexedRecord = payload.getIndexedRecord(newSchema.toAvroSchema(), props);
        T mergedRecord = indexedRecord.map(recordContext::convertAvroRecord).orElse(null);
        return BufferedRecords.fromEngineRecord(mergedRecord, newSchema, recordContext, orderingFields, newer.getRecordKey(), indexedRecord.isEmpty());
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to combine records", e);
    }
  }
}
