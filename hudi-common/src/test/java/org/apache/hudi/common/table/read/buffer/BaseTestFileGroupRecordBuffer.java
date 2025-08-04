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

package org.apache.hudi.common.table.read.buffer;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BaseTestFileGroupRecordBuffer {

  protected static final Schema SCHEMA = Schema.createRecord("test_record", null, "namespace", false,
      Arrays.asList(
          new Schema.Field("record_key", Schema.create(Schema.Type.STRING)),
          new Schema.Field("counter", Schema.create(Schema.Type.INT)),
          new Schema.Field("ts", Schema.create(Schema.Type.LONG))));

  protected static GenericRecord createTestRecord(String recordKey, int counter, long ts) {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("record_key", recordKey);
    record.put("counter", counter);
    record.put("ts", ts);
    return record;
  }

  protected static Map<Serializable, BufferedRecord> convertToBufferedRecordsMap(List<IndexedRecord> indexedRecords,
                                                                               HoodieReaderContext<IndexedRecord> readerContext,
                                                                               TypedProperties props, String[] orderingFieldNames) {
    return indexedRecords.stream().map(rec -> {
      HoodieAvroIndexedRecord indexedRecord = new HoodieAvroIndexedRecord(new HoodieKey(rec.get(0).toString(), ""), rec, null);
      return Pair.of(rec.get(0).toString(), (BufferedRecord) BufferedRecord.forRecordWithContext(indexedRecord, readerContext.getSchemaHandler().getRequestedSchema(),
          readerContext.getRecordContext(), props, orderingFieldNames));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  protected static Map<Serializable, BufferedRecord> convertToBufferedRecordsMapForDeletes(List<IndexedRecord> indexedRecords, boolean defaultOrderingValue) {
    return indexedRecords.stream().map(rec -> {
      return Pair.of(rec.get(0).toString(),
          (BufferedRecord) BufferedRecord.forDeleteRecord(DeleteRecord.create(new HoodieKey(rec.get(0).toString(), ""), defaultOrderingValue ? 0 : (Comparable) rec.get(2)),
              defaultOrderingValue ? 0 : (Comparable) rec.get(2)));
    }).collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }
}
