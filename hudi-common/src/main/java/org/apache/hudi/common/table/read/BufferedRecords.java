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

package org.apache.hudi.common.table.read;

import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;

import java.util.List;
import java.util.Properties;

/**
 * Factory to create {@link BufferedRecord}.
 */
public class BufferedRecords {
  // Special handling for SENTINEL record in Expression Payload
  public static final BufferedRecord SENTINEL = new BufferedRecord(null, null, null, null, false);

  public static <T> BufferedRecord<T> createDelete(String recordKey) {
    return new BufferedRecord<>(recordKey, null, null, null, HoodieOperation.DELETE);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields) {
    boolean isDelete = record.isDelete(schema, props);
    return forRecordWithContext(record, schema, recordContext, props, orderingFields, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields, boolean isDelete) {
    HoodieKey hoodieKey = record.getKey();
    T data = recordContext.extractDataFromRecord(record, schema, props);
    String recordKey = hoodieKey == null ? recordContext.getRecordKey(data, schema) : hoodieKey.getRecordKey();
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    return new BufferedRecord<>(recordKey, record.getOrderingValue(schema, props, orderingFields), data, schemaId, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(T record, Schema schema, RecordContext<T> recordContext, List<String> orderingFieldNames, boolean isDelete) {
    return forRecordWithContext(record, schema, recordContext, orderingFieldNames, null, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(
      T record, Schema schema, RecordContext<T> recordContext, List<String> orderingFieldNames, HoodieOperation hoodieOperation, boolean isDelete) {
    String recordKey = recordContext.getRecordKey(record, schema);
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    hoodieOperation = HoodieOperation.isUpdateBefore(hoodieOperation) || !isDelete ? hoodieOperation : HoodieOperation.DELETE;
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, hoodieOperation);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(T record, Schema schema, RecordContext<T> recordContext, String[] orderingFieldNames, String recordKey, boolean isDelete) {
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete);
  }

  public static <T> BufferedRecord<T> forDeleteRecord(DeleteRecord deleteRecord, Comparable orderingValue) {
    return new BufferedRecord<>(deleteRecord.getRecordKey(), orderingValue, null, null, HoodieOperation.DELETE);
  }

  public static <T> BufferedRecord<T> forDeleteRecord(DeleteRecord deleteRecord, Comparable orderingValue, HoodieOperation hoodieOperation) {
    hoodieOperation = HoodieOperation.isUpdateBefore(hoodieOperation) ? HoodieOperation.UPDATE_BEFORE : HoodieOperation.DELETE;
    return new BufferedRecord<>(deleteRecord.getRecordKey(), orderingValue, null, null, hoodieOperation);
  }
}
