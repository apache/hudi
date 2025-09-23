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

  public static <T> BufferedRecord<T> fromHoodieRecord(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields, DeleteContext deleteContext) {
    boolean isDelete = record.isDelete(deleteContext, props);
    return fromHoodieRecord(record, schema, recordContext, props, orderingFields, isDelete);
  }

  public static <T> BufferedRecord<T> fromHoodieRecord(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields, boolean isDelete) {
    HoodieKey hoodieKey = record.getKey();
    T data = recordContext.extractDataFromRecord(record, schema, props);
    String recordKey = hoodieKey == null ? recordContext.getRecordKey(data, schema) : hoodieKey.getRecordKey();
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = record.getOrderingValue(schema, props, orderingFields);
    return new BufferedRecord<>(recordKey, recordContext.convertOrderingValueToEngineType(orderingValue), data, schemaId, inferOperation(isDelete, record.getOperation()));
  }

  public static <T> BufferedRecord<T> fromHoodieRecordWithDeflatedRecord(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props,
                                                                         String[] orderingFields, DeleteContext deleteContext) {
    HoodieOperation hoodieOperation = record.getIgnoreIndexUpdate() ? HoodieOperation.UPDATE_BEFORE : record.getOperation();
    boolean isDelete = record.isDelete(deleteContext, props);
    return fromHoodieRecordWithDeflatedRecord(record, schema, recordContext, props, orderingFields, isDelete, hoodieOperation);
  }

  public static <T> BufferedRecord<T> fromHoodieRecordWithDeflatedRecord(HoodieRecord record, Schema schema, RecordContext<T> recordContext,
                                                                         Properties props, String[] orderingFields, boolean isDelete,
                                                                         HoodieOperation hoodieOperation) {
    HoodieKey hoodieKey = record.getKey();
    T data = recordContext.extractDeflatedDataFromRecord(record, schema, props);
    String recordKey = hoodieKey == null ? recordContext.getRecordKey(data, schema) : hoodieKey.getRecordKey();
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = record.getOrderingValue(schema, props, orderingFields);
    return new BufferedRecord<>(recordKey, recordContext.convertOrderingValueToEngineType(orderingValue), data, schemaId, inferOperation(isDelete, hoodieOperation));
  }

  public static <T> BufferedRecord<T> fromEngineRecord(T record, Schema schema, RecordContext<T> recordContext, List<String> orderingFieldNames, boolean isDelete) {
    String recordKey = recordContext.getRecordKey(record, schema);
    return fromEngineRecord(record, recordKey, schema, recordContext, orderingFieldNames, isDelete ? HoodieOperation.DELETE : null);
  }

  public static <T> BufferedRecord<T> fromEngineRecord(
      T record, String recordKey, Schema schema, RecordContext<T> recordContext, List<String> orderingFieldNames, HoodieOperation hoodieOperation) {
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, hoodieOperation);
  }

  public static <T> BufferedRecord<T> fromEngineRecord(T record, Schema schema, RecordContext<T> recordContext, String[] orderingFieldNames, String recordKey, boolean isDelete) {
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete ? HoodieOperation.DELETE : null);
  }

  public static <T> BufferedRecord<T> fromEngineRecord(T record, Schema schema, RecordContext<T> recordContext, Comparable orderingValue, String recordKey, boolean isDelete) {
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete ? HoodieOperation.DELETE : null);
  }

  public static <T> BufferedRecord<T> fromDeleteRecord(DeleteRecord deleteRecord, RecordContext<T> recordContext) {
    return new BufferedRecord<>(deleteRecord.getRecordKey(), recordContext.getOrderingValue(deleteRecord), null, null, HoodieOperation.DELETE);
  }

  public static <T> BufferedRecord<T> fromDeleteRecord(DeleteRecord deleteRecord, RecordContext<T> recordContext, HoodieOperation hoodieOperation) {
    hoodieOperation = HoodieOperation.isUpdateBefore(hoodieOperation) ? HoodieOperation.UPDATE_BEFORE : HoodieOperation.DELETE;
    return new BufferedRecord<>(deleteRecord.getRecordKey(), recordContext.getOrderingValue(deleteRecord), null, null, hoodieOperation);
  }

  public static <T> BufferedRecord<T> createDelete(String recordKey, Comparable orderingValue) {
    return new BufferedRecord<>(recordKey, orderingValue, null, null, HoodieOperation.DELETE);
  }

  /**
   * When creating buffered record from hoodie record, hoodie operation and isDelete are all there,
   * use this method as much as possible to keep the hoodie operation. For e.g, a -U record with isDelete as false.
   *
   * <p>This is useful for deduplication scenarios in write path, the -U is kept as best effort.
   * For FG reader output view for write path, the -U record can be ignored by both RLI metadata update and data write,
   * while regular D record is need for RLI metadata update but ignored by data write, setting up the -U correctly is critical to
   * distinguish these two different cases.
   */
  public static HoodieOperation inferOperation(boolean isDelete, HoodieOperation operation) {
    return isDelete ? HoodieOperation.isUpdateBefore(operation) ? operation : HoodieOperation.DELETE : operation;
  }
}
