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

import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.engine.RecordContext;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.UnaryOperator;

/**
 * Buffered Record used by file group reader.
 *
 * @param <T> The type of the engine specific row.
 */
public class BufferedRecord<T> implements Serializable {
  private final String recordKey;
  private final Comparable orderingValue;
  private T record;
  private final Integer schemaId;
  private final boolean isDelete;
  private HoodieOperation hoodieOperation;

  public BufferedRecord(String recordKey, Comparable orderingValue, T record, Integer schemaId, boolean isDelete) {
    this(recordKey, orderingValue, record, schemaId, null, isDelete);
  }

  public BufferedRecord(String recordKey, Comparable orderingValue, T record,
                        Integer schemaId, HoodieOperation hoodieOperation, boolean isDelete) {
    this.recordKey = recordKey;
    this.orderingValue = orderingValue;
    this.record = record;
    this.schemaId = schemaId;
    this.isDelete = isDelete;
    this.hoodieOperation = hoodieOperation;
  }

  public static <T> BufferedRecord<T> forRecordWithContext(
      HoodieRecord record, Schema schema, RecordContext<T> recordContext, DeleteContext deleteContext, Properties props, String[] orderingFields) {
    HoodieKey hoodieKey = record.getKey();
    T data = recordContext.extractDataFromRecord(record, schema, props);
    ValidationUtils.checkArgument(
        hoodieKey != null || data != null, "Either hoodieKey or record data should be present!");
    String recordKey = hoodieKey == null ? recordContext.getRecordKey(data, schema) : hoodieKey.getRecordKey();
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    boolean isDelete;
    try {
      isDelete = data == null ? record.isDelete(schema, props) : recordContext.isDeleteRecord(data, deleteContext);
    } catch (IOException e) {
      throw new HoodieException("Failed to get isDelete from record.", e);
    }
    HoodieOperation hoodieOperation = record.getOperation();
    if (record.getIgnoreIndexUpdate()) {
      hoodieOperation = HoodieOperation.UPDATE_BEFORE;
    }
    return new BufferedRecord<>(recordKey, record.getOrderingValue(schema, props, orderingFields), data, schemaId, hoodieOperation, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields) {
    try {
      boolean isDelete = record.isDelete(schema, props);
      return forRecordWithContext(record, schema, recordContext, props, orderingFields, isDelete);
    } catch (IOException e) {
      throw new HoodieException("Failed to get isDelete from record.", e);
    }
  }

  public static <T> BufferedRecord<T> forRecordWithContext(HoodieRecord record, Schema schema, RecordContext<T> recordContext, Properties props, String[] orderingFields, boolean isDelete) {
    HoodieKey hoodieKey = record.getKey();
    T data = recordContext.extractDataFromRecord(record, schema, props);
    String recordKey = hoodieKey == null ? recordContext.getRecordKey(data, schema) : hoodieKey.getRecordKey();
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    return new BufferedRecord<>(recordKey, record.getOrderingValue(schema, props, orderingFields), data, schemaId, isDelete);
  }

  /**
   * Efficient builder for BufferedRecord with only record key field initialized while null for other fields, this is only used
   * for reading path to get record key iterator.
   */
  public static <T> BufferedRecord<T> forRecordKey(T record, Schema schema, RecordContext<T> recordContext) {
    String recordKey = recordContext.getRecordKey(record, schema);
    return new BufferedRecord<>(recordKey, null, null, null, false);
  }

  /**
   * Efficient builder for BufferedRecord with only data field initialized while null for other fields, this is only used
   * for reading path to get engine-specific record iterator.
   */
  public static <T> BufferedRecord<T> forRecord(T record) {
    return new BufferedRecord<>(null, null, record, null, false);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(T record, Schema schema, RecordContext<T> recordContext, List<String> orderingFieldNames, boolean isDelete) {
    String recordKey = recordContext.getRecordKey(record, schema);
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(T record, Schema schema, RecordContext<T> recordContext, String[] orderingFieldNames, String recordKey, boolean isDelete) {
    Integer schemaId = recordContext.encodeAvroSchema(schema);
    Comparable orderingValue = recordContext.getOrderingValue(record, schema, orderingFieldNames);
    return new BufferedRecord<>(recordKey, orderingValue, record, schemaId, isDelete);
  }

  public static <T> BufferedRecord<T> forDeleteRecord(DeleteRecord deleteRecord, Comparable orderingValue) {
    return new BufferedRecord<>(deleteRecord.getRecordKey(), orderingValue, null, null, true);
  }

  public String getRecordKey() {
    return recordKey;
  }

  public Comparable getOrderingValue() {
    return orderingValue;
  }

  public T getRecord() {
    return record;
  }

  public Integer getSchemaId() {
    return schemaId;
  }

  public boolean isDelete() {
    return isDelete;
  }

  public boolean isCommitTimeOrderingDelete() {
    return isDelete && OrderingValues.isDefault(orderingValue);
  }

  public void setHoodieOperation(HoodieOperation hoodieOperation) {
    this.hoodieOperation = hoodieOperation;
  }

  public HoodieOperation getHoodieOperation() {
    return this.hoodieOperation;
  }

  public BufferedRecord<T> toBinary(HoodieReaderContext<T> readerContext) {
    if (record != null) {
      record = readerContext.seal(readerContext.toBinaryRow(readerContext.getRecordContext().getSchemaFromBufferRecord(this), record));
    }
    return this;
  }

  public BufferedRecord<T> seal(HoodieReaderContext<T> readerContext) {
    if (record != null) {
      this.record = readerContext.seal(record);
    }
    return this;
  }

  public BufferedRecord<T> project(UnaryOperator<T> converter) {
    if (record != null) {
      this.record = converter.apply(record);
    }
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BufferedRecord<?> that = (BufferedRecord<?>) o;
    return isDelete == that.isDelete && Objects.equals(recordKey, that.recordKey) && Objects.equals(orderingValue, that.orderingValue)
        && Objects.equals(record, that.record) && Objects.equals(schemaId, that.schemaId) && hoodieOperation == that.hoodieOperation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordKey, orderingValue, record, schemaId, isDelete, hoodieOperation);
  }
}
