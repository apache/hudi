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
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import static org.apache.hudi.common.model.HoodieRecord.DEFAULT_ORDERING_VALUE;

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

  private BufferedRecord(String recordKey, Comparable orderingValue, T record, Integer schemaId, boolean isDelete) {
    this.recordKey = recordKey;
    this.orderingValue = orderingValue;
    this.record = record;
    this.schemaId = schemaId;
    this.isDelete = isDelete;
  }

  public static <T> BufferedRecord<T> forRecordWithContext(HoodieRecord<T> record, Schema schema, HoodieReaderContext<T> readerContext, Properties props) {
    HoodieKey hoodieKey = record.getKey();
    String recordKey = hoodieKey == null ? readerContext.getRecordKey(record.getData(), schema) : hoodieKey.getRecordKey();
    Integer schemaId = readerContext.encodeAvroSchema(schema);
    boolean isDelete;
    try {
      isDelete = record.isDelete(schema, props);
    } catch (IOException e) {
      throw new HoodieException("Failed to get isDelete from record.", e);
    }
    return new BufferedRecord<>(recordKey, record.getOrderingValue(schema, props), record.getData(), schemaId, isDelete);
  }

  public static <T> BufferedRecord<T> forRecordWithContext(T record, Schema schema, HoodieReaderContext<T> readerContext, Option<String> orderingFieldName, boolean isDelete) {
    String recordKey = readerContext.getRecordKey(record, schema);
    Integer schemaId = readerContext.encodeAvroSchema(schema);
    Comparable orderingValue = readerContext.getOrderingValue(record, schema, orderingFieldName);
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
    return isDelete && getOrderingValue().equals(DEFAULT_ORDERING_VALUE);
  }

  public BufferedRecord<T> toBinary(HoodieReaderContext<T> readerContext) {
    if (record != null) {
      record = readerContext.seal(readerContext.toBinaryRow(readerContext.getSchemaFromBufferRecord(this), record));
    }
    return this;
  }
}
