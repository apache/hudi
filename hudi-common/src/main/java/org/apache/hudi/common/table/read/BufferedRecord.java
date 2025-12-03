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
import org.apache.hudi.common.model.HoodieOperation;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.OrderingValues;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * Buffered Record used by file group reader.
 *
 * @param <T> The type of the engine specific row.
 */
public class BufferedRecord<T> implements Serializable {
  private String recordKey;
  private T record;
  private final Comparable orderingValue;
  private final Integer schemaId;
  @Nullable private HoodieOperation hoodieOperation;

  public BufferedRecord() {
    this(null, null, null, null, null);
  }

  public BufferedRecord(String recordKey, Comparable orderingValue, T record, Integer schemaId, @Nullable HoodieOperation hoodieOperation) {
    this.recordKey = recordKey;
    this.orderingValue = orderingValue;
    this.record = record;
    this.schemaId = schemaId;
    this.hoodieOperation = hoodieOperation;
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
    return HoodieOperation.isDelete(hoodieOperation) || HoodieOperation.isUpdateBefore(hoodieOperation);
  }

  public boolean isEmpty() {
    return record == null;
  }

  public boolean isCommitTimeOrderingDelete() {
    return isDelete() && OrderingValues.isDefault(orderingValue);
  }

  public void setHoodieOperation(HoodieOperation hoodieOperation) {
    this.hoodieOperation = hoodieOperation;
  }

  public HoodieOperation getHoodieOperation() {
    return this.hoodieOperation;
  }

  public BufferedRecord<T> toBinary(RecordContext<T> recordContext) {
    if (record != null) {
      HoodieSchema schema = recordContext.getSchemaFromBufferRecord(this);
      // Schema can be null in test scenarios where schemas are not registered in the RecordContext (e.g. in tests)
      if (schema != null) {
        record = recordContext.seal(recordContext.toBinaryRow(schema, record));
      }
    }
    return this;
  }

  public BufferedRecord<T> seal(RecordContext<T> recordContext) {
    if (record != null) {
      this.record = recordContext.seal(record);
    }
    return this;
  }

  public BufferedRecord<T> project(UnaryOperator<T> converter) {
    if (record != null) {
      this.record = converter.apply(record);
    }
    return this;
  }

  public BufferedRecord<T> replaceRecord(T record) {
    this.record = record;
    return this;
  }

  public BufferedRecord<T> replaceRecordKey(String recordKey) {
    this.recordKey = recordKey;
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
    return Objects.equals(recordKey, that.recordKey) && Objects.equals(orderingValue, that.orderingValue)
        && Objects.equals(record, that.record) && Objects.equals(schemaId, that.schemaId) && hoodieOperation == that.hoodieOperation;
  }

  @Override
  public int hashCode() {
    return Objects.hash(recordKey, orderingValue, record, schemaId, hoodieOperation);
  }
}
