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

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Base class that provides support for seamlessly applying changes captured via Debezium.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshot inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public abstract class AbstractDebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractDebeziumAvroPayload.class);

  public AbstractDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AbstractDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.getRecordBytes().length == 0) {
      // use natural order for delete record
      return this;
    }
    if (((Comparable) oldValue.getOrderingValue()).compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    Option<IndexedRecord> insertValue = getInsertRecord(schema);
    return insertValue.isPresent() ? handleDeleteOperation(insertValue.get()) : Option.empty();
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    // Step 1: If the time occurrence of the current record in storage is higher than the time occurrence of the
    // insert record (including a delete record), pick the current record.
    Option<IndexedRecord> insertValue = getRecord(schema);
    if (!insertValue.isPresent()) {
      return Option.empty();
    }
    if (shouldPickCurrentRecord(currentValue, insertValue.get(), schema)) {
      return Option.of(currentValue);
    }
    // Step 2: Pick the insert record (as a delete record if it is a deleted event)
    return getInsertValue(schema);
  }

  protected abstract boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException;

  private Option<IndexedRecord> handleDeleteOperation(IndexedRecord insertRecord) {
    boolean delete = false;
    if (insertRecord instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) insertRecord;
      delete = isDebeziumDeleteRecord(record);
    }

    return delete ? Option.empty() : Option.of(insertRecord);
  }

  private Option<IndexedRecord> getInsertRecord(Schema schema) throws IOException {
    return super.getInsertValue(schema);
  }

  @Override
  protected boolean isDeleteRecord(GenericRecord record) {
    return isDebeziumDeleteRecord(record) || super.isDeleteRecord(record);
  }

  private static boolean isDebeziumDeleteRecord(GenericRecord record) {
    Object value = HoodieAvroUtils.getFieldVal(record, DebeziumConstants.FLATTENED_OP_COL_NAME);
    return value != null && value.toString().equalsIgnoreCase(DebeziumConstants.DELETE_OP);
  }
}
