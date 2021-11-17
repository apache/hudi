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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Base class that provides support for seamlessly applying changes captured via Debezium.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshort inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public abstract class AbstractDebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  private static final Logger LOG = LogManager.getLogger(AbstractDebeziumAvroPayload.class);

  public AbstractDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AbstractDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    IndexedRecord insertRecord = getInsertRecord(schema);
    return handleDeleteOperation(insertRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    // Step 1: If the time occurrence of the current record in storage is higher than the time occurrence of the
    // insert record (including a delete record), pick the current record.
    if (shouldPickCurrentRecord(currentValue, getInsertRecord(schema), schema)) {
      return Option.of(currentValue);
    }
    // Step 2: Pick the insert record (as a delete record if its a deleted event)
    return getInsertValue(schema);
  }

  protected abstract boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException;

  @Nullable
  private static Object getFieldVal(GenericRecord record, String fieldName) {
    Schema.Field recordField = record.getSchema().getField(fieldName);
    if (recordField == null) {
      return null;
    }

    return record.get(recordField.pos());
  }

  private Option<IndexedRecord> handleDeleteOperation(IndexedRecord insertRecord) {
    boolean delete = false;
    if (insertRecord instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) insertRecord;
      Object value = getFieldVal(record, DebeziumConstants.FLATTENED_OP_COL_NAME);
      delete = value != null && value.toString().equalsIgnoreCase(DebeziumConstants.DELETE_OP);
    }

    return delete ? Option.empty() : Option.of(insertRecord);
  }

  private IndexedRecord getInsertRecord(Schema schema) throws IOException {
    return super.getInsertValue(schema).get();
  }
}
