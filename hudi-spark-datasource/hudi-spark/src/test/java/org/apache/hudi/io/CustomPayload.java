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

package org.apache.hudi.io;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

public class CustomPayload implements HoodieRecordPayload<CustomPayload> {
  private final GenericRecord record;
  private final Comparable orderingValue;

  public CustomPayload(GenericRecord record, Comparable orderingValue) {
    this.record = record;
    this.orderingValue = orderingValue;
  }

  @Override
  public CustomPayload preCombine(CustomPayload other) {
    return this; // No-op for this test
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) {
    Long olderTimestamp = (Long) ((GenericRecord) currentValue).get("timestamp");
    Long newerTimestamp = orderingValue == null ? (Long) record.get("timestamp") : (Long) orderingValue;
    if (olderTimestamp.equals(newerTimestamp)) {
      // If the timestamps are the same, we do not update
      return handleDeleteRecord(currentValue);
    } else if (olderTimestamp < newerTimestamp) {
      // Custom payload chooses record with lower ordering value
      return handleDeleteRecord(currentValue);
    } else {
      // Custom payload chooses record with lower ordering value
      return handleDeleteRecord(record);
    }
  }

  private Option<IndexedRecord> handleDeleteRecord(IndexedRecord data) {
    // check for _hoodie_is_deleted field
    boolean isDeleted = data == null || (boolean) ((GenericRecord) data).get(HoodieRecord.HOODIE_IS_DELETED_FIELD);
    if (isDeleted) {
      return Option.empty(); // If the record is marked as deleted, return empty
    }
    return Option.of(data);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return handleDeleteRecord(record);
  }

  @Override
  public Comparable getOrderingValue() {
    return orderingValue;
  }
}
