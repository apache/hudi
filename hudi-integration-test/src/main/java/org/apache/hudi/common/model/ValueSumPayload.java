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

package org.apache.hudi.common.model;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class ValueSumPayload extends DefaultHoodieRecordPayload {
  private static final String INDIVIDUAL_VALUE_FIELD = "long_value";
  private static final String TOTAL_VALUE_FIELD = "sum_long_value";
  private AtomicBoolean isDeleteComputed = new AtomicBoolean(false);
  private boolean isDefaultRecordPayloadDeleted = false;

  public ValueSumPayload(Option<GenericRecord> record) {
    super(record.isPresent() ? Option.of(fillTotalValueIfNotPresent(record.get())) : Option.empty());
  }

  public ValueSumPayload(GenericRecord record, Comparable orderingVal) {
    super(fillTotalValueIfNotPresent(record), orderingVal);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue,
                                                   Schema schema,
                                                   Properties properties) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    try {
      GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
      if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
        // pick the payload with greatest ordering value
        return oldValue;
      } else {
        return this;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }

    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }

    /*
     * We reached a point where the value is disk is older than the incoming record.
     */
    //eventTime = updateEventTime(incomingRecord, properties);

    if (!isDeleteComputed.getAndSet(true)) {
      isDefaultRecordPayloadDeleted = isDeleteRecord(incomingRecord, properties);
    }
    /*
     * Now check if the incoming record is a delete record.
     */
    return isDefaultRecordPayloadDeleted ? Option.empty() : Option.of(incomingRecord);
  }

  private static GenericRecord fillTotalValueIfNotPresent(GenericRecord record) {
    if (extractLongValue(record, TOTAL_VALUE_FIELD) == null) {
      putTotalValue(record, extractLongValue(record, INDIVIDUAL_VALUE_FIELD));
    }
    return record;
  }

  private static Long extractLongValue(GenericRecord record, String fieldName) {
    Object value = record.get(fieldName);
    return value != null ? (Long) value : null;
  }

  private static void putTotalValue(GenericRecord record, Long totalValue) {
    record.put(TOTAL_VALUE_FIELD, totalValue);
  }
}
