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

package org.apache.hudi.common.model;

import static org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro;
import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal;

import java.util.Collections;
import java.util.Map;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;

/**
 * Default payload used for delta streamer.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field 2.
 * combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 */
public class OverwriteWithLatestAvroPayloadV1 extends BaseAvroPayload
    implements HoodieRecordPayload<OverwriteWithLatestAvroPayloadV1> {

  private Map<String, String> props;

  public OverwriteWithLatestAvroPayloadV1(GenericRecord record, Comparable orderingVal, Map<String, String> props) {
    super(record, orderingVal);
    this.props = props;
  }

  public OverwriteWithLatestAvroPayloadV1(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, (record1) -> 0, Collections.EMPTY_MAP); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayloadV1 preCombine(OverwriteWithLatestAvroPayloadV1 another) {
    // pick the payload with greatest ordering value
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      return another;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = bytesToAvro(recordBytes, schema);
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      return Option.of(indexedRecord);
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    GenericRecord incomingRecord = bytesToAvro(recordBytes, schema);
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    Object persistedOrderingVal = getNestedFieldVal((GenericRecord) currentValue, props.get(ORDERING_FIELD_OPT_KEY), true);
    Comparable incomingOrderingVal = (Comparable) getNestedFieldVal(incomingRecord, props.get(ORDERING_FIELD_OPT_KEY), false);

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (persistedOrderingVal != null && ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) > 0) {
      return Option.of(currentValue);
    }

    /*
     * We reached a point where the value is disk is older than the incoming record.
     * Now check if the incoming record is a delete record.
     */
    if (isDeleteRecord(incomingRecord)) {
      return Option.empty();
    } else {
      return Option.of(incomingRecord);
    }
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  private boolean isDeleteRecord(GenericRecord genericRecord) {
    Object deleteMarker = genericRecord.get("_hoodie_is_deleted");
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }
}