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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.*;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link HoodieRecordPayload} impl that honors ordering field in both preCombine and combineAndGetUpdateValue.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field 2. combineAndGetUpdateValue/getInsertValue - Chooses the latest record based on ordering field value.
 */
public class DefaultHoodieRecordPayload extends OverwriteWithLatestAvroPayload {
  public static final String METADATA_EVENT_TIME_KEY = "metadata.event_time.key";
  public static final String DELETE_KEY = "hoodie.payload.delete.field";
  public static final String DELETE_MARKER = "hoodie.payload.delete.marker";
  private Option<Object> eventTime = Option.empty();
  private AtomicBoolean isDeleteComputed = new AtomicBoolean(false);
  private boolean isDefaultRecordPayloadDeleted = false;

  public DefaultHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public DefaultHoodieRecordPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
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
    eventTime = updateEventTime(incomingRecord, properties);

    if (!isDeleteComputed.getAndSet(true)) {
      isDefaultRecordPayloadDeleted = isDeleteRecord(incomingRecord, properties);
    }
    /*
     * Now check if the incoming record is a delete record.
     */
    return isDefaultRecordPayloadDeleted ? Option.empty() : Option.of(incomingRecord);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
    eventTime = updateEventTime(incomingRecord, properties);

    if (!isDeleteComputed.getAndSet(true)) {
      isDefaultRecordPayloadDeleted = isDeleteRecord(incomingRecord, properties);
    }
    return isDefaultRecordPayloadDeleted ? Option.empty() : Option.of(incomingRecord);
  }

  public boolean isDeleted(Schema schema, Properties props) {
    if (recordBytes.length == 0) {
      return true;
    }
    try {
      if (!isDeleteComputed.getAndSet(true)) {
        GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
        isDefaultRecordPayloadDeleted = isDeleteRecord(incomingRecord, props);
      }
      return isDefaultRecordPayloadDeleted;
    } catch (IOException e) {
      throw new HoodieIOException("Deserializing bytes to avro failed ", e);
    }
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @param properties payload related properties
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord, Properties properties) {
    final String deleteKey = properties.getProperty(DELETE_KEY);
    if (StringUtils.isNullOrEmpty(deleteKey)) {
      return isDeleteRecord(genericRecord);
    }

    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(properties.getProperty(DELETE_MARKER)),
        () -> DELETE_MARKER + " should be configured with " + DELETE_KEY);
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(deleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(deleteKey);
    return deleteMarker != null && properties.getProperty(DELETE_MARKER).equals(deleteMarker.toString());
  }

  private static Option<Object> updateEventTime(GenericRecord record, Properties properties) {
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    String eventTimeField = properties
        .getProperty(HoodiePayloadProps.PAYLOAD_EVENT_TIME_FIELD_PROP_KEY);
    if (eventTimeField == null) {
      return Option.empty();
    }
    return Option.ofNullable(
        HoodieAvroUtils.getNestedFieldVal(
            record,
            eventTimeField,
            true,
            consistentLogicalTimestampEnabled)
    );
  }

  @Override
  public Option<Map<String, String>> getMetadata() {
    Map<String, String> metadata = new HashMap<>();
    if (eventTime.isPresent()) {
      metadata.put(METADATA_EVENT_TIME_KEY, String.valueOf(eventTime.get()));
    }
    return metadata.isEmpty() ? Option.empty() : Option.of(metadata);
  }

  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                Object incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    String orderField = ConfigUtils.getOrderingField(properties);
    if (orderField == null) {
      return true;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    Object persistedOrderingVal = HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue,
        orderField,
        true, consistentLogicalTimestampEnabled);
    Comparable incomingOrderingVal = null;
    if (incomingRecord instanceof IndexedRecord) {
      incomingOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) incomingRecord,
              orderField,
              true, consistentLogicalTimestampEnabled);
    } else if (incomingRecord instanceof Option) {
      incomingOrderingVal = ((Option<IndexedRecord>) incomingRecord).map(record-> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record,
              orderField,
              true, consistentLogicalTimestampEnabled)).orElse(orderingVal);
    }
    Pair<Comparable, Comparable> comparablePair = OrderingValueUtils.canonicalizeOrderingValue((Comparable)persistedOrderingVal, incomingOrderingVal);
    persistedOrderingVal = comparablePair.getLeft();
    incomingOrderingVal = comparablePair.getRight();
    return persistedOrderingVal == null || ((Comparable)persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}
