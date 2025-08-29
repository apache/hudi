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
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Default payload.
 * {@link HoodieRecordPayload} impl that honors ordering field in both preCombine and combineAndGetUpdateValue.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field
 * 2. combineAndGetUpdateValue/getInsertValue - Chooses the latest record based on ordering field value.
 */
public class DefaultHoodieRecordPayload extends OverwriteWithLatestAvroPayload {
  public static final String METADATA_EVENT_TIME_KEY = "metadata.event_time.key";
  public static final String DELETE_KEY = "hoodie.payload.delete.field";
  public static final String DELETE_MARKER = "hoodie.payload.delete.marker";
  private final AtomicBoolean isDeleteComputed = new AtomicBoolean(false);
  private boolean isDefaultRecordPayloadDeleted = false;

  public DefaultHoodieRecordPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public DefaultHoodieRecordPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, OrderingValues.getDefault()); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    Option<IndexedRecord> incomingRecord = recordBytes.length == 0 ? Option.empty() : Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }

    if (!isDeleteComputed.getAndSet(true)) {
      isDefaultRecordPayloadDeleted = incomingRecord.map(record -> isDeleteRecord((GenericRecord) record, properties)).orElse(true);
    }
    /*
     * Now check if the incoming record is a delete record.
     */
    return isDefaultRecordPayloadDeleted ? Option.empty() : incomingRecord;
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);

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

  @Override
  public Option<Map<String, String>> getMetadata() {
    return Option.empty();
  }

  protected boolean needUpdatingPersistedRecord(IndexedRecord currentValue,
                                                Option<IndexedRecord> incomingRecord, Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    String[] orderingFields = ConfigUtils.getOrderingFields(properties);
    if (orderingFields == null) {
      return true;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    Comparable persistedOrderingVal = OrderingValues.create(
        orderingFields,
        field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue, field, true, consistentLogicalTimestampEnabled));
    Comparable incomingOrderingVal = incomingRecord.map(record -> OrderingValues.create(
            orderingFields,
            field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record, field, true, consistentLogicalTimestampEnabled)))
        .orElse(orderingVal);
    // If the incoming record is a delete record without an ordering value, it is processed as "commit time" ordering.
    if (incomingRecord.isEmpty() && OrderingValues.isDefault(incomingOrderingVal)) {
      return true;
    }
    return persistedOrderingVal == null || persistedOrderingVal.compareTo(incomingOrderingVal) <= 0;
  }
}
