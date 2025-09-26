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

package org.apache.hudi.common.testutils.reader;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hudi.common.util.ConfigUtils.getOrderingFields;

public class HoodieRecordTestPayload extends OverwriteWithLatestAvroPayload {
  public static final String METADATA_EVENT_TIME_KEY = "metadata.event_time.key";
  public static final String DELETE_KEY = "hoodie.payload.delete.field";
  public static final String DELETE_MARKER = "hoodie.payload.delete.marker";
  private Option<Object> eventTime = Option.empty();
  private AtomicBoolean isDeleteComputed = new AtomicBoolean(false);
  private boolean isDefaultRecordPayloadDeleted = false;

  public HoodieRecordTestPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public HoodieRecordTestPayload preCombine(HoodieRecordTestPayload oldValue) {
    if (isEmptyRecord()) {
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
    // The new record is a delete record.
    if (isDeleted(schema, properties)) {
      String[] orderingFields = getOrderingFields(properties);
      // If orderingField cannot be found, we can not do the compare, then use the natural order.
      if (orderingFields == null) {
        return Option.empty();
      }

      // Otherwise, we compare their ordering values.
      Comparable<?> currentOrderingVal = OrderingValues.create(
          orderingFields,
          field -> (Comparable<?>) currentValue.get(currentValue.getSchema().getField(field).pos()));
      if (orderingVal.compareTo(currentOrderingVal) >= 0) {
        return Option.empty();
      }
      return Option.of(currentValue);
    }

    // If the new record is not a delete record.
    GenericRecord incomingRecord = (GenericRecord) getRecord(schema).get();

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
    if (isEmptyRecord()) {
      return Option.empty();
    }
    GenericRecord incomingRecord = (GenericRecord) getRecord(schema).get();
    eventTime = updateEventTime(incomingRecord, properties);

    if (!isDeleteComputed.getAndSet(true)) {
      isDefaultRecordPayloadDeleted = isDeleteRecord(incomingRecord, properties);
    }
    return isDefaultRecordPayloadDeleted ? Option.empty() : Option.of(incomingRecord);
  }

  public boolean isDeleted(Schema schema, Properties props) {
    if (isEmptyRecord()) {
      return true;
    }
    try {
      if (!isDeleteComputed.getAndSet(true)) {
        GenericRecord incomingRecord = (GenericRecord) getRecord(schema).get();
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
                                                IndexedRecord incomingRecord,
                                                Properties properties) {
    /*
     * Combining strategy here returns currentValue on disk if incoming record is older.
     * The incoming record can be either a delete (sent as an upsert with _hoodie_is_deleted set to true)
     * or an insert/update record. In any case, if it is older than the record in disk, the currentValue
     * in disk is returned (to be rewritten with new commit time).
     *
     * NOTE: Deletes sent via EmptyHoodieRecordPayload and/or Delete operation type do not hit this code path
     * and need to be dealt with separately.
     */
    String[] orderingFields = getOrderingFields(properties);
    if (orderingFields == null) {
      return true;
    }
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    Comparable<?> persistedOrderingVal = OrderingValues.create(
        orderingFields,
        field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue, field, true, consistentLogicalTimestampEnabled));
    Comparable<?> incomingOrderingVal = OrderingValues.create(
        orderingFields,
        field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) incomingRecord, field, true, consistentLogicalTimestampEnabled));
    return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}
