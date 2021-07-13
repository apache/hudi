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
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.hudi.avro.HoodieAvroUtils.getNestedFieldVal;

/**
 * Default payload used for delta streamer.
 *
 * <ol>
 * <li> preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li> combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 * </ol>
 */
public class OverwriteWithLatestAvroPayload extends BaseAvroPayload
    implements HoodieRecordPayload<OverwriteWithLatestAvroPayload> {

  // Constructor for write path with explicit ordering value.
  public OverwriteWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  // Constructor for read path with explicit ordering field.
  // Use natural order when orderingVal is null
  public OverwriteWithLatestAvroPayload(GenericRecord record, String orderingField) {
    super(record, getOrderingValWithDefault(record, orderingField));
  }

  public OverwriteWithLatestAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    // pick the payload with greatest ordering value
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    IndexedRecord indexedRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);
    if (isDeleteRecord((GenericRecord) indexedRecord)) {
      return Option.empty();
    } else {
      return Option.of(indexedRecord);
    }
  }

  /**
   * @param genericRecord instance of {@link GenericRecord} of interest.
   * @returns {@code true} if record represents a delete record. {@code false} otherwise.
   */
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    final String isDeleteKey = "_hoodie_is_deleted";
    // Modify to be compatible with new version Avro.
    // The new version Avro throws for GenericRecord.get if the field name
    // does not exist in the schema.
    if (genericRecord.getSchema().getField(isDeleteKey) == null) {
      return false;
    }
    Object deleteMarker = genericRecord.get(isDeleteKey);
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  /**
   * Returns the ordering value of given record {@code record} with default value.
   * The default value represents natural order.
   *
   * @param record The given record
   * @param orderingField The ordering field name
   *
   * @return the ordering value
   */
  protected static Comparable getOrderingValWithDefault(GenericRecord record, @Nullable String orderingField) {
    Comparable orderingVal = getOrderingVal(record, orderingField);
    return orderingVal == null ? 0 : orderingVal;
  }

  /**
   * Returns the ordering value of given record {@code record}.
   *
   * @param record The given record
   * @param orderingField The ordering field name
   *
   * @return the ordering value, or null if the {@code orderingField} is null
   */
  @Nullable
  protected static Comparable getOrderingVal(GenericRecord record, @Nullable String orderingField) {
    return orderingField == null ? null : (Comparable) getNestedFieldVal(record, orderingField, true);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return defaultValue == null ? value == null : defaultValue.toString().equals(String.valueOf(value));
  }
}
