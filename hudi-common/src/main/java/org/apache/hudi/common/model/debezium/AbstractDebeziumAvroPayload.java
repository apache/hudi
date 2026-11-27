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

import org.apache.hudi.common.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieDebeziumAvroPayloadException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;

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
@Slf4j
public abstract class AbstractDebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  public AbstractDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AbstractDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Properties properties) {
    // Same dispatch as combineAndGetUpdateValue: intra-batch dedup must order by the same column(s) as
    // the against-storage merge. Without this, a configured ordering field would still be fed into the
    // connector-specific preCombine (MySQL's "file.pos" seq parser, which throws on plain values).
    if (!getConfiguredOrderingFields(properties).isPresent()) {
      return preCombine(oldValue);
    }
    // Deliberately bypasses any connector-specific preCombine(oldValue) override: with ordering fields
    // configured, dedup must use the plain orderingVal comparison. The helper is private so the bypass
    // cannot be undone by a subclass override.
    return pickByOrderingValue(oldValue);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    return pickByOrderingValue(oldValue);
  }

  private OverwriteWithLatestAvroPayload pickByOrderingValue(OverwriteWithLatestAvroPayload oldValue) {
    if (oldValue.getRecordBytes().length == 0) {
      // use natural order for delete record
      return this;
    }
    if (((Comparable) oldValue.getOrderingValue()).compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    }
    return this;
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    Option<IndexedRecord> insertValue = getInsertRecord(schema);
    return insertValue.isPresent() ? handleDeleteOperation(insertValue.get()) : Option.empty();
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return combineAndGetUpdateValue(currentValue, schema, CollectionUtils.emptyProps());
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    // Step 1: If the time occurrence of the current record in storage is higher than the time occurrence of the
    // insert record (including a delete record), pick the current record.
    Option<IndexedRecord> insertValue = getRecord(schema);
    if (!insertValue.isPresent()) {
      return Option.empty();
    }
    Option<String[]> orderingFields = getConfiguredOrderingFields(properties);
    boolean pickCurrentRecord = orderingFields.isPresent()
        ? !needUpdatingPersistedRecord(currentValue, insertValue.get(), orderingFields.get(), properties)
        : shouldPickCurrentRecord(currentValue, insertValue.get(), schema);
    if (pickCurrentRecord) {
      return Option.of(currentValue);
    }
    // Step 2: Pick the insert record (as a delete record if it is a deleted event), reusing the record
    // deserialized for step 1; keep the base class's deleted-record short-circuit from getInsertValue
    return isDeletedRecord ? Option.empty() : handleDeleteOperation(insertValue.get());
  }

  protected abstract boolean shouldPickCurrentRecord(IndexedRecord currentRecord, IndexedRecord insertRecord, Schema schema) throws IOException;

  /**
   * Resolves the configured ordering field(s) to dispatch on, or empty when nothing is configured — in
   * which case the connector-hardcoded ordering ({@link #shouldPickCurrentRecord}) applies. Values are
   * trimmed so stray whitespace in user config cannot silently change which field is compared. A
   * composite (multi-field) ordering is supported element-wise.
   */
  protected Option<String[]> getConfiguredOrderingFields(Properties properties) {
    String[] orderingFields = ConfigUtils.getOrderingFields(properties);
    if (orderingFields == null || orderingFields.length == 0
        || (orderingFields.length == 1 && orderingFields[0].trim().isEmpty())) {
      return Option.empty();
    }
    // Trim in place: ConfigUtils returns a freshly split array on every call, so mutating it is safe
    // and avoids a second per-record array allocation on the merge path.
    for (int i = 0; i < orderingFields.length; i++) {
      orderingFields[i] = orderingFields[i].trim();
    }
    return Option.of(orderingFields);
  }

  /**
   * Mirrors {@code DefaultHoodieRecordPayload#needUpdatingPersistedRecord}, extended to composite ordering:
   * fields are compared element-wise and the first non-equal field decides; the record in storage needs
   * updating unless its ordering is strictly greater than the incoming record's (ties go to the incoming
   * record; a null persisted value, e.g. bootstrapped rows, takes the incoming). A null incoming ordering
   * value fails loudly with the field and record in the message, matching the connector paths, instead of
   * surfacing as a bare NPE.
   */
  private boolean needUpdatingPersistedRecord(IndexedRecord currentValue, IndexedRecord incomingRecord,
                                              String[] orderingFields, Properties properties) throws HoodieDebeziumAvroPayloadException {
    boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(properties.getProperty(
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
        KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));
    Comparable[] incomingOrderingVals = new Comparable[orderingFields.length];
    for (int i = 0; i < orderingFields.length; i++) {
      incomingOrderingVals[i] = (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) incomingRecord,
          orderingFields[i], true, consistentLogicalTimestampEnabled);
      if (incomingOrderingVals[i] == null) {
        throw new HoodieDebeziumAvroPayloadException(String.format("ordering field %s cannot be null in insert record: %s",
            orderingFields[i], incomingRecord));
      }
    }
    for (int i = 0; i < orderingFields.length; i++) {
      Object persistedOrderingVal = HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue,
          orderingFields[i], true, consistentLogicalTimestampEnabled);
      if (persistedOrderingVal == null) {
        return true;
      }
      int comparison = ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVals[i]);
      if (comparison != 0) {
        return comparison < 0;
      }
    }
    // all ordering fields equal: tie goes to the incoming record
    return true;
  }

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
