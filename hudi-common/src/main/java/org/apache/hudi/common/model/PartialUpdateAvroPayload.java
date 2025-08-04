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
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 * Payload clazz that is used for partial update Hudi Table.
 *
 * <p>Simplified partial update Logic:
 * <pre>
 *  1. #preCombine
 *  For records with the same record key in one batch
 *  or in the delta logs that belongs to same File Group,
 *  Checks whether one record's ordering value is larger than the other record.
 *  If yes, overwrites the existing one for specified fields that doesn't equal to null.
 *
 *  2. #combineAndGetUpdateValue
 *  For every incoming record with existing record in storage (same record key)
 *  Checks whether incoming record's ordering value is larger than the existing record.
 *  If yes, overwrites the existing one for specified fields that doesn't equal to null.
 *  else overwrites the incoming one with the existing record for specified fields that doesn't equal to null
 *  and returns a merged record.
 *
 *  Illustration with simple data.
 *  let's say the order field is 'ts' and schema is :
 *  {
 *    [
 *      {"name":"id","type":"string"},
 *      {"name":"ts","type":"long"},
 *      {"name":"name","type":"string"},
 *      {"name":"price","type":"string"}
 *    ]
 *  }
 *
 *  case 1
 *  Current data:
 *      id      ts      name    price
 *      1       1       name_1  price_1
 *  Insert data:
 *      id      ts      name    price
 *      1       2       null    price_2
 *
 *  Result data after #preCombine or #combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1       2       name_1  price_2
 *
 *  case 2
 *  Current data:
 *      id      ts      name    price
 *      1       2       name_1  null
 *  Insert data:
 *      id      ts      name    price
 *      1       1       null    price_1
 *
 *  Result data after preCombine or combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1       2       name_1  price_1
 * </pre>
 *
 * <p>Gotchas:
 * <p>In cases where a batch of records is preCombine before combineAndGetUpdateValue with the underlying records to be updated located in parquet files, the end states of records might not be as how
 * one will expect when applying a straightforward partial update.
 *
 * <p>Gotchas-Example:
 * <pre>
 *  -- Insertion order of records:
 *  INSERT INTO t1 VALUES (1, 'a1', 10, 1000);                          -- (1)
 *  INSERT INTO t1 VALUES (1, 'a1', 11, 999), (1, 'a1_0', null, 1001);  -- (2)
 *
 *  SELECT id, name, price, _ts FROM t1;
 *  -- One would the results to return:
 *  -- 1    a1_0    10.0    1001

 *  -- However, the results returned are:
 *  -- 1    a1_0    11.0    1001
 *
 *  -- This occurs as preCombine is applied on (2) first to return:
 *  -- 1    a1_0    11.0    1001
 *
 *  -- And this then combineAndGetUpdateValue with the existing oldValue:
 *  -- 1    a1_0    10.0    1000
 *
 *  -- To return:
 *  -- 1    a1_0    11.0    1001
 * </pre>
 */
public class PartialUpdateAvroPayload extends OverwriteNonDefaultsWithLatestAvroPayload {

  private static final Logger LOG = LoggerFactory.getLogger(PartialUpdateAvroPayload.class);

  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public PartialUpdateAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema, Properties properties) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for deleted record
      return this;
    }
    // pick the payload with greater ordering value as insert record
    final boolean shouldPickOldRecord = oldValue.orderingVal.compareTo(orderingVal) > 0;
    try {
      GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
      Option<IndexedRecord> mergedRecord = mergeOldRecord(oldRecord, schema, shouldPickOldRecord, true);
      if (mergedRecord.isPresent()) {
        return new PartialUpdateAvroPayload((GenericRecord) mergedRecord.get(),
            shouldPickOldRecord ? oldValue.orderingVal : this.orderingVal);
      }
    } catch (Exception ex) {
      LOG.warn("PartialUpdateAvroPayload precombine failed with ", ex);
      return this;
    }
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.mergeOldRecord(currentValue, schema, false, false);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    return mergeOldRecord(currentValue, schema, isRecordNewer(orderingVal, currentValue, prop), false);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return value == null;
  }

  // -------------------------------------------------------------------------
  //  Utilities
  // -------------------------------------------------------------------------

  /**
   * Merge old record with new record.
   *
   * @param oldRecord
   * @param schema
   * @param isOldRecordNewer
   * @param isPreCombining   flag for deleted record combine logic
   *                         1 preCombine: if delete record is newer, return merged record with _hoodie_is_deleted = true
   *                         2 combineAndGetUpdateValue:  if delete record is newer, return empty since we don't need to store deleted data to storage
   * @return
   * @throws IOException
   */
  private Option<IndexedRecord> mergeOldRecord(IndexedRecord oldRecord,
                                               Schema schema,
                                               boolean isOldRecordNewer, boolean isPreCombining) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema, isPreCombining);

    if (!recordOption.isPresent() && !isPreCombining) {
      // use natural order for delete record
      return Option.empty();
    }

    if (isOldRecordNewer && schema.getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD) != null) {
      // handling disorder, should use the metadata fields of the updating record
      return mergeDisorderRecordsWithMetadata(schema, (GenericRecord) oldRecord, (GenericRecord) recordOption.get(), isPreCombining);
    } else if (isOldRecordNewer) {
      return mergeRecords(schema, (GenericRecord) oldRecord, (GenericRecord) recordOption.get());
    } else {
      return mergeRecords(schema, (GenericRecord) recordOption.get(), (GenericRecord) oldRecord);
    }
  }

  /**
   * return itself as long as it called by preCombine
   * @param schema
   * @param isPreCombining
   * @return
   * @throws IOException
   */
  public Option<IndexedRecord> getInsertValue(Schema schema, boolean isPreCombining) throws IOException {
    if (recordBytes.length == 0 || (!isPreCombining && isDeletedRecord)) {
      return Option.empty();
    }

    return Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }

  /**
   * Merges the given disorder records with metadata.
   *
   * @param schema         The record schema
   * @param oldRecord      The current record from file
   * @param updatingRecord The incoming record
   * @return the merged record option
   */
  protected Option<IndexedRecord> mergeDisorderRecordsWithMetadata(
      Schema schema,
      GenericRecord oldRecord,
      GenericRecord updatingRecord, boolean isPreCombining) {
    if (isDeleteRecord(oldRecord) && !isPreCombining) {
      return Option.empty();
    } else {
      final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      List<Schema.Field> fields = schema.getFields();
      fields.forEach(field -> {
        final GenericRecord baseRecord;
        final GenericRecord mergedRecord;
        if (HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.containsKey(field.name())) {
          // this is a metadata field
          baseRecord = updatingRecord;
          mergedRecord = oldRecord;
        } else {
          baseRecord = oldRecord;
          mergedRecord = updatingRecord;
        }
        setField(baseRecord, mergedRecord, builder, field);
      });
      return Option.of(builder.build());
    }
  }

  /**
   * Returns whether the given record is newer than the record of this payload.
   *
   * @param orderingVal
   * @param record      The record
   * @param prop        The payload properties
   * @return true if the given record is newer
   */
  private static boolean isRecordNewer(Comparable orderingVal, IndexedRecord record, Properties prop) {
    String[] orderingFields = ConfigUtils.getOrderingFields(prop);
    if (orderingFields != null) {
      boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(prop.getProperty(
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));

      Comparable oldOrderingVal = OrderingValues.create(
          orderingFields,
          field -> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record, field, true, consistentLogicalTimestampEnabled));

      // pick the payload with greater ordering value as insert record
      return oldOrderingVal != null
          && OrderingValues.isSameClass(oldOrderingVal, orderingVal)
          && oldOrderingVal.compareTo(orderingVal) > 0;
    }
    return false;
  }
}
