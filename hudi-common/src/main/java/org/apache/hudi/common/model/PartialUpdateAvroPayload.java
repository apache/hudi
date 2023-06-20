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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

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
 *
 * Multiple ordering fields partial update Logic:
 * This feature aims to improve PartialUpdatePayload to handle multiple sources properly
 * Let's give you some cases about why we need multiple ordering fields
 * For example, we have 2 sources, one target table
 *
 * source1's fields: id, ts, name
 * source2's fields:id, ts, price
 * target tables's fields:
 *   id,ts,name, price
 * ts is the precombine field;
 *
 * in the 1st batch, we got two records from both sources:
 * Source1:
 *
 * id    ts    name
 * 1    1    name_1
 * Source 2:
 *
 * id    ts    price
 * 1    3    price_3
 * so the records in the target table should be:
 *
 * id    ts    name    price
 * 1    3    name_1    price_3
 * let's say in the 2nd batch, we got one event from the source1:
 * Source1:
 *
 * id    ts    name
 * 1    2    name_2
 * but name_2 won't be updated to the target table, since its ts value is smaller than the ts value in the target table.
 *
 * This feature will allow users to perform partial updates across sub-tables/sources by determining the state of a set of columns in a row based on an ordering/precombine column.
 *
 * As such, a table can have MULTIPLE ordering fields.
 *
 * This use case is suitable for wide Hudi tables that are created from smaller sub-tables,
 * where each of its sub-tables has its own precombine column, and where its records could be upserted out of order.
 * </pre>
 */
public class PartialUpdateAvroPayload extends OverwriteNonDefaultsWithLatestAvroPayload {

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
    try {
      PartialUpdateAvroPayload mergedRecord;
      if (isMultipleOrderFields(this.orderingVal.toString())) {
        mergedRecord = getPayloadWithMultipleOrderFields(oldValue, schema);
      } else {
        mergedRecord = getPayloadWithSingleOrderFields(oldValue, schema);
      }
      if (mergedRecord != null) {
        return mergedRecord;
      }
    } catch (Exception ex) {
      return this;
    }
    return this;
  }

  private PartialUpdateAvroPayload getPayloadWithSingleOrderFields(OverwriteWithLatestAvroPayload oldValue, Schema schema) throws IOException {
    final boolean shouldPickOldRecord = oldValue.orderingVal.compareTo(orderingVal) > 0;
    GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
    Option<IndexedRecord> mergedRecord = mergeOldRecord(oldRecord, schema, shouldPickOldRecord, true);
    if (mergedRecord.isPresent()) {
      return new PartialUpdateAvroPayload((GenericRecord) mergedRecord.get(),
          shouldPickOldRecord ? oldValue.orderingVal : this.orderingVal);
    }
    return null;
  }

  private PartialUpdateAvroPayload getPayloadWithMultipleOrderFields(OverwriteWithLatestAvroPayload oldValue, Schema schema) throws IOException {
    GenericRecord oldRecord = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schema);
    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
    MultipleOrderingInfo multipleOrderingInfo = MultipleOrderingInfo.newBuilder().withMultipleOrderingConfig(this.orderingVal.toString()).withGenericRecord(incomingRecord).build();
    Option<IndexedRecord> mergedRecord = overwritePartialOldRecord(oldRecord, incomingRecord, schema, multipleOrderingInfo);
    if (mergedRecord.isPresent()) {
      return new PartialUpdateAvroPayload((GenericRecord) mergedRecord.get(), this.orderingVal);
    }
    return null;
  }

  private Option<IndexedRecord> overwritePartialOldRecord(GenericRecord oldRecord,
                                                          GenericRecord incomingRecord,
                                                          Schema schema,
                                                          MultipleOrderingInfo multipleOrderingInfo) {
    GenericRecord resultRecord = incomingRecord;

    Map<String, Schema.Field> name2Field = schema.getFields().stream().collect(Collectors.toMap(Schema.Field::name, item -> item));

    multipleOrderingInfo.getOrderingVal2ColsInfoList().stream().forEach(orderingVal2ColsInfo -> {
      Comparable persistOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal(
          oldRecord, orderingVal2ColsInfo.getOrderingField(), true, false);

      // No update required
      if (persistOrderingVal == null && orderingVal2ColsInfo.getOrderingField().isEmpty()) {
        return;
      }

      // Pick the payload with greatest ordering value as insert record
      boolean useIncomingFieldValue = false;
      if (persistOrderingVal == null || (orderingVal2ColsInfo.getOrderingValue() != null && persistOrderingVal.compareTo(orderingVal2ColsInfo.getOrderingValue()) <= 0)) {
        useIncomingFieldValue = true;
      }

      // Initialise the fields of the sub-tables
      GenericRecord insertRecordForSubTable;
      if (!useIncomingFieldValue) {
        insertRecordForSubTable = oldRecord;
        orderingVal2ColsInfo.getColumnNames().stream()
            .filter(fieldName -> name2Field.containsKey(fieldName))
            .forEach(fieldName -> resultRecord.put(fieldName, insertRecordForSubTable.get(fieldName)));
        resultRecord.put(orderingVal2ColsInfo.getOrderingField(), persistOrderingVal);
      }
    });

    return Option.of(resultRecord);
  }

  public static boolean isMultipleOrderFields(String preCombineField) {
    if (preCombineField.split(":").length > 1) {
      return true;
    }
    return false;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    if (!isMultipleOrderFields(this.orderingVal.toString())) {
      return this.mergeOldRecord(currentValue, schema, false, false);
    } else {
      GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
      MultipleOrderingInfo multipleOrderingInfo = MultipleOrderingInfo.newBuilder().withMultipleOrderingConfig(this.orderingVal.toString())
          .withGenericRecord(incomingRecord).build();
      return this.overwritePartialOldRecord((GenericRecord) currentValue, incomingRecord, schema, multipleOrderingInfo);
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    if (!isMultipleOrderFields(this.orderingVal.toString())) {
      return mergeOldRecord(currentValue, schema, isRecordNewer(orderingVal, currentValue, prop), false);
    } else {
      GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
      MultipleOrderingInfo multipleOrderingInfo = MultipleOrderingInfo.newBuilder().withMultipleOrderingConfig(this.orderingVal.toString())
          .withGenericRecord(incomingRecord).build();
      return this.overwritePartialOldRecord((GenericRecord) currentValue, incomingRecord, schema, multipleOrderingInfo);
    }
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

    return Option.of((IndexedRecord) HoodieAvroUtils.bytesToAvro(recordBytes, schema));
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
    String orderingField = ConfigUtils.getOrderingField(prop);
    if (!StringUtils.isNullOrEmpty(orderingField)) {
      boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(prop.getProperty(
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));

      Comparable oldOrderingVal =
          (Comparable) HoodieAvroUtils.getNestedFieldVal(
              (GenericRecord) record,
              orderingField,
              true,
              consistentLogicalTimestampEnabled);

      // pick the payload with greater ordering value as insert record
      return oldOrderingVal != null
          && ReflectionUtils.isSameClass(oldOrderingVal, orderingVal)
          && oldOrderingVal.compareTo(orderingVal) > 0;
    }
    return false;
  }
}
