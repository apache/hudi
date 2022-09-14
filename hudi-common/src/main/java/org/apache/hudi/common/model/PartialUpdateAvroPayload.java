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
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.IOException;
import java.util.Properties;

/**
 * subclass of OverwriteNonDefaultsWithLatestAvroPayload used for Partial update Hudi Table.
 *
 * Simplified partial update Logic:
 *  1 preCombine
 *  For every record with duplicate record (same record key) in the same batch or in the delta logs that belongs to same File Group
 *      Check if one record's ordering value is larger than the other record. If yes,overwrite the exists one for specified fields
 *  that doesn't equal to null.
 *
 *  2 combineAndGetUpdateValue
 *  For every incoming record with exists record in storage (same record key)
 *      Check if incoming record's ordering value is larger than exists record. If yes,overwrite the exists one for specified fields
 *  that doesn't equal to null.
 *      else overwrite the incoming one with exists record for specified fields that doesn't equal to null
 *  get a merged record, write to file.
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
 *      1     , 1     , name_1, price_1
 *  Insert data:
 *      id      ts      name    price
 *      1     , 2     , null  , price_2
 *
 *  Result data after preCombine or combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1     , 2     , name_1  , price_2
 *
 *  case 2
 *  Current data:
 *      id      ts      name    price
 *      1     , 2     , name_1, null
 *  Insert data:
 *      id      ts      name    price
 *      1     , 1     , null  , price_1
 *
 *  Result data after preCombine or combineAndGetUpdateValue:
 *      id      ts      name    price
 *      1     , 2     , name_1  , price_1
 *
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field, then overwrite the older one for specified fields
 *  that doesn't equal null.
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite the older record for specified fields
 * that doesn't equal null.
 * </ol>
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
      // use natural order for delete record
      return this;
    }
    boolean isOldRecordNewer = false;
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value as insert record
      isOldRecordNewer = true;
    }
    try {
      GenericRecord indexedOldValue = (GenericRecord) oldValue.getInsertValue(schema).get();
      Option<IndexedRecord> optValue = combineAndGetUpdateValue(indexedOldValue, schema, isOldRecordNewer);
      if (optValue.isPresent()) {
        return new PartialUpdateAvroPayload((GenericRecord) optValue.get(),
            isOldRecordNewer ? oldValue.orderingVal : this.orderingVal);
      }
    } catch (Exception ex) {
      return this;
    }
    return this;
  }

  private Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, boolean shouldInsertCurrentValue) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);

    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord;
    GenericRecord currentRecord;
    if (shouldInsertCurrentValue) {
      insertRecord = (GenericRecord) currentValue;
      currentRecord = (GenericRecord) recordOption.get();
    } else {
      insertRecord = (GenericRecord) recordOption.get();
      currentRecord = (GenericRecord) currentValue;
    }

    return mergeRecords(schema, insertRecord, currentRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.combineAndGetUpdateValue(currentValue, schema, false);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {

    String orderingField = prop.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
    boolean isOldRecordNewer = false;

    if (!StringUtils.isNullOrEmpty(orderingField)) {

      boolean consistentLogicalTimestampEnabled = Boolean.parseBoolean(prop.getProperty(
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.key(),
          KeyGeneratorOptions.KEYGENERATOR_CONSISTENT_LOGICAL_TIMESTAMP_ENABLED.defaultValue()));

      Comparable oldOrderingVal = (Comparable)HoodieAvroUtils.getNestedFieldVal((GenericRecord) currentValue,
          orderingField,
          true, consistentLogicalTimestampEnabled);
      if (oldOrderingVal != null && ReflectionUtils.isSameClass(oldOrderingVal, orderingVal)
          && oldOrderingVal.compareTo(orderingVal) > 0) {
        // pick the payload with greatest ordering value as insert record
        isOldRecordNewer = true;
      }
    }
    return combineAndGetUpdateValue(currentValue, schema, isOldRecordNewer);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return value == null;
  }
}
