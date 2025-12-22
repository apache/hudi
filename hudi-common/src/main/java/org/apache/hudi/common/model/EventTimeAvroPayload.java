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

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro;

/**
 * The only difference with {@link DefaultHoodieRecordPayload} is that is does not
 * track the event time metadata for efficiency.
 */
public class EventTimeAvroPayload extends DefaultHoodieRecordPayload {

  public EventTimeAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public EventTimeAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    if ((recordBytes.length == 0 || isDeletedRecord) && orderingVal.equals(0)){
      //use natural for delete record
      return this;
    }
    Comparable oldValueOrderingVal = oldValue.orderingVal;
    Comparable thisOrderingVal = orderingVal;
    if (thisOrderingVal instanceof Utf8 && oldValueOrderingVal instanceof String){
      thisOrderingVal = thisOrderingVal.toString();
    }
    if (thisOrderingVal instanceof GenericData.Fixed && oldValueOrderingVal instanceof BigDecimal){
      Conversions.DecimalConversion conversion = new Conversions.DecimalConversion();
      thisOrderingVal = conversion.fromFixed((GenericData.Fixed) thisOrderingVal,((GenericData.Fixed) thisOrderingVal).getSchema(),((GenericData.Fixed) thisOrderingVal).getSchema().getLogicalType());
    }
    if (oldValueOrderingVal.compareTo(thisOrderingVal)>0){
      return oldValue;
    }else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    /*
     * Check if the incoming record is a delete record.
     */
//    if (recordBytes.length == 0 || isDeletedRecord) {
//      return Option.empty();
//    }
//
//    GenericRecord incomingRecord = bytesToAvro(recordBytes, schema);
    Option<IndexedRecord> incomingRecord = recordBytes.length==0 || isDeletedRecord ? Option.empty() : Option.of(bytesToAvro(recordBytes,schema));

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }

//    return Option.of(incomingRecord);
    return incomingRecord;
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0 || isDeletedRecord) {
      return Option.empty();
    }

    return Option.of(bytesToAvro(recordBytes, schema));
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
    Comparable incomingOrderingVal = incomingRecord.map(record-> (Comparable) HoodieAvroUtils.getNestedFieldVal((GenericRecord) record,
            orderField,
            true, consistentLogicalTimestampEnabled)).orElse(orderingVal);
    if (persistedOrderingVal instanceof Utf8){
      persistedOrderingVal=persistedOrderingVal.toString();
    }
    if (persistedOrderingVal instanceof GenericData.Fixed){
      Conversions.DecimalConversion conversion=new Conversions.DecimalConversion();
      persistedOrderingVal=conversion.fromFixed((GenericData.Fixed) persistedOrderingVal,((GenericData.Fixed) persistedOrderingVal).getSchema(),((GenericData.Fixed) persistedOrderingVal).getSchema().getLogicalType());
    }
    return persistedOrderingVal == null || ((Comparable) persistedOrderingVal).compareTo(incomingOrderingVal) <= 0;
  }
}
