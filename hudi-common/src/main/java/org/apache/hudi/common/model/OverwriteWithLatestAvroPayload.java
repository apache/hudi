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

import java.io.IOException;
import java.util.List;

/**
 * Default payload used for delta streamer.
 * <p>
 * 1. preCombine - Picks the latest delta record for a key, based on an ordering field 2.
 * combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 */
public class OverwriteWithLatestAvroPayload extends BaseAvroPayload
    implements HoodieRecordPayload<OverwriteWithLatestAvroPayload> {

  /**
   *
   */
  public OverwriteWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OverwriteWithLatestAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, (record1) -> 0); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload another,Schema schema) throws IOException {
    // pick the payload with greatest ordering value and aggregate all the fields,choosing the
    // value that is not null
    GenericRecord thisValue = (GenericRecord) HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
    GenericRecord anotherValue = (GenericRecord) HoodieAvroUtils.bytesToAvro(another.recordBytes,schema);
    List<Schema.Field> fields = schema.getFields();

    if (another.orderingVal.compareTo(orderingVal) > 0) {
      GenericRecord anotherRoc = combineAllFields(fields,anotherValue,thisValue);
      another.recordBytes = HoodieAvroUtils.avroToBytes(anotherRoc);
      return another;
    } else {
      GenericRecord thisRoc = combineAllFields(fields,thisValue,anotherValue);
      this.recordBytes = HoodieAvroUtils.avroToBytes(thisRoc);
      return this;
    }

  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload another) {
    // pick the payload with greatest ordering value
    if (another.orderingVal.compareTo(orderingVal) > 0) {
      return another;
    } else {
      return this;
    }
  }

  public GenericRecord combineAllFields(List<Schema.Field> fields,GenericRecord priorRec,GenericRecord inferiorRoc) {
    for (int i = 0; i < fields.size(); i++) {
      Object priorValue = priorRec.get(fields.get(i).name());
      Object inferiorValue = inferiorRoc.get(fields.get(i).name());
      Object defaultVal = fields.get(i).defaultVal();
      if (overwriteField(priorValue,defaultVal) && !overwriteField(inferiorValue,defaultVal)) {
        priorRec.put(fields.get(i).name(), inferiorValue);
      }
    }
    return priorRec;
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
    Object deleteMarker = genericRecord.get("_hoodie_is_deleted");
    return (deleteMarker instanceof Boolean && (boolean) deleteMarker);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return defaultValue == null ? value == null : defaultValue.toString().equals(value.toString());
  }
}
