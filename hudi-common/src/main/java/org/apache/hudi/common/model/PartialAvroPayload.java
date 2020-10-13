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

import java.io.IOException;
import java.util.List;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>Extract the features of OverwritePrecombineAvroPayload and OverwriteNonDefaultsWithLatestAvroPayload
 * </ol>
 */
public class PartialAvroPayload extends OverwriteWithLatestAvroPayload {
  public PartialAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload another, Schema schema) throws IOException {
    // pick the payload with greatest ordering value and aggregate all the fields,choosing the
    // value that is not null
    GenericRecord thisValue = (GenericRecord) HoodieAvroUtils.bytesToAvro(this.recordBytes, schema);
    GenericRecord anotherValue = (GenericRecord) HoodieAvroUtils.bytesToAvro(another.recordBytes, schema);
    List<Schema.Field> fields = schema.getFields();

    if (another.orderingVal.compareTo(orderingVal) > 0) {
      GenericRecord anotherRoc = combineAllFields(fields, anotherValue, thisValue);
      another.recordBytes = HoodieAvroUtils.avroToBytes(anotherRoc);
      return another;
    } else {
      GenericRecord thisRoc = combineAllFields(fields, thisValue, anotherValue);
      this.recordBytes = HoodieAvroUtils.avroToBytes(thisRoc);
      return this;
    }
  }

  public GenericRecord combineAllFields(List<Schema.Field> fields, GenericRecord priorRec, GenericRecord secPriorRoc) {
    for (int i = 0; i < fields.size(); i++) {
      Object priorValue = priorRec.get(fields.get(i).name());
      Object secPriorValue = secPriorRoc.get(fields.get(i).name());
      Object defaultVal = fields.get(i).defaultVal();
      if (overwriteField(priorValue, defaultVal) && !overwriteField(secPriorValue, defaultVal)) {
        priorRec.put(fields.get(i).name(), secPriorValue);
      }
    }
    return priorRec;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;

    if (isDeleteRecord(insertRecord)) {
      return Option.empty();
    } else {
      List<Schema.Field> fields = schema.getFields();
      fields.forEach(field -> {
        Object value = insertRecord.get(field.name());
        Object defaultValue = field.defaultVal();
        if (!overwriteField(value, defaultValue)) {
          currentRecord.put(field.name(), value);
        }
      });
      return Option.of(currentRecord);
    }
  }
}
