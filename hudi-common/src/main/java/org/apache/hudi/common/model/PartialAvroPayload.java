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
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

/**
 * subclass of OverwriteWithLatestAvroPayload.
 *
 * Extract the function precombine of UpdatePrecombineAvroPayload and combineAndGetUpdateValue of OverwriteNonDefaultsWithLatestAvroPayload.
 * Which means When more than one HoodieRecord have the same HoodieKey, this function will combine all fields(which is not null)
 * Before attempting to insert/upsert  And when insert/upsert into storage.
 */
public class PartialAvroPayload extends UpdatePrecombineAvroPayload {
  public PartialAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialAvroPayload(Option<GenericRecord> record) {
    super(record);
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
