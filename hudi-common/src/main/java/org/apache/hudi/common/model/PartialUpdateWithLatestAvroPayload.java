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

import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

import static org.apache.hudi.avro.HoodieAvroUtils.bytesToAvro;

/**
 * The only difference with {@link DefaultHoodieRecordPayload} is that support update partial fields
 * in latest record to old record instead of all fields.
 */
public class PartialUpdateWithLatestAvroPayload extends DefaultHoodieRecordPayload {

  public PartialUpdateWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.of(currentValue);
    }

    GenericRecord incomingRecord = bytesToAvro(recordBytes, schema);

    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }

    if (isDeleteRecord(incomingRecord)) {
      return Option.empty();
    }

    GenericRecord currentRecord = (GenericRecord) currentValue;
    // The field num in updated record may be less than old record, so only update these partial fields to old record.
    List<Schema.Field> fields = schema.getFields();
    fields.forEach(field -> {
      Object value = incomingRecord.get(field.name());
      if (Objects.nonNull(value)) {
        currentRecord.put(field.name(), value);
      }
    });

    return Option.of(currentValue);
  }
}
