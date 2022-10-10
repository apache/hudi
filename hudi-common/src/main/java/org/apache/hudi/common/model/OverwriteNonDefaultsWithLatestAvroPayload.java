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

import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.List;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class OverwriteNonDefaultsWithLatestAvroPayload extends OverwriteWithLatestAvroPayload {

  public OverwriteNonDefaultsWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OverwriteNonDefaultsWithLatestAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;

    return mergeRecords(schema, insertRecord, currentRecord);
  }

  /**
   * Merges the given records into one.
   * The fields in {@code baseRecord} has higher priority:
   * it is set up into the merged record if it is not null or equals to the default.
   *
   * @param schema       The record schema
   * @param baseRecord   The base record to merge with
   * @param mergedRecord The record to be merged
   *
   * @return the merged record option
   */
  protected Option<IndexedRecord> mergeRecords(Schema schema, GenericRecord baseRecord, GenericRecord mergedRecord) {
    if (isDeleteRecord(baseRecord)) {
      return Option.empty();
    } else {
      final GenericRecordBuilder builder = new GenericRecordBuilder(schema);
      List<Schema.Field> fields = schema.getFields();
      fields.forEach(field -> setField(baseRecord, mergedRecord, builder, field));
      return Option.of(builder.build());
    }
  }

  protected void setField(
      GenericRecord baseRecord,
      GenericRecord mergedRecord,
      GenericRecordBuilder builder,
      Schema.Field field) {
    Object value = baseRecord.get(field.name());
    value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
    Object defaultValue = field.defaultVal();
    if (!overwriteField(value, defaultValue)) {
      builder.set(field, value);
    } else {
      builder.set(field, mergedRecord.get(field.name()));
    }
  }
}
