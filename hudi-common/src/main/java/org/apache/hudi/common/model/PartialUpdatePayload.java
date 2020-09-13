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

public class PartialUpdatePayload extends OverwriteWithLatestAvroPayload {
  public PartialUpdatePayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdatePayload(Option<GenericRecord> record) {
    this(record.get(), (record1) -> 0); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (recordOption.isPresent()) {
      IndexedRecord record = recordOption.get();
      GenericRecord current = (GenericRecord) record;

      List<Schema.Field> fieldList = schema.getFields();
      GenericRecord last = (GenericRecord) currentValue;
      for (Schema.Field field : fieldList) {
        last.put(field.name(), current.get(field.name()));
      }
      return Option.ofNullable(last);
    }
    return recordOption;
  }
}