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
import org.apache.hudi.common.util.OrderingValues;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Objects;

/**
 * <ol>
 * <li> preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li> combineAndGetUpdateValue/getInsertValue - Simply overwrites storage with latest delta record
 * </ol>
 */
public class OverwriteWithLatestAvroPayload extends BaseAvroPayload
    implements HoodieRecordPayload<OverwriteWithLatestAvroPayload> {

  public OverwriteWithLatestAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public OverwriteWithLatestAvroPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, OrderingValues.getDefault()); // natural order
  }

  @Override
  public OverwriteWithLatestAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue) {
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (isEmptyRecord() || isDeletedRecord) {
      return Option.empty();
    }

    return getRecord(schema);
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    if (JsonProperties.NULL_VALUE.equals(defaultValue)) {
      return value == null;
    }
    return Objects.equals(value, defaultValue);
  }

  @Override
  public Comparable<?> getOrderingValue() {
    return this.orderingVal;
  }
}
