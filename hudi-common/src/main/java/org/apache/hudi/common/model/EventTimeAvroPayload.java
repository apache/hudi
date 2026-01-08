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
import org.apache.hudi.common.util.OrderingValueUtils;
import org.apache.hudi.common.util.collection.Pair;

import java.io.IOException;
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
    if ((recordBytes.length == 0 || isDeletedRecord) && DEFAULT_VALUE.equals(orderingVal)) {
      //use natural for delete record
      return this;
    }
    Pair<Comparable, Comparable> comparablePair = OrderingValueUtils.canonicalizeOrderingValue(oldValue.orderingVal, this.orderingVal);
    Comparable oldValueOrderingVal = comparablePair.getLeft();
    Comparable thisOrderingVal = comparablePair.getRight();
    if (oldValueOrderingVal.compareTo(thisOrderingVal) > 0) {
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    /*
     * Check if the incoming record is a delete record.
     */
    Option<IndexedRecord> incomingRecord = recordBytes.length == 0 || isDeletedRecord ? Option.empty() : Option.of(HoodieAvroUtils.bytesToAvro(recordBytes,schema));
    // Null check is needed here to support schema evolution. The record in storage may be from old schema where
    // the new ordering column might not be present and hence returns null.
    if (!needUpdatingPersistedRecord(currentValue, incomingRecord, properties)) {
      return Option.of(currentValue);
    }
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

}
