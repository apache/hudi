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
import org.apache.hudi.exception.HoodieException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;
import java.util.Properties;

public class AvroSummationPayload extends BaseAvroPayload
    implements HoodieRecordPayload<AvroSummationPayload> {

  public static final String SUM_INPUT_FIELDS = "hoodie.summation.payload.input.fields";
  public static final String SUM_OUTPUT_FIELDS = "hoodie.summation.payload.output.fields";

  public AvroSummationPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public AvroSummationPayload(Option<GenericRecord> record) {
    this(record.isPresent() ? record.get() : null, 0); // natural order
  }

  @Override
  public AvroSummationPayload preCombine(AvroSummationPayload oldValue) {
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }
    if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
      // pick the payload with greatest ordering value
      return oldValue;
    } else {
      return this;
    }
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return getInsertValue(schema);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties properties) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }

    GenericRecord incomingRecord = HoodieAvroUtils.bytesToAvro(recordBytes, schema);

    final String sumInputField = properties.getProperty(SUM_INPUT_FIELDS);
    final String sumOutputField = properties.getProperty(SUM_OUTPUT_FIELDS);

    if (incomingRecord.getSchema().getField(sumOutputField) == null || incomingRecord.getSchema().getField(sumInputField) == null) {
      throw new HoodieException("Sum input nor sum output field missing from table schema");
    }

    Long sumInput = (Long) incomingRecord.get(sumInputField);
    Long prevTotalSum = (Long) ((GenericRecord) currentValue).get(sumOutputField);
    long updatedSum = prevTotalSum + sumInput;
    incomingRecord.put(sumOutputField, updatedSum);
    return Option.of(incomingRecord);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    if (recordBytes.length == 0) {
      return Option.empty();
    }
    return Option.of(HoodieAvroUtils.bytesToAvro(recordBytes, schema));
  }

  @Override
  public Comparable<?> getOrderingValue() {
    return this.orderingVal;
  }
}