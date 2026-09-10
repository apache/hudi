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

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.OrderingValues;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * Tests {@link HoodieAvroRecord}, in particular that its ordering value keeps the type of the
 * ordering field even when the payload was built without one.
 */
class TestHoodieAvroRecord {

  private static final String SCHEMA_STR = "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
      + "{\"name\":\"_row_key\",\"type\":\"string\"},"
      + "{\"name\":\"ts\",\"type\":\"long\"}]}";
  private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_STR);
  private static final Schema NULLABLE_TS_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
          + "{\"name\":\"_row_key\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":[\"null\",\"long\"]}]}");
  private static final Schema DELETABLE_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
          + "{\"name\":\"_row_key\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\"},"
          + "{\"name\":\"_hoodie_is_deleted\",\"type\":\"boolean\",\"default\":false}]}");
  private static final String[] ORDERING_FIELDS = new String[] {"ts"};
  private static final long TS = 1757000000000L;

  private static GenericRecord avroRecord() {
    GenericRecord record = new GenericData.Record(SCHEMA);
    record.put("_row_key", "key1");
    record.put("ts", TS);
    return record;
  }

  /**
   * Reproduces the CoW UPDATE failure: prepped Spark SQL writes create the record through the
   * overload that carries no ordering value, so the payload defaults to {@code Integer} 0 while
   * the base record read from storage yields the {@code ts} value as a {@code Long}. Comparing the
   * two threw {@code ClassCastException: class java.lang.Long cannot be cast to class
   * java.lang.Integer} in BufferedRecordMergerFactory#shouldKeepNewerRecord.
   */
  @Test
  void testOrderingValueIsReadFromRecordWhenPayloadHasNone() {
    HoodieRecord<?> record = HoodieRecordUtils.createHoodieRecord(
        avroRecord(), new HoodieKey("key1", "p"), DefaultHoodieRecordPayload.class.getName(),
        Option.empty(), true, false);
    assertInstanceOf(HoodieAvroRecord.class, record);

    Comparable<?> orderingValue = record.getOrderingValue(HoodieSchema.fromAvroSchema(SCHEMA), new Properties(), ORDERING_FIELDS);

    assertInstanceOf(Long.class, orderingValue);
    assertEquals(TS, orderingValue);
  }

  private static HoodieRecord<?> payloadRecord(GenericRecord data) {
    return HoodieRecordUtils.createHoodieRecord(
        data, new HoodieKey("key1", "p"), DefaultHoodieRecordPayload.class.getName(),
        Option.empty(), true, false);
  }

  /**
   * A nullable ordering field holding null must keep the payload's default. Returning the null it
   * reads back would make BufferedRecordMergerFactory#shouldKeepNewerRecord throw NPE.
   */
  @Test
  void testNullOrderingFieldValueKeepsPayloadDefault() {
    GenericRecord data = new GenericData.Record(NULLABLE_TS_SCHEMA);
    data.put("_row_key", "key1");
    data.put("ts", null);
    HoodieRecord<?> record = payloadRecord(data);

    assertEquals(OrderingValues.getDefault(),
        record.getOrderingValue(HoodieSchema.fromAvroSchema(NULLABLE_TS_SCHEMA), new Properties(), ORDERING_FIELDS));
  }

  /**
   * Deletes keep the default ordering value. BufferedRecord#isCommitTimeOrderingDelete tests for
   * it, so giving a delete a real ordering value would change which delete wins.
   */
  @Test
  void testDeleteRecordKeepsPayloadDefault() {
    GenericRecord data = new GenericData.Record(DELETABLE_SCHEMA);
    data.put("_row_key", "key1");
    data.put("ts", TS);
    data.put("_hoodie_is_deleted", true);
    HoodieRecord<?> record = payloadRecord(data);

    assertEquals(OrderingValues.getDefault(),
        record.getOrderingValue(HoodieSchema.fromAvroSchema(DELETABLE_SCHEMA), new Properties(), ORDERING_FIELDS));
  }

  /**
   * A schema the payload cannot decode the record with must not turn an ordering value lookup into
   * a failure; the payload's default stands.
   */
  @Test
  void testUndecodableRecordSchemaKeepsPayloadDefault() {
    HoodieRecord<?> record = payloadRecord(avroRecord());
    Schema mismatched = new Schema.Parser().parse(
        "{\"type\":\"record\",\"name\":\"trip\",\"fields\":["
            + "{\"name\":\"extra_a\",\"type\":\"string\"},"
            + "{\"name\":\"extra_b\",\"type\":\"string\"},"
            + "{\"name\":\"_row_key\",\"type\":\"string\"},"
            + "{\"name\":\"ts\",\"type\":\"long\"}]}");

    assertEquals(OrderingValues.getDefault(),
        record.getOrderingValue(HoodieSchema.fromAvroSchema(mismatched), new Properties(), ORDERING_FIELDS));
  }

  /**
   * An ordering value supplied at construction time still wins, so payloads that carry their own
   * ordering semantics are unaffected.
   */
  @Test
  void testExplicitOrderingValueWins() {
    HoodieRecord<?> record = HoodieRecordUtils.createHoodieRecord(
        avroRecord(), 42L, new HoodieKey("key1", "p"), DefaultHoodieRecordPayload.class.getName(),
        true, false);

    assertEquals(42L, record.getOrderingValue(HoodieSchema.fromAvroSchema(SCHEMA), new Properties(), ORDERING_FIELDS));
  }

  /**
   * Without ordering fields the record keeps the payload's default, i.e. natural (arrival) order.
   */
  @Test
  void testDefaultOrderingValueWithoutOrderingFields() {
    HoodieRecord<?> record = HoodieRecordUtils.createHoodieRecord(
        avroRecord(), new HoodieKey("key1", "p"), DefaultHoodieRecordPayload.class.getName(),
        Option.empty(), true, false);

    assertEquals(OrderingValues.getDefault(),
        record.getOrderingValue(HoodieSchema.fromAvroSchema(SCHEMA), new Properties(), new String[0]));
  }
}
