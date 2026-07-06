/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.common.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.apache.hudi.common.avro.AvroRecordContext.getFieldValueFromIndexedRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestAvroRecordContext {

  private static Stream<Arguments> testConvertValueToEngineType() {
    return Stream.of(
        Arguments.of(1L, 1L),
        Arguments.of("test", new Utf8("test")),
        Arguments.of(new Utf8("utf8_string"), new Utf8("utf8_string")),
        Arguments.of(1.23, 1.23));
  }

  @ParameterizedTest
  @MethodSource
  void testConvertValueToEngineType(Comparable input, Comparable expected) {
    Comparable actual = AvroRecordContext.getFieldAccessorInstance().convertValueToEngineType(input);
    assertEquals(expected, actual);
  }

  private static final Schema RECORD_SCHEMA = new Schema.Parser().parse(
      "{\"type\":\"record\",\"name\":\"top\",\"fields\":["
          + "{\"name\":\"id\",\"type\":\"int\"},"
          + "{\"name\":\"name\",\"type\":[\"null\",\"string\"],\"default\":null},"
          + "{\"name\":\"address\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"address\",\"fields\":["
          + "{\"name\":\"city\",\"type\":\"string\"},"
          + "{\"name\":\"zip\",\"type\":[\"null\",\"int\"],\"default\":null}]}],\"default\":null},"
          + "{\"name\":\"multi\",\"type\":[\"null\",\"string\",\"int\"],\"default\":null}]}");

  private static GenericRecord buildRecord() {
    GenericRecord address = new GenericData.Record(RECORD_SCHEMA.getField("address").schema().getTypes().get(1));
    address.put("city", new Utf8("sf"));
    address.put("zip", 94105);
    GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
    record.put("id", 1);
    record.put("name", new Utf8("alice"));
    record.put("address", address);
    return record;
  }

  @Test
  void testGetFieldValueTopLevel() {
    GenericRecord record = buildRecord();
    assertEquals(1, getFieldValueFromIndexedRecord(record, "id"));
    assertEquals(new Utf8("alice"), getFieldValueFromIndexedRecord(record, "name"));
    assertNull(getFieldValueFromIndexedRecord(record, "multi"));
    assertNull(getFieldValueFromIndexedRecord(record, "missing"));
  }

  @Test
  void testGetFieldValueNested() {
    GenericRecord record = buildRecord();
    // intermediate segment unwraps the [null, record] union
    assertEquals(new Utf8("sf"), getFieldValueFromIndexedRecord(record, "address.city"));
    assertEquals(94105, getFieldValueFromIndexedRecord(record, "address.zip"));
    assertNull(getFieldValueFromIndexedRecord(record, "address.missing"));
    assertNull(getFieldValueFromIndexedRecord(record, "missing.nested"));
  }

  @Test
  void testGetFieldValueErrorCases() {
    GenericRecord record = buildRecord();
    // a union that is not [null, T] does not support field lookups
    assertThrows(IllegalStateException.class, () -> getFieldValueFromIndexedRecord(record, "multi.sub"));
    assertThrows(IllegalArgumentException.class, () -> getFieldValueFromIndexedRecord(record, ""));
  }

  @Test
  void testGetFieldValueAcrossEqualSchemaInstances() {
    // records from different files carry equal but distinct schema instances; both must intern to
    // the same canonical wrapper and resolve identically
    Schema schemaCopy = new Schema.Parser().parse(RECORD_SCHEMA.toString());
    GenericRecord record = buildRecord();
    GenericRecord recordWithCopy = new GenericData.Record(schemaCopy);
    for (Schema.Field field : RECORD_SCHEMA.getFields()) {
      recordWithCopy.put(field.pos(), record.get(field.pos()));
    }
    assertEquals(getFieldValueFromIndexedRecord(record, "id"), getFieldValueFromIndexedRecord(recordWithCopy, "id"));
    assertEquals(getFieldValueFromIndexedRecord(record, "address.city"), getFieldValueFromIndexedRecord(recordWithCopy, "address.city"));
  }
}
