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

package org.apache.hudi.common.model;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class TestSerializableIndexRecord {
  private final Schema schema = SchemaBuilder.record("test")
      .fields().optionalString("field_1").requiredInt("field_2").endRecord();

  @Test
  void testEqualsWithGenericRecord() {
    GenericRecord record1 = new GenericRecordBuilder(schema)
        .set("field_1", "value1")
        .set("field_2", 42)
        .build();

    GenericRecord record2 = new GenericRecordBuilder(schema)
        .set("field_1", "value2")
        .set("field_2", 42)
        .build();

    SerializableIndexedRecord serializableIndexedRecord = SerializableIndexedRecord.createInstance(record1);

    assertEquals(record1, serializableIndexedRecord.getRecord());
    assertEquals(serializableIndexedRecord, record1);

    assertNotEquals(record2, serializableIndexedRecord.getRecord());
    assertNotEquals(serializableIndexedRecord, record2);

    assertNotEquals(serializableIndexedRecord, null);
  }
}
