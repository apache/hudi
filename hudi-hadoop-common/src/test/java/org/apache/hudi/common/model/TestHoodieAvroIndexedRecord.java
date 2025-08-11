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

import org.apache.hudi.common.config.TypedProperties;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieAvroIndexedRecord {

  @Test
  void testIsBuiltInDelete() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .optionalBoolean("_hoodie_is_deleted")
        .requiredString("field2")
        .endRecord();
    GenericRecord record1 = new GenericRecordBuilder(schema)
        .set("_hoodie_is_deleted", true)
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(schema, new TypedProperties()));

    GenericRecord record2 = new GenericRecordBuilder(schema)
        .set("_hoodie_is_deleted", false)
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(schema, new TypedProperties()));
  }

  @Test
  void testIsDeleteWithCustomField() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .optionalString("custom_is_deleted")
        .requiredString("field2")
        .endRecord();
    GenericRecord record1 = new GenericRecordBuilder(schema)
        .set("custom_is_deleted", "d")
        .set("field2", "value2")
        .build();
    TypedProperties props = new TypedProperties();
    props.setProperty(DELETE_KEY, "custom_is_deleted");
    props.setProperty(DELETE_MARKER, "d");

    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(schema, props));

    GenericRecord record2 = new GenericRecordBuilder(schema)
        .set("custom_is_deleted", "a")
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(schema, props));
  }

  @Test
  void testIsDeleteWithOperationField() {
    Schema schema = SchemaBuilder.record("TestRecord")
        .fields()
        .optionalString("_hoodie_operation")
        .requiredString("field2")
        .endRecord();
    GenericRecord record1 = new GenericRecordBuilder(schema)
        .set("_hoodie_operation", HoodieOperation.DELETE.getName())
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(schema, new TypedProperties()));

    GenericRecord record2 = new GenericRecordBuilder(schema)
        .set("_hoodie_operation", HoodieOperation.INSERT.getName())
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(schema, new TypedProperties()));
  }
}
