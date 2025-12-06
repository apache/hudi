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
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.table.read.DeleteContext;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_KEY;
import static org.apache.hudi.common.model.DefaultHoodieRecordPayload.DELETE_MARKER;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieAvroIndexedRecord {

  @Test
  void testIsBuiltInDelete() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("_hoodie_is_deleted",
                HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.BOOLEAN)), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.STRING))
        ));
    GenericRecord record1 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("_hoodie_is_deleted", true)
        .set("field2", "value2")
        .build();
    TypedProperties props = new TypedProperties();
    DeleteContext deleteContext = new DeleteContext(props, hoodieSchema).withReaderSchema(hoodieSchema);
    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(deleteContext, props));

    GenericRecord record2 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("_hoodie_is_deleted", false)
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(deleteContext, props));
  }

  @Test
  void testIsDeleteWithCustomField() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("custom_is_deleted",
                HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.STRING))
        ));
    GenericRecord record1 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("custom_is_deleted", "d")
        .set("field2", "value2")
        .build();
    TypedProperties props = new TypedProperties();
    props.setProperty(DELETE_KEY, "custom_is_deleted");
    props.setProperty(DELETE_MARKER, "d");
    DeleteContext deleteContext = new DeleteContext(props, hoodieSchema).withReaderSchema(hoodieSchema);

    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(deleteContext, props));

    GenericRecord record2 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("custom_is_deleted", "a")
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(deleteContext, props));
  }

  @Test
  void testIsDeleteWithOperationField() {
    HoodieSchema hoodieSchema = HoodieSchema.createRecord("TestRecord", null, null,
        Arrays.asList(
            HoodieSchemaField.of("_hoodie_operation",
                HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)), null, HoodieSchema.NULL_VALUE),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.STRING))
        ));
    GenericRecord record1 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("_hoodie_operation", HoodieOperation.DELETE.getName())
        .set("field2", "value2")
        .build();
    TypedProperties props = new TypedProperties();
    DeleteContext deleteContext = new DeleteContext(props, hoodieSchema).withReaderSchema(hoodieSchema);
    HoodieAvroIndexedRecord indexedRecord1 = new HoodieAvroIndexedRecord(record1);
    assertTrue(indexedRecord1.isDelete(deleteContext, props));

    GenericRecord record2 = new GenericRecordBuilder(hoodieSchema.toAvroSchema())
        .set("_hoodie_operation", HoodieOperation.INSERT.getName())
        .set("field2", "value2")
        .build();
    HoodieAvroIndexedRecord indexedRecord2 = new HoodieAvroIndexedRecord(record2);
    assertFalse(indexedRecord2.isDelete(deleteContext, props));
  }
}
