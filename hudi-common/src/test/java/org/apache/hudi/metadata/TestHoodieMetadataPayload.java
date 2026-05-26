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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.common.model.HoodieRecord;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.hudi.metadata.HoodieTableMetadata.RECORDKEY_PARTITION_LIST;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestHoodieMetadataPayload {

  @Test
  void testGetInsertValueAcceptsEquivalentMetadataSchemaInstance() throws Exception {
    HoodieRecord<HoodieMetadataPayload> record =
        HoodieMetadataPayload.createPartitionListRecord(Arrays.asList("p1", "p2"));

    Schema equivalentSchema = new Schema.Parser().parse(HoodieMetadataRecord.getClassSchema().toString());
    IndexedRecord insertValue = record.getData().getInsertValue(equivalentSchema, null).get();

    assertTrue(insertValue instanceof HoodieMetadataRecord);
    HoodieMetadataRecord metadataRecord = (HoodieMetadataRecord) insertValue;
    assertEquals(RECORDKEY_PARTITION_LIST, metadataRecord.getKey());
    assertNotNull(metadataRecord.getFilesystemMetadata());
    assertEquals(2, metadataRecord.getFilesystemMetadata().size());
  }

  @Test
  void testGetInsertValueSupportsOlderMetadataSchemaWithoutVectorField() throws Exception {
    HoodieRecord<HoodieMetadataPayload> record =
        HoodieMetadataPayload.createPartitionListRecord(Arrays.asList("p1", "p2"));

    Schema oldSchema = createMetadataSchemaWithoutField(HoodieMetadataPayload.SCHEMA_FIELD_ID_VECTOR_INDEX);
    IndexedRecord insertValue = record.getData().getInsertValue(oldSchema, null).get();

    assertTrue(insertValue instanceof GenericRecord);
    GenericRecord genericRecord = (GenericRecord) insertValue;
    assertEquals(RECORDKEY_PARTITION_LIST, String.valueOf(genericRecord.get(HoodieMetadataPayload.KEY_FIELD_NAME)));
    assertNotNull(genericRecord.get(HoodieMetadataPayload.SCHEMA_FIELD_NAME_METADATA));
  }

  private static Schema createMetadataSchemaWithoutField(String fieldToDrop) {
    Schema currentSchema = HoodieMetadataRecord.getClassSchema();
    List<Schema.Field> fields = new ArrayList<>();
    for (Schema.Field field : currentSchema.getFields()) {
      if (!field.name().equals(fieldToDrop)) {
        fields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()));
      }
    }
    return Schema.createRecord(currentSchema.getName(), currentSchema.getDoc(), currentSchema.getNamespace(), false, fields);
  }
}
