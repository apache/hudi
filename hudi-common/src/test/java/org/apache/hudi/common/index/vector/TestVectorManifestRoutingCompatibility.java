/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.index.vector;

import org.apache.hudi.avro.model.HoodieVectorIndexManifest;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TestVectorManifestRoutingCompatibility {

  @Test
  void legacyGenericManifestWithoutRoutingFieldsRemainsReadable() {
    Schema current = HoodieVectorIndexManifest.getClassSchema();
    Schema legacy = Schema.createRecord(current.getName(), current.getDoc(), current.getNamespace(), false);
    List<Schema.Field> fields = current.getFields().stream()
        .filter(field -> !field.name().startsWith("routing"))
        .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
        .collect(Collectors.toList());
    legacy.setFields(fields);
    GenericRecord record = new GenericData.Record(legacy);
    for (Schema.Field field : legacy.getFields()) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Schema.Type.UNION) {
        record.put(field.name(), null);
        continue;
      }
      switch (fieldSchema.getType()) {
        case INT:
          record.put(field.name(), 1);
          break;
        case LONG:
          record.put(field.name(), 1L);
          break;
        case BOOLEAN:
          record.put(field.name(), false);
          break;
        case DOUBLE:
          record.put(field.name(), 0.0d);
          break;
        case STRING:
          record.put(field.name(), field.name().equals("state") ? "ACTIVE" : "value");
          break;
        default:
          throw new IllegalStateException("Unexpected manifest field: " + field.name());
      }
    }

    HoodieVectorIndexManifest restored = VectorIndexMetadataSerde.asManifest(record);
    assertEquals("ACTIVE", restored.getState().toString());
    assertNull(restored.getRoutingVersion());
    assertNull(restored.getRoutingArtifactDigest());
    assertNull(restored.getRoutingCoarseCentroids());
  }
}
