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

package org.apache.hudi.common.testutils;

import org.apache.hudi.common.model.HoodieKey;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Validates that {@link HoodieTestDataGenerator} exposes the unstructured-types schema as a
 * forward-looking opt-in (via {@code TRIP_EXAMPLE_SCHEMA} / {@code AVRO_SCHEMA_WITH_UNSTRUCTURED}),
 * while {@code AVRO_SCHEMA} and the no-arg constructor stay on the legacy
 * {@code TRIP_EXAMPLE_SCHEMA_NO_UNSTRUCTURED} until tests are migrated by follow-up PRs.
 */
class TestHoodieTestDataGenerator {

  @Test
  void defaultAvroSchemaIsLegacyAndOmitsUnstructuredTypes() {
    Schema schema = HoodieTestDataGenerator.AVRO_SCHEMA;
    assertNull(schema.getField("variant_data"));
    assertNull(schema.getField("embedding"));
    assertNull(schema.getField("blob_data"));
  }

  @Test
  void unstructuredAvroSchemaCarriesAllThreeLogicalTypes() {
    Schema schema = HoodieTestDataGenerator.AVRO_SCHEMA_WITH_UNSTRUCTURED;

    Schema variantField = unwrap(schema.getField("variant_data").schema());
    assertEquals(Schema.Type.RECORD, variantField.getType());
    assertEquals("variant", variantField.getLogicalType().getName());

    Schema vectorField = unwrap(schema.getField("embedding").schema());
    assertEquals(Schema.Type.FIXED, vectorField.getType());
    assertEquals("vector", vectorField.getLogicalType().getName());

    Schema blobField = unwrap(schema.getField("blob_data").schema());
    assertEquals(Schema.Type.RECORD, blobField.getType());
    assertEquals("blob", blobField.getLogicalType().getName());
  }

  @Test
  void defaultRecordsKeepLegacyShape() {
    HoodieTestDataGenerator gen = new HoodieTestDataGenerator(0L);
    HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), HoodieTestDataGenerator.DEFAULT_FIRST_PARTITION_PATH);
    GenericRecord rec = (GenericRecord) gen.generateRandomValue(key, "001");

    assertNotNull(rec.get("fare"));
    assertNull(rec.getSchema().getField("variant_data"));
    assertNull(rec.getSchema().getField("embedding"));
    assertNull(rec.getSchema().getField("blob_data"));
  }

  private static Schema unwrap(Schema s) {
    if (s.getType() != Schema.Type.UNION) {
      return s;
    }
    for (Schema branch : s.getTypes()) {
      if (branch.getType() != Schema.Type.NULL) {
        return branch;
      }
    }
    throw new IllegalStateException("union has no non-null branch: " + s);
  }
}
