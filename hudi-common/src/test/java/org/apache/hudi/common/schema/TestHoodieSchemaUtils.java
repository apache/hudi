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

package org.apache.hudi.common.schema;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link HoodieSchemaUtils}.
 */
public class TestHoodieSchemaUtils {

  static final String SIMPLE_SCHEMA = "{"
      + "\"type\":\"record\","
      + "\"name\":\"TestRecord\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"}"
      + "]}";

  static final String EVOLVED_SCHEMA = "{"
      + "\"type\":\"record\","
      + "\"name\":\"TestRecord\","
      + "\"fields\":["
      + "{\"name\":\"id\",\"type\":\"string\"},"
      + "{\"name\":\"name\",\"type\":\"string\"},"
      + "{\"name\":\"age\",\"type\":[\"null\",\"int\"],\"default\":null}"
      + "]}";

  @Test
  public void testCreateHoodieWriteSchema() {
    // Test with operation field
    HoodieSchema writeSchemaWithOp = HoodieSchemaUtils.createHoodieWriteSchema(SIMPLE_SCHEMA, true);

    assertNotNull(writeSchemaWithOp);
    assertEquals(HoodieSchemaType.RECORD, writeSchemaWithOp.getType());

    // Should have original fields plus metadata fields
    List<HoodieSchemaField> fields = writeSchemaWithOp.getFields();
    assertEquals(8, fields.size()); // Original 2 fields + metadata fields

    // Test without operation field
    HoodieSchema writeSchemaNoOp = HoodieSchemaUtils.createHoodieWriteSchema(SIMPLE_SCHEMA, false);

    assertNotNull(writeSchemaNoOp);
    assertEquals(HoodieSchemaType.RECORD, writeSchemaNoOp.getType());

    // Should have different number of fields
    assertEquals(7, writeSchemaNoOp.getFields().size());
  }

  @Test
  public void testCreateHoodieWriteSchemaValidation() {
    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createHoodieWriteSchema(null, true);
    });

    // Should throw on empty schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createHoodieWriteSchema("", true);
    });

    // Should throw on whitespace-only schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createHoodieWriteSchema("   ", true);
    });
  }

  @Test
  public void testAddMetadataFields() {
    HoodieSchema baseSchema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Test adding metadata fields with operation field
    HoodieSchema schemaWithMeta = HoodieSchemaUtils.addMetadataFields(baseSchema, true);

    assertNotNull(schemaWithMeta);
    assertEquals(schemaWithMeta.getFields().size(), baseSchema.getFields().size() + 6);

    // Test adding metadata fields without operation field  
    HoodieSchema schemaWithMetaNoOp = HoodieSchemaUtils.addMetadataFields(baseSchema, false);

    assertNotNull(schemaWithMetaNoOp);
    assertEquals(schemaWithMetaNoOp.getFields().size(), baseSchema.getFields().size() + 5);
  }

  @Test
  public void testAddMetadataFieldsValidation() {
    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.addMetadataFields(null, true);
    });
  }

  @Test
  public void testRemoveMetadataFields() {
    HoodieSchema baseSchema = HoodieSchema.parse(SIMPLE_SCHEMA);
    HoodieSchema schemaWithMeta = HoodieSchemaUtils.addMetadataFields(baseSchema, true);

    // Remove metadata fields
    HoodieSchema schemaWithoutMeta = HoodieSchemaUtils.removeMetadataFields(schemaWithMeta);

    assertNotNull(schemaWithoutMeta);
    // Should have fewer fields after removing metadata
    assertTrue(schemaWithoutMeta.getFields().size() < schemaWithMeta.getFields().size());
  }

  @Test
  public void testRemoveMetadataFieldsValidation() {
    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.removeMetadataFields(null);
    });
  }

  @Test
  public void testMergeSchemas() {
    HoodieSchema baseSchema = HoodieSchema.parse(SIMPLE_SCHEMA);
    HoodieSchema evolvedSchema = HoodieSchema.parse(EVOLVED_SCHEMA);

    // Merge schemas
    HoodieSchema mergedSchema = HoodieSchemaUtils.mergeSchemas(baseSchema, evolvedSchema);

    assertNotNull(mergedSchema);
    assertEquals(HoodieSchemaType.RECORD, mergedSchema.getType());

    // Merged schema should have fields from both schemas
    List<HoodieSchemaField> mergedFields = mergedSchema.getFields();
    assertTrue(mergedFields.size() >= evolvedSchema.getFields().size());
  }

  @Test
  public void testMergeSchemasValidation() {
    HoodieSchema schema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Should throw on null source schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.mergeSchemas(null, schema);
    });

    // Should throw on null target schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.mergeSchemas(schema, null);
    });
  }

  @Test
  public void testCreateNullableSchema() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Create nullable version
    HoodieSchema nullableSchema = HoodieSchemaUtils.createNullableSchema(stringSchema);

    assertNotNull(nullableSchema);
    assertEquals(HoodieSchemaType.UNION, nullableSchema.getType());
    assertTrue(nullableSchema.isNullable());
  }

  @Test
  public void testCreateNullableSchemaValidation() {
    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createNullableSchema(null);
    });
  }

  @Test
  public void testGetNonNullTypeFromUnion() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema nullableSchema = HoodieSchemaUtils.createNullableSchema(stringSchema);

    // Extract non-null type
    HoodieSchema nonNullType = HoodieSchemaUtils.getNonNullTypeFromUnion(nullableSchema);

    assertNotNull(nonNullType);
    assertEquals(HoodieSchemaType.STRING, nonNullType.getType());
  }

  @Test
  public void testGetNonNullTypeFromUnionValidation() {
    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.getNonNullTypeFromUnion(null);
    });
  }

  @Test
  public void testFindMissingFields() {
    HoodieSchema fullSchema = HoodieSchema.parse(EVOLVED_SCHEMA);
    HoodieSchema partialSchema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Find missing fields
    List<HoodieSchemaField> missingFields = HoodieSchemaUtils.findMissingFields(fullSchema, partialSchema);

    assertNotNull(missingFields);
    // Should find the 'age' field that's missing in partial schema
    assertEquals(1, missingFields.size());
    assertEquals("age", missingFields.get(0).name());
  }

  @Test
  public void testFindMissingFieldsWithExclusion() {
    HoodieSchema fullSchema = HoodieSchema.parse(EVOLVED_SCHEMA);
    HoodieSchema partialSchema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Find missing fields, excluding 'age' from check
    List<HoodieSchemaField> missingFields = HoodieSchemaUtils.findMissingFields(
        fullSchema, partialSchema, Collections.singleton("age"));

    assertNotNull(missingFields);
    // Should find no missing fields since 'age' is excluded
    assertEquals(0, missingFields.size());
  }

  @Test
  public void testFindMissingFieldsValidation() {
    HoodieSchema schema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Should throw on null table schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.findMissingFields(null, schema);
    });

    // Should throw on null writer schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.findMissingFields(schema, null);
    });
  }

  @Test
  public void testCreateNewSchemaField() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Create new field
    HoodieSchemaField newField = HoodieSchemaUtils.createNewSchemaField(
        "testField", stringSchema, "Test field documentation", "defaultValue");

    assertNotNull(newField);
    assertEquals("testField", newField.name());
    assertEquals(HoodieSchemaType.STRING, newField.schema().getType());
    assertEquals("Test field documentation", newField.doc().get());
    assertTrue(newField.hasDefaultValue());
    assertEquals("defaultValue", newField.defaultVal().get());
  }

  @Test
  public void testCreateNewSchemaFieldValidation() {
    HoodieSchema schema = HoodieSchema.create(HoodieSchemaType.STRING);

    // Should throw on null name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createNewSchemaField(null, schema, "doc", null);
    });

    // Should throw on empty name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createNewSchemaField("", schema, "doc", null);
    });

    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.createNewSchemaField("name", null, "doc", null);
    });
  }

  @Test
  public void testConsistencyWithAvroUtilities() {
    // Test that HoodieSchemaUtils produces equivalent results to AvroSchemaUtils
    String schemaString = SIMPLE_SCHEMA;

    // Compare createHoodieWriteSchema results
    Schema avroResult = HoodieAvroUtils.createHoodieWriteSchema(schemaString, true);
    HoodieSchema hoodieResult = HoodieSchemaUtils.createHoodieWriteSchema(schemaString, true);

    // Should produce equivalent schemas
    assertEquals(avroResult.toString(), hoodieResult.toString());
  }

  @Test
  public void testGetNestedFieldTopLevel() {
    // Create simple schema
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Test getting top-level field
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "id");

    assertNotNull(result);
    assertEquals("id", result.getLeft());
    assertEquals("id", result.getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.getRight().schema().getType());
  }

  @Test
  public void testGetNestedFieldSingleLevel() {
    // Create schema with nested record
    HoodieSchema addressSchema = HoodieSchema.createRecord(
        "Address",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("street", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    HoodieSchema schema = HoodieSchema.createRecord(
        "Person",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("address", addressSchema)
        )
    );

    // Test getting nested field
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "address.city");

    assertNotNull(result);
    assertEquals("address.city", result.getLeft());
    assertEquals("city", result.getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.getRight().schema().getType());
  }

  @Test
  public void testGetNestedFieldMultiLevel() {
    // Create 3-level nested schema
    HoodieSchema profileSchema = HoodieSchema.createRecord(
        "Profile",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("bio", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("ts_millis", HoodieSchema.createTimestampMillis())
        )
    );

    HoodieSchema userSchema = HoodieSchema.createRecord(
        "User",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("profile", profileSchema),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))
        )
    );

    HoodieSchema rootSchema = HoodieSchema.createRecord(
        "Root",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("user", userSchema),
            HoodieSchemaField.of("event_id", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Test getting deeply nested field
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(rootSchema, "user.profile.ts_millis");

    assertNotNull(result);
    assertEquals("user.profile.ts_millis", result.getLeft());
    assertEquals("ts_millis", result.getRight().name());
    assertEquals(HoodieSchemaType.LONG, result.getRight().schema().getType());
  }

  @Test
  public void testGetNestedFieldWithTimestampTypes() {
    // Create schema with different timestamp types
    HoodieSchema schema = HoodieSchema.createRecord(
        "TimestampRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("ts_millis", HoodieSchema.createTimestampMillis()),
            HoodieSchemaField.of("ts_micros", HoodieSchema.createTimestampMicros()),
            HoodieSchemaField.of("date_field", HoodieSchema.createDate())
        )
    );

    // Test timestamp-millis field
    Pair<String, HoodieSchemaField> millisResult = HoodieSchemaUtils.getNestedField(schema, "ts_millis");
    assertNotNull(millisResult);
    assertEquals("ts_millis", millisResult.getRight().name());
    assertEquals(HoodieSchemaType.LONG, millisResult.getRight().schema().getType());

    // Test timestamp-micros field
    Pair<String, HoodieSchemaField> microsResult = HoodieSchemaUtils.getNestedField(schema, "ts_micros");
    assertNotNull(microsResult);
    assertEquals("ts_micros", microsResult.getRight().name());
    assertEquals(HoodieSchemaType.LONG, microsResult.getRight().schema().getType());

    // Test date field
    Pair<String, HoodieSchemaField> dateResult = HoodieSchemaUtils.getNestedField(schema, "date_field");
    assertNotNull(dateResult);
    assertEquals("date_field", dateResult.getRight().name());
    assertEquals(HoodieSchemaType.INT, dateResult.getRight().schema().getType());
  }

  @Test
  public void testGetNestedFieldNonExistent() {
    // Create simple schema
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Test getting non-existent field
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "nonexistent");

    assertNull(result);
  }

  @Test
  public void testGetNestedFieldNonExistentNestedPath() {
    // Create schema with nested record
    HoodieSchema addressSchema = HoodieSchema.createRecord(
        "Address",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("street", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("city", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    HoodieSchema schema = HoodieSchema.createRecord(
        "Person",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("address", addressSchema)
        )
    );

    // Test getting non-existent nested field path
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "address.country");

    assertNull(result);

    // Test getting field from non-existent parent
    result = HoodieSchemaUtils.getNestedField(schema, "contact.email");

    assertNull(result);
  }

  @Test
  public void testGetNestedFieldInvalidPathNonRecordType() {
    // Create schema where field is not a record type
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("count", HoodieSchema.create(HoodieSchemaType.INT))
        )
    );

    // Test trying to get nested field from non-record type
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "count.nested");

    assertNull(result);
  }

  @Test
  public void testGetNestedFieldValidation() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.getNestedField(null, "id");
    });

    // Should throw on null field name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.getNestedField(schema, null);
    });

    // Should throw on empty field name
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaUtils.getNestedField(schema, "");
    });
  }

  @Test
  public void testGetNestedFieldWithNullableTypes() {
    // Create schema with nullable nested record
    HoodieSchema addressSchema = HoodieSchema.createRecord(
        "Address",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("street", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("zipcode", HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING)))
        )
    );

    HoodieSchema schema = HoodieSchema.createRecord(
        "Person",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("address", HoodieSchema.createNullable(addressSchema))
        )
    );

    // Test getting nested field through nullable parent
    Pair<String, HoodieSchemaField> result = HoodieSchemaUtils.getNestedField(schema, "address.street");

    assertNotNull(result);
    assertEquals("address.street", result.getLeft());
    assertEquals("street", result.getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.getRight().schema().getType());
  }
}
