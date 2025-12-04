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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createHoodieWriteSchema(null, true));

    // Should throw on empty schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createHoodieWriteSchema("", true));

    // Should throw on whitespace-only schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createHoodieWriteSchema("   ", true));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.addMetadataFields(null, true));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.removeMetadataFields(null));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.mergeSchemas(null, schema));

    // Should throw on null target schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.mergeSchemas(schema, null));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createNullableSchema(null));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.getNonNullTypeFromUnion(null));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.findMissingFields(null, schema));

    // Should throw on null writer schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.findMissingFields(schema, null));
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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createNewSchemaField(null, schema, "doc", null));

    // Should throw on empty name
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createNewSchemaField("", schema, "doc", null));

    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createNewSchemaField("name", null, "doc", null));
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "id");

    assertTrue(result.isPresent());
    assertEquals("id", result.get().getLeft());
    assertEquals("id", result.get().getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.get().getRight().schema().getType());
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "address.city");

    assertTrue(result.isPresent());
    assertEquals("address.city", result.get().getLeft());
    assertEquals("city", result.get().getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.get().getRight().schema().getType());
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(rootSchema, "user.profile.ts_millis");

    assertTrue(result.isPresent());
    assertEquals("user.profile.ts_millis", result.get().getLeft());
    assertEquals("ts_millis", result.get().getRight().name());
    assertEquals(HoodieSchemaType.TIMESTAMP, result.get().getRight().schema().getType());
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
    Option<Pair<String, HoodieSchemaField>> millisResult = HoodieSchemaUtils.getNestedField(schema, "ts_millis");
    assertTrue(millisResult.isPresent());
    assertEquals("ts_millis", millisResult.get().getLeft());
    assertEquals("ts_millis", millisResult.get().getRight().name());
    assertEquals(HoodieSchemaType.TIMESTAMP, millisResult.get().getRight().schema().getType());

    // Test timestamp-micros field
    Option<Pair<String, HoodieSchemaField>> microsResult = HoodieSchemaUtils.getNestedField(schema, "ts_micros");
    assertTrue(microsResult.isPresent());
    assertEquals("ts_micros", microsResult.get().getLeft());
    assertEquals("ts_micros", microsResult.get().getRight().name());
    assertEquals(HoodieSchemaType.TIMESTAMP, microsResult.get().getRight().schema().getType());

    // Test date field
    Option<Pair<String, HoodieSchemaField>> dateResult = HoodieSchemaUtils.getNestedField(schema, "date_field");
    assertTrue(dateResult.isPresent());
    assertEquals("date_field", dateResult.get().getLeft());
    assertEquals("date_field", dateResult.get().getRight().name());
    assertEquals(HoodieSchemaType.DATE, dateResult.get().getRight().schema().getType());
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "nonexistent");

    assertFalse(result.isPresent());
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "address.country");

    assertFalse(result.isPresent());

    // Test getting field from non-existent parent
    result = HoodieSchemaUtils.getNestedField(schema, "contact.email");

    assertFalse(result.isPresent());
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "count.nested");

    assertFalse(result.isPresent());
  }

  @Test
  public void testGetNestedFieldValidation() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Collections.singletonList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.getNestedField(null, "id"));

    // Should throw on null field name
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.getNestedField(schema, null));

    // Should throw on empty field name
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.getNestedField(schema, ""));
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
    Option<Pair<String, HoodieSchemaField>> result = HoodieSchemaUtils.getNestedField(schema, "address.street");

    assertTrue(result.isPresent());
    assertEquals("address.street", result.get().getLeft());
    assertEquals("street", result.get().getRight().name());
    assertEquals(HoodieSchemaType.STRING, result.get().getRight().schema().getType());
  }

  @Test
  public void testRemoveFields() {
    // Create schema with multiple fields
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        "org.apache.hudi.test",
        "Test record documentation",
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("email", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Remove some fields
    Set<String> fieldsToRemove = Collections.singleton("age");
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, fieldsToRemove);

    assertNotNull(resultSchema);
    assertEquals(HoodieSchemaType.RECORD, resultSchema.getType());

    // Should have 3 fields remaining (id, name, email)
    List<HoodieSchemaField> fields = resultSchema.getFields();
    assertEquals(3, fields.size());

    // Verify correct fields are present
    assertTrue(resultSchema.getField("id").isPresent());
    assertTrue(resultSchema.getField("name").isPresent());
    assertTrue(resultSchema.getField("email").isPresent());

    // Verify removed field is not present
    assertFalse(resultSchema.getField("age").isPresent());

    // Verify schema metadata is preserved
    assertEquals("TestRecord", resultSchema.getName());
    assertEquals("Test record documentation", resultSchema.getDoc().get());
    assertEquals("org.apache.hudi.test", resultSchema.getNamespace().get());
  }

  @Test
  public void testRemoveMultipleFields() {
    // Create schema with multiple fields
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("field3", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("field4", HoodieSchema.create(HoodieSchemaType.BOOLEAN)),
            HoodieSchemaField.of("field5", HoodieSchema.create(HoodieSchemaType.DOUBLE))
        )
    );

    // Remove multiple fields
    Set<String> fieldsToRemove = new HashSet<>(Arrays.asList("field2", "field4"));
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, fieldsToRemove);

    assertNotNull(resultSchema);

    // Should have 3 fields remaining (field1, field3, field5)
    List<HoodieSchemaField> fields = resultSchema.getFields();
    assertEquals(3, fields.size());

    // Verify correct fields are present
    assertTrue(resultSchema.getField("field1").isPresent());
    assertTrue(resultSchema.getField("field3").isPresent());
    assertTrue(resultSchema.getField("field5").isPresent());

    // Verify removed fields are not present
    assertFalse(resultSchema.getField("field2").isPresent());
    assertFalse(resultSchema.getField("field4").isPresent());
  }

  @Test
  public void testRemoveFieldsEmptySet() {
    // Create schema
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Remove no fields (empty set)
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, Collections.emptySet());

    assertNotNull(resultSchema);

    // Should have all original fields
    assertEquals(schema.getFields().size(), resultSchema.getFields().size());
    assertTrue(resultSchema.getField("id").isPresent());
    assertTrue(resultSchema.getField("name").isPresent());
  }

  @Test
  public void testRemoveFieldsNonExistentField() {
    // Create schema
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Try to remove non-existent field
    Set<String> fieldsToRemove = Collections.singleton("nonexistent");
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, fieldsToRemove);

    assertNotNull(resultSchema);

    // Should have all original fields
    assertEquals(schema.getFields().size(), resultSchema.getFields().size());
    assertTrue(resultSchema.getField("id").isPresent());
    assertTrue(resultSchema.getField("name").isPresent());
  }

  @Test
  public void testRemoveFieldsWithComplexTypes() {
    // Create schema with complex nested types
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
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("address", addressSchema),
            HoodieSchemaField.of("age", HoodieSchema.create(HoodieSchemaType.INT))
        )
    );

    // Remove the complex nested field
    Set<String> fieldsToRemove = Collections.singleton("address");
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, fieldsToRemove);

    assertNotNull(resultSchema);

    // Should have 3 fields remaining (id, name, age)
    List<HoodieSchemaField> fields = resultSchema.getFields();
    assertEquals(3, fields.size());

    // Verify complex field is removed
    assertFalse(resultSchema.getField("address").isPresent());

    // Verify other fields are present
    assertTrue(resultSchema.getField("id").isPresent());
    assertTrue(resultSchema.getField("name").isPresent());
    assertTrue(resultSchema.getField("age").isPresent());
  }

  @Test
  public void testRemoveFieldsPreservesFieldOrder() {
    // Create schema with fields in specific order
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Arrays.asList(
            HoodieSchemaField.of("field1", HoodieSchema.create(HoodieSchemaType.STRING)),
            HoodieSchemaField.of("field2", HoodieSchema.create(HoodieSchemaType.INT)),
            HoodieSchemaField.of("field3", HoodieSchema.create(HoodieSchemaType.LONG)),
            HoodieSchemaField.of("field4", HoodieSchema.create(HoodieSchemaType.BOOLEAN))
        )
    );

    // Remove middle field
    Set<String> fieldsToRemove = Collections.singleton("field2");
    HoodieSchema resultSchema = HoodieSchemaUtils.removeFields(schema, fieldsToRemove);

    assertNotNull(resultSchema);

    // Verify field order is preserved (field1, field3, field4)
    List<HoodieSchemaField> fields = resultSchema.getFields();
    assertEquals(3, fields.size());
    assertEquals("field1", fields.get(0).name());
    assertEquals("field3", fields.get(1).name());
    assertEquals("field4", fields.get(2).name());
  }

  @Test
  public void testRemoveFieldsValidation() {
    HoodieSchema schema = HoodieSchema.createRecord(
        "TestRecord",
        null,
        null,
        Collections.singletonList(
            HoodieSchemaField.of("id", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    // Should throw on null schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.removeFields(null, Collections.emptySet()));

    // Should throw on null fieldsToRemove
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.removeFields(schema, null));
  }

  @Test
  public void testRemoveFieldsConsistencyWithAvro() {
    // Test that HoodieSchemaUtils.removeFields produces equivalent results to HoodieAvroUtils.removeFields
    String schemaString = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":["
        + "{\"name\":\"id\",\"type\":\"string\"},"
        + "{\"name\":\"name\",\"type\":\"string\"},"
        + "{\"name\":\"age\",\"type\":\"int\"},"
        + "{\"name\":\"email\",\"type\":\"string\"}"
        + "]}";

    Schema avroSchema = new Schema.Parser().parse(schemaString);
    HoodieSchema hoodieSchema = HoodieSchema.parse(schemaString);

    Set<String> fieldsToRemove = new HashSet<>(Arrays.asList("age", "email"));

    // Apply removeFields using both approaches
    Schema avroResult = HoodieAvroUtils.removeFields(avroSchema, fieldsToRemove);
    HoodieSchema hoodieResult = HoodieSchemaUtils.removeFields(hoodieSchema, fieldsToRemove);

    // Should produce equivalent schemas
    assertEquals(avroResult.toString(), hoodieResult.toString());
  }
}
