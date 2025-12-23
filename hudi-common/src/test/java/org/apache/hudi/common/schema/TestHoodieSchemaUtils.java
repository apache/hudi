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

import org.apache.hudi.avro.AvroSchemaUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.internal.schema.HoodieSchemaException;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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

  private static final String EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"testrec\",\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
      + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"}]}";

  private static final String SCHEMA_WITH_AVRO_TYPES_STR = "{\"name\":\"TestRecordAvroTypes\",\"type\":\"record\",\"fields\":["
      // Primitive types
      + "{\"name\":\"booleanField\",\"type\":\"boolean\"},"
      + "{\"name\":\"intField\",\"type\":\"int\"},"
      + "{\"name\":\"longField\",\"type\":\"long\"},"
      + "{\"name\":\"floatField\",\"type\":\"float\"},"
      + "{\"name\":\"doubleField\",\"type\":\"double\"},"
      + "{\"name\":\"bytesField\",\"type\":\"bytes\"},"
      + "{\"name\":\"stringField\",\"type\":\"string\"},"
      + "{\"name\":\"secondLevelField\",\"type\":[\"null\", {\"name\":\"secondLevelField\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\",\"string\"],\"default\":null},"
      + "{\"name\":\"lastname\",\"type\":[\"null\",\"string\"],\"default\":null}"
      + "]}],\"default\":null},"
      // Logical types
      + "{\"name\":\"decimalField\",\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":20,\"scale\":5},"
      + "{\"name\":\"timeMillisField\",\"type\":\"int\",\"logicalType\":\"time-millis\"},"
      + "{\"name\":\"timeMicrosField\",\"type\":\"long\",\"logicalType\":\"time-micros\"},"
      + "{\"name\":\"timestampMillisField\",\"type\":\"long\",\"logicalType\":\"timestamp-millis\"},"
      + "{\"name\":\"timestampMicrosField\",\"type\":\"long\",\"logicalType\":\"timestamp-micros\"},"
      + "{\"name\":\"localTimestampMillisField\",\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"},"
      + "{\"name\":\"localTimestampMicrosField\",\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}"
      + "]}";

  private static final String SCHEMA_WITH_NON_NULLABLE_FIELD =
      "{\"type\": \"record\",\"name\": \"testrec3\",\"fields\": [ "
          + "{\"name\": \"timestamp\",\"type\": \"double\"},{\"name\": \"_row_key\", \"type\": \"string\"},"
          + "{\"name\": \"non_pii_col\", \"type\": \"string\"},"
          + "{\"name\": \"pii_col\", \"type\": \"string\", \"column_category\": \"user_profile\"},"
          + "{\"name\": \"nullable_field\",\"type\": [\"null\" ,\"string\"],\"default\": null},"
          + "{\"name\": \"non_nullable_field_wo_default\",\"type\": \"string\"},"
          + "{\"name\": \"non_nullable_field_with_default\",\"type\": \"string\", \"default\": \"dummy\"}]}";

  private static String SCHEMA_WITH_NESTED_FIELD_LARGE_STR = "{\"name\":\"MyClass\",\"type\":\"record\",\"namespace\":\"com.acme.avro\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":\"string\"},"
      + "{\"name\":\"lastname\",\"type\":\"string\"},"
      + "{\"name\":\"nested_field\",\"type\":[\"null\"," + SCHEMA_WITH_AVRO_TYPES_STR + "],\"default\":null},"
      + "{\"name\":\"student\",\"type\":{\"name\":\"student\",\"type\":\"record\",\"fields\":["
      + "{\"name\":\"firstname\",\"type\":[\"null\" ,\"string\"],\"default\": null},{\"name\":\"lastname\",\"type\":[\"null\" ,\"string\"],\"default\": null}]}}]}";

  private static final String SCHEMA_WITH_DECIMAL_FIELD = "{\"type\":\"record\",\"name\":\"record\",\"fields\":["
      + "{\"name\":\"key_col\",\"type\":[\"null\",\"int\"],\"default\":null},"
      + "{\"name\":\"decimal_col\",\"type\":[\"null\","
      + "{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":8,\"scale\":4}],\"default\":null}]}";

  private static HoodieSchema SCHEMA_WITH_NESTED_FIELD_LARGE = HoodieSchema.parse(SCHEMA_WITH_NESTED_FIELD_LARGE_STR);

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
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createHoodieWriteSchema((String) null, true));

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
    HoodieSchema schemaWithMetaNoOp = HoodieSchemaUtils.addMetadataFields(baseSchema);

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

    // Null fieldsToRemove should return original schema unchanged
    assertSame(schema, HoodieSchemaUtils.removeFields(schema, null));
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

  @Test
  void testAreSchemasProjectionEquivalentRecordSchemas() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFieldCountInRecords() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[]}");
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNestedRecordSchemas() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"Outer1\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner1\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"Outer2\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner2\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentArraySchemas() {
    HoodieSchema s1 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema s2 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentElementTypeInArray() {
    HoodieSchema s1 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema s2 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapSchemas() {
    HoodieSchema s1 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema s2 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentMapValueTypes() {
    HoodieSchema s1 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema s2 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullableSchemaComparison() {
    HoodieSchema s1 = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT));
    HoodieSchema s2 = HoodieSchema.create(HoodieSchemaType.INT);
    s2.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentListVsString() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema listSchema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(listSchema, stringSchema));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(stringSchema, listSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapVsString() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema mapSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(mapSchema, stringSchema));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(stringSchema, mapSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualFixedSchemas() {
    HoodieSchema s1 = HoodieSchema.createFixed("F", null, null, 16);
    HoodieSchema s2 = HoodieSchema.createFixed("F", null, null, 16);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFixedSize() {
    HoodieSchema s1 = HoodieSchema.createFixed("F", null, null, 8);
    HoodieSchema s2 = HoodieSchema.createFixed("F", null, null, 4);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnums() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentEnumSymbols() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("X", "Y"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B"));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnumSymbolSubset() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s2, s1));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualDecimalLogicalTypes() {
    HoodieSchema s1 = HoodieSchema.createDecimal(12, 2);
    HoodieSchema s2 = HoodieSchema.createDecimal(12, 2);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true

    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentPrecision() {
    HoodieSchema s1 = HoodieSchema.createDecimal(12, 2);
    HoodieSchema s2 = HoodieSchema.createDecimal(13, 2);
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentLogicalVsNoLogicalType() {
    HoodieSchema s1 = HoodieSchema.createDecimal(10, 2);
    HoodieSchema s2 = HoodieSchema.create(HoodieSchemaType.BYTES);

    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentSameReferenceSchema() {
    HoodieSchema s = HoodieSchema.create(HoodieSchemaType.STRING);
    assertTrue(HoodieSchemaUtils.areSchemasProjectionEquivalent(s, s));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullSchemaComparison() {
    HoodieSchema s = HoodieSchema.create(HoodieSchemaType.STRING);
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(null, s));
    assertFalse(HoodieSchemaUtils.areSchemasProjectionEquivalent(s, null));
  }

  @Test
  void testPruneRecordFields() {
    String dataSchemaStr = "{ \"type\": \"record\", \"name\": \"Person\", \"fields\": ["
        + "{ \"name\": \"name\", \"type\": \"string\" },"
        + "{ \"name\": \"age\", \"type\": \"int\" },"
        + "{ \"name\": \"email\", \"type\": [\"null\", \"string\"], \"default\": null }"
        + "]}";

    String requiredSchemaStr = "{ \"type\": \"record\", \"name\": \"Person\", \"fields\": ["
        + "{ \"name\": \"name\", \"type\": \"string\" }"
        + "]}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredSchemaStr);

    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());

    assertEquals(1, pruned.getFields().size());
    assertEquals("name", pruned.getFields().get(0).name());
  }

  @Test
  void testPruningPreserveNullable() {
    String dataSchemaStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"Container\","
        + "\"fields\": ["
        + "  {"
        + "    \"name\": \"foo\","
        + "    \"type\": [\"null\", {"
        + "      \"type\": \"record\","
        + "      \"name\": \"Foo\","
        + "      \"fields\": ["
        + "        {\"name\": \"field1\", \"type\": \"string\"},"
        + "        {\"name\": \"field2\", \"type\": \"int\"}"
        + "      ]"
        + "    }],"
        + "    \"default\": null"
        + "  }"
        + "]"
        + "}";

    String requiredFooStr = "{"
        + "\"type\": \"record\","
        + "\"name\": \"Foo\","
        + "\"fields\": ["
        + "  {\"name\": \"field1\", \"type\": \"string\"}"
        + "]"
        + "}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredFooStr);

    HoodieSchema fooFieldSchema = dataSchema.getField("foo").get().schema();
    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(fooFieldSchema, requiredSchema, Collections.emptySet());

    assertEquals(HoodieSchemaType.UNION, pruned.getType());

    HoodieSchema prunedRecord = pruned.getTypes().stream()
        .filter(s -> s.getType() == HoodieSchemaType.RECORD)
        .collect(Collectors.toList()).get(0);
    assertTrue(prunedRecord.getField("field1").isPresent());
    assertTrue(prunedRecord.getField("field2").isEmpty());
  }

  @Test
  void testArrayElementPruning() {
    String dataSchemaStr = "{ \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\": \"Item\", \"fields\": ["
        + "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"string\"}"
        + "]}}";

    String requiredSchemaStr = "{ \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\": \"Item\", \"fields\": ["
        + "{\"name\": \"b\", \"type\": \"string\"}"
        + "]}}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredSchemaStr);

    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());
    HoodieSchema itemSchema = pruned.getElementType();

    assertEquals(1, itemSchema.getFields().size());
    assertEquals("b", itemSchema.getFields().get(0).name());
  }

  @Test
  void testMapValuePruning() {
    String dataSchemaStr = "{ \"type\": \"map\", \"values\": { \"type\": \"record\", \"name\": \"Entry\", \"fields\": ["
        + "{\"name\": \"x\", \"type\": \"int\"}, {\"name\": \"y\", \"type\": \"string\"}"
        + "]}}";

    String requiredSchemaStr = "{ \"type\": \"map\", \"values\": { \"type\": \"record\", \"name\": \"Entry\", \"fields\": ["
        + "{\"name\": \"y\", \"type\": \"string\"}"
        + "]}}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredSchemaStr);

    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());
    HoodieSchema valueSchema = pruned.getValueType();

    assertEquals(1, valueSchema.getFields().size());
    assertEquals("y", valueSchema.getFields().get(0).name());
  }

  @Test
  void testPruningExcludedFieldIsPreservedIfMissingInDataSchema() {
    String dataSchemaStr = "{ \"type\": \"record\", \"name\": \"Rec\", \"fields\": ["
        + "{\"name\": \"existing\", \"type\": \"int\"}"
        + "]}";

    String requiredSchemaStr = "{ \"type\": \"record\", \"name\": \"Rec\", \"fields\": ["
        + "{\"name\": \"existing\", \"type\": \"int\"},"
        + "{\"name\": \"missing\", \"type\": \"string\", \"default\": \"default\"}"
        + "]}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredSchemaStr);

    Set<String> mandatoryFields = Collections.singleton("missing");

    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, mandatoryFields);

    assertEquals(2, pruned.getFields().size());
    assertTrue(pruned.getField("missing").isPresent());
    assertEquals(HoodieSchemaType.STRING, pruned.getField("missing").get().schema().getType());
  }

  @Test
  void testPruningMandatoryFieldsOnlyApplyToTopLevel() {
    String dataSchemaStr = "{ \"type\": \"record\", \"name\": \"Rec\", \"fields\": ["
        + "{\"name\": \"existing\", \"type\": \"int\"},"
        + "{\"name\": \"nestedRecord\", \"type\": {"
        + "  \"type\": \"record\", \"name\": \"NestedRec\", \"fields\": ["
        + "    {\"name\": \"nestedField\", \"type\": \"string\"}"
        + "  ]"
        + "}}"
        + "]}";

    String requiredSchemaStr = "{ \"type\": \"record\", \"name\": \"Rec\", \"fields\": ["
        + "{\"name\": \"existing\", \"type\": \"int\"},"
        + "{\"name\": \"topLevelMissing\", \"type\": \"string\", \"default\": \"default\"},"
        + "{\"name\": \"nestedRecord\", \"type\": {"
        + "  \"type\": \"record\", \"name\": \"NestedRec\", \"fields\": ["
        + "    {\"name\": \"nestedField\", \"type\": \"string\"},"
        + "    {\"name\": \"nestedMissing\", \"type\": \"int\", \"default\": 0}"
        + "  ]"
        + "}}"
        + "]}";

    HoodieSchema dataSchema = HoodieSchema.parse(dataSchemaStr);
    HoodieSchema requiredSchema = HoodieSchema.parse(requiredSchemaStr);

    // Both "topLevelMissing" and "nestedMissing" are in mandatory fields
    // but only "topLevelMissing" should be preserved since mandatory fields
    // only apply to top-level fields
    Set<String> mandatoryFields = new HashSet<>(Arrays.asList("topLevelMissing", "nestedMissing"));

    HoodieSchema pruned = HoodieSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, mandatoryFields);

    // Should have 3 top-level fields: existing, topLevelMissing, nestedRecord
    assertEquals(3, pruned.getFields().size());

    // Top-level mandatory field should be preserved even though missing from data schema
    assertTrue(pruned.getField("topLevelMissing").isPresent());
    assertEquals(HoodieSchemaType.STRING, pruned.getField("topLevelMissing").get().schema().getType());

    // Nested record should exist
    assertTrue(pruned.getField("nestedRecord").isPresent());
    HoodieSchema nestedSchema = pruned.getField("nestedRecord").get().schema();

    // Nested record should only have 1 field (nestedField) - nestedMissing should NOT be preserved
    // because mandatory fields only apply to top-level
    assertEquals(1, nestedSchema.getFields().size());
    assertTrue(nestedSchema.getField("nestedField").isPresent());
    assertTrue(nestedSchema.getField("nestedMissing").isEmpty()); // This should be empty - not preserved
  }

  @Test
  public void testGenerateProjectionSchema() {
    HoodieSchema originalSchema = HoodieSchemaUtils.addMetadataFields(HoodieSchema.parse(EXAMPLE_SCHEMA));

    HoodieSchema schema1 = HoodieSchemaUtils.generateProjectionSchema(originalSchema, Arrays.asList("_row_key", "timestamp"));
    assertEquals(2, schema1.getFields().size());
    List<String> fieldNames1 = schema1.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.toList());
    assertTrue(fieldNames1.contains("_row_key"));
    assertTrue(fieldNames1.contains("timestamp"));

    Throwable caughtException = assertThrows(HoodieException.class, () ->
        HoodieSchemaUtils.generateProjectionSchema(originalSchema, Arrays.asList("_row_key", "timestamp", "fake_field")));
    assertTrue(caughtException.getMessage().contains("Field fake_field not found in log schema. Query cannot proceed!"));
  }

  @Test
  public void testAppendFieldsToSchemaDedupNested() {
    HoodieSchema fullSchema = HoodieSchema.parse("{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"example.schema\",\n"
        + "  \"name\": \"source\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"number\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"nested_record\",\n"
        + "      \"type\": {\n"
        + "        \"name\": \"nested\",\n"
        + "        \"type\": \"record\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"string\",\n"
        + "            \"type\": [\"null\", \"string\"]\n"
        + "          },\n"
        + "          {\n"
        + "            \"name\": \"long\",\n"
        + "            \"type\": [\"null\", \"long\"]\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"other\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n");

    HoodieSchema missingFieldSchema = HoodieSchema.parse("{\n"
        + "  \"type\": \"record\",\n"
        + "  \"namespace\": \"example.schema\",\n"
        + "  \"name\": \"source\",\n"
        + "  \"fields\": [\n"
        + "    {\n"
        + "      \"name\": \"number\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"nested_record\",\n"
        + "      \"type\": {\n"
        + "        \"name\": \"nested\",\n"
        + "        \"type\": \"record\",\n"
        + "        \"fields\": [\n"
        + "          {\n"
        + "            \"name\": \"string\",\n"
        + "            \"type\": [\"null\", \"string\"]\n"
        + "          }\n"
        + "        ]\n"
        + "      }\n"
        + "    },\n"
        + "    {\n"
        + "      \"name\": \"other\",\n"
        + "      \"type\": [\"null\", \"int\"]\n"
        + "    }\n"
        + "  ]\n"
        + "}\n");

    Option<HoodieSchemaField> missingField = HoodieSchemaUtils.findNestedField(fullSchema, "nested_record.long");
    assertTrue(missingField.isPresent());
    HoodieSchema actual = HoodieSchemaUtils.appendFieldsToSchemaDedupNested(missingFieldSchema, Collections.singletonList(missingField.get()));
    assertEquals(fullSchema, actual);
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullSchema() {
    // This test should throw an IllegalArgumentException
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaUtils.createNewSchemaFromFieldsWithReference(null, Collections.emptyList()));
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullObjectProps() {
    // Create a schema without any object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [] }";
    HoodieSchema schema = HoodieSchema.parse(schemaStr);

    // Ensure getObjectProps returns null by mocking or creating a schema without props
    HoodieSchema newSchema = HoodieSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.emptyList());

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(0, newSchema.getFields().size());
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_WithObjectProps() {
    // Create a schema with object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [], \"prop1\": \"value1\" }";
    HoodieSchema schema = HoodieSchema.parse(schemaStr);

    // Add an object property to the schema
    schema.addProp("prop1", "value1");

    // Create new fields to add
    HoodieSchemaField newField = HoodieSchemaField.of("newField", HoodieSchema.create(HoodieSchemaType.STRING), null, null);
    HoodieSchema newSchema = HoodieSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.singletonList(newField));

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(1, newSchema.getFields().size());
    assertEquals("value1", newSchema.getProp("prop1"));
    assertEquals("newField", newSchema.getFields().get(0).name());
  }

  static Stream<Arguments> getExpectedSchemaForFields() {
    // Projection of two nested fields. secondLevelField is entirely projected since both its fields are included
    List<String> fields1 = Arrays.asList("nested_field.secondLevelField.firstname", "nested_field.secondLevelField.lastname");
    // Expected schema - top level field and one nested field
    String expectedSchema1 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"firstname\", \"type\": [\"null\", \"string\"], \"default\": null },\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null }\n"
            + "  ]\n"
            + "}";

    // Projection of first level nested field and top level field which contains the nested field
    // Also include the nested field twice
    // Expected schema - top level field
    List<String> fields2 = Arrays.asList("nested_field.secondLevelField.lastname", "nested_field",
        "nested_field.secondLevelField.lastname");
    String expectedSchema2 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", " + SCHEMA_WITH_AVRO_TYPES_STR + "], \"default\": null }\n"
            + "  ]\n"
            + "}";

    // Projection of non overlapping nested field and top level field with nested fields
    // Expected schema - top level field and one nested field
    List<String> fields3 = Arrays.asList("student.lastname", "nested_field");
    String expectedSchema3 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", " + SCHEMA_WITH_AVRO_TYPES_STR + "], \"default\": null },\n"
            + "    { \"name\": \"student\", \"type\": {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"student\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "      ]\n"
            + "    }}\n"
            + "  ]\n"
            + "}";

    // Projection of two nested fields
    // Expected schema - two nested fields
    List<String> fields4 = Arrays.asList("student.lastname", "nested_field.secondLevelField.lastname");
    String expectedSchema4 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null },\n"
            + "    { \"name\": \"student\", \"type\": {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"student\",\n"
            + "      \"namespace\": \"com.acme.avro\","
            + "      \"fields\": [\n"
            + "        { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "      ]\n"
            + "    }}\n"
            + "  ]\n"
            + "}";

    // Projection of top level field and nested field column
    List<String> fields5 = Arrays.asList("firstname", "nested_field.secondLevelField.lastname", "nested_field.longField");
    // Expected schema - top level field and one nested field
    String expectedSchema5 =
        "{\n"
            + "  \"type\": \"record\",\n"
            + "  \"name\": \"MyClass\",\n"
            + "  \"doc\": \"\",\n"
            + "  \"namespace\": \"com.acme.avro\",\n"
            + "  \"fields\": [\n"
            + "    { \"name\": \"firstname\", \"type\": \"string\" },\n"
            + "    { \"name\": \"nested_field\", \"type\": [\"null\", {\n"
            + "      \"type\": \"record\",\n"
            + "      \"name\": \"TestRecordAvroTypes\",\n"
            + "      \"fields\": [\n"
            + "        { \"name\": \"longField\", \"type\": \"long\" },\n"
            + "        { \"name\": \"secondLevelField\", \"type\": [\"null\", {\n"
            + "          \"type\": \"record\",\n"
            + "          \"name\": \"secondLevelField\",\n"
            + "          \"fields\": [\n"
            + "            { \"name\": \"lastname\", \"type\": [\"null\", \"string\"], \"default\": null }\n"
            + "          ]\n"
            + "        }], \"default\": null }\n"
            + "      ]\n"
            + "    }], \"default\": null }\n"
            + "  ]\n"
            + "}";

    return Stream.of(
        Arguments.of(fields1, expectedSchema1),
        Arguments.of(fields2, expectedSchema2),
        Arguments.of(fields3, expectedSchema3),
        Arguments.of(fields4, expectedSchema4),
        Arguments.of(fields5, expectedSchema5));
  }

  @ParameterizedTest
  @MethodSource("getExpectedSchemaForFields")
  void testProjectSchemaWithNullableAndNestedFields(List<String> projectedFields, String expectedSchemaStr) {
    HoodieSchema expectedSchema = HoodieSchema.parse(expectedSchemaStr);
    HoodieSchema projectedSchema = HoodieSchemaUtils.projectSchema(SCHEMA_WITH_NESTED_FIELD_LARGE, projectedFields);
    assertEquals(expectedSchema, projectedSchema);
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(projectedSchema, expectedSchema, false));
  }

  @Test
  public void testConvertValueForSpecificDataTypes_NullSchema() {
    // Test with null schema - should return value unchanged
    String testValue = "test_value";
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(null, testValue, false);
    assertEquals(testValue, result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_NullValue_NullableSchema() {
    // Test with null value and nullable schema - should return null
    HoodieSchema nullableIntSchema = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.INT));
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(nullableIntSchema, null, false);
    assertNull(result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_NullValue_NonNullableSchema() {
    // Test with null value and non-nullable schema - should throw exception
    HoodieSchema nonNullableSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    assertThrows(IllegalStateException.class, () ->
        HoodieSchemaUtils.convertValueForSpecificDataTypes(nonNullableSchema, null, false));
  }

  @Test
  public void testConvertValueForSpecificDataTypes_DateLogicalType() {
    // Create date schema
    HoodieSchema dateSchema = HoodieSchema.createDate();

    // Test value: epoch days for 2023-01-01
    int epochDays = 19358;
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(dateSchema, epochDays, false);
    assertNotNull(result);
    assertTrue(result instanceof LocalDate);
    assertEquals(LocalDate.of(2023, 1, 1), result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_TimestampMillis_Enabled() {
    // Create timestamp-millis schema
    HoodieSchema timestampMillisSchema = HoodieSchema.createTimestampMillis();

    // Test value: milliseconds for 2023-01-01 00:00:00
    long millis = 1672560000000L;
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(timestampMillisSchema, millis, true);
    assertNotNull(result);
    assertTrue(result instanceof Timestamp);
    assertEquals(new Timestamp(millis), result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_TimestampMillis_Disabled() {
    // Create timestamp-millis schema
    HoodieSchema timestampMillisSchema = HoodieSchema.createTimestampMillis();
    long millis = 1672560000000L;
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(timestampMillisSchema, millis, false);
    assertEquals(millis, result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_TimestampMicros_Enabled() {
    // Create timestamp-micros schema
    HoodieSchema timestampMicrosSchema = HoodieSchema.createTimestampMicros();

    // Test value: microseconds for 2023-01-01 00:00:00
    long micros = 1672560000000000L;
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(timestampMicrosSchema, micros, true);
    assertNotNull(result);
    assertTrue(result instanceof Timestamp);
    assertEquals(new Timestamp(micros / 1000), result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_DecimalBytes() {
    // Create decimal schema with precision=10, scale=2
    HoodieSchema decimalSchema = HoodieSchema.createDecimal(10, 2);

    // Create test value: 1234.56
    BigDecimal expectedDecimal = new BigDecimal("1234.56");
    ByteBuffer byteBuffer = ByteBuffer.wrap(expectedDecimal.unscaledValue().toByteArray());
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(decimalSchema, byteBuffer, false);
    assertNotNull(result);
    assertTrue(result instanceof BigDecimal);
    assertEquals(expectedDecimal, result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_NonLogicalType() {
    // Test with non-logical type (plain string) - should return unchanged
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    String testValue = "test_string";
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(stringSchema, testValue, false);
    assertEquals(testValue, result);
  }

  @Test
  public void testConvertValueForSpecificDataTypes_UnionWithNull() {
    // Test with union type containing null
    HoodieSchema dateSchema = HoodieSchema.createDate();
    HoodieSchema nullableDateSchema = HoodieSchema.createNullable(dateSchema);

    // Test with non-null value
    int epochDays = 19358; // 2023-01-01
    Object result = HoodieSchemaUtils.convertValueForSpecificDataTypes(nullableDateSchema, epochDays, false);
    assertNotNull(result);
    assertTrue(result instanceof LocalDate);
    assertEquals(LocalDate.of(2023, 1, 1), result);
  }

  @Test
  void testHasDecimalField() {
    assertTrue(HoodieSchemaUtils.hasDecimalField(HoodieSchema.parse(SCHEMA_WITH_DECIMAL_FIELD)));
    assertFalse(HoodieSchemaUtils.hasDecimalField(HoodieSchema.parse(EVOLVED_SCHEMA)));
    assertFalse(HoodieSchemaUtils.hasDecimalField(HoodieSchema.parse(SCHEMA_WITH_NON_NULLABLE_FIELD)));
    assertTrue(HoodieSchemaUtils.hasDecimalField(HoodieTestDataGenerator.HOODIE_SCHEMA));
    assertTrue(HoodieSchemaUtils.hasDecimalField(HoodieTestDataGenerator.HOODIE_TRIP_ENCODED_DECIMAL_SCHEMA));
    HoodieSchema recordWithMapAndArray = HoodieSchema.createRecord("recordWithMapAndArray", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("mapfield", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT)), null, null),
            HoodieSchemaField.of("arrayfield", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)), null, null)
        ));
    assertFalse(HoodieSchemaUtils.hasDecimalField(recordWithMapAndArray));
    HoodieSchema recordWithDecMapAndArray = HoodieSchema.createRecord("recordWithDecMapAndArray", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("mapfield", HoodieSchema.createMap(HoodieSchema.createDecimal(10, 6)), null, null),
            HoodieSchemaField.of("arrayfield", HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT)), null, null)
        ));
    assertTrue(HoodieSchemaUtils.hasDecimalField(recordWithDecMapAndArray));
    HoodieSchema recordWithMapAndDecArray = HoodieSchema.createRecord("recordWithMapAndDecArray", null, null, false,
        Arrays.asList(
            HoodieSchemaField.of("mapfield", HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.INT)), null, null),
            HoodieSchemaField.of("arrayfield", HoodieSchema.createArray(HoodieSchema.createDecimal(10, 6)), null, null)
        ));
    assertTrue(HoodieSchemaUtils.hasDecimalField(recordWithMapAndDecArray));
  }

  @Test
  void testHasSmallPrecisionDecimalField() {
    assertTrue(HoodieSchemaUtils.hasSmallPrecisionDecimalField(HoodieSchema.parse(SCHEMA_WITH_DECIMAL_FIELD)));
    assertFalse(HoodieSchemaUtils.hasSmallPrecisionDecimalField(HoodieSchema.parse(SCHEMA_WITH_AVRO_TYPES_STR)));
    assertFalse(HoodieSchemaUtils.hasSmallPrecisionDecimalField(HoodieSchema.parse(EXAMPLE_SCHEMA)));
  }

  @Test
  void testResolveUnionSchemaWithNonUnionSchema() {
    // Non-union schemas should be returned as-is
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema result = HoodieSchemaUtils.resolveUnionSchema(stringSchema, "any");

    assertSame(stringSchema, result);
  }

  @Test
  void testResolveUnionSchemaWithSimpleNullableUnion() {
    // Simple nullable union: ["null", "string"] should return the non-null type efficiently
    HoodieSchema nullableString = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema result = HoodieSchemaUtils.resolveUnionSchema(nullableString, "string");

    assertEquals(HoodieSchemaType.STRING, result.getType());
  }

  @Test
  void testResolveUnionSchemaWithSimpleNullableRecord() {
    // Test with nullable record type
    HoodieSchema personSchema = HoodieSchema.createRecord(
        "Person",
        null,
        null,
        Collections.singletonList(
            HoodieSchemaField.of("name", HoodieSchema.create(HoodieSchemaType.STRING))
        )
    );

    HoodieSchema nullablePerson = HoodieSchema.createNullable(personSchema);
    HoodieSchema result = HoodieSchemaUtils.resolveUnionSchema(nullablePerson, "Person");

    assertEquals(HoodieSchemaType.RECORD, result.getType());
    assertEquals("Person", result.getName());
    assertFalse(result.isNullable());
  }

  @Test
  void testResolveUnionSchemaWithComplexUnionMatchingFullName() {
    // Complex union with 3+ types, matching by fullName
    String unionSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Container\","
        + "\"fields\":[{"
        + "  \"name\":\"data\","
        + "  \"type\":[\"null\","
        + "    {\"type\":\"record\",\"name\":\"PersonRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]},"
        + "    {\"type\":\"record\",\"name\":\"CompanyRecord\",\"fields\":[{\"name\":\"companyName\",\"type\":\"string\"}]}"
        + "  ]"
        + "}]}";

    HoodieSchema containerSchema = HoodieSchema.parse(unionSchemaJson);
    HoodieSchema dataFieldSchema = containerSchema.getField("data").get().schema();

    // Resolve to PersonRecord
    HoodieSchema personResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "PersonRecord");
    assertEquals(HoodieSchemaType.RECORD, personResult.getType());
    assertEquals("PersonRecord", personResult.getName());
    assertFalse(personResult.isNullable());
    assertTrue(personResult.getField("name").isPresent());

    // Resolve to CompanyRecord
    HoodieSchema companyResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "CompanyRecord");
    assertEquals(HoodieSchemaType.RECORD, companyResult.getType());
    assertEquals("CompanyRecord", companyResult.getName());
    assertFalse(companyResult.isNullable());
    assertTrue(companyResult.getField("companyName").isPresent());
  }

  @Test
  void testResolveUnionSchemaWithNonNullableTwoTypeUnion() {
    // Union of two non-nullable types should use the complex resolution path
    String unionSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Container\","
        + "\"fields\":[{"
        + "  \"name\":\"data\","
        + "  \"type\":["
        + "    {\"type\":\"record\",\"name\":\"TypeA\",\"fields\":[{\"name\":\"fieldA\",\"type\":\"string\"}]},"
        + "    {\"type\":\"record\",\"name\":\"TypeB\",\"fields\":[{\"name\":\"fieldB\",\"type\":\"int\"}]}"
        + "  ]"
        + "}]}";

    HoodieSchema containerSchema = HoodieSchema.parse(unionSchemaJson);
    HoodieSchema dataFieldSchema = containerSchema.getField("data").get().schema();

    // Resolve to TypeA
    HoodieSchema typeAResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "TypeA");
    assertEquals(HoodieSchemaType.RECORD, typeAResult.getType());
    assertEquals("TypeA", typeAResult.getName());
    assertFalse(typeAResult.isNullable());

    // Resolve to TypeB
    HoodieSchema typeBResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "TypeB");
    assertEquals(HoodieSchemaType.RECORD, typeBResult.getType());
    assertEquals("TypeB", typeBResult.getName());
    assertFalse(typeAResult.isNullable());
  }

  @Test
  void testResolveUnionSchemaThrowsExceptionWhenNoMatch() {
    // Complex union where the requested fullName doesn't match any type
    String unionSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Container\","
        + "\"fields\":[{"
        + "  \"name\":\"data\","
        + "  \"type\":[\"null\","
        + "    {\"type\":\"record\",\"name\":\"PersonRecord\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]},"
        + "    {\"type\":\"record\",\"name\":\"CompanyRecord\",\"fields\":[{\"name\":\"companyName\",\"type\":\"string\"}]}"
        + "  ]"
        + "}]}";

    HoodieSchema containerSchema = HoodieSchema.parse(unionSchemaJson);
    HoodieSchema dataFieldSchema = containerSchema.getField("data").get().schema();

    // Try to resolve to a type that doesn't exist in the union
    HoodieSchemaException exception = assertThrows(
        HoodieSchemaException.class,
        () -> HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "AnimalRecord")
    );

    assertTrue(exception.getMessage().contains("Unsupported UNION type"));
    assertTrue(exception.getMessage().contains("Only UNION of a null type and a non-null type is supported"));
  }

  @Test
  void testResolveUnionSchemaWithNamespacedRecords() {
    // Test with fully qualified names (with namespace)
    String unionSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"Container\","
        + "\"namespace\":\"com.example\","
        + "\"fields\":[{"
        + "  \"name\":\"data\","
        + "  \"type\":[\"null\","
        + "    {\"type\":\"record\",\"name\":\"Person\",\"namespace\":\"com.example.model\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]},"
        + "    {\"type\":\"record\",\"name\":\"Company\",\"namespace\":\"com.example.model\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"}]}"
        + "  ]"
        + "}]}";

    HoodieSchema containerSchema = HoodieSchema.parse(unionSchemaJson);
    HoodieSchema dataFieldSchema = containerSchema.getField("data").get().schema();

    // Resolve using fully qualified name
    HoodieSchema personResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "com.example.model.Person");
    assertEquals(HoodieSchemaType.RECORD, personResult.getType());
    assertEquals("Person", personResult.getName());
    assertFalse(personResult.isNullable());
    assertEquals("com.example.model", personResult.getNamespace().get());

    // Resolve Company
    HoodieSchema companyResult = HoodieSchemaUtils.resolveUnionSchema(dataFieldSchema, "com.example.model.Company");
    assertEquals(HoodieSchemaType.RECORD, companyResult.getType());
    assertEquals("Company", companyResult.getName());
    assertFalse(companyResult.isNullable());
  }

  @Test
  void testResolveUnionSchemaWithPrimitiveTypes() {
    // Test union containing primitive types (although less common)
    // Union of null and string, but passed through the full name matching path
    HoodieSchema nullableString = HoodieSchema.createNullable(HoodieSchema.create(HoodieSchemaType.STRING));

    // For simple 2-element nullable union, should use fast path
    HoodieSchema result = HoodieSchemaUtils.resolveUnionSchema(nullableString, "string");
    assertEquals(HoodieSchemaType.STRING, result.getType());
  }

  @Test
  void testResolveUnionSchemaConsistencyWithOriginalAvroImpl() {
    // Verify that HoodieSchemaUtils.resolveUnionSchema produces equivalent results to the original AvroSchemaUtils.resolveUnionSchema
    String unionSchemaJson = "{"
        + "\"type\":\"record\","
        + "\"name\":\"TestRecord\","
        + "\"fields\":[{"
        + "  \"name\":\"unionField\","
        + "  \"type\":[\"null\","
        + "    {\"type\":\"record\",\"name\":\"TypeA\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]},"
        + "    {\"type\":\"record\",\"name\":\"TypeB\",\"fields\":[{\"name\":\"b\",\"type\":\"string\"}]}"
        + "  ]"
        + "}]}";

    Schema avroSchema = new Schema.Parser().parse(unionSchemaJson);
    HoodieSchema hoodieSchema = HoodieSchema.parse(unionSchemaJson);

    Schema avroFieldSchema = avroSchema.getField("unionField").schema();
    HoodieSchema hoodieFieldSchema = hoodieSchema.getField("unionField").get().schema();

    // Resolve using both implementations
    Schema avroResult = AvroSchemaUtils.resolveUnionSchema(avroFieldSchema, "TypeA");
    HoodieSchema hoodieResult = HoodieSchemaUtils.resolveUnionSchema(hoodieFieldSchema, "TypeA");

    // Should produce equivalent schemas
    assertEquals(avroResult, hoodieResult.toAvroSchema());
  }
}
