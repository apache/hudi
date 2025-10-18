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

package org.apache.hudi.avro;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;

public class TestAvroSchemaUtils {

  private static final String SOURCE_SCHEMA = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "        \"name\" : \"f1\",\n"
      + "        \"type\" : [ \"null\", {\n"
      + "           \"type\" : \"fixed\",\n"
      + "           \"name\" : \"f1\",\n"
      + "           \"namespace\" : \"\",\n"
      + "           \"size\" : 5,\n"
      + "           \"logicalType\" : \"decimal\",\n"
      + "           \"precision\" : 10,\n"
      + "           \"scale\" : 2\n"
      + "           }],\n"
      + "       \"default\" : null\n"
      + "    },\n"
      + "    {\n"
      + "         \"name\" : \"arrayInt\",\n"
      + "          \"type\" : [ \"null\", {\n"
      + "            \"type\" : \"array\",\n"
      + "            \"items\" : [ \"null\", \"int\" ]\n"
      + "           } ],\n"
      + "          \"default\" : null\n"
      + "    },\n"
      + "    {\n"
      + "         \"name\" : \"mapStrInt\",\n"
      + "         \"type\" : [ \"null\", {\n"
      + "           \"type\" : \"map\",\n"
      + "           \"values\" : [ \"null\", \"int\" ]\n"
      + "         } ],\n"
      + "         \"default\" : null\n"
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
      + "    { \n"
      + "      \"name\" : \"f_enum\",\n"
      + "      \"type\" : [ \"null\", {\n"
      + "        \"type\" : \"enum\",\n"
      + "        \"name\" : \"Visibility\",\n"
      + "        \"namespace\" : \"common.Types\",\n"
      + "        \"symbols\" : [ \"UNKNOWN\", \"PUBLIC\", \"PRIVATE\", \"SHARED\" ]\n"
      + "         }]\n"
      + "   }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_STRICT = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"int\"]\n"
      + "    },\n"
      + "    {\n"
      + "        \"name\" : \"f1\",\n"
      + "        \"type\" : [ \"null\", {\n"
      + "           \"type\" : \"fixed\",\n"
      + "           \"name\" : \"fixed\",\n"
      + "           \"namespace\" : \"example.schema.source.f1\",\n"
      + "           \"size\" : 5,\n"
      + "           \"logicalType\" : \"decimal\",\n"
      + "           \"precision\" : 10,\n"
      + "           \"scale\" : 2\n"
      + "           }],\n"
      + "       \"default\" : null\n"
      + "      },\n"
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
      + "    }\n"
      + "  ]\n"
      + "}\n";

  private static final String PROJECTED_NESTED_SCHEMA_WITH_PROMOTION = "{\n"
      + "  \"type\": \"record\",\n"
      + "  \"namespace\": \"example.schema\",\n"
      + "  \"name\": \"source\",\n"
      + "  \"fields\": [\n"
      + "    {\n"
      + "      \"name\": \"number\",\n"
      + "      \"type\": [\"null\", \"long\"]\n"
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
      + "        ]  \n"
      + "      }\n"
      + "    }\n"
      + "  ]\n"
      + "}\n";

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullSchema() {
    // This test should throw an IllegalArgumentException
    assertThrows(IllegalArgumentException.class, () -> AvroSchemaUtils.createNewSchemaFromFieldsWithReference(null, Collections.emptyList()));
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_NullObjectProps() {
    // Create a schema without any object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [] }";
    Schema schema = new Schema.Parser().parse(schemaStr);

    // Ensure getObjectProps returns null by mocking or creating a schema without props
    Schema newSchema = AvroSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.emptyList());

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(0, newSchema.getFields().size());
  }

  @Test
  public void testCreateNewSchemaFromFieldsWithReference_WithObjectProps() {
    // Create a schema with object properties
    String schemaStr = "{ \"type\": \"record\", \"name\": \"TestRecord\", \"fields\": [], \"prop1\": \"value1\" }";
    Schema schema = new Schema.Parser().parse(schemaStr);

    // Add an object property to the schema
    schema.addProp("prop1", "value1");

    // Create new fields to add
    Schema.Field newField = new Schema.Field("newField", Schema.create(Schema.Type.STRING), null, (Object) null);
    Schema newSchema = AvroSchemaUtils.createNewSchemaFromFieldsWithReference(schema, Collections.singletonList(newField));

    // Validate the new schema
    assertEquals("TestRecord", newSchema.getName());
    assertEquals(1, newSchema.getFields().size());
    assertEquals("value1", newSchema.getProp("prop1"));
    assertEquals("newField", newSchema.getFields().get(0).name());
  }

  @Test
  public void testIsStrictProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_STRICT);

    // Case #1: Validate proper (nested) projected record schema

    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema
    assertTrue(
        AvroSchemaUtils.isStrictProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));

    // Case #5: Validate project with field nullability changed
    // Note: for array type, the nullability of element's type will be changed after conversion:
    // AvroSchemaConverter: Avro Schema -> Parquet MessageType -> Avro Schema
    MessageType messageType = new AvroSchemaConverter().convert(sourceSchema);
    Schema converted = new AvroSchemaConverter().convert(messageType);
    assertTrue(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, converted));
  }

  @Test
  public void testIsCompatibleProjection() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Schema projectedNestedSchema = new Schema.Parser().parse(PROJECTED_NESTED_SCHEMA_WITH_PROMOTION);

    // Case #1: Validate proper (nested) projected record schema (with promotion,
    //          number field promoted from int to long)

    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, sourceSchema));
    assertTrue(AvroSchemaUtils.isCompatibleProjectionOf(sourceSchema, projectedNestedSchema));

    // NOTE: That [[isStrictProjectionOf]] should be false in that case
    assertFalse(AvroSchemaUtils.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(AvroSchemaUtils.isCompatibleProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createArray(sourceSchema),
            Schema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createMap(sourceSchema),
            Schema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema (with promotion)
    assertTrue(
        AvroSchemaUtils.isCompatibleProjectionOf(
            Schema.createUnion(Schema.create(Schema.Type.NULL), sourceSchema),
            Schema.createUnion(Schema.create(Schema.Type.NULL), projectedNestedSchema)));
  }

  private static final Schema FULL_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"record\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"c\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}");

  private static final Schema SHORT_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"record\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}\n");

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatibleProjectionNotAllowed(boolean shouldValidate) {
    assertThrows(SchemaCompatibilityException.class,
        () -> AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, false, Collections.emptySet()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatibleProjectionAllowed(boolean shouldValidate) {
    AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, true, Collections.emptySet());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatiblePartitionDropCols(boolean shouldValidate) {
    AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, SHORT_SCHEMA, shouldValidate, false, Collections.singleton("c"));
  }

  private static final Schema BROKEN_SCHEMA = new Schema.Parser().parse("{\n"
      + "  \"type\" : \"record\",\n"
      + "  \"name\" : \"broken\",\n"
      + "  \"fields\" : [ {\n"
      + "    \"name\" : \"a\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"b\",\n"
      + "    \"type\" : [ \"null\", \"int\" ],\n"
      + "    \"default\" : null\n"
      + "  }, {\n"
      + "    \"name\" : \"c\",\n"
      + "    \"type\" : [ \"null\", \"boolean\" ],\n"
      + "    \"default\" : null\n"
      + "  } ]\n"
      + "}");

  @Test
  public void  testBrokenSchema() {
    assertThrows(SchemaBackwardsCompatibilityException.class,
        () -> AvroSchemaUtils.checkSchemaCompatible(FULL_SCHEMA, BROKEN_SCHEMA, true, false, Collections.emptySet()));
  }

  @Test
  public void testAppendFieldsToSchemaDedupNested() {
    Schema fullSchema = new Schema.Parser().parse("{\n"
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

    Schema missingFieldSchema = new Schema.Parser().parse("{\n"
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

    Option<Schema.Field> missingField = AvroSchemaUtils.findNestedField(fullSchema, "nested_record.long");
    assertTrue(missingField.isPresent());
    assertEquals(fullSchema, AvroSchemaUtils.appendFieldsToSchemaDedupNested(missingFieldSchema, Collections.singletonList(missingField.get())));
  }

  @Test
  public void testFindNestedFieldType() {
    Schema sourceSchema = new Schema.Parser().parse(SOURCE_SCHEMA);
    Option<Schema.Type> field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "number");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.INT, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.string");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.STRING, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.long");
    assertTrue(field.isPresent());
    assertEquals(Schema.Type.LONG, field.get());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, null);
    assertTrue(field.isEmpty());

    field = AvroSchemaUtils.findNestedFieldType(sourceSchema, "");
    assertTrue(field.isEmpty());

    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "long"));
    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "nested_record.bool"));
    assertThrows(HoodieAvroSchemaException.class, () -> AvroSchemaUtils.findNestedFieldType(sourceSchema, "non_present_field.also_not_present"));
  }

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
  }

  @Test
  void testAreSchemasProjectionEquivalentRecordSchemas() {
    Schema s1 = parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    Schema s2 = parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFieldCountInRecords() {
    Schema s1 = parse("{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");
    Schema s2 = parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[]}");
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNestedRecordSchemas() {
    Schema s1 = parse("{\"type\":\"record\",\"name\":\"Outer1\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner1\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    Schema s2 = parse("{\"type\":\"record\",\"name\":\"Outer2\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner2\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentArraySchemas() {
    Schema s1 = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema s2 = Schema.createArray(Schema.create(Schema.Type.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentElementTypeInArray() {
    Schema s1 = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema s2 = Schema.createArray(Schema.create(Schema.Type.INT));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapSchemas() {
    Schema s1 = Schema.createMap(Schema.create(Schema.Type.LONG));
    Schema s2 = Schema.createMap(Schema.create(Schema.Type.LONG));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentMapValueTypes() {
    Schema s1 = Schema.createMap(Schema.create(Schema.Type.LONG));
    Schema s2 = Schema.createMap(Schema.create(Schema.Type.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullableSchemaComparison() {
    Schema s1 = AvroSchemaUtils.createNullableSchema(Schema.create(Schema.Type.INT));
    Schema s2 = Schema.create(Schema.Type.INT);
    s2.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentListVsString() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema listSchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(listSchema, stringSchema));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(stringSchema, listSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapVsString() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema mapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(mapSchema, stringSchema));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(stringSchema, mapSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualFixedSchemas() {
    Schema s1 = Schema.createFixed("F", null, null, 16);
    Schema s2 = Schema.createFixed("F", null, null, 16);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFixedSize() {
    Schema s1 = Schema.createFixed("F", null, null, 8);
    Schema s2 = Schema.createFixed("F", null, null, 4);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnums() {
    Schema s1 = Schema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    Schema s2 = Schema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentEnumSymbols() {
    Schema s1 = Schema.createEnum("E", null, null, Arrays.asList("X", "Y"));
    Schema s2 = Schema.createEnum("E", null, null, Arrays.asList("A", "B"));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnumSymbolSubset() {
    Schema s1 = Schema.createEnum("E", null, null, Arrays.asList("A", "B"));
    Schema s2 = Schema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s2, s1));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualDecimalLogicalTypes() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s1);

    Schema s2 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s2);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true

    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentPrecision() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s1);

    Schema s2 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(13, 2).addToSchema(s2);

    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentLogicalVsNoLogicalType() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(10, 2).addToSchema(s1);

    Schema s2 = Schema.create(Schema.Type.BYTES);

    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentSameReferenceSchema() {
    Schema s = Schema.create(Schema.Type.STRING);
    assertTrue(AvroSchemaUtils.areSchemasProjectionEquivalent(s, s));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullSchemaComparison() {
    Schema s = Schema.create(Schema.Type.STRING);
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(null, s));
    assertFalse(AvroSchemaUtils.areSchemasProjectionEquivalent(s, null));
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

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredSchemaStr);

    Schema pruned = AvroSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());

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

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredFooStr);

    Schema fooFieldSchema = dataSchema.getField("foo").schema();
    Schema pruned = AvroSchemaUtils.pruneDataSchema(fooFieldSchema, requiredSchema, Collections.emptySet());

    assertEquals(Schema.Type.UNION, pruned.getType());

    Schema prunedRecord = pruned.getTypes().stream()
        .filter(s -> s.getType() == Schema.Type.RECORD)
        .collect(Collectors.toList()).get(0);
    assertNotNull(prunedRecord.getField("field1"));
    assertNull(prunedRecord.getField("field2"));
  }

  @Test
  void testArrayElementPruning() {
    String dataSchemaStr = "{ \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\": \"Item\", \"fields\": ["
        + "{\"name\": \"a\", \"type\": \"int\"}, {\"name\": \"b\", \"type\": \"string\"}"
        + "]}}";

    String requiredSchemaStr = "{ \"type\": \"array\", \"items\": { \"type\": \"record\", \"name\": \"Item\", \"fields\": ["
        + "{\"name\": \"b\", \"type\": \"string\"}"
        + "]}}";

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredSchemaStr);

    Schema pruned = AvroSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());
    Schema itemSchema = pruned.getElementType();

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

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredSchemaStr);

    Schema pruned = AvroSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, Collections.emptySet());
    Schema valueSchema = pruned.getValueType();

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

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredSchemaStr);

    Set<String> mandatoryFields = Collections.singleton("missing");

    Schema pruned = AvroSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, mandatoryFields);

    assertEquals(2, pruned.getFields().size());
    assertNotNull(pruned.getField("missing"));
    assertEquals("string", pruned.getField("missing").schema().getType().getName());
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

    Schema dataSchema = parse(dataSchemaStr);
    Schema requiredSchema = parse(requiredSchemaStr);

    // Both "topLevelMissing" and "nestedMissing" are in mandatory fields
    // but only "topLevelMissing" should be preserved since mandatory fields
    // only apply to top-level fields
    Set<String> mandatoryFields = new HashSet<>(Arrays.asList("topLevelMissing", "nestedMissing"));

    Schema pruned = AvroSchemaUtils.pruneDataSchema(dataSchema, requiredSchema, mandatoryFields);

    // Should have 3 top-level fields: existing, topLevelMissing, nestedRecord
    assertEquals(3, pruned.getFields().size());

    // Top-level mandatory field should be preserved even though missing from data schema
    assertNotNull(pruned.getField("topLevelMissing"));
    assertEquals("string", pruned.getField("topLevelMissing").schema().getType().getName());

    // Nested record should exist
    assertNotNull(pruned.getField("nestedRecord"));
    Schema nestedSchema = pruned.getField("nestedRecord").schema();

    // Nested record should only have 1 field (nestedField) - nestedMissing should NOT be preserved
    // because mandatory fields only apply to top-level
    assertEquals(1, nestedSchema.getFields().size());
    assertNotNull(nestedSchema.getField("nestedField"));
    assertNull(nestedSchema.getField("nestedMissing")); // This should be null - not preserved
  }
}
