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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.AssertionsKt.assertNotNull;

public class TestAvroSchemaUtils {

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

  private static Schema parse(String json) {
    return new Schema.Parser().parse(json);
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
