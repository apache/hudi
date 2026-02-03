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
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;
import org.apache.hudi.exception.SchemaCompatibilityException;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;

import static org.apache.hudi.common.schema.TestHoodieSchemaUtils.EVOLVED_SCHEMA;
import static org.apache.hudi.common.schema.TestHoodieSchemaUtils.SIMPLE_SCHEMA;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSchemaCompatibility {

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
  public void testIsStrictProjection() {
    HoodieSchema sourceSchema = HoodieSchema.parse(SOURCE_SCHEMA);
    HoodieSchema projectedNestedSchema = HoodieSchema.parse(PROJECTED_NESTED_SCHEMA_STRICT);

    // Case #1: Validate proper (nested) projected record schema

    assertTrue(HoodieSchemaCompatibility.isStrictProjectionOf(sourceSchema, sourceSchema));
    assertTrue(HoodieSchemaCompatibility.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(HoodieSchemaCompatibility.isStrictProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema
    assertTrue(
        HoodieSchemaCompatibility.isStrictProjectionOf(
            HoodieSchema.createArray(sourceSchema),
            HoodieSchema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema
    assertTrue(
        HoodieSchemaCompatibility.isStrictProjectionOf(
            HoodieSchema.createMap(sourceSchema),
            HoodieSchema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema
    assertTrue(
        HoodieSchemaCompatibility.isStrictProjectionOf(
            HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.NULL), sourceSchema),
            HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.NULL), projectedNestedSchema)));

    // Case #5: Validate project with field nullability changed
    // Note: for array type, the nullability of element's type will be changed after conversion:
    // AvroSchemaConverter: Avro Schema -> Parquet MessageType -> Avro Schema
    MessageType messageType = new AvroSchemaConverter().convert(sourceSchema.toAvroSchema());
    HoodieSchema converted = HoodieSchema.fromAvroSchema(new AvroSchemaConverter().convert(messageType));
    assertTrue(HoodieSchemaCompatibility.isStrictProjectionOf(sourceSchema, converted));
  }

  @Test
  public void testIsCompatibleProjection() {
    HoodieSchema sourceSchema = HoodieSchema.parse(SOURCE_SCHEMA);
    HoodieSchema projectedNestedSchema = HoodieSchema.parse(PROJECTED_NESTED_SCHEMA_WITH_PROMOTION);

    // Case #1: Validate proper (nested) projected record schema (with promotion,
    //          number field promoted from int to long)

    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(sourceSchema, sourceSchema));
    assertTrue(HoodieSchemaCompatibility.isCompatibleProjectionOf(sourceSchema, projectedNestedSchema));

    // NOTE: That [[isStrictProjectionOf]] should be false in that case
    assertFalse(HoodieSchemaCompatibility.isStrictProjectionOf(sourceSchema, projectedNestedSchema));
    // NOTE: That the opposite have to be false: if schema B is a projection of A,
    //       then A could be a projection of B iff A == B
    assertFalse(HoodieSchemaCompatibility.isCompatibleProjectionOf(projectedNestedSchema, sourceSchema));

    // Case #2: Validate proper (nested) projected array schema (with promotion)
    assertTrue(
        HoodieSchemaCompatibility.isCompatibleProjectionOf(
            HoodieSchema.createArray(sourceSchema),
            HoodieSchema.createArray(projectedNestedSchema)));

    // Case #3: Validate proper (nested) projected map schema (with promotion)
    assertTrue(
        HoodieSchemaCompatibility.isCompatibleProjectionOf(
            HoodieSchema.createMap(sourceSchema),
            HoodieSchema.createMap(projectedNestedSchema)));

    // Case #4: Validate proper (nested) projected union schema (with promotion)
    assertTrue(
        HoodieSchemaCompatibility.isCompatibleProjectionOf(
            HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.NULL), sourceSchema),
            HoodieSchema.createUnion(HoodieSchema.create(HoodieSchemaType.NULL), projectedNestedSchema)));
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
        () -> HoodieSchemaCompatibility.checkSchemaCompatible(HoodieSchema.fromAvroSchema(FULL_SCHEMA), HoodieSchema.fromAvroSchema(SHORT_SCHEMA), shouldValidate, false));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatibleProjectionAllowed(boolean shouldValidate) {
    HoodieSchemaCompatibility.checkSchemaCompatible(HoodieSchema.fromAvroSchema(FULL_SCHEMA), HoodieSchema.fromAvroSchema(SHORT_SCHEMA), shouldValidate, true);
  }

  @ParameterizedTest(name = "[{index}] oldSize={0}, oldPrecision={1}, oldScale={2} -> newSize={3}, newPrecision={4}, newScale={5}")
  @MethodSource("provideCompatibleDecimalSchemas")
  void testCompatibleDecimalSchemas(int oldSize, int oldPrecision, int oldScale,
                                    int newSize, int newPrecision, int newScale) {
    HoodieSchema oldSchema = createFixedDecimalSchema(oldSize, oldPrecision, oldScale);
    HoodieSchema newSchema = createFixedDecimalSchema(newSize, newPrecision, newScale);

    assertDoesNotThrow(() ->
            HoodieSchemaCompatibility.checkSchemaCompatible(oldSchema, newSchema, true, false, Collections.emptySet()),
        "Schemas should be compatible"
    );
  }

  @ParameterizedTest(name = "[{index}] oldSize={0}, oldPrecision={1}, oldScale={2} -> newSize={3}, newPrecision={4}, newScale={5}")
  @MethodSource("provideIncompatibleDecimalSchemas")
  void testIncompatibleDecimalSchemas(int oldSize, int oldPrecision, int oldScale,
                                      int newSize, int newPrecision, int newScale) {
    HoodieSchema oldSchema = createFixedDecimalSchema(oldSize, oldPrecision, oldScale);
    HoodieSchema newSchema = createFixedDecimalSchema(newSize, newPrecision, newScale);

    assertThrows(Exception.class, () ->
            HoodieSchemaCompatibility.checkSchemaCompatible(oldSchema, newSchema, true, false, Collections.emptySet()),
        "Schemas should be incompatible"
    );
  }

  private static Stream<Arguments> provideCompatibleDecimalSchemas() {
    return Stream.of(
        // Same size, same precision and scale
        Arguments.of(8, 10, 2, 8, 10, 2),

        // Same size, increased precision, same scale
        Arguments.of(8, 10, 2, 8, 15, 2),

        // Same size, increased precision and increased scale
        Arguments.of(16, 20, 5, 16, 25, 10)
    );
  }

  private static Stream<Arguments> provideIncompatibleDecimalSchemas() {
    return Stream.of(
        // Same size, decreased precision
        Arguments.of(8, 15, 2, 8, 10, 2),

        // Same size, same precision, increased scale
        Arguments.of(8, 10, 2, 8, 10, 5),

        // Same size, decreased precision, same scale
        Arguments.of(16, 25, 3, 16, 20, 3),

        // Same size, both decreased precision and increased scale
        Arguments.of(8, 18, 4, 8, 15, 6)
    );
  }

  private HoodieSchema createFixedDecimalSchema(int size, int precision, int scale) {
    Schema fixedSchema = SchemaBuilder.fixed("FixedDecimal").size(size);
    Schema decimalSchema = LogicalTypes.decimal(precision, scale).addToSchema(fixedSchema);

    return HoodieSchema.fromAvroSchema(SchemaBuilder.record("FixedDecimalSchema")
        .fields()
        .name("decimalField").type(decimalSchema).noDefault()
        .endRecord());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIsCompatiblePartitionDropCols(boolean shouldValidate) {
    HoodieSchemaCompatibility.checkSchemaCompatible(HoodieSchema.fromAvroSchema(FULL_SCHEMA), HoodieSchema.fromAvroSchema(SHORT_SCHEMA), shouldValidate, false, Collections.singleton("c"));
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
        () -> HoodieSchemaCompatibility.checkSchemaCompatible(HoodieSchema.fromAvroSchema(FULL_SCHEMA), HoodieSchema.fromAvroSchema(BROKEN_SCHEMA), true, false, Collections.emptySet()));
  }

  @Test
  void testAreSchemasProjectionEquivalentRecordSchemas() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[{\"name\":\"f1\",\"type\":\"int\"}]}");
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFieldCountInRecords() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R1\",\"fields\":[{\"name\":\"a\",\"type\":\"int\"}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"R2\",\"fields\":[]}");
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNestedRecordSchemas() {
    HoodieSchema s1 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"Outer1\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner1\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    HoodieSchema s2 = HoodieSchema.parse("{\"type\":\"record\",\"name\":\"Outer2\",\"fields\":[{\"name\":\"inner\","
        + "\"type\":{\"type\":\"record\",\"name\":\"Inner2\",\"fields\":[{\"name\":\"x\",\"type\":\"string\"}]}}]}");
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentArraySchemas() {
    HoodieSchema s1 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema s2 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentElementTypeInArray() {
    HoodieSchema s1 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    HoodieSchema s2 = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.INT));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapSchemas() {
    HoodieSchema s1 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema s2 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentMapValueTypes() {
    HoodieSchema s1 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.LONG));
    HoodieSchema s2 = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullableSchemaComparison() {
    HoodieSchema s1 = HoodieSchema.createNullable(HoodieSchemaType.INT);
    HoodieSchema s2 = HoodieSchema.create(HoodieSchemaType.INT);
    s2.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentListVsString() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema listSchema = HoodieSchema.createArray(HoodieSchema.create(HoodieSchemaType.STRING));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(listSchema, stringSchema));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(stringSchema, listSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentMapVsString() {
    HoodieSchema stringSchema = HoodieSchema.create(HoodieSchemaType.STRING);
    HoodieSchema mapSchema = HoodieSchema.createMap(HoodieSchema.create(HoodieSchemaType.STRING));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(mapSchema, stringSchema));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(stringSchema, mapSchema));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualFixedSchemas() {
    HoodieSchema s1 = HoodieSchema.createFixed("F", null, null, 16);
    HoodieSchema s2 = HoodieSchema.createFixed("F", null, null, 16);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentFixedSize() {
    HoodieSchema s1 = HoodieSchema.createFixed("F", null, null, 8);
    HoodieSchema s2 = HoodieSchema.createFixed("F", null, null, 4);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnums() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentEnumSymbols() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("X", "Y"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B"));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentEnumSymbolSubset() {
    HoodieSchema s1 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B"));
    HoodieSchema s2 = HoodieSchema.createEnum("E", null, null, Arrays.asList("A", "B", "C"));
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s1, s2));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s2, s1));
  }

  @Test
  void testAreSchemasProjectionEquivalentEqualDecimalLogicalTypes() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s1);

    Schema s2 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s2);
    s1.addProp("prop1", "value1"); // prevent Objects.equals from returning true

    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(HoodieSchema.fromAvroSchema(s1), HoodieSchema.fromAvroSchema(s2)));
  }

  @Test
  void testAreSchemasProjectionEquivalentDifferentPrecision() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(12, 2).addToSchema(s1);

    Schema s2 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(13, 2).addToSchema(s2);

    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(HoodieSchema.fromAvroSchema(s1), HoodieSchema.fromAvroSchema(s2)));
  }

  @Test
  void testAreSchemasProjectionEquivalentLogicalVsNoLogicalType() {
    Schema s1 = Schema.create(Schema.Type.BYTES);
    LogicalTypes.decimal(10, 2).addToSchema(s1);

    HoodieSchema s2 = HoodieSchema.create(HoodieSchemaType.BYTES);

    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(HoodieSchema.fromAvroSchema(s1), s2));
  }

  @Test
  void testAreSchemasProjectionEquivalentSameReferenceSchema() {
    HoodieSchema s = HoodieSchema.create(HoodieSchemaType.STRING);
    assertTrue(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s, s));
  }

  @Test
  void testAreSchemasProjectionEquivalentNullSchemaComparison() {
    HoodieSchema s = HoodieSchema.create(HoodieSchemaType.STRING);
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(null, s));
    assertFalse(HoodieSchemaCompatibility.areSchemasProjectionEquivalent(s, null));
  }

  @Test
  public void testIsSchemaCompatible() {
    HoodieSchema baseSchema = HoodieSchema.parse(SIMPLE_SCHEMA);
    HoodieSchema evolvedSchema = HoodieSchema.parse(EVOLVED_SCHEMA);

    // Test schema compatibility (evolved schema should be compatible for reading base schema data)
    boolean compatible = HoodieSchemaCompatibility.isSchemaCompatible(baseSchema, evolvedSchema);
    assertTrue(compatible);

    // Test with projection allowed
    boolean compatibleWithProjection = HoodieSchemaCompatibility.isSchemaCompatible(baseSchema, evolvedSchema, true);
    assertTrue(compatibleWithProjection);
  }

  @Test
  public void testIsSchemaCompatibleValidation() {
    HoodieSchema schema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Should throw on null previous schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaCompatibility.isSchemaCompatible(null, schema);
    });

    // Should throw on null new schema
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaCompatibility.isSchemaCompatible(schema, null);
    });
  }

  @Test
  public void testIsSchemaCompatibleWithCheckNamingAndAllowProjection() {
    // Test with checkNaming=true, allowProjection=true
    HoodieSchema baseSchema = HoodieSchema.parse(SIMPLE_SCHEMA);
    HoodieSchema evolvedSchema = HoodieSchema.parse(EVOLVED_SCHEMA);

    // Evolved schema should be compatible with base schema (can read data written with base schema)
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(baseSchema, evolvedSchema, true, true));

    // Base schema should also be compatible with evolved schema if projection is allowed
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(evolvedSchema, baseSchema, true, true));
  }

  @Test
  public void testIsSchemaCompatibleWithCheckNamingNoProjection() {
    // Test with checkNaming=true, allowProjection=false
    HoodieSchema fullSchema = HoodieSchema.fromAvroSchema(FULL_SCHEMA);
    HoodieSchema shortSchema = HoodieSchema.fromAvroSchema(SHORT_SCHEMA);

    // Short schema is not compatible with full schema when projection is not allowed
    assertFalse(HoodieSchemaCompatibility.isSchemaCompatible(fullSchema, shortSchema, true, false));

    // Full schema should be compatible with short schema (has all fields)
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(shortSchema, fullSchema, true, false));
  }

  @Test
  public void testIsSchemaCompatibleWithoutCheckNaming() {
    // Test with checkNaming=false, allowProjection=true
    // Create schemas with different names but same structure
    String schema1 = "{\"type\":\"record\",\"name\":\"Record1\",\"namespace\":\"ns1\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"int\"}]}";
    String schema2 = "{\"type\":\"record\",\"name\":\"Record2\",\"namespace\":\"ns2\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"int\"}]}";

    HoodieSchema s1 = HoodieSchema.parse(schema1);
    HoodieSchema s2 = HoodieSchema.parse(schema2);

    // With checkNaming=false, schemas with different names should be compatible
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(s1, s2, false, true));
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(s2, s1, false, true));
  }

  @Test
  public void testIsSchemaCompatibleWithTypePromotion() {
    // Test type promotion scenarios
    String intSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"num\",\"type\":\"int\"}]}";
    String longSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"num\",\"type\":\"long\"}]}";

    HoodieSchema intS = HoodieSchema.parse(intSchema);
    HoodieSchema longS = HoodieSchema.parse(longSchema);

    // Long can read int (type promotion)
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(intS, longS, true, true));

    // Int cannot read long (no type demotion)
    assertFalse(HoodieSchemaCompatibility.isSchemaCompatible(longS, intS, true, true));
  }

  @Test
  public void testIsSchemaCompatibleWithNestedSchemas() {
    // Test with nested record schemas
    String nestedSchema1 = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"},"
        + "{\"name\":\"field2\",\"type\":\"int\"}]}}]}";

    String nestedSchema2 = "{\"type\":\"record\",\"name\":\"Outer\",\"fields\":["
        + "{\"name\":\"inner\",\"type\":{\"type\":\"record\",\"name\":\"Inner\",\"fields\":["
        + "{\"name\":\"field1\",\"type\":\"string\"},"
        + "{\"name\":\"field2\",\"type\":\"int\"},"
        + "{\"name\":\"field3\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}";

    HoodieSchema s1 = HoodieSchema.parse(nestedSchema1);
    HoodieSchema s2 = HoodieSchema.parse(nestedSchema2);

    // Schema with added nullable field should be compatible
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(s1, s2, true, true));

    // Reverse should also work with projection allowed
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(s2, s1, true, true));
  }

  @Test
  public void testIsSchemaCompatibleWithArrays() {
    // Test with array type changes
    String arrayIntSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"int\"}}]}";
    String arrayLongSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"arr\",\"type\":{\"type\":\"array\",\"items\":\"long\"}}]}";

    HoodieSchema intArray = HoodieSchema.parse(arrayIntSchema);
    HoodieSchema longArray = HoodieSchema.parse(arrayLongSchema);

    // Array element type promotion
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(intArray, longArray, true, true));
  }

  @Test
  public void testIsSchemaCompatibleWithMaps() {
    // Test with map value type changes
    String mapIntSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"int\"}}]}";
    String mapLongSchema = "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
        + "{\"name\":\"map\",\"type\":{\"type\":\"map\",\"values\":\"long\"}}]}";

    HoodieSchema intMap = HoodieSchema.parse(mapIntSchema);
    HoodieSchema longMap = HoodieSchema.parse(mapLongSchema);

    // Map value type promotion
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(intMap, longMap, true, true));
  }

  @SuppressWarnings("DataFlowIssue")
  @Test
  public void testIsSchemaCompatibleValidationBothParams() {
    HoodieSchema schema = HoodieSchema.parse(SIMPLE_SCHEMA);

    // Should throw on null previous schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaCompatibility.isSchemaCompatible(null, schema, true, true));

    // Should throw on null new schema
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaCompatibility.isSchemaCompatible(schema, null, true, true));

    // Should throw on both null
    assertThrows(IllegalArgumentException.class, () -> HoodieSchemaCompatibility.isSchemaCompatible(null, null, true, true));
  }

  @Test
  public void testIsSchemaCompatibleConsistencyWithAvro() {
    // Verify HoodieSchemaCompatibility results match AvroSchemaUtils for various scenarios
    HoodieSchema s1 = HoodieSchema.parse(SIMPLE_SCHEMA);
    HoodieSchema s2 = HoodieSchema.parse(EVOLVED_SCHEMA);

    // Test all combinations of checkNaming and allowProjection
    for (boolean checkNaming : Arrays.asList(true, false)) {
      for (boolean allowProjection : Arrays.asList(true, false)) {
        boolean avroResult = AvroSchemaUtils.isSchemaCompatible(
            s1.toAvroSchema(), s2.toAvroSchema(), checkNaming, allowProjection);
        boolean hoodieResult = HoodieSchemaCompatibility.isSchemaCompatible(
            s1, s2, checkNaming, allowProjection);

        assertEquals(avroResult, hoodieResult,
            String.format("Results should match for checkNaming=%s, allowProjection=%s",
                checkNaming, allowProjection));
      }
    }
  }

  @Test
  public void testIsSchemaCompatibleIdenticalSchemas() {
    // Test with identical schemas
    HoodieSchema schema = HoodieSchema.parse(SOURCE_SCHEMA);

    // Identical schemas should always be compatible
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(schema, schema, true, true));
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(schema, schema, true, false));
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(schema, schema, false, true));
    assertTrue(HoodieSchemaCompatibility.isSchemaCompatible(schema, schema, false, false));
  }
}
