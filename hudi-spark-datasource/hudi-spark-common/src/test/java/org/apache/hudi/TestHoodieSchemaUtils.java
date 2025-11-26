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

package org.apache.hudi;

import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.avro.AvroSchemaTestUtils.createArrayField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createMapField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createNestedField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createNullableArrayField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createNullablePrimitiveField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createPrimitiveField;
import static org.apache.hudi.avro.AvroSchemaTestUtils.createRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSchemaUtils {

  @Test
  void testSchemaWithNullField() {
    Schema withNullfield = createRecord("nullRecord", createPrimitiveField("nullField", Schema.Type.NULL));
    assertThrows(HoodieNullSchemaTypeException.class,
        () -> deduceWriterSchema(withNullfield, null));
  }

  @Test
  void testSimplePromotionWithComplexFields() {
    Schema start = createRecord("simple", createPrimitiveField("f", Schema.Type.INT));
    Schema end = createRecord("simple", createPrimitiveField("f", Schema.Type.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("nested", createNestedField("f", Schema.Type.INT));
    end = createRecord("nested", createNestedField("f", Schema.Type.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("arrayRec", createArrayField("f", Schema.Type.INT));
    end = createRecord("arrayRec", createArrayField("f", Schema.Type.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("mapRec", createMapField("f", Schema.Type.INT));
    end = createRecord("mapRec", createMapField("f", Schema.Type.LONG));
    assertEquals(end, deduceWriterSchema(end, start));
  }

  @Test
  void testAllowedTypePromotions() {
    Schema.Type[] promotionTypes = new Schema.Type[]{Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING, Schema.Type.BYTES};
    Map<Schema.Type, Pair<Integer,Integer>> allowedPromotions = new HashMap<>();
    //allowedPromotions.key can be promoted to any type in the range allowedPromotions.value
    allowedPromotions.put(Schema.Type.INT, Pair.of(0, 4));
    allowedPromotions.put(Schema.Type.LONG, Pair.of(1, 4));
    allowedPromotions.put(Schema.Type.FLOAT, Pair.of(2, 4));
    allowedPromotions.put(Schema.Type.DOUBLE, Pair.of(3, 4));
    allowedPromotions.put(Schema.Type.STRING, Pair.of(4, 4));
    allowedPromotions.put(Schema.Type.BYTES, Pair.of(5, 5));

    Map<Schema.Type, Schema> schemaMap = new HashMap<>();
    for (Schema.Type type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    for (int i = 0; i < promotionTypes.length; i++) {
      Schema startSchema = schemaMap.get(promotionTypes[i]);
      Pair<Integer,Integer> minMax = allowedPromotions.get(promotionTypes[i]);
      for (int j = minMax.getLeft(); j <= minMax.getRight(); j++) {
        Schema endSchema = schemaMap.get(promotionTypes[j]);
        assertEquals(endSchema, deduceWriterSchema(endSchema, startSchema));
      }
    }
  }

  @Test
  void testReversePromotions() {
    Schema.Type[] promotionTypes = new Schema.Type[]{Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.STRING, Schema.Type.BYTES};
    Map<Schema.Type, Pair<Integer,Integer>> reversePromotions = new HashMap<>();
    //Incoming data types in the range reversePromotions.value will be promoted to reversePromotions.key
    //if reversePromotions.key is the current table schema
    reversePromotions.put(Schema.Type.INT, Pair.of(0, 0));
    reversePromotions.put(Schema.Type.LONG, Pair.of(0, 1));
    reversePromotions.put(Schema.Type.FLOAT, Pair.of(0, 2));
    reversePromotions.put(Schema.Type.DOUBLE, Pair.of(0, 3));
    reversePromotions.put(Schema.Type.STRING, Pair.of(0, 5));
    reversePromotions.put(Schema.Type.BYTES, Pair.of(4, 5));

    Map<Schema.Type, Schema> schemaMap = new HashMap<>();
    for (Schema.Type type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    for (int i = 0; i < promotionTypes.length; i++) {
      Schema startSchema = schemaMap.get(promotionTypes[i]);
      Pair<Integer,Integer> minMax = reversePromotions.get(promotionTypes[i]);
      for (int j = minMax.getLeft(); j <= minMax.getRight(); j++) {
        Schema endSchema = schemaMap.get(promotionTypes[j]);
        assertEquals(startSchema, deduceWriterSchema(endSchema, startSchema));
      }
    }
  }

  @Test
  void testIllegalPromotionsBetweenPrimitives() {
    Schema.Type[] promotionTypes = new Schema.Type[]{Schema.Type.INT, Schema.Type.LONG, Schema.Type.FLOAT, Schema.Type.DOUBLE, Schema.Type.BYTES};
    Map<Schema.Type, Schema> schemaMap = new HashMap<>();
    for (Schema.Type type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    String[] fieldNames = new String[]{"rec.simpleField", "rec.arrayField.element", "rec.mapField.value", "rec.nestedField.nested"};
    //int, long, float, double can't be promoted to bytes
    for (int i = 0; i < 4; i++) {
      Schema startSchema = schemaMap.get(promotionTypes[i]);
      Schema endSchema = schemaMap.get(Schema.Type.BYTES);
      Throwable t = assertThrows(SchemaBackwardsCompatibilityException.class,
          () -> deduceWriterSchema(endSchema, startSchema));
      String baseString = String.format("TYPE_MISMATCH: reader type 'BYTES' not compatible with writer type '%s' for field '%%s'",
          promotionTypes[i].getName().toUpperCase());
      for (String fieldName : fieldNames) {
        assertTrue(t.getMessage().contains(String.format(baseString, fieldName)));
      }
    }
  }

  @Test
  void testIllegalPromotionsBetweenComplexFields() {
    String[] typeNames = new String[]{"INT", "ARRAY", "MAP", "RECORD"};
    Schema[] fieldTypes = new Schema[]{createRecord("rec", createPrimitiveField("testField", Schema.Type.INT)),
        createRecord("rec", createArrayField("testField", Schema.Type.INT)),
        createRecord("rec", createMapField("testField", Schema.Type.INT)),
        createRecord("rec", createNestedField("testField", Schema.Type.INT))};

    for (int i = 0; i < fieldTypes.length; i++) {
      for (int j = 0; j < fieldTypes.length; j++) {
        if (i != j) {
          Schema startSchema = fieldTypes[i];
          Schema endSchema = fieldTypes[j];
          Throwable t = assertThrows(SchemaBackwardsCompatibilityException.class,
              () -> deduceWriterSchema(startSchema, endSchema));
          String errorMessage = String.format("Schema validation backwards compatibility check failed with the following issues: "
              + "{TYPE_MISMATCH: reader type '%s' not compatible with writer type '%s' for field 'rec.testField'}", typeNames[i], typeNames[j]);
          assertTrue(t.getMessage().startsWith(errorMessage));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans =  {true, false})
  void testMissingColumn(boolean allowDroppedColumns) {
    //simple case
    Schema start = createRecord("missingSimpleField",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createPrimitiveField("field3", Schema.Type.INT));
    Schema end = createRecord("missingSimpleField",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field3", Schema.Type.INT));
    try {
      Schema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      Schema expected = createRecord("missingSimpleField",
          createPrimitiveField("field1", Schema.Type.INT),
          createNullablePrimitiveField("field2", Schema.Type.INT),
          createPrimitiveField("field3", Schema.Type.INT));
      assertEquals(expected, actual);
      assertTrue(allowDroppedColumns);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowDroppedColumns);
      assertTrue(e.getMessage().contains("missingSimpleField.field2"));
    }

    //complex case
    start = createRecord("missingComplexField",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createArrayField("field3", createRecord("nestedRecord",
                createPrimitiveField("nestedField1", Schema.Type.INT),
                createPrimitiveField("nestedField2", Schema.Type.INT),
                createPrimitiveField("nestedField3", Schema.Type.INT))),
        createPrimitiveField("field4", Schema.Type.INT));
    end = createRecord("missingComplexField",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createPrimitiveField("field4", Schema.Type.INT));
    try {
      Schema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      Schema expected = createRecord("missingComplexField",
          createPrimitiveField("field1", Schema.Type.INT),
          createPrimitiveField("field2", Schema.Type.INT),
          createNullableArrayField("field3", createRecord("nestedRecord",
              createPrimitiveField("nestedField1", Schema.Type.INT),
              createPrimitiveField("nestedField2", Schema.Type.INT),
              createPrimitiveField("nestedField3", Schema.Type.INT))),
          createPrimitiveField("field4", Schema.Type.INT));
      assertEquals(expected, actual);
      assertTrue(allowDroppedColumns);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowDroppedColumns);
      assertTrue(e.getMessage().contains("missingComplexField.field3"));
    }

    //partial missing field
    end = createRecord("missingComplexField",
        createPrimitiveField("field1", Schema.Type.INT),
        createArrayField("field3", createRecord("nestedRecord",
            createPrimitiveField("nestedField2", Schema.Type.INT),
            createPrimitiveField("nestedField3", Schema.Type.INT))),
        createPrimitiveField("field4", Schema.Type.INT));
    try {
      Schema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      Schema expected = createRecord("missingComplexField",
          createPrimitiveField("field1", Schema.Type.INT),
          createNullablePrimitiveField("field2", Schema.Type.INT),
          createArrayField("field3", createRecord("nestedRecord",
              createNullablePrimitiveField("nestedField1", Schema.Type.INT),
              createPrimitiveField("nestedField2", Schema.Type.INT),
              createPrimitiveField("nestedField3", Schema.Type.INT))),
          createPrimitiveField("field4", Schema.Type.INT));
      assertEquals(expected, actual);
      assertTrue(allowDroppedColumns);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowDroppedColumns);
      assertTrue(e.getMessage().contains("missingComplexField.field3.element.nestedRecord.nestedField1"));
      assertTrue(e.getMessage().contains("missingComplexField.field2"));
    }
  }

  @Test
  void testFieldReordering() {
    // field order changes and incoming schema is missing an existing field
    Schema start = createRecord("reorderFields",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createPrimitiveField("field3", Schema.Type.INT));
    Schema end = createRecord("reorderFields",
        createPrimitiveField("field3", Schema.Type.INT),
        createPrimitiveField("field1", Schema.Type.INT));
    Schema expected = createRecord("reorderFields",
        createPrimitiveField("field1", Schema.Type.INT),
        createNullablePrimitiveField("field2", Schema.Type.INT),
        createPrimitiveField("field3", Schema.Type.INT));
    assertEquals(expected, deduceWriterSchema(end, start, true));

    // nested field ordering changes and new field is added
    start = createRecord("reorderNestedFields",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField1", Schema.Type.INT),
            createPrimitiveField("nestedField2", Schema.Type.INT),
            createPrimitiveField("nestedField3", Schema.Type.INT))),
        createPrimitiveField("field4", Schema.Type.INT));
    end = createRecord("reorderNestedFields",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createPrimitiveField("field5", Schema.Type.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField2", Schema.Type.INT),
            createPrimitiveField("nestedField1", Schema.Type.INT),
            createPrimitiveField("nestedField3", Schema.Type.INT),
            createPrimitiveField("nestedField4", Schema.Type.INT))),
        createPrimitiveField("field4", Schema.Type.INT));

    expected = createRecord("reorderNestedFields",
        createPrimitiveField("field1", Schema.Type.INT),
        createPrimitiveField("field2", Schema.Type.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField1", Schema.Type.INT),
            createPrimitiveField("nestedField2", Schema.Type.INT),
            createPrimitiveField("nestedField3", Schema.Type.INT),
            createNullablePrimitiveField("nestedField4", Schema.Type.INT))),
        createPrimitiveField("field4", Schema.Type.INT),
        createNullablePrimitiveField("field5", Schema.Type.INT));
    assertEquals(expected, deduceWriterSchema(end, start, true));
  }

  private static Schema deduceWriterSchema(Schema incomingSchema, Schema latestTableSchema) {
    return deduceWriterSchema(incomingSchema, latestTableSchema, false);
  }

  private static final TypedProperties TYPED_PROPERTIES = new TypedProperties();

  private static Schema deduceWriterSchema(Schema incomingSchema, Schema latestTableSchema, Boolean addNull) {
    TYPED_PROPERTIES.setProperty(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(), addNull.toString());

    // Convert latestTableSchema to Option<HoodieSchema>
    Option<HoodieSchema> latestTableSchemaOpt = latestTableSchema != null
        ? Option.of(HoodieSchema.fromAvroSchema(latestTableSchema))
        : Option.empty();

    // Call deduceWriterSchema and convert result back to Avro Schema
    return HoodieSchemaUtils.deduceWriterSchema(
        HoodieSchema.fromAvroSchema(incomingSchema),
        latestTableSchemaOpt,
        Option.empty(),
        TYPED_PROPERTIES
    ).toAvroSchema();
  }

}
