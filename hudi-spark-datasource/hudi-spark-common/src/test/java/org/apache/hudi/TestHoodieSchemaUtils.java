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
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieNullSchemaTypeException;
import org.apache.hudi.exception.MissingSchemaFieldException;
import org.apache.hudi.exception.SchemaBackwardsCompatibilityException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.Map;

import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createArrayField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createMapField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNestedField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullableArrayField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createNullablePrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createPrimitiveField;
import static org.apache.hudi.common.schema.HoodieSchemaTestUtils.createRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestHoodieSchemaUtils {

  @Test
  void testSchemaWithNullField() {
    HoodieSchema withNullfield = createRecord("nullRecord", createPrimitiveField("nullField", HoodieSchemaType.NULL));
    assertThrows(HoodieNullSchemaTypeException.class,
        () -> deduceWriterSchema(withNullfield, null));
  }

  @Test
  void testSimplePromotionWithComplexFields() {
    HoodieSchema start = createRecord("simple", createPrimitiveField("f", HoodieSchemaType.INT));
    HoodieSchema end = createRecord("simple", createPrimitiveField("f", HoodieSchemaType.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("nested", createNestedField("f", HoodieSchemaType.INT));
    end = createRecord("nested", createNestedField("f", HoodieSchemaType.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("arrayRec", createArrayField("f", HoodieSchemaType.INT));
    end = createRecord("arrayRec", createArrayField("f", HoodieSchemaType.LONG));
    assertEquals(end, deduceWriterSchema(end, start));

    start = createRecord("mapRec", createMapField("f", HoodieSchemaType.INT));
    end = createRecord("mapRec", createMapField("f", HoodieSchemaType.LONG));
    assertEquals(end, deduceWriterSchema(end, start));
  }

  @Test
  void testAllowedTypePromotions() {
    HoodieSchemaType[] promotionTypes =
            new HoodieSchemaType[]{HoodieSchemaType.INT, HoodieSchemaType.LONG, HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE, HoodieSchemaType.STRING, HoodieSchemaType.BYTES};
    Map<HoodieSchemaType, Pair<Integer,Integer>> allowedPromotions = new HashMap<>();
    //allowedPromotions.key can be promoted to any type in the range allowedPromotions.value
    allowedPromotions.put(HoodieSchemaType.INT, Pair.of(0, 4));
    allowedPromotions.put(HoodieSchemaType.LONG, Pair.of(1, 4));
    allowedPromotions.put(HoodieSchemaType.FLOAT, Pair.of(2, 4));
    allowedPromotions.put(HoodieSchemaType.DOUBLE, Pair.of(3, 4));
    allowedPromotions.put(HoodieSchemaType.STRING, Pair.of(4, 4));
    allowedPromotions.put(HoodieSchemaType.BYTES, Pair.of(5, 5));

    Map<HoodieSchemaType, HoodieSchema> schemaMap = new HashMap<>();
    for (HoodieSchemaType type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    for (int i = 0; i < promotionTypes.length; i++) {
      HoodieSchema startSchema = schemaMap.get(promotionTypes[i]);
      Pair<Integer,Integer> minMax = allowedPromotions.get(promotionTypes[i]);
      for (int j = minMax.getLeft(); j <= minMax.getRight(); j++) {
        HoodieSchema endSchema = schemaMap.get(promotionTypes[j]);
        assertEquals(endSchema, deduceWriterSchema(endSchema, startSchema));
      }
    }
  }

  @Test
  void testReversePromotions() {
    HoodieSchemaType[] promotionTypes =
            new HoodieSchemaType[]{HoodieSchemaType.INT, HoodieSchemaType.LONG, HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE, HoodieSchemaType.STRING, HoodieSchemaType.BYTES};
    Map<HoodieSchemaType, Pair<Integer,Integer>> reversePromotions = new HashMap<>();
    //Incoming data types in the range reversePromotions.value will be promoted to reversePromotions.key
    //if reversePromotions.key is the current table schema
    reversePromotions.put(HoodieSchemaType.INT, Pair.of(0, 0));
    reversePromotions.put(HoodieSchemaType.LONG, Pair.of(0, 1));
    reversePromotions.put(HoodieSchemaType.FLOAT, Pair.of(0, 2));
    reversePromotions.put(HoodieSchemaType.DOUBLE, Pair.of(0, 3));
    reversePromotions.put(HoodieSchemaType.STRING, Pair.of(0, 5));
    reversePromotions.put(HoodieSchemaType.BYTES, Pair.of(4, 5));

    Map<HoodieSchemaType, HoodieSchema> schemaMap = new HashMap<>();
    for (HoodieSchemaType type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    for (int i = 0; i < promotionTypes.length; i++) {
      HoodieSchema startSchema = schemaMap.get(promotionTypes[i]);
      Pair<Integer,Integer> minMax = reversePromotions.get(promotionTypes[i]);
      for (int j = minMax.getLeft(); j <= minMax.getRight(); j++) {
        HoodieSchema endSchema = schemaMap.get(promotionTypes[j]);
        assertEquals(startSchema, deduceWriterSchema(endSchema, startSchema));
      }
    }
  }

  @Test
  void testIllegalPromotionsBetweenPrimitives() {
    HoodieSchemaType[] promotionTypes = new HoodieSchemaType[]{HoodieSchemaType.INT, HoodieSchemaType.LONG, HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE, HoodieSchemaType.BYTES};
    Map<HoodieSchemaType, HoodieSchema> schemaMap = new HashMap<>();
    for (HoodieSchemaType type : promotionTypes) {
      schemaMap.put(type, createRecord("rec",
          createPrimitiveField("simpleField", type),
          createArrayField("arrayField", type),
          createMapField("mapField", type),
          createNestedField("nestedField", type)));
    }

    String[] fieldNames = new String[]{"rec.simpleField", "rec.arrayField.element", "rec.mapField.value", "rec.nestedField.nested"};
    //int, long, float, double can't be promoted to bytes
    for (int i = 0; i < 4; i++) {
      HoodieSchema startSchema = schemaMap.get(promotionTypes[i]);
      HoodieSchema endSchema = schemaMap.get(HoodieSchemaType.BYTES);
      Throwable t = assertThrows(SchemaBackwardsCompatibilityException.class,
          () -> deduceWriterSchema(endSchema, startSchema));
      String baseString = String.format("TYPE_MISMATCH: reader type 'BYTES' not compatible with writer type '%s' for field '%%s'",
          promotionTypes[i].name().toUpperCase());
      for (String fieldName : fieldNames) {
        assertTrue(t.getMessage().contains(String.format(baseString, fieldName)));
      }
    }
  }

  @Test
  void testIllegalPromotionsBetweenComplexFields() {
    String[] typeNames = new String[]{"INT", "ARRAY", "MAP", "RECORD"};
    HoodieSchema[] fieldTypes = new HoodieSchema[]{createRecord("rec", createPrimitiveField("testField", HoodieSchemaType.INT)),
        createRecord("rec", createArrayField("testField", HoodieSchemaType.INT)),
        createRecord("rec", createMapField("testField", HoodieSchemaType.INT)),
        createRecord("rec", createNestedField("testField", HoodieSchemaType.INT))};

    for (int i = 0; i < fieldTypes.length; i++) {
      for (int j = 0; j < fieldTypes.length; j++) {
        if (i != j) {
          HoodieSchema startSchema = fieldTypes[i];
          HoodieSchema endSchema = fieldTypes[j];
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
    HoodieSchema start = createRecord("missingSimpleField",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createPrimitiveField("field3", HoodieSchemaType.INT));
    HoodieSchema end = createRecord("missingSimpleField",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field3", HoodieSchemaType.INT));
    try {
      HoodieSchema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      HoodieSchema expected = createRecord("missingSimpleField",
          createPrimitiveField("field1", HoodieSchemaType.INT),
          createNullablePrimitiveField("field2", HoodieSchemaType.INT),
          createPrimitiveField("field3", HoodieSchemaType.INT));
      assertEquals(expected, actual);
      assertTrue(allowDroppedColumns);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowDroppedColumns);
      assertTrue(e.getMessage().contains("missingSimpleField.field2"));
    }

    //complex case
    start = createRecord("missingComplexField",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createArrayField("field3", createRecord("nestedRecord",
                createPrimitiveField("nestedField1", HoodieSchemaType.INT),
                createPrimitiveField("nestedField2", HoodieSchemaType.INT),
                createPrimitiveField("nestedField3", HoodieSchemaType.INT))),
        createPrimitiveField("field4", HoodieSchemaType.INT));
    end = createRecord("missingComplexField",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createPrimitiveField("field4", HoodieSchemaType.INT));
    try {
      HoodieSchema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      HoodieSchema expected = createRecord("missingComplexField",
          createPrimitiveField("field1", HoodieSchemaType.INT),
          createPrimitiveField("field2", HoodieSchemaType.INT),
          createNullableArrayField("field3", createRecord("nestedRecord",
              createPrimitiveField("nestedField1", HoodieSchemaType.INT),
              createPrimitiveField("nestedField2", HoodieSchemaType.INT),
              createPrimitiveField("nestedField3", HoodieSchemaType.INT))),
          createPrimitiveField("field4", HoodieSchemaType.INT));
      assertEquals(expected, actual);
      assertTrue(allowDroppedColumns);
    } catch (MissingSchemaFieldException e) {
      assertFalse(allowDroppedColumns);
      assertTrue(e.getMessage().contains("missingComplexField.field3"));
    }

    //partial missing field
    end = createRecord("missingComplexField",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createArrayField("field3", createRecord("nestedRecord",
            createPrimitiveField("nestedField2", HoodieSchemaType.INT),
            createPrimitiveField("nestedField3", HoodieSchemaType.INT))),
        createPrimitiveField("field4", HoodieSchemaType.INT));
    try {
      HoodieSchema actual = deduceWriterSchema(end, start, allowDroppedColumns);
      HoodieSchema expected = createRecord("missingComplexField",
          createPrimitiveField("field1", HoodieSchemaType.INT),
          createNullablePrimitiveField("field2", HoodieSchemaType.INT),
          createArrayField("field3", createRecord("nestedRecord",
              createNullablePrimitiveField("nestedField1", HoodieSchemaType.INT),
              createPrimitiveField("nestedField2", HoodieSchemaType.INT),
              createPrimitiveField("nestedField3", HoodieSchemaType.INT))),
          createPrimitiveField("field4", HoodieSchemaType.INT));
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
    HoodieSchema start = createRecord("reorderFields",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createPrimitiveField("field3", HoodieSchemaType.INT));
    HoodieSchema end = createRecord("reorderFields",
        createPrimitiveField("field3", HoodieSchemaType.INT),
        createPrimitiveField("field1", HoodieSchemaType.INT));
    HoodieSchema expected = createRecord("reorderFields",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createNullablePrimitiveField("field2", HoodieSchemaType.INT),
        createPrimitiveField("field3", HoodieSchemaType.INT));
    assertEquals(expected, deduceWriterSchema(end, start, true));

    // nested field ordering changes and new field is added
    start = createRecord("reorderNestedFields",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField1", HoodieSchemaType.INT),
            createPrimitiveField("nestedField2", HoodieSchemaType.INT),
            createPrimitiveField("nestedField3", HoodieSchemaType.INT))),
        createPrimitiveField("field4", HoodieSchemaType.INT));
    end = createRecord("reorderNestedFields",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createPrimitiveField("field5", HoodieSchemaType.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField2", HoodieSchemaType.INT),
            createPrimitiveField("nestedField1", HoodieSchemaType.INT),
            createPrimitiveField("nestedField3", HoodieSchemaType.INT),
            createPrimitiveField("nestedField4", HoodieSchemaType.INT))),
        createPrimitiveField("field4", HoodieSchemaType.INT));

    expected = createRecord("reorderNestedFields",
        createPrimitiveField("field1", HoodieSchemaType.INT),
        createPrimitiveField("field2", HoodieSchemaType.INT),
        createArrayField("field3", createRecord("reorderNestedFields.field3",
            createPrimitiveField("nestedField1", HoodieSchemaType.INT),
            createPrimitiveField("nestedField2", HoodieSchemaType.INT),
            createPrimitiveField("nestedField3", HoodieSchemaType.INT),
            createNullablePrimitiveField("nestedField4", HoodieSchemaType.INT))),
        createPrimitiveField("field4", HoodieSchemaType.INT),
        createNullablePrimitiveField("field5", HoodieSchemaType.INT));
    assertEquals(expected, deduceWriterSchema(end, start, true));
  }

  private static HoodieSchema deduceWriterSchema(HoodieSchema incomingSchema, HoodieSchema latestTableSchema) {
    return deduceWriterSchema(incomingSchema, latestTableSchema, false);
  }

  private static final TypedProperties TYPED_PROPERTIES = new TypedProperties();

  private static HoodieSchema deduceWriterSchema(HoodieSchema incomingSchema, HoodieSchema latestTableSchema, Boolean addNull) {
    TYPED_PROPERTIES.setProperty(HoodieCommonConfig.SET_NULL_FOR_MISSING_COLUMNS.key(), addNull.toString());

    // Call deduceWriterSchema directly with HoodieSchema - no conversion needed
    return HoodieSchemaUtils.deduceWriterSchema(
        incomingSchema,
        Option.ofNullable(latestTableSchema),
        Option.empty(),
        TYPED_PROPERTIES
    );
  }

}
