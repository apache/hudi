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

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchemaType}.
 */
public class TestHoodieSchemaType {
  private static final Map<HoodieSchemaType, Schema> TEST_SCHEMAS = buildSampleSchemasForType();

  @ParameterizedTest
  @EnumSource(HoodieSchemaType.class)
  public void testAvroTypeRoundTripConversion(HoodieSchemaType hoodieType) {
    // Test conversion from Hudi to Avro and back
    HoodieSchemaType convertedBack = HoodieSchemaType.fromAvro(TEST_SCHEMAS.get(hoodieType));

    assertEquals(hoodieType, convertedBack,
        "Round-trip conversion should preserve type: " + hoodieType);
  }

  @Test
  public void testFromAvroTypeWithNull() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaType.fromAvro(null);
    }, "Should throw exception for null Avro type");
  }

  @Test
  public void testPrimitiveTypes() {
    // Test primitive type identification
    assertTrue(HoodieSchemaType.STRING.isPrimitive(), "STRING should be primitive");
    assertTrue(HoodieSchemaType.INT.isPrimitive(), "INT should be primitive");
    assertTrue(HoodieSchemaType.LONG.isPrimitive(), "LONG should be primitive");
    assertTrue(HoodieSchemaType.FLOAT.isPrimitive(), "FLOAT should be primitive");
    assertTrue(HoodieSchemaType.DOUBLE.isPrimitive(), "DOUBLE should be primitive");
    assertTrue(HoodieSchemaType.BOOLEAN.isPrimitive(), "BOOLEAN should be primitive");
    assertTrue(HoodieSchemaType.BYTES.isPrimitive(), "BYTES should be primitive");
    assertTrue(HoodieSchemaType.NULL.isPrimitive(), "NULL should be primitive");
    assertTrue(HoodieSchemaType.FIXED.isPrimitive(), "FIXED should be primitive");

    assertFalse(HoodieSchemaType.RECORD.isPrimitive(), "RECORD should not be primitive");
    assertFalse(HoodieSchemaType.ENUM.isPrimitive(), "ENUM should not be primitive");
    assertFalse(HoodieSchemaType.ARRAY.isPrimitive(), "ARRAY should not be primitive");
    assertFalse(HoodieSchemaType.MAP.isPrimitive(), "MAP should not be primitive");
    assertFalse(HoodieSchemaType.UNION.isPrimitive(), "UNION should not be primitive");
  }

  @Test
  public void testComplexTypes() {
    // Test complex type identification
    assertTrue(HoodieSchemaType.RECORD.isComplex(), "RECORD should be complex");
    assertTrue(HoodieSchemaType.ENUM.isComplex(), "ENUM should be complex");
    assertTrue(HoodieSchemaType.ARRAY.isComplex(), "ARRAY should be complex");
    assertTrue(HoodieSchemaType.MAP.isComplex(), "MAP should be complex");
    assertTrue(HoodieSchemaType.UNION.isComplex(), "UNION should be complex");

    assertFalse(HoodieSchemaType.STRING.isComplex(), "STRING should not be complex");
    assertFalse(HoodieSchemaType.INT.isComplex(), "INT should not be complex");
    assertFalse(HoodieSchemaType.LONG.isComplex(), "LONG should not be complex");
    assertFalse(HoodieSchemaType.FLOAT.isComplex(), "FLOAT should not be complex");
    assertFalse(HoodieSchemaType.DOUBLE.isComplex(), "DOUBLE should not be complex");
    assertFalse(HoodieSchemaType.BOOLEAN.isComplex(), "BOOLEAN should not be complex");
    assertFalse(HoodieSchemaType.BYTES.isComplex(), "BYTES should not be complex");
    assertFalse(HoodieSchemaType.NULL.isComplex(), "NULL should not be complex");
    assertFalse(HoodieSchemaType.FIXED.isComplex(), "FIXED should not be complex");
  }

  @Test
  public void testNumericTypes() {
    // Test numeric type identification
    assertTrue(HoodieSchemaType.INT.isNumeric(), "INT should be numeric");
    assertTrue(HoodieSchemaType.LONG.isNumeric(), "LONG should be numeric");
    assertTrue(HoodieSchemaType.FLOAT.isNumeric(), "FLOAT should be numeric");
    assertTrue(HoodieSchemaType.DOUBLE.isNumeric(), "DOUBLE should be numeric");

    assertFalse(HoodieSchemaType.STRING.isNumeric(), "STRING should not be numeric");
    assertFalse(HoodieSchemaType.BOOLEAN.isNumeric(), "BOOLEAN should not be numeric");
    assertFalse(HoodieSchemaType.BYTES.isNumeric(), "BYTES should not be numeric");
    assertFalse(HoodieSchemaType.NULL.isNumeric(), "NULL should not be numeric");
    assertFalse(HoodieSchemaType.FIXED.isNumeric(), "FIXED should not be numeric");
    assertFalse(HoodieSchemaType.RECORD.isNumeric(), "RECORD should not be numeric");
    assertFalse(HoodieSchemaType.ENUM.isNumeric(), "ENUM should not be numeric");
    assertFalse(HoodieSchemaType.ARRAY.isNumeric(), "ARRAY should not be numeric");
    assertFalse(HoodieSchemaType.MAP.isNumeric(), "MAP should not be numeric");
    assertFalse(HoodieSchemaType.UNION.isNumeric(), "UNION should not be numeric");
  }

  @Test
  public void testTypeClassification() {
    // Verify that primitive and complex are mutually exclusive and complete
    for (HoodieSchemaType type : HoodieSchemaType.values()) {
      assertTrue(type.isPrimitive() ^ type.isComplex(),
          "Type should be either primitive or complex, but not both: " + type);
    }
  }

  @Test
  public void testSpecificTypeMappings() {
    // Test specific known mappings
    assertEquals(Schema.Type.STRING, HoodieSchemaType.STRING.toAvroType());
    assertEquals(Schema.Type.INT, HoodieSchemaType.INT.toAvroType());
    assertEquals(Schema.Type.LONG, HoodieSchemaType.LONG.toAvroType());
    assertEquals(Schema.Type.FLOAT, HoodieSchemaType.FLOAT.toAvroType());
    assertEquals(Schema.Type.DOUBLE, HoodieSchemaType.DOUBLE.toAvroType());
    assertEquals(Schema.Type.BOOLEAN, HoodieSchemaType.BOOLEAN.toAvroType());
    assertEquals(Schema.Type.BYTES, HoodieSchemaType.BYTES.toAvroType());
    assertEquals(Schema.Type.NULL, HoodieSchemaType.NULL.toAvroType());
    assertEquals(Schema.Type.RECORD, HoodieSchemaType.RECORD.toAvroType());
    assertEquals(Schema.Type.ENUM, HoodieSchemaType.ENUM.toAvroType());
    assertEquals(Schema.Type.ARRAY, HoodieSchemaType.ARRAY.toAvroType());
    assertEquals(Schema.Type.MAP, HoodieSchemaType.MAP.toAvroType());
    assertEquals(Schema.Type.UNION, HoodieSchemaType.UNION.toAvroType());
    assertEquals(Schema.Type.FIXED, HoodieSchemaType.FIXED.toAvroType());
  }

  @Test
  public void testFromAvro() {
    // Test reverse mappings
    assertEquals(HoodieSchemaType.STRING, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.STRING)));
    assertEquals(HoodieSchemaType.INT, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.INT)));
    assertEquals(HoodieSchemaType.LONG, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.LONG)));
    assertEquals(HoodieSchemaType.FLOAT, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.FLOAT)));
    assertEquals(HoodieSchemaType.DOUBLE, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.DOUBLE)));
    assertEquals(HoodieSchemaType.BOOLEAN, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.BOOLEAN)));
    assertEquals(HoodieSchemaType.BYTES, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.BYTES)));
    assertEquals(HoodieSchemaType.NULL, HoodieSchemaType.fromAvro(Schema.create(Schema.Type.NULL)));
    assertEquals(HoodieSchemaType.RECORD, HoodieSchemaType.fromAvro(Schema.createRecord("record", null, null, false)));
    assertEquals(HoodieSchemaType.ENUM, HoodieSchemaType.fromAvro(Schema.createEnum("enum", null, null, Arrays.asList("A", "B", "C"))));
    assertEquals(HoodieSchemaType.ARRAY, HoodieSchemaType.fromAvro(Schema.createArray(Schema.create(Schema.Type.STRING))));
    assertEquals(HoodieSchemaType.MAP, HoodieSchemaType.fromAvro(Schema.createMap(Schema.create(Schema.Type.STRING))));
    assertEquals(HoodieSchemaType.UNION, HoodieSchemaType.fromAvro(Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT))));
    assertEquals(HoodieSchemaType.FIXED, HoodieSchemaType.fromAvro(Schema.createFixed("fixed", null, null, 10)));
    assertEquals(HoodieSchemaType.TIMESTAMP, HoodieSchemaType.fromAvro(LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    assertEquals(HoodieSchemaType.TIMESTAMP, HoodieSchemaType.fromAvro(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    assertEquals(HoodieSchemaType.TIMESTAMP, HoodieSchemaType.fromAvro(LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    assertEquals(HoodieSchemaType.TIMESTAMP, HoodieSchemaType.fromAvro(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))));
    assertEquals(HoodieSchemaType.TIME, HoodieSchemaType.fromAvro(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))));
    assertEquals(HoodieSchemaType.TIME, HoodieSchemaType.fromAvro(LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT))));
    assertEquals(HoodieSchemaType.DECIMAL, HoodieSchemaType.fromAvro(LogicalTypes.decimal(10, 5).addToSchema(Schema.create(Schema.Type.BYTES))));
    assertEquals(HoodieSchemaType.DATE, HoodieSchemaType.fromAvro(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))));
    assertEquals(HoodieSchemaType.UUID, HoodieSchemaType.fromAvro(LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING))));
  }

  private static Map<HoodieSchemaType, Schema> buildSampleSchemasForType() {
    Map<HoodieSchemaType, Schema> map = new EnumMap<>(HoodieSchemaType.class);
    // Standard types
    map.put(HoodieSchemaType.STRING, Schema.create(Schema.Type.STRING));
    map.put(HoodieSchemaType.INT, Schema.create(Schema.Type.INT));
    map.put(HoodieSchemaType.LONG, Schema.create(Schema.Type.LONG));
    map.put(HoodieSchemaType.FLOAT, Schema.create(Schema.Type.FLOAT));
    map.put(HoodieSchemaType.DOUBLE, Schema.create(Schema.Type.DOUBLE));
    map.put(HoodieSchemaType.BOOLEAN, Schema.create(Schema.Type.BOOLEAN));
    map.put(HoodieSchemaType.BYTES, Schema.create(Schema.Type.BYTES));
    map.put(HoodieSchemaType.NULL, Schema.create(Schema.Type.NULL));
    map.put(HoodieSchemaType.RECORD, Schema.createRecord("record", null, null, false));
    map.put(HoodieSchemaType.ENUM, Schema.createEnum("enum", null, null, Arrays.asList("A", "B", "C")));
    map.put(HoodieSchemaType.ARRAY, Schema.createArray(Schema.create(Schema.Type.STRING)));
    map.put(HoodieSchemaType.MAP, Schema.createMap(Schema.create(Schema.Type.STRING)));
    map.put(HoodieSchemaType.UNION, Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.INT)));
    map.put(HoodieSchemaType.FIXED, Schema.createFixed("fixed", null, null, 10));
    // Logical types
    map.put(HoodieSchemaType.TIMESTAMP,
        LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)));
    map.put(HoodieSchemaType.TIME,
        LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)));
    map.put(HoodieSchemaType.DECIMAL,
        LogicalTypes.decimal(10, 5).addToSchema(Schema.create(Schema.Type.BYTES)));
    map.put(HoodieSchemaType.DATE,
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)));
    map.put(HoodieSchemaType.UUID,
        LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)));
    return map;
  }
}
