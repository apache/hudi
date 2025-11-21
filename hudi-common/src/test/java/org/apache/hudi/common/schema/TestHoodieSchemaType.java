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

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Test class for {@link HoodieSchemaType}.
 */
public class TestHoodieSchemaType {

  @ParameterizedTest
  @EnumSource(HoodieSchemaType.class)
  public void testAvroTypeRoundTripConversion(HoodieSchemaType hoodieType) {
    // Test conversion from Hudi to Avro and back
    Schema.Type avroType = hoodieType.toAvroType();
    HoodieSchemaType convertedBack = HoodieSchemaType.fromAvroType(avroType);

    assertEquals(hoodieType, convertedBack,
        "Round-trip conversion should preserve type: " + hoodieType);
  }

  @ParameterizedTest
  @EnumSource(Schema.Type.class)
  public void testFromAvroTypeMapping(Schema.Type avroType) {
    // Test that all Avro types can be converted to Hudi types
    HoodieSchemaType hoodieType = HoodieSchemaType.fromAvroType(avroType);
    Schema.Type convertedBack = hoodieType.toAvroType();

    assertEquals(avroType, convertedBack,
        "Avro type conversion should be consistent: " + avroType);
  }

  @Test
  public void testFromAvroTypeWithNull() {
    assertThrows(IllegalArgumentException.class, () -> {
      HoodieSchemaType.fromAvroType(null);
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
  public void testFromAvroTypeSpecificMappings() {
    // Test reverse mappings
    assertEquals(HoodieSchemaType.STRING, HoodieSchemaType.fromAvroType(Schema.Type.STRING));
    assertEquals(HoodieSchemaType.INT, HoodieSchemaType.fromAvroType(Schema.Type.INT));
    assertEquals(HoodieSchemaType.LONG, HoodieSchemaType.fromAvroType(Schema.Type.LONG));
    assertEquals(HoodieSchemaType.FLOAT, HoodieSchemaType.fromAvroType(Schema.Type.FLOAT));
    assertEquals(HoodieSchemaType.DOUBLE, HoodieSchemaType.fromAvroType(Schema.Type.DOUBLE));
    assertEquals(HoodieSchemaType.BOOLEAN, HoodieSchemaType.fromAvroType(Schema.Type.BOOLEAN));
    assertEquals(HoodieSchemaType.BYTES, HoodieSchemaType.fromAvroType(Schema.Type.BYTES));
    assertEquals(HoodieSchemaType.NULL, HoodieSchemaType.fromAvroType(Schema.Type.NULL));
    assertEquals(HoodieSchemaType.RECORD, HoodieSchemaType.fromAvroType(Schema.Type.RECORD));
    assertEquals(HoodieSchemaType.ENUM, HoodieSchemaType.fromAvroType(Schema.Type.ENUM));
    assertEquals(HoodieSchemaType.ARRAY, HoodieSchemaType.fromAvroType(Schema.Type.ARRAY));
    assertEquals(HoodieSchemaType.MAP, HoodieSchemaType.fromAvroType(Schema.Type.MAP));
    assertEquals(HoodieSchemaType.UNION, HoodieSchemaType.fromAvroType(Schema.Type.UNION));
    assertEquals(HoodieSchemaType.FIXED, HoodieSchemaType.fromAvroType(Schema.Type.FIXED));
  }
}
