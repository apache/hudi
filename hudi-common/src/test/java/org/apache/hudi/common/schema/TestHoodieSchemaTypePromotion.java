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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the internal promotion matrix used by schema compatibility checks.
 */
public class TestHoodieSchemaTypePromotion {

  @Test
  public void testSameTypeAlwaysPromotable() {
    for (HoodieSchemaType type : HoodieSchemaType.values()) {
      assertTrue(HoodieSchemaTypePromotion.canPromote(type, type),
          "same type should promote to itself: " + type);
    }
  }

  @Test
  public void testIntWidening() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.LONG, HoodieSchemaType.INT));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.FLOAT, HoodieSchemaType.INT));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.DOUBLE, HoodieSchemaType.INT));
  }

  @Test
  public void testLongWidening() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.FLOAT, HoodieSchemaType.LONG));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.DOUBLE, HoodieSchemaType.LONG));
    // LONG cannot read a wider reader-narrows case
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.LONG, HoodieSchemaType.FLOAT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.LONG, HoodieSchemaType.DOUBLE));
  }

  @Test
  public void testFloatWidening() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.DOUBLE, HoodieSchemaType.FLOAT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.FLOAT, HoodieSchemaType.DOUBLE));
  }

  @Test
  public void testNarrowingNotAllowed() {
    // reader cannot narrow: long data cannot be read by an int reader, etc.
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.LONG));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.FLOAT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.DOUBLE));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.LONG, HoodieSchemaType.DOUBLE));
    // FLOAT reader CAN read LONG writer (Avro promotion allows this despite the mantissa
    // precision loss). Asserted here as canonical to guard against regressions.
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.FLOAT, HoodieSchemaType.LONG));
  }

  @Test
  public void testStringBytesBidirectional() {
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.STRING, HoodieSchemaType.BYTES));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.BYTES, HoodieSchemaType.STRING));
    // STRING can also read numeric writer types
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.STRING, HoodieSchemaType.INT));
    assertTrue(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.STRING, HoodieSchemaType.DOUBLE));
    // BYTES cannot read numeric types
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.BYTES, HoodieSchemaType.INT));
  }

  @Test
  public void testUnrelatedTypesNotPromotable() {
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.BOOLEAN, HoodieSchemaType.INT));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.INT, HoodieSchemaType.BOOLEAN));
    assertFalse(HoodieSchemaTypePromotion.canPromote(HoodieSchemaType.LONG, HoodieSchemaType.STRING));
  }

  @Test
  public void testDecimalWideningSameSizeIncreasedPrecision() {
    HoodieSchema writer = fixedDecimal(8, 10, 2);
    HoodieSchema reader = fixedDecimal(8, 15, 2);
    assertTrue(HoodieSchemaTypePromotion.isDecimalWidening(reader, writer));
  }

  @Test
  public void testDecimalWideningIdenticalIsAllowed() {
    HoodieSchema writer = fixedDecimal(8, 10, 2);
    HoodieSchema reader = fixedDecimal(8, 10, 2);
    assertTrue(HoodieSchemaTypePromotion.isDecimalWidening(reader, writer));
  }

  @Test
  public void testDecimalWideningRejectsDecreasedPrecision() {
    HoodieSchema writer = fixedDecimal(8, 15, 2);
    HoodieSchema reader = fixedDecimal(8, 10, 2);
    assertFalse(HoodieSchemaTypePromotion.isDecimalWidening(reader, writer));
  }

  @Test
  public void testDecimalWideningRejectsIncreasedScaleWithoutRoom() {
    // integer digits shrink from 8 to 5, so widening is invalid
    HoodieSchema writer = fixedDecimal(8, 10, 2);
    HoodieSchema reader = fixedDecimal(8, 10, 5);
    assertFalse(HoodieSchemaTypePromotion.isDecimalWidening(reader, writer));
  }

  @Test
  public void testDecimalWideningRejectsDifferentFixedSize() {
    HoodieSchema writer = fixedDecimal(8, 10, 2);
    HoodieSchema reader = fixedDecimal(16, 10, 2);
    assertFalse(HoodieSchemaTypePromotion.isDecimalWidening(reader, writer));
  }

  @Test
  public void testDecimalWideningRejectsNonDecimal() {
    HoodieSchema decimal = fixedDecimal(8, 10, 2);
    HoodieSchema plainInt = HoodieSchema.create(HoodieSchemaType.INT);
    assertFalse(HoodieSchemaTypePromotion.isDecimalWidening(decimal, plainInt));
    assertFalse(HoodieSchemaTypePromotion.isDecimalWidening(plainInt, decimal));
  }

  private static HoodieSchema fixedDecimal(int size, int precision, int scale) {
    return HoodieSchema.createDecimal("FixedDecimal", null, null, precision, scale, size);
  }
}
