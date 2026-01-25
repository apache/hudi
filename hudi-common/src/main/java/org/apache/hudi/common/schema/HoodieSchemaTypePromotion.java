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

/**
 * Defines type promotion rules for HoodieSchema compatibility checking.
 *
 * <p>Type promotion allows a reader schema with a "wider" type to read data
 * written with a "narrower" type. This follows Avro's type promotion rules.</p>
 *
 * <p>Supported promotions:
 * <ul>
 *   <li>INT → LONG, FLOAT, DOUBLE</li>
 *   <li>LONG → FLOAT, DOUBLE</li>
 *   <li>FLOAT → DOUBLE</li>
 *   <li>STRING ↔ BYTES (bidirectional)</li>
 *   <li>Decimal precision widening: (p2-s2) ≥ (p1-s1) and s2 ≥ s1</li>
 * </ul>
 * </p>
 *
 * <p>This class is package-private and used internally by schema compatibility checkers.</p>
 */
class HoodieSchemaTypePromotion {

  // Prevent instantiation
  private HoodieSchemaTypePromotion() {
  }

  /**
   * Checks if the reader type can be promoted from the writer type.
   * This allows type widening (e.g., int → long) but not narrowing.
   *
   * @param readerType the type in the reader schema
   * @param writerType the type in the writer schema
   * @return true if readerType can read values of writerType through promotion
   */
  static boolean canPromote(HoodieSchemaType readerType, HoodieSchemaType writerType) {
    if (readerType == writerType) {
      return true;
    }

    switch (readerType) {
      case LONG:
        // LONG can read INT
        return writerType == HoodieSchemaType.INT;

      case FLOAT:
        // FLOAT can read INT, LONG
        return writerType == HoodieSchemaType.INT || writerType == HoodieSchemaType.LONG;

      case DOUBLE:
        // DOUBLE can read INT, LONG, FLOAT
        return writerType == HoodieSchemaType.INT
            || writerType == HoodieSchemaType.LONG
            || writerType == HoodieSchemaType.FLOAT;

      case STRING:
        // STRING can read BYTES and numeric types
        return writerType == HoodieSchemaType.BYTES
            || isNumericType(writerType);

      case BYTES:
        // BYTES can read STRING
        return writerType == HoodieSchemaType.STRING;

      default:
        return false;
    }
  }

  /**
   * Checks if the given type is numeric.
   *
   * @param type the schema type to check
   * @return true if the type is INT, LONG, FLOAT, or DOUBLE
   */
  private static boolean isNumericType(HoodieSchemaType type) {
    return type == HoodieSchemaType.INT
        || type == HoodieSchemaType.LONG
        || type == HoodieSchemaType.FLOAT
        || type == HoodieSchemaType.DOUBLE;
  }

  /**
   * Checks if decimal schema widening is valid between reader and writer schemas.
   *
   * <p>Decimal widening is valid if:
   * <ul>
   *   <li>Both schemas are decimals with the same underlying type (FIXED or BYTES)</li>
   *   <li>Reader precision and scale are equal or wider than writer's</li>
   *   <li>Specifically: (readerPrecision - readerScale) ≥ (writerPrecision - writerScale)</li>
   *   <li>And: readerScale ≥ writerScale</li>
   * </ul>
   * </p>
   *
   * @param readerSchema the reader schema (must be DECIMAL type)
   * @param writerSchema the writer schema (must be DECIMAL type)
   * @return true if the reader decimal can read the writer decimal
   */
  static boolean isDecimalWidening(HoodieSchema readerSchema, HoodieSchema writerSchema) {
    if (readerSchema.getType() != HoodieSchemaType.DECIMAL || writerSchema.getType() != HoodieSchemaType.DECIMAL) {
      return false;
    }

    HoodieSchema.Decimal readerDecimal = (HoodieSchema.Decimal) readerSchema;
    HoodieSchema.Decimal writerDecimal = (HoodieSchema.Decimal) writerSchema;

    // Both must use the same underlying representation (FIXED vs BYTES)
    if (readerDecimal.isFixed() != writerDecimal.isFixed()) {
      return false;
    }

    // If both use FIXED, they must have the same size
    if (readerDecimal.isFixed() && readerDecimal.getFixedSize() != writerDecimal.getFixedSize()) {
      return false;
    }

    int readerPrecision = readerDecimal.getPrecision();
    int readerScale = readerDecimal.getScale();
    int writerPrecision = writerDecimal.getPrecision();
    int writerScale = writerDecimal.getScale();

    // Same precision and scale is always compatible
    if (readerPrecision == writerPrecision && readerScale == writerScale) {
      return true;
    }

    // Check widening rules:
    // 1. Reader scale must be >= writer scale
    // 2. Reader's integer digits must be >= writer's integer digits
    //    (precision - scale) represents the number of integer digits
    return (readerPrecision - readerScale) >= (writerPrecision - writerScale)
        && readerScale >= writerScale;
  }
}
