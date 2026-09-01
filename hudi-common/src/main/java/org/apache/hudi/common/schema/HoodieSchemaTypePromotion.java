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
 * The single table of primitive widening promotions, used by {@link HoodieSchemaProjectionChecker} and, for the
 * primitive cases, by {@link HoodieSchemaCompatibilityChecker}.
 *
 * <p>A promotion lets a reader schema with a wider type read data written with a narrower one:</p>
 * <ul>
 *   <li>INT -&gt; LONG, FLOAT, DOUBLE</li>
 *   <li>LONG -&gt; FLOAT, DOUBLE</li>
 *   <li>FLOAT -&gt; DOUBLE</li>
 *   <li>STRING &lt;-&gt; BYTES (bidirectional)</li>
 *   <li>STRING &lt;- any numeric type</li>
 *   <li>decimal widening, see {@link #isDecimalWidening(HoodieSchema, HoodieSchema)}</li>
 * </ul>
 *
 * <p>Logical-type-over-primitive promotions are deliberately NOT in this table. A TIMESTAMP reader over a
 * LONG writer, or a UUID reader over a STRING writer, is accepted by
 * {@link HoodieSchemaCompatibilityChecker} for reader/writer compatibility, but it must not make a bare
 * long a "compatible projection" of a timestamp: writer-schema deduction would then silently drop the
 * logical type. Compatibility and projection are different questions, so they use different tables.</p>
 *
 * <p>One more difference is documented rather than resolved:
 * {@link #isDecimalWidening(HoodieSchema, HoodieSchema)} additionally requires the same backing (fixed
 * versus bytes) and, for fixed, an equal fixed size, whereas the decimal check in
 * {@code HoodieSchemaCompatibilityChecker} compares only precision and scale.</p>
 *
 * <p>This class is package-private and used only by {@link HoodieSchemaProjectionChecker}.</p>
 */
class HoodieSchemaTypePromotion {

  // Prevent instantiation
  private HoodieSchemaTypePromotion() {
  }

  /**
   * Checks if the reader type can be promoted from the writer type.
   * This allows type widening (e.g., int -&gt; long) but not narrowing.
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
        return writerType == HoodieSchemaType.BYTES || writerType.isNumeric();

      case BYTES:
        // BYTES can read STRING
        return writerType == HoodieSchemaType.STRING;

      default:
        return false;
    }
  }

  /**
   * Checks if decimal schema widening is valid between reader and writer schemas.
   *
   * <p>Decimal widening is valid if:
   * <ul>
   *   <li>Both schemas are decimals with the same underlying type (FIXED or BYTES)</li>
   *   <li>Reader precision and scale are equal or wider than writer's</li>
   *   <li>Specifically: (readerPrecision - readerScale) &gt;= (writerPrecision - writerScale)</li>
   *   <li>And: readerScale &gt;= writerScale</li>
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
