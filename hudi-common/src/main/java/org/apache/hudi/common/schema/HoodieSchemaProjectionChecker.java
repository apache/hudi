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

import org.apache.hudi.common.util.Option;

import java.util.List;

/**
 * Validates schema projection relationships for HoodieSchema.
 *
 * <p>This class checks if one schema is a projection of another schema.
 * A projection means that the target schema can be constructed from the source schema,
 * possibly with fewer fields or compatible type transformations.</p>
 *
 * <p>Two types of projections are supported:
 * <ul>
 *   <li><b>Strict Projection:</b> Types must match exactly (no type promotions)</li>
 *   <li><b>Compatible Projection:</b> Allows type promotions (INT → LONG, etc.)</li>
 * </ul>
 * </p>
 *
 * <p>Projection rules:
 * <ul>
 *   <li><b>RECORD:</b> Every target field must exist in source with compatible schema</li>
 *   <li><b>ARRAY:</b> Element types must be projections of each other</li>
 *   <li><b>MAP:</b> Value types must be projections of each other</li>
 *   <li><b>UNION:</b> Same size, each branch must be projection of corresponding branch</li>
 *   <li><b>Atomic types:</b> Use type equality predicate (strict vs compatible)</li>
 * </ul>
 * </p>
 *
 * <p>This class is package-private and used internally by HoodieSchemaCompatibility.</p>
 */
class HoodieSchemaProjectionChecker {

  // Prevent instantiation
  private HoodieSchemaProjectionChecker() {
  }

  /**
   * Validates whether the target schema is a strict projection of the source schema.
   *
   * <p>In a strict projection, atomic types must match exactly - no type promotions are allowed.
   * For example, if source has an INT field, target must also have INT (not LONG).</p>
   *
   * @param sourceSchema the schema to project from
   * @param targetSchema the schema to project to
   * @return true if targetSchema is a strict projection of sourceSchema
   */
  static boolean isStrictProjectionOf(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    return isProjectionOfInternal(sourceSchema, targetSchema, HoodieSchemaProjectionChecker::strictTypeEquals);
  }

  /**
   * Validates whether the target schema is a compatible projection of the source schema.
   *
   * <p>In a compatible projection, type promotions are allowed (e.g., INT → LONG).
   * This is useful for query optimization where a reader can request a wider type
   * than what was written.</p>
   *
   * @param sourceSchema the schema to project from
   * @param targetSchema the schema to project to
   * @return true if targetSchema is a compatible projection of sourceSchema
   */
  static boolean isCompatibleProjectionOf(HoodieSchema sourceSchema, HoodieSchema targetSchema) {
    return isProjectionOfInternal(sourceSchema, targetSchema, HoodieSchemaProjectionChecker::compatibleTypeEquals);
  }

  /**
   * Internal projection checking logic with configurable type equality predicate.
   *
   * @param sourceSchema         the schema to project from
   * @param targetSchema         the schema to project to
   * @param typeEqualityPredicate predicate to check if two atomic types are compatible
   * @return true if targetSchema is a projection of sourceSchema according to the predicate
   */
  private static boolean isProjectionOfInternal(HoodieSchema sourceSchema,
                                                HoodieSchema targetSchema,
                                                TypeEqualityPredicate typeEqualityPredicate) {
    if (sourceSchema.getType() == targetSchema.getType()) {
      switch (sourceSchema.getType()) {
        case RECORD:
          // For records, every target field must exist in source with compatible schema
          for (HoodieSchemaField targetField : targetSchema.getFields()) {
            Option<HoodieSchemaField> sourceFieldOpt = sourceSchema.getField(targetField.name());
            if (!sourceFieldOpt.isPresent()) {
              return false;
            }
            HoodieSchemaField sourceField = sourceFieldOpt.get();
            if (!isProjectionOfInternal(sourceField.schema(), targetField.schema(), typeEqualityPredicate)) {
              return false;
            }
          }
          return true;

        case ARRAY:
          // Array element types must be projections of each other
          return isProjectionOfInternal(sourceSchema.getElementType(), targetSchema.getElementType(), typeEqualityPredicate);

        case MAP:
          // Map value types must be projections of each other
          return isProjectionOfInternal(sourceSchema.getValueType(), targetSchema.getValueType(), typeEqualityPredicate);

        case UNION:
          // Union types must have same size and each branch must be a projection
          List<HoodieSchema> sourceTypes = sourceSchema.getTypes();
          List<HoodieSchema> targetTypes = targetSchema.getTypes();

          if (sourceTypes.size() != targetTypes.size()) {
            return false;
          }

          for (int i = 0; i < sourceTypes.size(); i++) {
            if (!isProjectionOfInternal(sourceTypes.get(i), targetTypes.get(i), typeEqualityPredicate)) {
              return false;
            }
          }
          return true;

        default:
          // For other types (primitives, fixed, enum, etc.), fall through to type equality check
          break;
      }
    }

    // Use the type equality predicate to check if types are compatible
    return typeEqualityPredicate.test(sourceSchema, targetSchema);
  }

  /**
   * Strict type equality - types must match exactly.
   */
  private static boolean strictTypeEquals(HoodieSchema source, HoodieSchema target) {
    HoodieSchema nonNullSource = source.getNonNullType();
    HoodieSchema nonNullTarget = target.getNonNullType();

    // Special case for enums and strings - allow projection between them
    if (nonNullSource.getType() == HoodieSchemaType.ENUM && nonNullTarget.getType() == HoodieSchemaType.STRING
        || nonNullSource.getType() == HoodieSchemaType.STRING && nonNullTarget.getType() == HoodieSchemaType.ENUM) {
      return true;
    }

    // For strict projection, remaining types must be exactly the same
    if (nonNullSource.getType() != nonNullTarget.getType()) {
      return false;
    }

    // Additional checks for specific types
    switch (nonNullSource.getType()) {
      case FIXED:
        // Fixed types must have same size
        return nonNullSource.getFixedSize() == nonNullTarget.getFixedSize();

      case ENUM:
        // Enum types must have same name and symbols
        if (!nonNullSource.getName().equals(nonNullTarget.getName())) {
          return false;
        }
        return nonNullSource.getEnumSymbols().equals(nonNullTarget.getEnumSymbols());

      case DECIMAL:
        // Decimal types must have same precision and scale
        HoodieSchema.Decimal sourceDecimal = (HoodieSchema.Decimal) nonNullSource;
        HoodieSchema.Decimal targetDecimal = (HoodieSchema.Decimal) nonNullTarget;
        return sourceDecimal.getPrecision() == targetDecimal.getPrecision()
            && sourceDecimal.getScale() == targetDecimal.getScale();

      case TIME:
        // Time types must have same precision
        HoodieSchema.Time sourceTime = (HoodieSchema.Time) nonNullSource;
        HoodieSchema.Time targetTime = (HoodieSchema.Time) nonNullTarget;
        return sourceTime.getPrecision() == targetTime.getPrecision();

      case TIMESTAMP:
        // Timestamp types must have same precision and UTC adjustment
        HoodieSchema.Timestamp sourceTs = (HoodieSchema.Timestamp) nonNullSource;
        HoodieSchema.Timestamp targetTs = (HoodieSchema.Timestamp) nonNullTarget;
        return sourceTs.getPrecision() == targetTs.getPrecision()
            && sourceTs.isUtcAdjusted() == targetTs.isUtcAdjusted();

      default:
        // For primitive types, type equality is sufficient
        return true;
    }
  }

  /**
   * Compatible type equality - allows type promotions.
   */
  private static boolean compatibleTypeEquals(HoodieSchema source, HoodieSchema target) {
    // First check if types are exactly equal
    if (strictTypeEquals(source, target)) {
      return true;
    }

    // Check if type promotion is allowed
    if (HoodieSchemaTypePromotion.canPromote(target.getType(), source.getType())) {
      return true;
    }

    // Check decimal widening
    if (source.getType() == HoodieSchemaType.DECIMAL && target.getType() == HoodieSchemaType.DECIMAL) {
      return HoodieSchemaTypePromotion.isDecimalWidening(target, source);
    }

    return false;
  }

  /**
   * Functional interface for type equality checking.
   */
  @FunctionalInterface
  private interface TypeEqualityPredicate {
    boolean test(HoodieSchema source, HoodieSchema target);
  }
}
