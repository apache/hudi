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

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines equality comparison rules for HoodieSchema schemas for schema evolution purposes.
 *
 * <p>This class provides schema comparison logic that focuses only on attributes that affect
 * data readers/writers, ignoring metadata like documentation, namespace, and aliases which
 * don't impact schema evolution compatibility.</p>
 *
 * <h2>Common Rules Across All Types</h2>
 * Included in equality check:
 * <ul>
 *   <li>Name/identifier</li>
 *   <li>Type including primitive type, complex type (see below), and logical type</li>
 * </ul>
 * Excluded from equality check:
 * <ul>
 *   <li>Namespace</li>
 *   <li>Documentation</li>
 *   <li>Aliases</li>
 *   <li>Custom properties</li>
 * </ul>
 *
 * <h2>Type-Specific Rules</h2>
 *
 * <h3>Record</h3>
 * Included:
 * <ul>
 *   <li>Field names</li>
 *   <li>Field types</li>
 *   <li>Field order attribute</li>
 *   <li>Default values</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Field documentation</li>
 *   <li>Field aliases</li>
 * </ul>
 *
 * <h3>Enum</h3>
 * Included:
 * <ul>
 *   <li>Name</li>
 *   <li>Symbol order</li>
 *   <li>Symbol value</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Custom properties</li>
 * </ul>
 *
 * <h3>Array</h3>
 * Included:
 * <ul>
 *   <li>Items schema</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Documentation</li>
 *   <li>Custom properties</li>
 * </ul>
 *
 * <h3>Map</h3>
 * Included:
 * <ul>
 *   <li>Values schema</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Documentation</li>
 *   <li>Custom properties</li>
 * </ul>
 *
 * <h3>Fixed</h3>
 * Included:
 * <ul>
 *   <li>Size</li>
 *   <li>Name</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Namespace</li>
 *   <li>Aliases</li>
 * </ul>
 *
 * <h3>Union</h3>
 * Included:
 * <ul>
 *   <li>Member types</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Member order</li>
 * </ul>
 *
 * <h3>Logical Types</h3>
 * Included:
 * <ul>
 *   <li>Logical type name (via schema subclass)</li>
 *   <li>Underlying primitive type</li>
 *   <li>Decimal precision/scale (if applicable)</li>
 *   <li>Timestamp/Time precision (if applicable)</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Documentation</li>
 *   <li>Custom properties</li>
 * </ul>
 */
public class HoodieSchemaComparatorForSchemaEvolution {

  protected HoodieSchemaComparatorForSchemaEvolution() {
  }

  private static final HoodieSchemaComparatorForSchemaEvolution VALIDATOR = new HoodieSchemaComparatorForSchemaEvolution();

  public static boolean schemaEquals(HoodieSchema s1, HoodieSchema s2) {
    return VALIDATOR.schemaEqualsInternal(s1, s2);
  }

  protected boolean schemaEqualsInternal(HoodieSchema s1, HoodieSchema s2) {
    if (s1 == s2) {
      return true;
    }
    if (s1 == null || s2 == null) {
      return false;
    }
    if (s1.getType() != s2.getType()) {
      return false;
    }

    switch (s1.getType()) {
      case RECORD:
        return recordSchemaEquals(s1, s2);
      case ENUM:
        return enumSchemaEquals(s1, s2);
      case ARRAY:
        return arraySchemaEquals(s1, s2);
      case MAP:
        return mapSchemaEquals(s1, s2);
      case FIXED:
        return fixedSchemaEquals(s1, s2);
      case UNION:
        return unionSchemaEquals(s1, s2);
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
      case DECIMAL:
      case TIME:
      case TIMESTAMP:
      case DATE:
      case UUID:
        return primitiveSchemaEquals(s1, s2);
      default:
        throw new IllegalArgumentException("Unknown schema type: " + s1.getType());
    }
  }

  protected boolean validateRecord(HoodieSchema s1, HoodieSchema s2) {
    if (s1.isError() != s2.isError()) {
      return false;
    }

    return logicalTypeSchemaEquals(s1, s2);
  }

  private boolean recordSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    if (!validateRecord(s1, s2)) {
      return false;
    }

    List<HoodieSchemaField> fields1 = s1.getFields();
    List<HoodieSchemaField> fields2 = s2.getFields();

    if (fields1.size() != fields2.size()) {
      return false;
    }

    for (int i = 0; i < fields1.size(); i++) {
      if (!fieldEquals(fields1.get(i), fields2.get(i))) {
        return false;
      }
    }
    return true;
  }

  protected boolean validateField(HoodieSchemaField f1, HoodieSchemaField f2) {
    if (!f1.name().equals(f2.name())) {
      return false;
    }

    if (f1.order() != f2.order()) {
      return false;
    }

    // Check if both have default values
    if (f1.hasDefaultValue() != f2.hasDefaultValue()) {
      return false;
    }

    // If both have default values, they must be equal
    if (f1.hasDefaultValue() && !f1.defaultVal().get().equals(f2.defaultVal().get())) {
      return false;
    }

    return true;
  }

  private boolean fieldEquals(HoodieSchemaField f1, HoodieSchemaField f2) {
    if (!validateField(f1, f2)) {
      return false;
    }

    return schemaEqualsInternal(f1.schema(), f2.schema());
  }

  protected boolean enumSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    // Check name equality first
    if (!s1.getName().equals(s2.getName())) {
      return false;
    }

    List<String> symbols1 = s1.getEnumSymbols();
    List<String> symbols2 = s2.getEnumSymbols();

    // Quick size check before creating sets
    if (symbols1.size() != symbols2.size()) {
      return false;
    }

    return symbols1.equals(symbols2);
  }

  protected boolean unionSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    List<HoodieSchema> types1 = s1.getTypes();
    List<HoodieSchema> types2 = s2.getTypes();

    if (types1.size() != types2.size()) {
      return false;
    }

    // Create sets of effectively equal types
    Set<SchemaWrapper> set1 = types1.stream().map(SchemaWrapper::new).collect(Collectors.toSet());
    Set<SchemaWrapper> set2 = types2.stream().map(SchemaWrapper::new).collect(Collectors.toSet());

    // Compare sets instead of ordered lists
    return set1.equals(set2);
  }

  private boolean arraySchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    return schemaEqualsInternal(s1.getElementType(), s2.getElementType());
  }

  private boolean mapSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    return schemaEqualsInternal(s1.getValueType(), s2.getValueType());
  }

  protected boolean validateFixed(HoodieSchema s1, HoodieSchema s2) {
    return s1.getName().equals(s2.getName()) && s1.getFixedSize() == s2.getFixedSize();
  }

  private boolean fixedSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    if (!validateFixed(s1, s2)) {
      return false;
    }
    return logicalTypeSchemaEquals(s1, s2);
  }

  private static boolean primitiveSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    // For primitive types, just check logical type
    return logicalTypeSchemaEquals(s1, s2);
  }

  private static boolean logicalTypeSchemaEquals(HoodieSchema s1, HoodieSchema s2) {
    // Check if both schemas are of the same logical type class
    boolean s1IsDecimal = s1.getType() == HoodieSchemaType.DECIMAL;
    boolean s2IsDecimal = s2.getType() == HoodieSchemaType.DECIMAL;
    boolean s1IsTimestamp = s1.getType() == HoodieSchemaType.TIMESTAMP;
    boolean s2IsTimestamp = s2.getType() == HoodieSchemaType.TIMESTAMP;
    boolean s1IsTime = s1.getType() == HoodieSchemaType.TIME;
    boolean s2IsTime = s2.getType() == HoodieSchemaType.TIME;

    // If one is a logical type and the other isn't, they're not equal
    if (s1IsDecimal != s2IsDecimal || s1IsTimestamp != s2IsTimestamp || s1IsTime != s2IsTime) {
      return false;
    }

    // If both are decimals, compare precision, scale, and underlying type (FIXED vs BYTES)
    if (s1IsDecimal) {
      HoodieSchema.Decimal d1 = (HoodieSchema.Decimal) s1;
      HoodieSchema.Decimal d2 = (HoodieSchema.Decimal) s2;
      // Check if both use same underlying representation (FIXED vs BYTES)
      if (d1.isFixed() != d2.isFixed()) {
        return false;
      }
      return d1.getPrecision() == d2.getPrecision() && d1.getScale() == d2.getScale();
    }

    // If both are timestamps, compare precision and UTC adjustment
    if (s1IsTimestamp) {
      HoodieSchema.Timestamp t1 = (HoodieSchema.Timestamp) s1;
      HoodieSchema.Timestamp t2 = (HoodieSchema.Timestamp) s2;
      return t1.getPrecision() == t2.getPrecision() && t1.isUtcAdjusted() == t2.isUtcAdjusted();
    }

    // If both are time types, compare precision
    // Note: time-millis is INT, time-micros is LONG, so they have different underlying types
    // which is reflected in their precision values
    if (s1IsTime) {
      HoodieSchema.Time t1 = (HoodieSchema.Time) s1;
      HoodieSchema.Time t2 = (HoodieSchema.Time) s2;
      return t1.getPrecision() == t2.getPrecision();
    }

    // For non-logical types, they're equal
    return true;
  }

  /**
   * Wrapper class to use HoodieSchema in HashSet with our custom equality
   */
  static class SchemaWrapper {
    private final HoodieSchema schema;

    public SchemaWrapper(HoodieSchema schema) {
      this.schema = schema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaWrapper that = (SchemaWrapper) o;
      return schemaEquals(schema, that.schema);
    }

    @Override
    public int hashCode() {
      // This is a simplified hash code that considers only the type
      // It's not perfect but good enough for our use case
      return schema.getType().hashCode();
    }
  }
}