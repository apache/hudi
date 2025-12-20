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

package org.apache.hudi.avro;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes.Decimal;
import org.apache.avro.Schema;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines equality comparison rules for Avro schemas.
 *
 * <p> Why not using built-in Avro schema `equals` function:</p>
 * <p> The built-in function considers all attributes including customized
 * attributes like field documentation and namespace, which should be ignored
 * from the perspective of evaluating schema evolution. Hence a customized
 * equal function is implemented to capture only the attributes that affect
 * avro readers/writers interpreting data.
 * </p>
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
 *   <li>Logical type name</li>
 *   <li>Underlying primitive type</li>
 *   <li>Decimal precision/scale (if applicable)</li>
 * </ul>
 * Excluded:
 * <ul>
 *   <li>Documentation</li>
 *   <li>Custom properties</li>
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class AvroSchemaComparatorForSchemaEvolution {

  private static final AvroSchemaComparatorForSchemaEvolution VALIDATOR = new AvroSchemaComparatorForSchemaEvolution();

  public static boolean schemaEquals(Schema s1, Schema s2) {
    return VALIDATOR.schemaEqualsInternal(s1, s2);
  }

  protected boolean schemaEqualsInternal(Schema s1, Schema s2) {
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
        return primitiveSchemaEquals(s1, s2);
      default:
        throw new IllegalArgumentException("Unknown schema type: " + s1.getType());
    }
  }

  protected boolean validateRecord(Schema s1, Schema s2) {
    if (s1.isError() != s2.isError()) {
      return false;
    }

    return logicalTypeSchemaEquals(s1, s2);
  }

  private boolean recordSchemaEquals(Schema s1, Schema s2) {
    if (!validateRecord(s1, s2)) {
      return false;
    }

    List<Schema.Field> fields1 = s1.getFields();
    List<Schema.Field> fields2 = s2.getFields();

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

  protected boolean validateField(Schema.Field f1, Schema.Field f2) {
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
    if (f1.hasDefaultValue() && !f1.defaultVal().equals(f2.defaultVal())) {
      return false;
    }

    return true;
  }

  private boolean fieldEquals(Schema.Field f1, Schema.Field f2) {
    if (!validateField(f1, f2)) {
      return false;
    }

    return schemaEqualsInternal(f1.schema(), f2.schema());
  }

  protected boolean enumSchemaEquals(Schema s1, Schema s2) {
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

  protected boolean unionSchemaEquals(Schema s1, Schema s2) {
    List<Schema> types1 = s1.getTypes();
    List<Schema> types2 = s2.getTypes();

    if (types1.size() != types2.size()) {
      return false;
    }

    // Create sets of effectively equal types
    Set<SchemaWrapper> set1 = types1.stream().map(SchemaWrapper::new).collect(Collectors.toSet());
    Set<SchemaWrapper> set2 = types2.stream().map(SchemaWrapper::new).collect(Collectors.toSet());

    // Compare sets instead of ordered lists
    return set1.equals(set2);
  }

  private boolean arraySchemaEquals(Schema s1, Schema s2) {
    return schemaEqualsInternal(s1.getElementType(), s2.getElementType());
  }

  private boolean mapSchemaEquals(Schema s1, Schema s2) {
    return schemaEqualsInternal(s1.getValueType(), s2.getValueType());
  }

  protected boolean validateFixed(Schema s1, Schema s2) {
    return s1.getName().equals(s2.getName()) && s1.getFixedSize() == s2.getFixedSize();
  }

  private boolean fixedSchemaEquals(Schema s1, Schema s2) {
    if (!validateFixed(s1, s2)) {
      return false;
    }
    return logicalTypeSchemaEquals(s1, s2);
  }

  private static boolean primitiveSchemaEquals(Schema s1, Schema s2) {
    // For primitive types, just check logical type
    return logicalTypeSchemaEquals(s1, s2);
  }

  private static boolean logicalTypeSchemaEquals(Schema s1, Schema s2) {
    LogicalType lt1 = s1.getLogicalType();
    LogicalType lt2 = s2.getLogicalType();

    if (lt1 == null && lt2 == null) {
      return true;
    }
    if (lt1 == null || lt2 == null) {
      return false;
    }

    // Check logical type name
    if (!lt1.getName().equals(lt2.getName())) {
      return false;
    }

    if (lt1 instanceof Decimal) {
      Decimal d1 = (Decimal) lt1;
      Decimal d2 = (Decimal) lt2;
      return d1.getPrecision() == d2.getPrecision() && d1.getScale() == d2.getScale();
    }

    return true;
  }

  /**
   * Wrapper class to use Schema in HashSet with our custom equality
   */
  static class SchemaWrapper {
    private final Schema schema;

    public SchemaWrapper(Schema schema) {
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