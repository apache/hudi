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

package org.apache.hudi.schema;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.Schema;

import java.io.Serializable;
import java.util.Objects;

/**
 * Wrapper class for Avro Schema.Field that provides Hudi-specific field functionality
 * while maintaining binary compatibility with Avro.
 *
 * <p>This class encapsulates an Avro Schema.Field and provides a consistent interface
 * for field operations across the Hudi codebase. It maintains full compatibility with
 * Avro by delegating all operations to the underlying Avro field.</p>
 *
 * <p>Usage example:
 * <pre>{@code
 * // Create from Avro field
 * Schema.Field avroField = new Schema.Field("name", Schema.create(Schema.Type.STRING));
 * HoodieSchemaField hoodieField = HoodieSchemaField.fromAvroField(avroField);
 *
 * // Access field properties
 * String name = hoodieField.name();
 * HoodieSchema schema = hoodieField.schema();
 * Option<Object> defaultValue = hoodieField.defaultVal();
 * }</pre></p>
 *
 * @since 1.2.0
 */
public class HoodieSchemaField implements Serializable {

  private static final long serialVersionUID = 1L;

  private final Schema.Field avroField;
  private final HoodieSchema hoodieSchema;

  /**
   * Creates a new HoodieSchemaField wrapping the given Avro field.
   *
   * @param avroField the Avro field to wrap, cannot be null
   */
  public HoodieSchemaField(Schema.Field avroField) {
    ValidationUtils.checkArgument(avroField != null, "Avro field cannot be null");
    this.avroField = avroField;
    this.hoodieSchema = new HoodieSchema(avroField.schema());
  }

  /**
   * Factory method to create HoodieSchemaField from an Avro field.
   *
   * @param avroField the Avro field to wrap
   * @return new HoodieSchemaField instance
   * @throws IllegalArgumentException if avroField is null
   */
  public static HoodieSchemaField fromAvroField(Schema.Field avroField) {
    return new HoodieSchemaField(avroField);
  }

  /**
   * Creates a new HoodieSchemaField with the specified properties.
   *
   * @param name       the name of the field
   * @param schema     the schema of the field
   * @param doc        the documentation string, can be null
   * @param defaultVal the default value, can be null
   * @return new HoodieSchemaField instance
   */
  public static HoodieSchemaField of(String name, HoodieSchema schema, String doc, Object defaultVal) {
    return of(name, schema, doc, defaultVal, HoodieFieldOrder.ASCENDING);
  }

  /**
   * Creates a new HoodieSchemaField with the specified properties, including field order.
   *
   * @param name       the name of the field
   * @param schema     the schema of the field
   * @param doc        the documentation string, can be null
   * @param defaultVal the default value, can be null
   * @param order      the field order for sorting
   * @return new HoodieSchemaField instance
   */
  public static HoodieSchemaField of(String name, HoodieSchema schema, String doc, Object defaultVal, HoodieFieldOrder order) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Field schema cannot be null");
    ValidationUtils.checkArgument(order != null, "Field order cannot be null");

    Schema avroSchema = schema.getAvroSchema();
    ValidationUtils.checkState(avroSchema != null, "Schema's Avro schema cannot be null");

    Schema.Field avroField = new Schema.Field(name, avroSchema, doc, defaultVal, order.toAvroOrder());
    return new HoodieSchemaField(avroField);
  }

  /**
   * Creates a new HoodieSchemaField with the specified name and schema.
   *
   * @param name   the name of the field
   * @param schema the schema of the field
   * @return new HoodieSchemaField instance
   */
  public static HoodieSchemaField of(String name, HoodieSchema schema) {
    return of(name, schema, null, null);
  }

  /**
   * Creates a new HoodieSchemaField with the specified name, schema, and doc.
   *
   * @param name   the name of the field
   * @param schema the schema of the field
   * @param doc    the documentation string
   * @return new HoodieSchemaField instance
   */
  public static HoodieSchemaField of(String name, HoodieSchema schema, String doc) {
    return of(name, schema, doc, null);
  }

  /**
   * Creates a metadata field for Hudi internal use.
   * This is a convenience method for creating fields that are part of Hudi's metadata.
   *
   * @param name   the metadata field name
   * @param schema the metadata field schema
   * @return new HoodieSchemaField configured as a metadata field
   * @throws IllegalArgumentException if name is null/empty or schema is null
   */
  public static HoodieSchemaField createMetadataField(String name, HoodieSchema schema) {
    ValidationUtils.checkArgument(name != null && !name.isEmpty(), "Metadata field name cannot be null or empty");
    ValidationUtils.checkArgument(schema != null, "Metadata field schema cannot be null");

    return HoodieSchemaField.of(name, schema, "Hudi metadata field: " + name, HoodieJsonProperties.NULL_VALUE);
  }

  /**
   * Returns the name of this field.
   *
   * @return the field name
   */
  public String name() {
    return avroField.name();
  }

  /**
   * Returns the schema of this field.
   *
   * @return the field schema as HoodieSchema
   */
  public HoodieSchema schema() {
    return hoodieSchema;
  }

  /**
   * Returns the documentation string for this field.
   *
   * @return Option containing the documentation string, or Option.empty() if none
   */
  public Option<String> doc() {
    return Option.ofNullable(avroField.doc());
  }

  /**
   * Returns the default value for this field.
   *
   * @return Option containing the default value, or Option.empty() if none
   */
  public Option<Object> defaultVal() {
    if (avroField != null && avroField.hasDefaultValue()) {
      return Option.of(avroField.defaultVal());
    }
    return Option.empty();
  }

  /**
   * Returns the sort order for this field.
   *
   * @return the field order
   */
  public HoodieFieldOrder order() {
    return HoodieFieldOrder.fromAvroOrder(avroField.order());
  }

  /**
   * Returns the position of this field within its enclosing record.
   *
   * @return the field position (0-based index)
   */
  public int pos() {
    return avroField.pos();
  }

  /**
   * Checks if this field has a default value.
   *
   * @return true if the field has a default value
   */
  public boolean hasDefaultValue() {
    return avroField.hasDefaultValue();
  }

  /**
   * Returns custom properties attached to this field.
   *
   * @return map of custom properties
   */
  public java.util.Map<String, Object> getObjectProps() {
    return avroField.getObjectProps();
  }

  /**
   * Returns the value of a custom property.
   *
   * @param key the property key
   * @return the property value, or null if not found
   */
  public Object getProp(String key) {
    return avroField.getProp(key);
  }

  /**
   * Adds a custom property to this field.
   *
   * @param key   the property key
   * @param value the property value
   */
  public void addProp(String key, Object value) {
    ValidationUtils.checkArgument(key != null && !key.isEmpty(), "Property key cannot be null or empty");
    avroField.addProp(key, value);
  }

  /**
   * Returns the underlying Avro field for compatibility purposes.
   *
   * <p>This method is provided for gradual migration and should be used
   * sparingly. New code should prefer the HoodieSchemaField API.</p>
   *
   * @return the wrapped Avro Schema.Field
   */
  public Schema.Field getAvroField() {
    return avroField;
  }

  /**
   * Creates a copy of this field with a new name.
   *
   * @param newName the new name for the field
   * @return new HoodieSchemaField with the specified name
   */
  public HoodieSchemaField withName(String newName) {
    ValidationUtils.checkArgument(newName != null && !newName.isEmpty(), "Field name cannot be null or empty");

    Schema.Field newAvroField = new Schema.Field(newName, avroField.schema(), avroField.doc(),
        avroField.hasDefaultValue() ? avroField.defaultVal() : null, avroField.order());
    return new HoodieSchemaField(newAvroField);
  }

  /**
   * Creates a copy of this field with a new schema.
   *
   * @param newSchema the new schema for the field
   * @return new HoodieSchemaField with the specified schema
   */
  public HoodieSchemaField withSchema(HoodieSchema newSchema) {
    ValidationUtils.checkArgument(newSchema != null, "Field schema cannot be null");

    Schema newAvroSchema = newSchema.getAvroSchema();
    ValidationUtils.checkState(newAvroSchema != null, "New schema's Avro schema cannot be null");

    Schema.Field newAvroField = new Schema.Field(avroField.name(), newAvroSchema, avroField.doc(),
        avroField.hasDefaultValue() ? avroField.defaultVal() : null, avroField.order());
    return new HoodieSchemaField(newAvroField);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    HoodieSchemaField that = (HoodieSchemaField) obj;
    return Objects.equals(avroField.name(), that.avroField.name())
        && Objects.equals(avroField.schema(), that.avroField.schema())
        && Objects.equals(avroField.doc(), that.avroField.doc())
        && Objects.equals(avroField.defaultVal(), that.avroField.defaultVal())
        && Objects.equals(avroField.order(), that.avroField.order());
  }

  @Override
  public int hashCode() {
    return Objects.hash(avroField.name(), avroField.schema(), avroField.doc(),
        avroField.defaultVal(), avroField.order());
  }

  // Additional field utility methods

  @Override
  public String toString() {
    return "HoodieSchemaField{"
        + "name='" + name() + '\''
        + ", type=" + schema().getType()
        + ", hasDefault=" + hasDefaultValue()
        + ", order=" + order()
        + '}';
  }

  /**
   * Creates a nullable version of this field by wrapping its schema in a union with null.
   * This is useful for schema evolution where fields need to become optional.
   *
   * @return new HoodieSchemaField with nullable schema
   */
  public HoodieSchemaField makeNullable() {
    if (schema().isNullable()) {
      return this; // Already nullable
    }

    HoodieSchema nullableSchema = HoodieSchemaUtils.createNullableSchema(schema());
    return HoodieSchemaField.of(name(), nullableSchema, doc().orElse(null), defaultVal().orElse(null));
  }

  /**
   * Creates a copy of this field with new documentation.
   *
   * @param newDoc the new documentation for the field
   * @return new HoodieSchemaField with the specified documentation
   */
  public HoodieSchemaField withDoc(String newDoc) {
    return HoodieSchemaField.of(name(), schema(), newDoc, defaultVal().orElse(null));
  }

  /**
   * Creates a copy of this field with a new default value.
   *
   * @param newDefaultValue the new default value for the field
   * @return new HoodieSchemaField with the specified default value
   */
  public HoodieSchemaField withDefaultValue(Object newDefaultValue) {
    return HoodieSchemaField.of(name(), schema(), doc().orElse(null), newDefaultValue);
  }

  /**
   * Checks if this field is nullable (i.e., its schema is a union with null).
   *
   * @return true if the field can contain null values
   */
  public boolean isNullable() {
    return schema().isNullable();
  }

  /**
   * Checks if this field is optional (i.e., has a default value or is nullable).
   *
   * @return true if the field is optional in records
   */
  public boolean isOptional() {
    return hasDefaultValue() || isNullable();
  }

  /**
   * Gets the non-null type from this field's schema if it's a union with null.
   * If the schema is not a union or doesn't contain null, returns the schema as-is.
   *
   * @return the non-null schema type
   */
  public HoodieSchema getNonNullSchema() {
    if (!isNullable()) {
      return schema();
    }
    return HoodieSchemaUtils.getNonNullTypeFromUnion(schema());
  }
}
